#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <time.h>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <thread>
//#include "extra_rdma_connnect.h"
#include "rdma_connect.h"
#include "db.h"
#include "backup.h"
#include "define.h"
#define GC_TRIGER_NUM 1024
#define GC_PER_NUM 512
#define MAX_KV_LEN_FROM_CLIENT 8*1024
extern struct config_t config;
extern int K;
extern int BACKUP_MODE;
BackupNode backup_node;
DB *global_db;
void primary(struct resources *res);
void backup();


int resources_create_for_master(struct resources *res)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;
    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;
    int rc = 0;
    std::cout<<"try connect to matser with ip = "<<config.master_server_name<<" ,port = "<<config.master_tcp_port<<std::endl;
    res->master_sock = sock_connect(config.master_server_name, config.master_tcp_port);
    if(res->master_sock < 0)
    {
        fprintf(stderr, "failed to establish TCP connection to master_server %s\n",config.master_server_name);
        rc = -1;
        goto resources_create_exit;
    }
     fprintf(stdout, "searching for IB devices in host\n");
 
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if(!dev_list)
    {
        fprintf(stderr, "failed to get IB devices list\n");
        rc = 1;
        goto resources_create_exit;
    }
    if(!num_devices)
    {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        goto resources_create_exit;
    }
    fprintf(stdout, "found %d device(s)\n", num_devices);
 
    /* search for the specific device we want to work with */
    for(i = 0; i < num_devices; i ++)
    {
        if(!config.dev_name)
        {
            config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n", config.dev_name);
        }
		/* find the specific device */
        if(!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name))
        {
            ib_dev = dev_list[i];
            break;
        }
    }
 
    /* if the device wasn't found in host */
    if(!ib_dev)
    {
        fprintf(stderr, "IB device %s wasn't found\n", config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
 
    /* get device handle */
    res->ib_ctx = ibv_open_device(ib_dev);
    if(!res->ib_ctx)
    {
        fprintf(stderr, "failed to open device %s\n", config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
 
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
 
    /* query port properties */
    if(ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr))
    {
        fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
        rc = 1;
        goto resources_create_exit;
    }
 
    /* allocate Protection Domain */
    res->pd = ibv_alloc_pd(res->ib_ctx);
    if(!res->pd)
    {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* each side will send only one WR, so Completion Queue with 1 entry is enough */
    cq_size = 1;    
    res->master_cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    if(!res->master_cq)
    {
        fprintf(stderr, "failed to create master CQ with %u entries for master\n", cq_size);
        rc = 1;
        goto resources_create_exit;
    }
    /* allocate the memory buffer that will hold the data */
    res->master_msg_buf = (char *) malloc(MAX_MASTER_MSG_BUF_SIZE);
    res->master_buf = (char *) malloc(MAX_MASTER_BUF_SIZE);
    fprintf(stdout, "申请内存master bug and msg_buf\n");
    if(!res->master_msg_buf)
    {
        fprintf(stderr, "failed to malloc %Zu bytes to master_msg_buf\n", 128);
        rc = 1;
        goto resources_create_exit;
    }
    /* register the memory buffer */
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    res->master_mr = ibv_reg_mr(res->pd, res->master_buf, MAX_MASTER_BUF_SIZE, mr_flags);
    res->master_msg_mr = ibv_reg_mr(res->pd, res->master_msg_buf, MAX_MASTER_MSG_BUF_SIZE, mr_flags);
    fprintf(stdout, "注册master_buf内存到pd\n");
    if(!res->master_mr)
    {
        fprintf(stderr, "ibv_reg_mrfailed with mr_flags=0x%x\n", mr_flags);
        rc = 1;
        goto resources_create_exit;
    }
    fprintf(stdout, "master_MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                res->master_buf, res->master_mr->lkey, res->master_mr->rkey, mr_flags);           
    /* create the Queue Pair */
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.send_cq = res->master_cq;
    qp_init_attr.recv_cq = res->master_cq;
    res->master_qp = ibv_create_qp(res->pd, &qp_init_attr);
    if(!res->master_qp)
    {
            fprintf(stderr, "failed to create master_QP\n");
            rc = 1;
            goto resources_create_exit;
    }
    fprintf(stdout, "master_QP was created, QP number=0x%x\n",res->master_qp->qp_num);
    
    
    
 
resources_create_exit:
    if(rc)
    {
        /* Error encountered, cleanup */
        
            if(res->master_qp)
            {
                ibv_destroy_qp(res->master_qp);
                res->master_qp = NULL;
            }
            if(res->master_mr)
            {
                ibv_dereg_mr(res->master_mr);
                res->master_mr = NULL;
            }
            if(res->master_buf)
            {
                free(res->master_buf);
                res->master_buf = NULL;
            }
            if(res->master_msg_mr)
            {
                ibv_dereg_mr(res->master_msg_mr);
                res->master_mr = NULL;
            }
            if(res->master_msg_buf)
            {
                free(res->master_msg_buf);
                res->master_msg_buf = NULL;
            }
            if(res->master_cq)
            {
                ibv_destroy_cq(res->master_cq);
                res->master_cq = NULL;
            }
            if(res->master_sock >= 0)
            {
                if(close(res->master_sock))
                {
                    fprintf(stderr, "failed to close socket\n");
                }
                res->master_sock = -1;
            }
        
        
        if(res->pd)
        {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if(res->ib_ctx)
        {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if(dev_list)
        {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
        
    }
    return rc;
}


static int resources_create(struct resources *res)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;
    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;
    int rc = 0;
    /* if client side */
    
    if(config.server_name[0])
    {
        for(int n=0;n<K;n++){
            std::cout<<"sock_connect with node: "<<n<<"tcp_port= "<<config.tcp_port[n]<<std::endl;
            res->sock[n] = sock_connect(config.server_name[n], config.tcp_port[n]);
            std::cout<<"sock_connect finish"<<std::endl;
            if(res->sock[n] < 0)
            {
                fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                        config.server_name[n], config.tcp_port[n]);
                rc = -1;
                goto resources_create_exit;
            }
        }
    }
    else
    {
        for(int n=0;n<K;n++){
            fprintf(stdout, "waiting on port %d for TCP connection\n", config.tcp_port[n]);
            res->sock[n] = sock_connect(NULL, config.tcp_port[n]);
            if(res->sock[n] < 0)
            {
                fprintf(stderr, "failed to establish TCP connection with client on port %d\n",
                        config.tcp_port[n]);
                rc = -1;
                assert(0);
                goto resources_create_exit;
            }
            fprintf(stdout, "TCP port: %d connection was established \n",config.tcp_port[n]);
        }
        
    }
    cq_size = 1;
    
    for(int n=0;n<K;n++){
        res->cq[n] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if(!res->cq[n])
        {
            fprintf(stderr, "failed to create CQ with %u entries for node:%d\n", cq_size,n);
            rc = 1;
            goto resources_create_exit;
        }
    }
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    /* allocate the memory buffer that will hold the data */
    if(!config.server_name[0]){
        for(int n=0;n<K;n++){
            res->msg_buf[n] = (char *) malloc(MAX_MSG_SIZE);
            fprintf(stdout, "申请内存msg_buf for node:%d\n",n);      
        } 
        for(int n=0;n<K;n++){
        res->mr[n] = ibv_reg_mr(res->pd, res->buf[n], MAX_SGE_SIZE, mr_flags);
        res->msg_mr[n] = ibv_reg_mr(res->pd, res->msg_buf[n], MAX_MSG_SIZE, mr_flags);
        fprintf(stdout, "注册buf内存到pd for node:%d\n",n);
        if(!res->mr[n])
        {
            fprintf(stderr, "ibv_reg_mr for node:%d failed with mr_flags=0x%x\n", n,mr_flags);
            rc = 1;
            goto resources_create_exit;
        }
        fprintf(stdout, "MR for node:%d was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                n,res->buf[n], res->mr[n]->lkey, res->mr[n]->rkey, mr_flags);
        }  
    }else{
        res->msg_buf[0] = (char *) malloc(MAX_MSG_SIZE);
        res->gc_msg_buf = (char *) malloc(16);
        res->mr[0] = ibv_reg_mr(res->pd, res->buf[0], MAX_SGE_SIZE, mr_flags);
        res->msg_mr[0] = ibv_reg_mr(res->pd, res->msg_buf[0], MAX_MSG_SIZE, mr_flags);
        res->gc_msg_mr = ibv_reg_mr(res->pd, res->gc_msg_buf, 16, mr_flags);
        fprintf(stdout, "MR for node:%d was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                0,res->buf[0], res->mr[0]->lkey, res->mr[0]->rkey, mr_flags);
    }
     
    /* register the memory buffer */
    
    
    /* create the Queue Pair */
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    for(int n=0;n<K;n++){
        qp_init_attr.send_cq = res->cq[n];
        qp_init_attr.recv_cq = res->cq[n];
        res->qp[n] = ibv_create_qp(res->pd, &qp_init_attr);
        if(!res->qp[n])
        {
            fprintf(stderr, "failed to create QP for node:%d\n",n);
            rc = 1;
            goto resources_create_exit;
        }
        fprintf(stdout, "QP for node:%d was created, QP number=0x%x\n",n, res->qp[n]->qp_num);
    }
    
    
 
resources_create_exit:
    if(rc)
    {
        /* Error encountered, cleanup */
        for(int n=0;n<K;n++){
            if(res->qp[n])
            {
                ibv_destroy_qp(res->qp[n]);
                res->qp[n] = NULL;
            }
            if(res->mr[n])
            {
                ibv_dereg_mr(res->mr[n]);
                res->mr[n] = NULL;
            }
            if(res->buf[n])
            {
                free(res->buf[n]);
                res->buf[n] = NULL;
            }
            if(res->cq[n])
            {
                ibv_destroy_cq(res->cq[n]);
                res->cq[n] = NULL;
            }
            if(res->sock[n] >= 0)
            {
                if(close(res->sock[n]))
                {
                    fprintf(stderr, "failed to close socket\n");
                }
                res->sock[n] = -1;
            }
        }
        
        if(res->pd)
        {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if(res->ib_ctx)
        {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if(dev_list)
        {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
        
    }
    return rc;
}

extern int max_qp_num; 
int resources_create_for_client(struct resources *res)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;
    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;
    int rc = 0;
    std::cout<<"listen on clinet port: "<<config.client_tcp_port<<std::endl;
    res->client_sock = sock_connect(NULL, config.client_tcp_port);
    if(res->client_sock < 0)
    {
        fprintf(stderr, "failed to establish TCP connection to client %s\n");
        rc = -1;
        assert(0);
    }
    std::cout<<"sock_connect finish"<<std::endl;
    /* each side will send only one WR, so Completion Queue with 1 entry is enough */
    cq_size = 1;    
    for(int i=0;i<max_qp_num;i++){
        res->client_cq[i] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if(!res->client_cq[i])
        {
            fprintf(stderr, "failed to create master CQ with %u entries for client:%d\n", cq_size,i);
            assert(0);
        }
    }
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    for(int i=0;i<max_qp_num;i++){
        res->client_buf[i] = (char *) malloc(MAX_KV_LEN_FROM_CLIENT);
        res->client_mr[i] = ibv_reg_mr(res->pd, res->client_buf[i], MAX_KV_LEN_FROM_CLIENT, mr_flags);
        if(!res->client_mr[i])
        {
            fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
            assert(0);
        }
        fprintf(stdout, "client_mr[%d] was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                    i,res->client_buf[i], res->client_mr[i]->lkey, res->client_mr[i]->rkey, mr_flags);  
    }
             
    /* create the Queue Pair */
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    for(int i=0;i<max_qp_num;i++){
        qp_init_attr.send_cq = res->client_cq[i];
        qp_init_attr.recv_cq = res->client_cq[i];
        res->client_qp[i] = ibv_create_qp(res->pd, &qp_init_attr);
        if(!res->client_qp[i])
        {
            assert(0);
        }
        fprintf(stdout, "client_qp[%d] was created, QP number=0x%x\n",i,res->client_qp[i]->qp_num);
    }
    return rc;
}




void write_file(const char *buf,uint32_t len,std::string target_path,uint32_t node_id){
    std::ofstream fout;
    std::string prex;
    if(node_id==10)
        prex = "../rocksdb_lsm/";
    else if(node_id==11)
        prex = "../value_sgement/";
    else if(node_id==12)
        prex = "../parity_sgement/";
    else
        prex = "../rocksdb_lsm_"+std::to_string(node_id)+'/';
    prex.append(target_path);
    fout.open(prex,std::ios::out|std::ios::binary);
    fout.seekp(0,std::ios::beg);
    fout.write(buf,len);
    fout.close();
}
void make_mani_path(std::string &target_path,uint32_t id){
    target_path="MANIFEST-";
    std::string tmp1(6,'0');
    std::string tmp2 = std::to_string(id);
    uint32_t tmp_size = tmp2.size();
    for(int l=0;l<tmp_size;l++)
        tmp1[6-tmp_size+l]=tmp2[l];
    target_path.append(tmp1);
}
void make_sst_path(std::string &target_path,uint32_t id){
    target_path = "000000.sst";
    std::string tmp2 = std::to_string(id);
    uint32_t tmp_size = tmp2.size();
    for(int l=0;l<tmp_size;l++)
        target_path[6-tmp_size+l]=tmp2[l];
}
void exit(struct resources *res){
    if(resources_destroy(res))
    {
        fprintf(stderr, "failed to destroy resources\n");
    }
    if(config.dev_name)
    {
        free((char *) config.dev_name);
    }
}
bool backup_recover_finish = false;
std::mutex finish_lock;
char *write_buf;
uint32_t recover_gc_num;
extern uint32_t max_parity_id;
void store_file_p(char *buf,struct resources* res,int file_type_);
void listen_flush_tail(struct resources *res);
std::map<uint32_t,uint32_t> current_sst_id_list[4];
int store_file_b(char *buf){
    uint32_t file_type,node_id,file_id,file_len;
    memcpy(&file_type,buf+4,4);
    memcpy(&node_id,buf+8,4);
    memcpy(&file_id,buf+12,4);
    memcpy(&file_len,buf+16,4);
    std::cout<<"file_type == "<<file_type<<std::endl;
    std::cout<<"file_len =="<<file_len<<std::endl;
    //memcpy(write_buf,buf+20,file_len);
    write_buf = buf + 20;
    std::string target_path;
    switch(file_type){
        case 1:
        //尽量不使用这一类型传输，因为最大id还可能会变
        
        break;
        case 2:
            backup_node.db_list[node_id].tail_sge_ptr->sge_id = file_id;
            memcpy(backup_node.db_list[node_id].tail_sge_ptr->buf.data(),write_buf,file_len);
        break;
        case 3:
            target_path = "IDENTITY";
            write_file(write_buf,file_len,target_path,node_id);
        break;
        case 4:
            
            make_mani_path(target_path,file_id);
            write_file(write_buf,file_len,target_path,node_id);
            backup_node.db_list[node_id].current_mani_id = file_id;
        break;
        case 5:
            
            make_sst_path(target_path,file_id);
            write_file(write_buf,file_len,target_path,node_id);
            current_sst_id_list[node_id][file_id] = 0;
        break;
        case 6:
        //为了复用flush_tail的逻辑，必须id从小到大连续传输
            backup_node.db_list[node_id].tail_sge_ptr->sge_id = file_id;
            memcpy(backup_node.db_list[node_id].tail_sge_ptr->buf.data(),write_buf,file_len);
            backup_node.flush_tail(node_id);
        break;
        case 7:
            return 1;
        break;
        default:
            assert(0);
    }
        return 0;
}
// void backup_recover_task(struct resources* res){
//     listen_flush_tail(res);
// }
void backup_recover_task(struct resources* res){
    BACKUP_MODE=2;
    fprintf(stdout, "begin backup recovery task\n");
    std::cout<<"begin backup recovery task"<<std::endl;
    for(int i=0;i<K;i++){
        poll_completion(res,i,597);
    }
    fprintf(stdout,"poll all invalid message before recovery\n");
    for(int i=0;i<K;i++){
        int r = post_receive_msg(res,i,i,MAX_MSG_SIZE,570);
        assert(r==0);
    }
    
    int poll_id = 0;
    bool finish_list[4]={false,false,false,false};
    while(1){
        
        bool check_finsh;
        finish_lock.lock();
        check_finsh = backup_recover_finish;
        finish_lock.unlock();
        if(check_finsh==true)
            break;
        if(finish_list[0]&&finish_list[1]&&finish_list[2]&&finish_list[3])
            break;
        int poll_result;
        struct ibv_wc wc;
        if(finish_list[poll_id]==true)
            poll_result = 0;
        else
            poll_result = ibv_poll_cq(res->cq[poll_id], 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            assert(0);
        }else if(poll_result == 0){
            //
        }else{
            // if(wc.status!=IBV_WC_SUCCESS)
            //     std::cout<<"wc.status="<<wc.status<<std::endl;
            
            if(wc.status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", 
                        wc.status, wc.vendor_err);
            }
            assert(wc.status==IBV_WC_SUCCESS);
            uint32_t msg_type;
            
            memcpy(&msg_type,res->msg_buf[poll_id],4);
            fprintf(stdout, "receive message from primary node[%d], type = %d \n",poll_id,msg_type);
            if(msg_type==1){
                store_file_b(res->msg_buf[poll_id]);
                post_receive_msg(res,poll_id,poll_id,MAX_MSG_SIZE,602);
            }
            else if(msg_type==2){
                finish_list[poll_id]=true;
            }
            else{
                fprintf(stdout,"illegal type\n");
            }
        }  
        poll_id++;
        if(poll_id>=4)
            poll_id=0;
    }
    fprintf(stdout,"LSM recovery finish, begin listen flush tail\n");
    //delete write_buf;
    listen_flush_tail(res);
}
void recover_primary(struct resources *res,uint32_t fail_id);
double total_recover_time = 0,build_memtable_time = 0;
clock_t total_beg,total_end;
int main(int argc, char *argv[])
{
    struct resources res;
    uint32_t max_sge_size = MAX_SGE_SIZE;
    
    config.dev_name = NULL;
    config.server_name[0]  = NULL;
    config.server_name[1]  = NULL;
    config.tcp_port[0] = 19875;
    config.master_tcp_port = 19991;
    config.master_server_name = "10.118.0.53";
    config.ib_port = 1;
    config.gid_idx = 0;
    config.node_id = 0;
    BACKUP_MODE=2;
    

    char temp_char;
 
    /* parse the command line parameters */
    while(1)
    {
        int c;
		/* Designated Initializer */
        static struct option long_options[] =
        {
            {.name = "port", .has_arg = 1, .val = 'p' },
            {.name = "backup_mode", .has_arg = 1, .val = 'b' },
            {.name = "ib-dev", .has_arg = 1, .val = 'd' },
            {.name = "ib-port", .has_arg = 1, .val = 'i' },
            {.name = "gid-idx", .has_arg = 1, .val = 'g' },
            {.name = "node_id", .has_arg = 1, .val = 'n' },
            {.name = "server", .has_arg = 1, .val = 's' },
            {.name = "client", .has_arg = 1, .val = 'c' },
            {.name = "client2", .has_arg = 1, .val = 'C' },
            {.name = "master", .has_arg = 1, .val = 'm' },
            {.name = NULL, .has_arg = 0, .val = '\0'}
        };
        /*关于参数设置，gid必须两端都一样, ib-port必须两端等于0
        */
        c = getopt_long(argc, argv, "p:b:d:i:g:n:c:C:m:s", long_options, NULL);
        if(c == -1)
        {
            break;
        }
        switch(c)
        {
        case 'p':
            config.tcp_port[0] = strtoul(optarg, NULL, 0);
            break;
        case 'b':
            BACKUP_MODE = strtoul(optarg, NULL, 0);
            std::cout<<"BACKUP_MODE="<<BACKUP_MODE<<std::endl;
            break;
        case 'd':
            config.dev_name = strdup(optarg);
            break;
        case 'i':
            config.ib_port = strtoul(optarg, NULL, 0);
            if(config.ib_port < 0)
            {
                usage(argv[0]);
                return 1;
            }
            break;
        case 'g':
            config.gid_idx = strtoul(optarg, NULL, 0);
            if(config.gid_idx < 0)
            {
                usage(argv[0]);
                return 1;
            }
            break;
        case 'n':
            config.node_id = strtoul(optarg, NULL, 0);
            break;
        case 'c':
            config.server_name[0] = strdup(optarg);
            
            break;
        case 'C':
            config.server_name[1] = strdup(optarg);
            
            break;
        case 's':
            config.server_name[0] = NULL;
            config.server_name[1] = NULL;
            break;
        case 'm':
            config.master_server_name = strdup(optarg);
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    
    config.master_tcp_port += 6 + config.node_id;
 
    /* print the used parameters for info*/
    print_config();
    /* init all of the resources, so cleanup will be easy */
    //resources_init(&res);
    //initilize backup_node before create res to prepare sge for buf(MR)
   
    /* create resources before using them */
    if(resources_create_for_master(&res))
    {
        fprintf(stderr, "failed to create resources\n");
        exit(&res);
        return 1;
    }
    std::cout<<"finish res create"<<std::endl;
    /* connect the QPs */
    if(connect_qp_for_master(&res))
    {
        fprintf(stderr, "failed to connect QPs\n");
        exit(&res);
        return 1;
    }
    std::cout <<"finish connect qp with master"<<std::endl;
    if(poll_completion(&res,10,727))
    {
        fprintf(stderr, "poll completion failed\n");
        exit(&res);
        return 1;
    }
    std::cout<<"recv from master is fine"<<std::endl;
    post_receive(&res,10,10,44,734);
    while(1){
        int poll_result;
        struct ibv_wc wc;
        poll_result = ibv_poll_cq(res.master_cq, 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            assert(0);
        }else if(poll_result == 0){
            //
        }else{
            clock_t beg,end;
            double duration;
            beg = clock();
            assert(wc.status==IBV_WC_SUCCESS);
            uint32_t msg_type;
            memcpy(&msg_type,res.master_buf,4);
            if(msg_type==1){
                
                uint32_t node_type,node_id;
                uint32_t new_tcp_ports[4];
                memcpy(&node_type,res.master_buf+4,4);
                memcpy(&node_id,res.master_buf+8,4);
                memcpy(&new_tcp_ports[0],res.master_buf+12,16);
                config.node_id = node_id;
                std::cout<<"^^^^node: "<<node_id<<" fail,its node_type is "<<node_type <<std::endl;
                if(node_type == 1){
                    max_qp_num/=4;
                    K = 2;
                    //config.server_name = "10.118.0.53";
                    //std::string path = "../rocksdb_lsm";
                    // system("mkdir ../rocksdb_lsm");
                    // system("mkdir ../value_sgement");
                    //global_db = new DB(path,&res,config.node_id);
                    //res.buf[0] = global_db->tail_sge_ptr->buf.data();
                    res.buf[0] = new char[MAX_SGE_SIZE];
                }else if(node_type == 2){
                    K = 4;
                    config.server_name[0] = NULL;
                    config.server_name[1] = NULL;
                    // system("mkdir ../rocksdb_lsm_0");
                    // system("mkdir ../rocksdb_lsm_1");
                    // system("mkdir ../rocksdb_lsm_2");
                    // system("mkdir ../rocksdb_lsm_3");
                    // system("mkdir ../value_sgement");
                    // system("mkdir ../parity_sgement");
                    backup_node.init(config.node_id);
                    for(int i=0;i<K;i++){
                        res.buf[i]=backup_node.db_list[i].tail_sge_ptr->buf.data();
                    }
                    
                }
                else{
                    assert(0);
                }
                clock_t beg_reconnect,end_reconnect;
                double duration_reconnect;
                beg_reconnect = clock();
                for(int i =0;i<K;i++){
                    config.tcp_port[i] = new_tcp_ports[i];
                    std::cout<<"new tcp_port_["<<i<<"]="<<new_tcp_ports[i]<<std::endl;
                }
                std::cout<<"create res begin"<<std::endl;
                if(resources_create(&res))
                {
                    fprintf(stderr, "failed to create resources\n");
                    exit(&res);
                    assert(0);
                }
                std::cout<<"connect qp begin"<<std::endl;
                if(connect_qp(&res))
                {
                    fprintf(stderr, "failed to connect QPs\n");
                    exit(&res);
                    assert(0);
                }
                std::cout<<"connect qp finish"<<std::endl;
                
                
                end_reconnect = clock();
                duration_reconnect = (double)(end_reconnect - beg_reconnect)/CLOCKS_PER_SEC;
                std::cout<<"reconnect_time = "<<duration_reconnect<<std::endl;
                
                // for(int n=0;n<K;n++){
                //     int mem_id;
                //     if(config.server_name)
                //         mem_id =0;
                //     else
                //         mem_id = n;
                //     char *rec_buf = "reconnect with extra";
                //     memcpy(res.buf[0],rec_buf,21);
                //     if(post_send(&res, IBV_WR_SEND,n,0,0,21,800))
                //     {
                //         fprintf(stderr, "failed to post sr,line = 616\n");
                //         assert(0);
                //     }
                // }
                
                // for(int n=0;n<K;n++){
                //     if(poll_completion(&res,n,808))
                //     {
                //         fprintf(stderr, "poll completion failed\n");
                //         exit(&res);
                //         assert(0);
                //     }
                // }
                std::cout<<"reconnect finish"<<std::endl;
                //write_buf = new char[MAX_MSG_SIZE];
                if(config.server_name[0]==NULL){
                    // std::thread recover_th(backup_recover_task,&res);
                    // recover_th.detach();
                    backup_recover_task(&res);
                }
                else{
                    config.extra_server_name[0] = NULL;
                    uint32_t extra_tcp_ports[4];
                    memcpy(&extra_tcp_ports[0],res.master_buf+28,16);
                    for(int i=0;i<4;i++)
                        config.extra_tcp_port[i]= extra_tcp_ports[i];
                    std::cout<<"create res for extra"<<std::endl;
                    resources_create_for_extra(&res,0);
                    std::cout<<"connect qp for extra"<<std::endl;
                    connect_qp_for_extra(&res,0);
                    
                    recover_primary(&res,config.node_id);
                    //post_receive(&res,10,10,MAX_MSG_SIZE,822);
                }
                    
            }
            else if(msg_type == 2){
                std::cout<<"matser_msg_type==2"<<std::endl;
                
                store_file_p(res.master_buf,&res,0);
                //memset(res.master_buf,0,MAX_MASTER_BUF_SIZE);
                post_receive(&res,10,10,MAX_MSG_SIZE,826);
            }
            else if(msg_type == 3){
                end = clock();
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                total_recover_time += duration;
                std::cout<<"master_msg_type==3"<<std::endl;
                finish_lock.lock();
                backup_recover_finish = true;
                finish_lock.unlock();
                //delete write_buf;
                if(config.server_name[0])
                    primary(&res);
                else
                    backup();
            }
            else{
                assert(0);
            }
        }
    }
}
bool ready_to_deal_req = false;

extern char* recovery_finish_id_list;
extern std::mutex finish_id_list_lock;
extern std::vector<int> prioity_id_list;
extern bool new_lsm;
void rebuild_memtable_task(){
    if(new_lsm==true){
        ready_to_deal_req = true;
        return ;
    }
    std::cout<<"begin recover memtable of rocksdb"<<std::endl;
    clock_t beg,end;
    double duration,total_check_memtable_time=0;
    ValueSGE_ptr target_sge_ptr;
    target_sge_ptr = global_db->tail_sge_ptr;
    std::string key;
    uint32_t sge_id,offset_of_sgement;
    bool find_finish = false;
    uint32_t offset;
    std::vector<ValueSGE_ptr> reput_sge_list;
    int key_len;
    beg = clock();
    while(1){
        //std::cout<<"check sge_id = "<<target_sge_ptr->sge_id<<std::endl;
        for(offset=0;offset<target_sge_ptr->buf.size();){
            // memcpy(&key_len,target_sge_ptr->buf.data()+offset,4);
            // if(key_len==0)
            //     break;
            uint32_t new_offset = target_sge_ptr->get_kv_for_build(offset,key);
            bool status = global_db->get_from_rocksdb(key,sge_id,offset_of_sgement);
            bool get_res_is_older_than_this_kv = sge_id < target_sge_ptr->sge_id || sge_id == target_sge_ptr->sge_id && offset_of_sgement < offset;
            bool exist = status && !get_res_is_older_than_this_kv;
            //bool exist = sge_id==target_sge_ptr->sge_id &&offset_of_sgement==offset;
            if(!exist){
                break;
            }else{
                find_finish = true;
                offset = new_offset;
                
            }
        }
        if(!find_finish){
            //std::cout<<"all of kvs in this sge have not put into rocksdb in memtable"<<std::endl;
            if(target_sge_ptr->sge_id == global_db->head_sge_id.load()){
                std::cout<<"rocksdb is empty"<<std::endl;
                break;
            }
            reput_sge_list.emplace_back(target_sge_ptr);  
            target_sge_ptr = std::make_shared<NAM_SGE::ValueSGE>(target_sge_ptr->sge_id-1,MAX_SGE_SIZE);
            target_sge_ptr->read_sge(target_sge_ptr->sge_id,MAX_SGE_SIZE);
        }
        else{
            std::cout<<"find finish!!! "<<std::endl;
            break;
        }
    }
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    total_check_memtable_time += duration;
    beg = clock();
    int reput_sge_size = reput_sge_list.size();
    int reput_kv_count = 0;
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    options.disableWAL = true;
    for(int i=0;i<=reput_sge_size;i++){
        //std::cout<<"reput sge_id : "<<target_sge_ptr->sge_id<<std::endl;
        while(offset<target_sge_ptr->buf.size()){
            memcpy(&key_len,target_sge_ptr->buf.data()+offset,4);
            if(key_len==0){
                //std::cout<<"reput sge_id : "<<target_sge_ptr->sge_id<<" finish^^^"<<std::endl;
                break;
            }
                
            uint32_t new_offset = target_sge_ptr->get_kv_for_build(offset,key);
            
            std::string lsm_index;
            lsm_index.clear();
            lsm_index.append((const char*)&target_sge_ptr->sge_id,4);
            lsm_index.append((const char*)&offset,4);
            batch.Put(key, lsm_index);
            
            //auto status = global_db->rocksdb_ptr->Put(options,key,lsm_index);
            reput_kv_count++;
            offset = new_offset;
        }
        if(i == reput_sge_size)
            break;
        target_sge_ptr = reput_sge_list[reput_sge_size-1-i];
        offset = 0;
    }
    auto status = global_db->rocksdb_ptr->Write(options, &batch);
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    build_memtable_time += duration;
    std::cout<<"memtable recover finish, reput kv_num is "<<reput_kv_count<<std::endl;
    std::cout<<"check_memtable_time = "<<total_check_memtable_time<<std::endl;
    std::cout<<"build_memtable_time = "<<build_memtable_time<<std::endl;
    std::cout<<"recover_gc_num is "<<recover_gc_num<<std::endl;
    if(recover_gc_num>0){
        global_db->gc(recover_gc_num);
    }
    ready_to_deal_req = true;
}
extern uint32_t max_vsge_id_g;

int put_num=0,get_num=0;
double total_put_time=0,total_get_time=0;
// void listen_client_task(struct resources *res,int poll_id){
//     int rc=0;
//     post_receive_client(res,poll_id,poll_id,MAX_KV_LEN_FROM_CLIENT,758);
//     while(1){
//         int poll_result;
//         struct ibv_wc wc;
//         poll_result = ibv_poll_cq(res->client_cq[poll_id], 1, &wc);
//         if(poll_result < 0)
//         {
//             /* poll CQ failed */
//             fprintf(stderr, "poll CQ failed\n");
//             rc = 1;
//         }else if(poll_result == 0){
//             //
//         }else{
//             clock_t beg,end;
//             double duration;
//             int opcode,key_len,value_len;
//             beg = clock();
//             memcpy(&opcode,res->client_buf[poll_id],4);
//             memcpy(&key_len,res->client_buf[poll_id]+4,4);
//             memcpy(&value_len,res->client_buf[poll_id]+8,4);
//             //std::cout<<"recv a req, op= "<<opcode<<std::endl;
//             int status;
//             if(opcode == 1){
//                 put_num++;
//                 std::string key(res->client_buf[poll_id]+12,key_len);
//                 std::string value(res->client_buf[poll_id]+12+key_len,value_len);
//                 //key.assign((const char*)(res.client_buf+12),key_len);
//                 //value.assign((const char*)(res.client_buf+12+key_len),value_len);
//                 //std::cout<<"key="<<key<<" ,value_len="<<value_len<<std::endl;
                
//                 auto ret = global_db->put(key,value);
//                 status = ret?0:1;
//                 int finish_flag = 1;
//                 memcpy(res->client_buf[poll_id],&finish_flag,4);
//                 memcpy(res->client_buf[poll_id],&status,4);
//                 end = clock();
//                 post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,4,780);
//                 poll_completion_client(res,poll_id,781);
                
//                 //std::cout<<"deal finish,reply to client"<<std::endl;
                
//                 duration = (double)(end - beg)/CLOCKS_PER_SEC;
//                 total_put_time += duration;
                
//             }else if(opcode == 2){
//                 get_num++;
//                 std::string key(res->client_buf[poll_id]+8,key_len);
//                 std::string value;
//                 auto ret = global_db->get(key,value);
//                 status = ret?0:1;
//                 uint32_t value_len = value.size();
//                 memcpy(res->client_buf[poll_id],&status,4);
//                 memcpy(res->client_buf[poll_id]+4,&value_len,4);
//                 memcpy(res->client_buf[poll_id]+8,value.data(),value_len);
//                 end = clock();
//                 post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,8+value_len,780);
//                 poll_completion_client(res,poll_id,781);
                
//                 duration = (double)(end - beg)/CLOCKS_PER_SEC;
//                 total_get_time += duration;
//                 if(get_num>0&&get_num%100000==0){
//                     std::cout<<"avg_deal_get_time="<<total_get_time/get_num<<std::endl;
//                     std::cout<<"key = "<<key<<std::endl;
//                     std::cout<<"value = "<<value<<std::endl;
//                 }

//             }else{
//                 std::cout<<"error!! ilegal opcode :"<<opcode<<std::endl;
                
//             }
//             post_receive_client(res,poll_id,poll_id,MAX_KV_LEN_FROM_CLIENT,758);
//         }
//     }
// }

void listen_client_task(struct resources *res,int poll_id){
    post_receive_client(res,poll_id,poll_id,128*1024,758);
    while(1){
        int poll_result;
        struct ibv_wc wc;
        poll_result = ibv_poll_cq(res->client_cq[poll_id], 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
        
        }else if(poll_result == 0){
            //
        }else{
            clock_t beg,end;
            double duration;
            int opcode,key_len,value_len;
            beg = clock();
            memcpy(&opcode,res->client_buf[poll_id],4);
            memcpy(&key_len,res->client_buf[poll_id]+4,4);
            memcpy(&value_len,res->client_buf[poll_id]+8,4);
            //std::cout<<"recv a req, op= "<<opcode<<std::endl;
            int status;
            if(opcode == 1){
                put_num++;
                std::string key(res->client_buf[poll_id]+12,key_len);
                std::string value(res->client_buf[poll_id]+12+key_len,value_len);
                //key.assign((const char*)(res.client_buf+12),key_len);
                //value.assign((const char*)(res.client_buf+12+key_len),value_len);
                //std::cout<<"key="<<key<<" ,value_len="<<value_len<<std::endl;
                
                auto ret = global_db->put(key,value);
                status = ret?0:1;
                int finish_flag = 1;
                memcpy(res->client_buf[poll_id],&finish_flag,4);
                memcpy(res->client_buf[poll_id],&status,4);
                end = clock();
                post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,4,780);
                poll_completion_client(res,poll_id,781);
                
                //std::cout<<"deal finish,reply to client"<<std::endl;
                
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                total_put_time += duration;
                
            }else if(opcode == 2){
                get_num++;
                std::string key(res->client_buf[poll_id]+8,key_len);
                std::string value;
                auto ret = global_db->get(key,value);
                // if(ret){
                //     read_cache[key]=value;
                // }
                status = ret?0:1;
                if(max_parity_id>0||max_vsge_id_g>0)
                    status = 3;
                uint32_t value_len = value.size();
                memcpy(res->client_buf[poll_id],&status,4);
                memcpy(res->client_buf[poll_id]+4,&value_len,4);
                memcpy(res->client_buf[poll_id]+8,value.data(),value_len);
                end = clock();
                post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,8+value_len,780);
                poll_completion_client(res,poll_id,781);
                
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                total_get_time += duration;
                if(get_num>0&&get_num%100000==0){
                    std::cout<<"avg_deal_get_time="<<total_get_time/get_num<<std::endl;
                    //std::cout<<"key = "<<key<<std::endl;
                    //std::cout<<"value = "<<value<<std::endl;
                }

            }else if(opcode==3){
                //sub read
                int sge_id,offset;
                memcpy(&sge_id,res->client_buf[poll_id]+4,4);
                memcpy(&offset,res->client_buf[poll_id]+8,4);
                if(sge_id < max_parity_id || (max_parity_id==0&&sge_id<max_vsge_id_g) ) {
                    if(recovery_finish_id_list[sge_id]==0){
                        
                        recovery_finish_id_list[sge_id]=1;
                        prioity_id_list.push_back(sge_id);
                        
                    }
                }
                std::string key,value;
                NAM_SGE::read_kv_from_sge_for_primary(sge_id,offset,key,value);
                int status=0;
                uint32_t value_len = value.size();
                memcpy(res->client_buf[poll_id],&status,4);
                memcpy(res->client_buf[poll_id]+4,&value_len,4);
                memcpy(res->client_buf[poll_id]+8,value.data(),value_len);
                end = clock();
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,8+value_len,780);
                poll_completion_client(res,poll_id,781);
            }
            else{
                std::cout<<"error!! ilegal opcode :"<<opcode<<std::endl;
                
            }
            post_receive_client(res,poll_id,poll_id,128*1024,758);
        }
    }
}


void primary(struct resources *res){
    
    

    std::cout<<"recovery finish"<<std::endl;
    
    config.client_tcp_port = 19900+config.node_id;
    while(!ready_to_deal_req)
        ;
    resources_create_for_client(res);
    std::cout<<"create res for client finish"<<std::endl;
    connect_qp_for_client(res);
    std::cout<<"connect qp with client finish"<<std::endl;
    for(int i=0;i<2;i++){
        uint32_t flush_tail = 4;
        memcpy(res->msg_buf[0]+20,&flush_tail,4);
        post_send_msg(res,IBV_WR_SEND,i,0,20,16,1008);
        poll_completion(res,i,1009);
    }
    for(int i=1;i<max_qp_num;i++){
        std::thread listen_client_th(listen_client_task,res,i);
        listen_client_th.detach();
    }
    listen_client_task(res,0);
    while(1);
}
void backup(){
    std::cout<<"backup recover finish"<<std::endl;
}

void send_buf(struct resources *res,uint32_t file_type,uint32_t file_id,uint32_t *ret_id,uint32_t *ret_len,uint32_t extra_id){
    memcpy(res->extra_buf[extra_id],&file_type,4);
    memcpy(res->extra_buf[extra_id]+4,&file_id,4);
    int rc;
    rc = post_send(res,IBV_WR_SEND,11+extra_id,11+extra_id,0,8,1023);
    assert(!rc);
    rc = poll_completion(res,11+extra_id,1025);
    assert(!rc);
    rc = post_receive(res,11+extra_id,11+extra_id,65*1024*1024,1027);
    assert(!rc);
    rc = poll_completion(res,11+extra_id,1029);
    assert(!rc);
    memcpy(ret_id,res->extra_buf[extra_id],4);
    memcpy(ret_len,res->extra_buf[extra_id]+4,4);
}

void store_file_p_task(struct resources *res,std::vector<char *> *buf_list,std::mutex *buf_list_lock,bool *recover_finish){
    while(*recover_finish==false){
        //buf_list_lock->lock();    
        char *cur_buf =nullptr;
        if(buf_list->size()>0){
            cur_buf = buf_list->back();
            buf_list->pop_back();
        }
        //buf_list_lock->unlock();
        if(cur_buf ==nullptr)
            continue;
        store_file_p(cur_buf,res,6);
        delete cur_buf;
    }
}
uint32_t min_vsge_id=1;
extern uint32_t pre_order_num;
void decode_task(uint32_t min_parity_id,uint32_t max_parity_id,uint32_t max_vsge_id,struct resources *res,uint32_t fail_id){
    double duration;
    max_vsge_id_g = max_vsge_id;
    uint32_t file_id,file_len;
    if(max_parity_id==0){
        for(;;){
            int id;
            if(prioity_id_list.size()>0){
                id = prioity_id_list.back();
                prioity_id_list.pop_back();
            }
            else{
                for(id=max_vsge_id;id>=min_vsge_id;id--){
                    if(recovery_finish_id_list[id]==0)
                        break;
                }
            }
            if(id<min_vsge_id)
                break;
            recovery_finish_id_list[id]=1;
            send_buf(res,6,id,&file_id,&file_len,0);
            store_file_p(res->extra_buf[0],res,6);
        }
    }
    else{
        for(int id=max_vsge_id;id>=min_vsge_id;id--){
            //std::cout<<"get vsge: "<<id<<std::endl;
            send_buf(res,6,id,&file_id,&file_len,0);
            store_file_p(res->extra_buf[0],res,6);
        }
        Encoder encoder;
        char *result_buf = new char[65*1024*1024];
        encoder.init_encode(4,2,MAX_SGE_SIZE);
        uint8_t error_list[1] = {fail_id};
        encoder.init_decode(&error_list[0],1);
        uint8_t *decode_src[6];
        uint8_t *decode_dest[1];
        decode_src[4]=(uint8_t *)(res->extra_buf[0]+8);

        for(int i=0,j=1;i<4;i++){
            if(i==fail_id)
                continue;
            //decode_src[i]=(uint8_t *)(res->buf[i]+8);
            decode_src[i]=(uint8_t *)(res->extra_buf[j++]+8);
        }
        //decode_dest[0] = (uint8_t *)(res->buf[fail_id]+16);
        decode_dest[0] = (uint8_t *)(result_buf+8);
    //end = clock();
    //duration = (double)(end - beg)/CLOCKS_PER_SEC;
    //total_recover_time += duration;
    //time_except_vsge+=duration;
        for(;;){
            int id;
            if(prioity_id_list.size()>0){
                id = prioity_id_list.back();
                prioity_id_list.pop_back();
            }
            else{
                for(id=max_parity_id;id>=min_parity_id;id--){
                    if(recovery_finish_id_list[id]==0)
                        break;
                }
            }
            if(id<min_parity_id)
                break;
            recovery_finish_id_list[id]=1;
            for(int i=0;i<4;i++){
                //beg = clock();
                send_buf(res,6,id,&file_id,&file_len,i);
                //std::cout<<"get sge_id="<<id<<" from extra_recover_node : "<<i<<" file_len="<<file_len<<std::endl;
                memset(res->extra_buf[i]+8+file_len,0,MAX_SGE_SIZE-file_len);
                //end = clock();
                //duration = (double)(end - beg)/CLOCKS_PER_SEC;
                //total_recover_time += duration;
                //total_rdma_time += duration;
            }
            //std::cout<<"decode begin"<<std::endl;
            //beg = clock();
            encoder.decode_data(decode_src,decode_dest);
            //end = clock();
            //duration = (double)(end - beg)/CLOCKS_PER_SEC;
        // total_decode_time += duration;
            //std::cout<<"decode finish,send to fail node"<<std::endl;
            // std::ofstream fout;
            // std::string path = "vsge_"+std::to_string(id);
            // fout.open(path,std::ios::out|std::ios::binary);
            // fout.seekp(0,std::ios::beg);
            // fout.write(res->buf[fail_id]+16,file_len);
            // fout.close();
            uint32_t vsge_len = MAX_SGE_SIZE;
            memcpy(result_buf,&id,4);
            memcpy(result_buf+4,&vsge_len,4);
            //char *cur_buf = new char[MAX_SGE_SIZE+8];
            //memcpy(cur_buf,result_buf,MAX_SGE_SIZE+8);
            //buf_list_lock.lock();
            //buf_list.push_back(cur_buf);
            //buf_list_lock.unlock();
            store_file_p(result_buf,res,6);
        }
        delete result_buf;
        
    }
    max_parity_id = 0;
    max_vsge_id_g =0;
    //store_file_p_task_finish = true;
    std::cout<<"inform node : 4 ,listenr finish"<<std::endl;
    uint32_t file_type = 7;
    //beg = clock();
    for(int i =0;i<4;i++){
        memcpy(res->extra_buf[i],&file_type,4);
        std::cout<<"inform extra_recover_node : "<<i <<" ,listenr finish"<<std::endl;
        post_send(res,IBV_WR_SEND,11+i,11+i,0,8,1071);
        poll_completion(res,11+i,1071);
    }
    // end = clock();
    
    // total_rdma_time += duration;
    total_end = clock();
     duration = (double)(total_end - total_beg)/CLOCKS_PER_SEC;
     total_recover_time += duration;
    
    resources_destroy_for_extra(res);
    // end = clock();
    // duration = (double)(end - beg_total)/CLOCKS_PER_SEC;
    // total_recover_time = duration;
    std::cout<<"total_recover_time= "<<total_recover_time<<" pre_order_num="<<pre_order_num<<std::endl;
    // std::cout<<"decode_time= "<<total_decode_time<<std::endl;
    // std::cout<<"total_rdma_time= "<<total_rdma_time<<std::endl;
}
void recover_primary(struct resources *res,uint32_t fail_id){
    clock_t beg,end,beg_total;
    double duration,total_recover_time=0,total_decode_time=0,time_except_vsge=0,total_rdma_time=0;
    //get id range from backup node 0
    std::cout<<"begin recover_primary"<<std::endl;
    // beg = clock();
    // beg_total = clock();
    uint32_t file_id,file_len;
    total_beg = clock();
    bool store_file_p_task_finish = false;
    std::vector<char *>buf_list;
    std::mutex buf_list_lock;
    //std::thread store_file_p_th(store_file_p_task,res,&buf_list,&buf_list_lock,&store_file_p_task_finish);

    //IDENTITY
    
    
    send_buf(res,3,0,&file_id,&file_len,0);
    std::cout<<"get IDENTITY ,size ="<<file_len<<std::endl;
    store_file_p(res->extra_buf[0],res,3);
    //MANIFEST
    std::cout<<"get MANIFEST"<<file_len<<std::endl;
    send_buf(res,4,0,&file_id,&file_len,0);
    
    int mani_id = file_id;
    int mani_size = file_len;
    std::cout<<"id = "<<file_id<<" ,len = "<<file_len<<std::endl;
    store_file_p(res->extra_buf[0],res,4);
    //sst id list  file_len = 4*id_num
    std::cout<<"get sst_id list"<<std::endl;
    send_buf(res,8,0,&file_id,&file_len,0);
    std::vector<uint32_t> sst_id_list;
    std::cout<<"id_list: ";
    int max_sst_id=0;
    for(int i=0;i<file_len;i+=4){
        uint32_t tmp;
        memcpy(&tmp,res->extra_buf[0]+8+i,4);
        std::cout<<tmp<<" ,";
        sst_id_list.emplace_back(tmp);
        if(i==file_len-4)
            max_sst_id = tmp;
    }
    std::cout<<std::endl;
    //every sst file
    int i=0;
    for(uint32_t sst_id : sst_id_list){
        std::cout<<"get sst "<<sst_id<<std::endl;
        send_buf(res,5,sst_id,&file_id,&file_len,0);
        store_file_p(res->extra_buf[0],res,5);
        std::cout<<"size = "<<file_len<<std::endl;
    }
    std::cout<<"get id_range"<<std::endl;
    send_buf(res,1,0,&file_id,&file_len,0);
    uint32_t min_parity_id,max_vsge_id;
    memcpy(&min_parity_id,res->extra_buf[0]+8,4);
    memcpy(&max_parity_id,res->extra_buf[0]+12,4);
    memcpy(&min_vsge_id,res->extra_buf[0]+16,4);
    memcpy(&max_vsge_id,res->extra_buf[0]+20,4);
    memcpy(&recover_gc_num,res->extra_buf[0]+24,4);
    std::cout<<"min_parity_id ="<<min_parity_id<<" ,max_parity_id="<<max_parity_id<<"min_vsge_id ="<<min_vsge_id<<" ,max_vsge_id= "<<max_vsge_id<<" ,recover_gc_num="<<recover_gc_num<<std::endl;
    if(max_parity_id>0){
        recovery_finish_id_list = new char[max_parity_id+1];
        memset(recovery_finish_id_list,0,max_parity_id+1);
    }
    else{
        recovery_finish_id_list = new char[max_vsge_id+1];
        memset(recovery_finish_id_list,0,max_vsge_id+1);
    }
    
    //id range
    store_file_p(res->extra_buf[0],res,1);
    global_db->max_rocks_id = max_sst_id;
    for(auto id:sst_id_list){
        global_db->rocks_id_list[id]=0;
    }
    std::string target_path;
    make_mani_path(target_path,mani_id);
    global_db->last_manifest_path = target_path;
    global_db->last_mani_size = mani_size;
    //tail sge
    std::cout<<"get tail_sge"<<std::endl;
    send_buf(res,2,0,&file_id,&file_len,0);
    store_file_p(res->extra_buf[0],res,2);
    
    //every vsge not encoded
    
    std::thread decode_vsge_th(decode_task,min_parity_id,max_parity_id,max_vsge_id,res,fail_id);
    decode_vsge_th.detach();
    std::thread rebuild_memtable_th(rebuild_memtable_task);
    rebuild_memtable_th.detach();
   
    //store_file_p_th.join();
    primary(res);
}
void store_file_p(char *buf,struct resources* res,int file_type_){
    uint32_t file_type,file_id,file_len;
    if(file_type_!=0){
        file_type = file_type_;
        buf-=8;
    }
    else{
        memcpy(&file_type,buf+4,4);
    }
    memcpy(&file_id,buf+8,4);
    memcpy(&file_len,buf+12,4);
    // std::cout<<"file_type == "<<file_type<<std::endl;
    // std::cout<<"file_len =="<<file_len<<std::endl;
    // std::cout<<"file_id =="<<file_id<<std::endl;
    //memcpy(write_buf,buf+16,file_len);
    write_buf = buf + 16;
    std::string target_path;
    std::string current_path;
    uint32_t key_len,offset,new_offset;
    uint32_t value_length;
    int mr_flags;    
    switch(file_type){
        case 1:
            target_path = "../rocksdb_lsm";
            std::cout<<"open rocksdb"<<std::endl;
            global_db = new DB(target_path,res,config.node_id);
            ibv_dereg_mr(res->mr[0]);
            delete res->buf[0];
            res->buf[0] = global_db->tail_sge_ptr->buf.data();
            mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
            res->mr[0] = ibv_reg_mr(res->pd, res->buf[0], MAX_SGE_SIZE, mr_flags);
            uint32_t min_id,max_id;
            memcpy(&min_id,write_buf,4);
            memcpy(&max_id,write_buf+12,4);
            std::cout<<"min_id="<<min_id<<" ,max_id="<<max_id<<std::endl;
            //memcpy(&recover_gc_num,write_buf+16,4);

            global_db->head_sge_id.store(min_id);
            //姑且让max_id是tail的id
            global_db->cur_sge_id.store(max_id+2);
        break;
        case 2:
            //global_db->tail_sge_ptr = std::make_shared<NAM_SGE::ValueSGE>(file_id,MAX_SGE_SIZE);
            global_db->tail_sge_ptr->sge_id = file_id;
            global_db->tail_sge_ptr->buf.resize(file_len);
            global_db->tail_sge_ptr->cur_offset = file_len;
            memcpy(global_db->tail_sge_ptr->buf.data(),write_buf,file_len);

        break;
        case 3:
            target_path = "IDENTITY";
            write_file(write_buf,file_len,target_path,10);
        break;
        case 4:
            
            make_mani_path(target_path,file_id);
            std::cout<<"write_file: "<<target_path<<std::endl;
            write_file(write_buf,file_len,target_path,10);
            current_path = "CURRENT";
            target_path.append((const char *)"\n");
            std::cout<<"write_file: CURRENT"<<std::endl;
            write_file(target_path.data(),target_path.size(),current_path,10);
        break;
        case 5:
            make_sst_path(target_path,file_id);
            write_file(write_buf,file_len,target_path,10);
        break;
        case 6:
            if(file_id==0 && file_len==0)
                break;
            for(offset=0;offset<MAX_SGE_SIZE;)
            {   
                //memcpy(&key_len,write_buf+offset,4);
                
                key_len = *( (uint32_t*)(write_buf+offset ) );
                if(key_len==0)
                    break;
                value_length = *( (uint32_t*)(write_buf+offset+4 ) );
                //std::cout<<"key_len = "<<key_len<<" ,value_len = "<<value_length<<std::endl;
                //key.assign(write_buf,offset+8,key_len);
                new_offset = offset+ 8+key_len+value_length;
                offset = new_offset;
            }
            file_len = offset;
            target_path="value_sge_"+std::to_string(file_id);
            //std::cout<<"real_file_len = "<<file_len<<std::endl;
            write_file(write_buf,file_len,target_path,11);
        break;
        case 7:
        break;
        default:
            assert(0);
    }
    
}
void remove_rocks_file(std::string &target_path,uint32_t node_id){
    std::string prex = "../rocksdb_lsm_"+std::to_string(node_id)+'/';
    prex.append(target_path);
    std::remove((const char*)prex.c_str());
}
uint32_t syn_gc_finish_id[4];
uint32_t syn_gc_begin_id[4];
bool is_gc_list[4];
bool is_gc_backup = false;

void listen_flush_tail(struct resources *res){
    
    double total_flush_tail_time[K] = {0},total_send_index_time[K] = {0};
    double duration;
    clock_t beg,end;
    // if(backup_node.parity_node_id==0)
    //     std::thread fragement_num_th(fragement_num_task);
    // fragement_num_th.detach();
    for(int i=0;i<K;i++){
        syn_gc_begin_id[i]=0;
    }
    for(int i=0;i<K;i++){
        syn_gc_finish_id[i]=0;
    }
    uint32_t syn_gc_end_id;
    
    for(int i=0;i<K;i++)
        is_gc_list[i] = false;
 
    for(int i=0;i<K;i++)
        post_receive_msg(res,i,i,16,711);
    int poll_id = 0;
    int flush_tail_count=0;
    while(1){
        int poll_result;
        struct ibv_wc wc;
        poll_result = ibv_poll_cq(res->cq[poll_id], 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
        }else if(poll_result == 0){
            //
        }else{
            if(wc.status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", 
                        wc.status, wc.vendor_err);
            }
            uint32_t msg_type;
            memcpy(&msg_type,res->msg_buf[poll_id],4);
            if(msg_type==1){
                beg = clock();
                //std::cout<<"get flush tail msg from node: "<<poll_id<<std::endl;
                uint32_t remote_sge_id;
                memcpy(&remote_sge_id,res->msg_buf[poll_id]+4,4);
                backup_node.db_list[poll_id].tail_sge_ptr->sge_id=remote_sge_id;
                if(syn_gc_begin_id[poll_id]==0){
                    syn_gc_begin_id[poll_id]=remote_sge_id;
                    syn_gc_finish_id[poll_id]=syn_gc_begin_id[poll_id]-1;
                }
                if(remote_sge_id!=backup_node.db_list[poll_id].tail_sge_ptr->sge_id)
                    std::cout<<"id error!!!remote_sge_id= "<<remote_sge_id<<" ,tail_sge_id= "<<backup_node.db_list[poll_id].tail_sge_ptr->sge_id<<std::endl;
                #if DEBUG
                int sge_len;
                
                //memcpy(&sge_len,backup_node.db_list[poll_id].tail_sge_ptr->buf.data()+MAX_SGE_SIZE-4,4);
                //std::cout<<"$$$vsge from node: "<<poll_id<<" ,id="<<backup_node.db_list[poll_id].tail_sge_ptr->sge_id <<std::endl;//" ,len="<<sge_len<<std::endl;
                //int first_key_len,first_value_len;
                //memcpy(&first_key_len,backup_node.db_list[poll_id].tail_sge_ptr->buf.data(),4);
                //memcpy(&first_value_len,backup_node.db_list[poll_id].tail_sge_ptr->buf.data()+4,4);
                //std::cout<<"first key_len = "<<first_key_len<<"  ,first_value_len="<<first_value_len<<std::endl;
                // sge_len = *( (uint32_t*)(backup_node.db_list[poll_id].tail_sge_ptr->buf.data()+MAX_SGE_SIZE-4 ) );
                // std::cout<<"sge_len read again,sge_len = "<<sge_len<<std::endl;
                // std::string key,value;
                //uint32_t tmp = backup_node.db_list[poll_id].tail_sge_ptr->get_kv_for_gc(0,key,value);
                // std::cout<<"^^^^^^^^first kv of node:"<<poll_id<<" ,sge_id:"<<backup_node.db_list[poll_id].tail_sge_ptr->sge_id<<std::endl;
                // std::cout<<key<<std::endl;
                // std::cout<<value<<std::endl;
                // backup_node.db_list[poll_id].tail_sge_ptr->get_kv_for_gc(tmp,key,value);
                // std::cout<<"^^^^^^^^second kv of node:"<<poll_id<<" ,sge_id:"<<backup_node.db_list[poll_id].tail_sge_ptr->sge_id<<std::endl;
                // std::cout<<key<<std::endl;
                // std::cout<<value<<std::endl;
                // for(int offset=0;offset < sge_len;){
                    
                //     uint32_t new_offset = backup_node.db_list[poll_id].tail_sge_ptr->get_kv_for_gc(offset,key,value);
                //     std::cout<<"^^^^^^^^kv of node:"<<poll_id<<" ,sge_id:"<<backup_node.db_list[poll_id].tail_sge_ptr->sge_id<<std::endl;
                //     std::cout<<key<<std::endl;
                //     std::cout<<value<<std::endl;
                //     offset = new_offset;
                // }
                #endif
                ibv_dereg_mr(res->mr[poll_id]);
                backup_node.flush_tail(poll_id);
                if(backup_node.parity_node_id == 0){
                    
                    backup_node.parity_id_lock.lock();
                    if(backup_node.parity_sge_num>GC_TRIGER_NUM){
                        if(backup_node.syn_gc_lock.try_lock()){
                            #if DEBUG
                            std::cout<<"trigger gc"<<std::endl;
                            #endif
                            syn_gc_end_id = backup_node.min_parity_sge_id + GC_PER_NUM-1; 
                            is_gc_backup = true;
                            // for(int i=0;i<K;i++){  
                            //     //syn_gc_begin_id[i]++; //= backup_node.min_parity_sge_id;                             
                            //     syn_gc_finish_id[i] =  backup_node.min_parity_sge_id - 1;                                
                            // }
                        }
                    }
                    backup_node.parity_id_lock.unlock();
                    
                }
                int mr_flags;
                mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
                res->buf[poll_id] = backup_node.db_list[poll_id].tail_sge_ptr->buf.data();
                res->mr[poll_id] = ibv_reg_mr(res->pd, res->buf[poll_id], MAX_SGE_SIZE, mr_flags);
                uint32_t new_rkey =  res->mr[poll_id]->rkey;//htonl(res->mr[poll_id]->rkey);
                uint64_t new_address = (uint64_t)res->buf[poll_id];//htonll((uintptr_t)res->buf[poll_id]);
                //std::cout<<"new addr="<<new_address<<" ,new rkey="<<new_rkey<<std::endl;
                memcpy(res->msg_buf[poll_id],&new_rkey,4);
                memcpy(res->msg_buf[poll_id]+4,&new_address,8);
                uint32_t msg_byte4=0,msg_byte5=0;
                if(is_gc_backup == true && is_gc_list[poll_id] == false){
                    std::cout<<"make msg_byte4=1"<<std::endl;
                    is_gc_list[poll_id] = true;
                    msg_byte4 = 1;
                    msg_byte5 = GC_PER_NUM;
                }
                memcpy(res->msg_buf[poll_id]+12,&msg_byte4,4);
                memcpy(res->msg_buf[poll_id]+16,&msg_byte5,4);
                post_send_msg(res,IBV_WR_SEND,poll_id,poll_id,0,20,804);
                if(poll_completion(res,poll_id,805)){
                    fprintf(stderr, "error!!!main.cpp:806\n");
                }
                end = clock();
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                total_flush_tail_time[poll_id] += duration;
                if(++flush_tail_count%400==0)
                    std::cout<<"total_flush_tail_time["<<poll_id<<"] = "<<total_flush_tail_time[poll_id]<<std::endl;
            }
            else if(msg_type==2){
                beg = clock();
                uint32_t new_sst_num,delete_sst_num,mani_size;
                memcpy(&new_sst_num,res->msg_buf[poll_id]+4,4);
                memcpy(&delete_sst_num,res->msg_buf[poll_id]+8,4);
                memcpy(&mani_size,res->msg_buf[poll_id]+12,4);
                //std::cout<<"node:"<<poll_id <<" SEND Index: new_num"<<new_sst_num<<" ,delete_num:"<<delete_sst_num<<"MANI_SIZE="<<mani_size<<std::endl;
                post_receive_msg(res,poll_id,poll_id,mani_size+4,820);
                poll_completion(res,poll_id,821);
                uint32_t mani_id;
                memcpy(&mani_id,res->msg_buf[poll_id],4);
                std::string target_path;
                make_mani_path(target_path,mani_id);
                write_file(res->msg_buf[poll_id]+4,mani_size,target_path,poll_id);
                if(mani_id!=backup_node.db_list[poll_id].current_mani_id){
                    make_mani_path(target_path,backup_node.db_list[poll_id].current_mani_id);
                    remove_rocks_file(target_path,poll_id);
                    backup_node.db_list[poll_id].current_mani_id = mani_id;
                }
                //std::cout<<"write MANIFEST"<<std::endl;
                uint32_t sst_id,sst_len;
                
                target_path = "000000.sst";
                for(int k=0;k<new_sst_num;k++){
                    post_receive_msg(res,poll_id,poll_id,MAX_MSG_SIZE,837);
                    poll_completion(res,poll_id,838);
                    memcpy(&sst_id,res->msg_buf[poll_id],4);
                    memcpy(&sst_len,res->msg_buf[poll_id]+4,4);
                    
                    std::string tmp = std::to_string(sst_id);
                    uint32_t tmp_size = tmp.size();
                    for(int l=0;l<tmp_size;l++)
                        target_path[6-tmp_size+l]=tmp[l];
                    write_file(res->msg_buf[poll_id]+8,sst_len,target_path,poll_id);
                    current_sst_id_list[poll_id][sst_id] = 0;
                    std::cout<<"create "<<target_path<<" size="<<sst_len<<std::endl;
                    //此处计划之后改成启动线程异步处理
                }
                if(delete_sst_num>0){
                    
                    post_receive_msg(res,poll_id,poll_id,4*delete_sst_num,853);
                    poll_completion(res,poll_id,854);
                }
                
                for(int k=0;k<delete_sst_num;k++){
                    memcpy(&sst_id,res->msg_buf[poll_id]+4*k,4);
                    target_path = "000000.sst";
                    std::string tmp = std::to_string(sst_id);
                    uint32_t tmp_size = tmp.size();
                    for(int l=0;l<tmp_size;l++)
                        target_path[6-tmp_size+l]=tmp[l];
                    remove_rocks_file(target_path,poll_id);
                    auto iter = current_sst_id_list[poll_id].find(sst_id);
                    if(iter!= current_sst_id_list[poll_id].end())
                        current_sst_id_list[poll_id].erase(iter);
                    else{
                        std::cout<<"error!delete id= "<<sst_id<<" from id_list,but not find"<<std::endl;
                    }
                    //std::cout<<"delete "<<target_path<<std::endl;
                }
                end = clock();
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                total_send_index_time[poll_id] += duration;
                std::cout<<"total_send_index_time["<<poll_id<<"] = "<<total_send_index_time[poll_id]<<std::endl;
                
            }else if(msg_type==3){
                uint32_t gc_finish_num;
                uint32_t gc_remove_num = 0;
                memcpy(&gc_finish_num,res->msg_buf[poll_id]+4,4);
                if(gc_finish_num==0)
                    std::cout<<"error!!!gc_finish num==0"<<std::endl;
                
                syn_gc_finish_id[poll_id]+=gc_finish_num;
                //std::cout<<"node: "<< poll_id<<" ,gc_finish num="<<gc_finish_num<<" ,syn_finish_id= "<<syn_gc_finish_id[poll_id]<<std::endl;
                uint32_t remove_begin_id = syn_gc_begin_id[poll_id];
                while(syn_gc_begin_id[poll_id]<=syn_gc_finish_id[0]&&syn_gc_begin_id[poll_id]<=syn_gc_finish_id[1]&&syn_gc_begin_id[poll_id]<=syn_gc_finish_id[2]&&syn_gc_begin_id[poll_id]<=syn_gc_finish_id[3]){
                    bool is_first = true;
                    std::cout<<"node: "<<poll_id<<" begin_id="<<syn_gc_begin_id[poll_id]<<" ,++"<<std::endl;
                    for(int i=0;i<K;i++){
                        if(i!=poll_id && syn_gc_begin_id[i]>syn_gc_begin_id[poll_id])
                            is_first = false;
                    }
                    if(is_first)
                        backup_node.remove_sge(syn_gc_begin_id[poll_id]);
                    gc_remove_num++;
                    syn_gc_begin_id[poll_id]++;
                }
                //std::cout<<"remove_begin_id= "<<remove_begin_id<<" ,remove_num= "<<gc_remove_num<<std::endl;
                memcpy(res->msg_buf[poll_id],&remove_begin_id,4);
                memcpy(res->msg_buf[poll_id]+4,&gc_remove_num,4);
                post_send_msg(res,IBV_WR_SEND,poll_id,poll_id,0,8,903);
                poll_completion(res,poll_id,904);
                if(backup_node.parity_node_id==0){
                    if(syn_gc_finish_id[0]==syn_gc_end_id&&syn_gc_finish_id[1]==syn_gc_end_id&&syn_gc_finish_id[2]==syn_gc_end_id&&syn_gc_finish_id[3]==syn_gc_end_id){
                        is_gc_backup = false;
                        for(int i=0;i<K;i++)
                            is_gc_list[i]=false;
                        backup_node.syn_gc_lock.unlock();
                        #if DEBUG
                            std::cout<<"finish gc"<<std::endl;
                        #endif
                    }
                }
            }
            else if(msg_type==4){
                
                uint32_t remove_num,remove_beg_id;
                memcpy(&remove_beg_id,res->msg_buf[poll_id]+4,4);
                memcpy(&remove_num,res->msg_buf[poll_id]+8,4);
                syn_gc_finish_id[poll_id]+=remove_num;
                for(int i=0;i<remove_num;i++){
                    std::string path;
                    path.clear();
                    path.append("../value_sgement/value_sge_");
                    path.append(std::to_string(poll_id));
                    path.push_back('_');
                    path.append(std::to_string(remove_beg_id+i));
                    std::remove((const char*)path.c_str());
                } 
            }
            // else if(msg_type==5){
                
                
                
                
            //     clock_t beg,end;
            //     uint32_t remove_num,remove_beg_id;
                
            //     memcpy(&remove_beg_id,res->msg_buf[poll_id]+4,4);
            //     memcpy(&remove_num,res->msg_buf[poll_id]+8,4);
            //     {
            //         clock_t beg,end;
            //         beg = clock();
            //         post_receive_msg(res,poll_id,poll_id,16*1024*1024,984);
            //         poll_completion(res,poll_id,985);
            //         end = clock();
            //         duration = (double)(end - beg)/CLOCKS_PER_SEC;
            //         //gc_rdma_time +=duration;
            //     }
            //     //std::cout<<"node: "<<poll_id <<" msg_type==5    remove_id="<<remove_beg_id<<std::endl;
            //     syn_gc_finish_id[poll_id]+=remove_num;
            //     std::string path;
            //     for(int i=0;i<remove_num;i++){
            //         clock_t beg,end;
            //         double duration;
            //         if((syn_gc_finish_id[0]<remove_beg_id+i) || (syn_gc_finish_id[1]<remove_beg_id+i)||(syn_gc_finish_id[2]<remove_beg_id+i)||(syn_gc_finish_id[3]<remove_beg_id+i)){
            //             // std::thread update_parity_th(update_parity_task,poll_id,remove_beg_id+i,res);
            //             // update_parity_th.detach();
            //         }
            //         else{
            //             backup_node.remove_sge(remove_beg_id+i);
            //         }    
            //     }
                
            //     //std::cout<<"finish msg_type=5"<<std::endl;
            // }
            else{

                std::cout<<"^^^^^error!!!!!!&&&&&&&&msg_type error********"<<std::endl;
                std::cout<<"msg_type= "<<msg_type<<" from node:"<<poll_id<<std::endl;
            }
            post_receive_msg(res,poll_id,poll_id,16,922);
        }
        poll_id++;
        if(poll_id >= K)
            poll_id = 0;
    }
}