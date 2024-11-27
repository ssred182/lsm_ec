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
#include <unordered_map>
#include <time.h>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <thread>
#include <assert.h>
#include "db.h"
#include "rdma_connect.h"
#include "define.h"
#define KV_NUM  10*1024
#define V_LEN 1024
#define MAX_KV_LEN_FROM_CLIENT 128*1024
class KV{
    public:
        std::string key;
        std::string value;
        KV(const std::string &key_,const std::string &value_){
            key = key_;
            value = value_;
        }
};

extern struct config_t config;
extern int K;
int resources_create(struct resources *res,size_t mr_size)
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
            res->sock[n] = sock_connect(config.server_name[n], config.tcp_port[n]);
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
                goto resources_create_exit;
            }
            fprintf(stdout, "TCP port: %d connection was established \n",config.tcp_port[n]);
        }
        
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
 
    /* if there isn't any IB device in host */
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
    for(int n=0;n<K;n++){
        res->cq[n] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if(!res->cq[n])
        {
            fprintf(stderr, "failed to create CQ with %u entries for node:%d\n", cq_size,n);
            rc = 1;
            goto resources_create_exit;
        }
    }
    
    size = mr_size;
    /* allocate the memory buffer that will hold the data */

         res->msg_buf[0] = (char *) malloc(MAX_MSG_SIZE);
         res->msg_buf[1] = (char *) malloc(MAX_MSG_SIZE);
            res->gc_msg_buf = (char *) malloc(16*1024*1024+16);
         fprintf(stdout, "申请内存msg_buf\n");
        if(!res->msg_buf[0])
        {
            fprintf(stderr, "failed to malloc %Zu bytes to memory buffer\n", size);
            rc = 1;
            goto resources_create_exit;
        }
        memset(res->buf[0], 0 , MAX_SGE_SIZE);
        memset(res->msg_buf[0], 0 , size); 
        memset(res->gc_msg_buf, 0 , 16); 
    /* register the memory buffer */
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
        res->mr[0] = ibv_reg_mr(res->pd, res->buf[0], MAX_SGE_SIZE, mr_flags);
        res->msg_mr[0] = ibv_reg_mr(res->pd, res->msg_buf[0], MAX_MSG_SIZE, mr_flags);
        res->msg_mr[1] = ibv_reg_mr(res->pd, res->msg_buf[1], MAX_MSG_SIZE, mr_flags);
        res->gc_msg_mr = ibv_reg_mr(res->pd, res->gc_msg_buf, 16*1024*1024+16, mr_flags);
        fprintf(stdout, "注册buf内存到pd\n");
        if(!res->mr[0])
        {
            fprintf(stderr, "ibv_reg_mrfailed with mr_flags=0x%x\n", mr_flags);
            rc = 1;
            goto resources_create_exit;
        }
        fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                res->buf[0], res->mr[0]->lkey, res->mr[0]->rkey, mr_flags);
    
    
    
 
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
            if(res->mr[0])
            {
                ibv_dereg_mr(res->mr[0]);
                res->mr[0] = NULL;
            }
            if(res->buf[0])
            {
                free(res->buf[0]);
                res->buf[0] = NULL;
            }
            if(res->msg_mr[0])
            {
                ibv_dereg_mr(res->msg_mr[0]);
                res->mr[0] = NULL;
            }
            if(res->msg_buf[0])
            {
                free(res->msg_buf[0]);
                res->msg_buf[0] = NULL;
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
   
    res->master_sock = sock_connect(config.master_server_name, config.master_tcp_port);
    if(res->master_sock < 0)
    {
        fprintf(stderr, "failed to establish TCP connection to master_server %s\n",config.master_server_name);
        rc = -1;
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
    for(int i=0;i<MAX_QP_NUM;i++){
        res->client_cq[i] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if(!res->client_cq[i])
        {
            fprintf(stderr, "failed to create master CQ with %u entries for client:%d\n", cq_size,i);
            assert(0);
        }
    }
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    int size_c = 128*1024;//MAX_KV_LEN_FROM_CLIENT
    for(int i=0;i<MAX_QP_NUM;i++){
        res->client_buf[i] = (char *) malloc(size_c);
        res->client_mr[i] = ibv_reg_mr(res->pd, res->client_buf[i], size_c, mr_flags);
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
    for(int i=0;i<MAX_QP_NUM;i++){
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














uint32_t read_file(const char *buf,std::string &target_path,uint32_t node_id){
    std::ifstream fin;
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
    fin.open(prex,std::ios::in|std::ios::binary);
    fin.seekg(0,std::ios::end);
    uint32_t file_size = fin.tellg();
    fin.seekg(0,std::ios::beg);
    fin.read((char*)buf,file_size);
    fin.close();
    return file_size;
}







// static uint32_t read_file_vsge(const char* buf,const std::string &target_path){
//     std::ifstream fin;
//     std::string prex = "../value_sgement/";
//     prex.append(target_path);
//     fin.open(prex,std::ios::in|std::ios::binary);
//     fin.seekg(0,std::ios::end);
//     uint32_t file_size = fin.tellg();
//     fin.seekg(0,std::ios::beg);
//     fin.read((char*)buf,file_size);
//     fin.close();
//     return file_size;
// }

// static uint32_t read_file(const char* buf,const std::string &target_path){
//     std::ifstream fin;
//     std::string prex = "../rocksdb_lsm/";
//     prex.append(target_path);
//     fin.open(prex,std::ios::in|std::ios::binary);
//     fin.seekg(0,std::ios::end);
//     uint32_t file_size = fin.tellg();
//     fin.seekg(0,std::ios::beg);
//     fin.read((char*)buf,file_size);
//     fin.close();
//     return file_size;
// }
void master_caller(struct resources *res);

extern double flush_tail_time,send_index_time,rdma_write_time,local_put_sge_time,local_put_rocks_time,gc_local_time,gc_rdma_time;
int rc = 1;
DB *test_db;
extern int BACKUP_MODE;
int put_num=0,get_num=0,sub_read_count=0,push_count=0;
double total_put_time=0,total_get_time=0,sub_read_time=0,push_time=0;
//std::unordered_map<std::string,std::string> read_cache;
void listen_client_task(struct resources *res,int poll_id){
    post_receive_client(res,poll_id,poll_id,128*1024,758);
    bool first_sub_read=true;
    bool first_get=true;
    while(1){
        int poll_result;
        struct ibv_wc wc;
        poll_result = ibv_poll_cq(res->client_cq[poll_id], 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            rc = 1;
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
                
                auto ret = test_db->put(key,value);
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
                if(first_get){
                    std::cout<<"get , key="<< key<<std::endl;
                }
                std::string value;
                auto ret = test_db->get(key,value);
                if(first_get){
                    first_get=false;
                    std::cout<<"value_len="<< value.size()<<std::endl;
                }
                // if(ret){
                //     read_cache[key]=value;
                // }
                status = ret?0:1;
                uint32_t value_len = value.size();
                if(value_len>4096){
                    value.clear();
                    value_len=0;
                }
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
                if(first_sub_read){
                    first_sub_read=false;
                    std::cout<<"subread ,vsge_id="<<sge_id<<" offset="<<offset<<std::endl;
                }
                int key_len;
                memcpy(&key_len,res->client_buf[poll_id]+12,4);
                std::string key(res->client_buf[poll_id]+16,key_len);
                std::string value;
                if(test_db->get_memtable(key,value)==false){
                    value.clear();
                    NAM_SGE::read_kv_from_sge(sge_id,offset,key,value);
                }
                //test_db->get_kv_with_position(sge_id,offset,key,value);
                
                //NAM_SGE::read_kv_from_sge(sge_id,offset,key,value);
                int status=0;
                uint32_t value_len = value.size();
                 //std::cout<<"key="<<key<<" vlaue_len="<<value_len<<std::endl;
                memcpy(res->client_buf[poll_id],&status,4);
                memcpy(res->client_buf[poll_id]+4,&value_len,4);
                memcpy(res->client_buf[poll_id]+8,value.data(),value_len);
                end = clock();
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                sub_read_time+=duration;
                sub_read_count++;
                if(sub_read_count%100000==0){
                    std::cout<<"avg_deal_subget_time="<<sub_read_time/sub_read_count<<std::endl;
                }
                post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,8+value_len,780);
                poll_completion_client(res,poll_id,781);
            }else if(opcode==4){
                //push kvs to backup for read cache
                int backup_node_id;
                int key_num=0;
                memcpy(&backup_node_id,res->client_buf[poll_id]+4,4);
                memcpy(&key_num,res->client_buf[poll_id]+8,4);
                auto push_func = [](int backup_node_id,int key_num,struct resources *res,int poll_id){
                    clock_t beg,end;
                    double duration;
                    int message_type=7;
                    test_db->rdma_msg_lock[1].lock();
                    
                    int offset_c=12;
                    int offset_b=8;
                
                    for(int i=0;i<key_num;++i){
                        int key_len;

                        memcpy(&key_len,res->client_buf[poll_id]+offset_c,4);
                        std::string key(res->client_buf[poll_id]+offset_c+4,key_len);
                        // if(i%100==0)
                        //     std::cout<<"key="<<key<<std::endl;
                        memcpy(res->msg_buf[1]+offset_b,res->client_buf[poll_id]+offset_c,key_len+4);
                        offset_c+=key_len+4;
                        
                        std::string value;
                        //auto iter=read_cache.find(key);
                        bool ret;
                        // if(iter!=read_cache.end()){
                        //     value=iter->second;
                        //     read_cache.erase(iter);
                        // }
                        // else
                            ret = test_db->get(key,value);
                        // if(i%100==0)
                        //     std::cout<<"val="<<value<<std::endl;
                        if(ret){
                            int value_len=value.size();
                            offset_b+=key_len+4;
                            memcpy(res->msg_buf[1]+offset_b,&value_len,value_len);
                            offset_b+=4;
                            memcpy(res->msg_buf[1]+offset_b,(const char*)value.data(),value_len);
                            offset_b+=value_len;
                        }
                    }
                    //std::cout<<"push lru to backup_node_id = "<<backup_node_id<<" recv_msg_len="<<offset_c<<" send_msg_len="<<offset_b<<" kv_num="<<key_num<<std::endl;
                    test_db->rdma_msg_lock[0].lock();
                    //send msg_type and key_num
                    memcpy(res->msg_buf[1],&message_type,4);
                    memcpy(res->msg_buf[1]+4,&key_num,4);
                    post_send_msg(test_db->res_ptr,IBV_WR_SEND,backup_node_id,1,0,8,683);
                    poll_completion(test_db->res_ptr,backup_node_id,684);
                    //send kvs
                    post_send_msg(test_db->res_ptr,IBV_WR_SEND,backup_node_id,1,8,offset_b-8,698);
                    poll_completion(test_db->res_ptr,backup_node_id,699);
                    test_db->rdma_msg_lock[0].unlock();
                    test_db->rdma_msg_lock[1].unlock();
                    end = clock();
                    duration = (double)(end - beg)/CLOCKS_PER_SEC;
                    push_time+=duration;
                    push_count++;
                    if(push_count%100==0)
                        std::cout<<"avg push time = "<<push_time/push_count<<std::endl;
                };
                test_db->thread_pool.submit(push_func,backup_node_id,key_num,res,poll_id);
            }
            else{
                std::cout<<"error!! ilegal opcode :"<<opcode<<std::endl;
                
            }
            post_receive_client(res,poll_id,poll_id,128*1024,758);
        }
    }
}


int main(int argc, char *argv[])
{
    //read_cache.reserve(64*1024);
    struct resources res;
    uint32_t max_sge_size = MAX_SGE_SIZE;
    BACKUP_MODE=2;
    config.dev_name = NULL;
    config.server_name[0]  = NULL;
    config.server_name[1]  = NULL;
    config.tcp_port[0] = 19875;
    config.master_tcp_port = 19991;
    config.client_tcp_port = 19900;
    config.master_server_name = "10.118.0.53";
    config.extra_server_name[0] = "10.118.0.53";
    config.extra_server_name[1] = "10.118.0.53";
    config.ib_port = 1;
    config.gid_idx = 0;
    config.node_id = 0;

    

    char temp_char;
 
    /* parse the command line parameters */
    while(1)
    {
        int c;
		/* Designated Initializer */
        static struct option long_options[] =
        {
            {.name = "port", .has_arg = 1, .val = 'p' },
            {.name = "ib-dev", .has_arg = 1, .val = 'd' },
            {.name = "ib-port", .has_arg = 1, .val = 'i' },
            {.name = "gid-idx", .has_arg = 1, .val = 'g' },
            {.name = "node_id", .has_arg = 1, .val = 'n' },
            {.name = "server", .has_arg = 1, .val = 's' },
            {.name = "backup", .has_arg = 1, .val = 'b' },
            {.name = "client", .has_arg = 1, .val = 'c' },
            {.name = "client2", .has_arg = 1, .val = 'C' },
            {.name = "master", .has_arg = 1, .val = 'm' },
            {.name = "extra", .has_arg = 1, .val = 'e' },
            {.name = "extra2", .has_arg = 1, .val = 'E' },
            {.name = NULL, .has_arg = 0, .val = '\0'}
        };
        /*关于参数设置，gid必须两端都一样, ib-port必须两端等于0
        */
        c = getopt_long(argc, argv, "p:d:i:g:n:c:C:m:e:E:sb:", long_options, NULL);
        if(c == -1)
        {
            break;
        }
        switch(c)
        {
        case 'p':
            config.tcp_port[0] = strtoul(optarg, NULL, 0);
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
            K = 2;
            config.server_name[0] = strdup(optarg);
            
            break;
        case 'C':
            K = 2;
            config.server_name[1] = strdup(optarg);
            break;
        case 'm':
            config.master_server_name = strdup(optarg);
            break;
        case 'e':
            config.extra_server_name[0] = strdup(optarg);
            break;
        case 'E':
            config.extra_server_name[1] = strdup(optarg);
            break;
        case 's':
            K = 4;
            config.server_name[0] = NULL;
            config.server_name[1] = NULL;
            break;
        case 'b':
            BACKUP_MODE = strtoul(optarg, NULL, 0);
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    
   
 
    /* print the used parameters for info*/
    
    /* init all of the resources, so cleanup will be easy */
    resources_init(&res);
    print_config();
    std::cout<<"BACKPU_MODE == "<<BACKUP_MODE<<std::endl;
    config.client_tcp_port+=config.node_id;
    //initilize backup_node before create res to prepare sge for buf(MR)
    std::string path = "../rocksdb_lsm";
    test_db = new DB(path,&res,config.node_id);
    std::cout<<"create rocksdb successfully"<<std::endl;
    if(BACKUP_MODE==2){
        test_db->GC_MODE=1;
    }
    else if(BACKUP_MODE==1 || BACKUP_MODE==3){
        test_db->GC_MODE=2;
    }
    res.buf[0] = test_db->tail_sge_ptr->buf.data();

    /* create resources before using them */
    if(resources_create(&res,max_sge_size))
    {
        fprintf(stderr, "failed to create resources\n");
        assert(0);
    }
    std::cout<<"finish res create"<<std::endl;
    /* connect the QPs */
    if(connect_qp(&res))
    {
        fprintf(stderr, "failed to connect QPs\n");
        assert(0);
    }
    if(config.server_name[0])
	{
        std::string target_path = "IDENTITY";
        read_file(res.buf[0],target_path,10);
        for(int n=0;n<K;n++){
            if(post_send(&res, IBV_WR_SEND,n,0,0,37,409))
            {
                fprintf(stderr, "failed to post sr\n");
                assert(0);
            }
        }
        std::cout<<"send IDENTITY"<<std::endl;
        
	}
    /* in both sides we expect to get a completion */
    for(int n=0;n<K;n++){
        if(poll_completion(&res,n,419))
        {
            fprintf(stderr, "poll completion failed\n");
            assert(0);
        }
    }
    std::cout<<"finish connect qp"<<std::endl;
    for(int n=0;n<K;n++){
        if(config.server_name[n])
        {
            fprintf(stdout, "recv from qp:%d is: '%s'\n", n,res.buf[0]);
        }
    }
    std::cout<<"create res for master"<<std::endl;
    if(resources_create_for_master(&res))
    {
        fprintf(stderr, "failed to create resources for master\n");
        assert(0);
    }
    std::cout<<"connect with master"<<std::endl;
    if(connect_qp_for_master(&res))
    {
        fprintf(stderr, "failed to connect QPs with master\n");
        assert(0);
    }
    
    /* let the server post the sr */
    
    if(poll_completion(&res,10,593))
    {
        fprintf(stderr, "poll completion for master failed\n");
        assert(0);
    }
    std::cout<<"recv from master is fine"<<std::endl;
    
    std::thread master_caller_th(master_caller,&res);
    master_caller_th.detach();
    std::cout<<"create res for client"<<std::endl;
    resources_create_for_client(&res);
    std::cout<<"create res for client finish"<<std::endl;
    connect_qp_for_client(&res);
    std::cout<<"connect qp with client finish"<<std::endl;
    for(int i=1;i<MAX_QP_NUM;i++){
        std::thread listen_client_th(listen_client_task,&res,i);
        listen_client_th.detach();
    }
    listen_client_task(&res,0);
   
    
    
    // while(1)
    //     ;
   
    std::cout<<"flush_tail time= "<<flush_tail_time<<std::endl;
    #if SEND_INDEX==1
    std::cout<<"send index time= "<<send_index_time<<std::endl;
    #endif
    std::cout<<"rdma write time= "<<rdma_write_time<<std::endl;
    std::cout<<"local_put_sge time= "<<local_put_sge_time<<std::endl;
    std::cout<<"local_put_rocks time= "<<local_put_rocks_time<<std::endl;
    std::cout<<"gc_local_time= "<<gc_local_time<<std::endl;
    std::cout<<"gc_rdma_time= "<<gc_rdma_time<<std::endl;
    while(1)
        ;
 
    if(resources_destroy(&res))
    {
        fprintf(stderr, "failed to destroy resources\n");
        rc = 1;
    }
    if(config.dev_name)
    {
        free((char *) config.dev_name);
    }
    fprintf(stdout, "\ntest result is %d\n", rc);
    return rc;
}

void send_master_buf(struct resources *res,uint32_t file_id, uint32_t file_len){
    memcpy(res->master_buf,&file_id,4);
    memcpy(res->master_buf+4,&file_len,4);
    post_send(res,IBV_WR_SEND,10,10,0,8+file_len,688);
    poll_completion(res,10,689);
}

void listen_recover_from_master(struct resources *res,uint32_t fail_id){
    uint32_t file_type,file_id,file_len;
    bool recover_finish = false;
    std::cout<<"listen_recover_from_master begin"<<std::endl;
    post_receive(res,10,10,8,694);
    while(1){
        int poll_result;
        struct ibv_wc wc;
        poll_result = ibv_poll_cq(res->master_cq, 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            rc = 1;
        }else if(poll_result == 0){
            //
        }else{
            memcpy(&file_type,res->master_buf,4);
            memcpy(&file_id,res->master_buf+4,4);
            std::string target_path;
            switch(file_type){
                
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    assert(0);
                break;
                case 6:
                    std::cout<<"file_type is vsge,id = "<<file_id<<" "<<std::endl;
                    target_path.append("../value_sgement/value_sge_");
                    target_path.append(std::to_string(file_id));
                    //memset(res->master_buf+8,0,MAX_SGE_SIZE);
                    file_len = read_file(res->master_buf+8,target_path,11);
                    //send_master_buf(res,file_id,file_len);
                break;
                case 7:
                    recover_finish = true;
                    std::cout<<"master_recover_listener finish"<<std::endl;
                break;
                case 8:
                break;
            }
            if(recover_finish==true)
                break;
            else{
                send_master_buf(res,file_id,file_len);
                post_receive(res,10,10,8,755);
            }
        }
    }
}

void send_extra_buf(struct resources *res,uint32_t file_id, uint32_t file_len,int extra_id){
    memcpy(res->extra_buf[extra_id],&file_id,4);
    memcpy(res->extra_buf[extra_id]+4,&file_len,4);
    post_send(res,IBV_WR_SEND,11+extra_id,11+extra_id,0,8+file_len,688);
    poll_completion(res,11+extra_id,689);
}

void listen_recover_from_extra(struct resources *res,uint32_t fail_id,int extra_id){
    uint32_t file_type,file_id,file_len;
    bool recover_finish = false;
    std::cout<<"listen_recover_from_extra begin"<<std::endl;
    post_receive(res,11+extra_id,11+extra_id,8,891);
    while(1){
        int poll_result;
        struct ibv_wc wc;
        poll_result = ibv_poll_cq(res->extra_cq[extra_id], 1, &wc);
        if(poll_result < 0)
        {
            /* poll CQ failed */
            fprintf(stderr, "poll CQ failed\n");
            rc = 1;
        }else if(poll_result == 0){
            //
        }else{
            memcpy(&file_type,res->extra_buf[extra_id],4);
            memcpy(&file_id,res->extra_buf[extra_id]+4,4);
            std::string target_path;
            
            switch(file_type){
                
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    assert(0);
                break;
                case 6:
                    std::cout<<"extra: "<<extra_id<<"file_type is vsge,id = "<<file_id<<" "<<std::endl;
                    target_path.append("../value_sgement/value_sge_");
                    target_path.append(std::to_string(file_id));
                    //memset(res->master_buf+8,0,MAX_SGE_SIZE);
                    file_len = read_file(res->extra_buf[extra_id]+8,target_path,11);
                    //send_master_buf(res,file_id,file_len);
                break;
                case 7:
                    recover_finish = true;
                    std::cout<<"extra_recover_listener finish"<<std::endl;
                break;
                case 8:
                break;
            }
            if(recover_finish==true)
                break;
            else{
                send_extra_buf(res,file_id,file_len,extra_id);
                post_receive(res,11+extra_id,11+extra_id,8,755);
            }
        }
    }
    resources_destroy_for_extra(res);
}


void backup_recover_task(struct resources *res,uint32_t fail_id);
void master_caller(struct resources *res){
    memset(res->master_msg_buf,0,MAX_MASTER_MSG_BUF_SIZE);
    std::cout<<"master_caller begin"<<std::endl;
    char temp_char;
    if(sock_sync_data(res->master_sock, 1, "Q", &temp_char))  /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error of master after QPs are were moved to RTS\n");
        assert(0);
    }
    std::cout<<"master_caller sync finish"<<std::endl;
    while(1){
        usleep(100*1000);
        uint32_t msg_finish,msg_type,fail_num,fail_id,fail_id2;
        memcpy(&msg_finish,res->master_msg_buf+4,4);
        memcpy(&msg_type,res->master_msg_buf+8,4);
        memcpy(&fail_num,res->master_msg_buf+12,4);
        memcpy(&fail_id,res->master_msg_buf+16,4);
        memcpy(&fail_id2,res->master_msg_buf+4,4);
        uint32_t name_len,new_tcp,new_tcp2;
        char new_server_name_buf[16];
        memcpy(&name_len,res->master_msg_buf+20,4);
        memcpy(new_server_name_buf,res->master_msg_buf+24,16);
        memcpy(&new_tcp,res->master_msg_buf+40,4);
        memcpy(&new_tcp2,res->master_msg_buf+68,4);
        if(msg_finish == 1){ 
            if(msg_type!=1)
                std::cout<<"recv ret information from master,msg_type is "<<msg_type<<std::endl;
            if(msg_type==2){
                // config.server_name = new char[name_len+1];
                // for(int i=0;i<name_len;i++)
                //     config.server_name[i] = new_server_name_buf[i];
                // config.server_name[name_len] = '\0';
                std::cout<<"backup node["<<fail_id<<"] fail"<<std::endl;
                config.server_name[fail_id] = config.extra_server_name[0];
                config.tcp_port[fail_id] = new_tcp;
                std::cout<<"begin reconnect qp"<<std::endl;
                reconnect_qp(res,fail_id);
                std::cout<<"reconnect finish"<<std::endl;
                backup_recover_task(res,fail_id);
                while(1);
            }
            else if(msg_type==4){
                // config.extra_server_name[0] = new char[name_len+1];
                // for(int i=0;i<name_len;i++)
                //     config.extra_server_name[0][i] = new_server_name_buf[i];
                // config.extra_server_name[0][name_len] = '\0';
                
                config.extra_tcp_port[0] = new_tcp;
                resources_create_for_extra(res,0);
                std::cout<<"create res for extra0 finish"<<std::endl;
                connect_qp_for_extra(res,0);     
                listen_recover_from_extra(res,fail_id,0);    
                if(fail_num==2){
                    config.extra_tcp_port[1] = new_tcp2;
                    resources_create_for_extra(res,1);
                    std::cout<<"create res for extra1 finish"<<std::endl;
                    connect_qp_for_extra(res,1);
                    listen_recover_from_extra(res,fail_id,1);
                }
                //std::thread listen_from_extra_th(listen_recover_from_extra,res,fail_id2,1);
                
                
                while(1);
            }
            memset(res->master_msg_buf,0,MAX_MASTER_MSG_BUF_SIZE);
        }
        uint32_t send_flag =1;
        memcpy(res->master_msg_buf,&send_flag,4);
        //std::cout<<"tell master i am fine"<<std::endl;
        post_send_msg(res,IBV_WR_RDMA_WRITE,10,10,0,4,799);
        poll_completion_quick(res,10,800);
        
    }
}
void send_msg_buf_to_extra(struct resources *res,uint32_t file_type,uint32_t node_id,uint32_t file_id,uint32_t file_len,uint32_t fail_id){
    uint32_t msg_type = 1;
    memcpy(res->msg_buf[3],&msg_type,4);
    memcpy(res->msg_buf[3]+4,&file_type,4);
    memcpy(res->msg_buf[3]+8,&node_id,4);
    memcpy(res->msg_buf[3]+12,&file_id,4);
    memcpy(res->msg_buf[3]+16,&file_len,4);
    post_send_msg(res,IBV_WR_SEND,fail_id,3,0,20+file_len,810);
    poll_completion(res,fail_id,811);
}

void make_sst_path(std::string &target_path,uint32_t id){
    target_path = "000000.sst";
    std::string tmp2 = std::to_string(id);
    uint32_t tmp_size = tmp2.size();
    for(int l=0;l<tmp_size;l++)
        target_path[6-tmp_size+l]=tmp2[l];
}


void backup_recover_task(struct resources *res,uint32_t fail_id){
    post_send_msg(res,IBV_WR_SEND,fail_id,fail_id,0,4,1168);
    poll_completion(res,fail_id,1169);
    std::cout<<"finish invalid message channel"<<std::endl;
    uint32_t cur_recover_vsge_id=0;
    if(cur_recover_vsge_id==0)
        cur_recover_vsge_id = test_db->head_sge_id;
    res->msg_buf[3] = new char[MAX_MSG_SIZE];
    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    res->msg_mr[3] = ibv_reg_mr(res->pd, res->msg_buf[3], MAX_MSG_SIZE, mr_flags);
    uint32_t file_id,file_len;
    std::string target_path;
    target_path = "IDENTITY";
    std::cout<<"reover IDENTITY begin"<<std::endl;
    file_len = read_file(res->msg_buf[3]+20,target_path,10);
    send_msg_buf_to_extra(res,3,config.node_id,0,file_len,fail_id);    
    std::cout<<"reover IDENTITY finish"<<std::endl;
    std::cout<<"reover MANIFEST begin"<<std::endl;
    get_manipath(target_path);
    file_id = atoi(target_path.data()+9);
    file_len = read_file(res->msg_buf[3]+20,target_path,10);
    send_msg_buf_to_extra(res,4,config.node_id,file_id,file_len,fail_id); 
    std::cout<<"reover MANIFEST finish"<<std::endl;
    test_db->rocks_id_list_lock.lock(); 
    for(auto pair : test_db->rocks_id_list){
        file_id = pair.first;
        make_sst_path(target_path,file_id);
        std::cout<<"read_file : "<<target_path<<std::endl;
        file_len = read_file(res->msg_buf[3]+20,target_path,10);
        std::cout<<"read "<<target_path<<" ,len="<<file_len<<std::endl;
        send_msg_buf_to_extra(res,5,config.node_id,file_id,file_len,fail_id);  
        std::cout<<"reover "<<target_path<<" finish"<<std::endl;
    }
    test_db->rocks_id_list_lock.unlock();
    uint32_t msg_type = 2;
    memcpy(res->msg_buf[3],&msg_type,4);
    post_send_msg(res,IBV_WR_SEND,fail_id,3,0,4,888);
    poll_completion(res,fail_id,889);
    uint32_t tail_id;
    bool is_turn_tail = false;
    // while(1){
    //     std::cout<<"cur_recover vsge_id = "<<cur_recover_vsge_id<<std::endl;
    //     test_db->put_mutex.lock();
    //     tail_id = test_db->tail_sge_ptr->sge_id;
        
    //     if(cur_recover_vsge_id < tail_id)
    //         is_turn_tail = true;
    //     if(is_turn_tail){
    //         file_id = tail_id;
    //         file_len = test_db->tail_sge_ptr->cur_offset;
    //         memcpy(res->msg_buf[3]+20,test_db->tail_sge_ptr->buf.data(),file_len);
    //         send_msg_buf_to_extra(res,2,config.node_id,file_id,file_len,fail_id);  
    //         test_db->put_mutex.unlock();
    //         break;
    //     }else{
    //         test_db->put_mutex.unlock();
    //     }
    //     target_path = "value_sge_"+std::to_string(cur_recover_vsge_id);
    //     file_len = read_file(res->msg_buf[3]+20,target_path,11);
    //     std::cout<<"read "<<target_path<<" ,len="<<file_len<<std::endl;
    //     send_msg_buf_to_extra(res,6,config.node_id,cur_recover_vsge_id++,file_len,fail_id);
    //     std::cout<<"reover "<<target_path<<" finish"<<std::endl; 
    // }
    
    if(test_db->GC_MODE==1){
        cur_recover_vsge_id = test_db->head_sge_id_after_gc;
        while(1){
            std::cout<<"cur_recover vsge_id = "<<cur_recover_vsge_id<<std::endl;
            test_db->put_mutex.lock();
            tail_id = test_db->tail_sge_ptr->sge_id;
            
            if(cur_recover_vsge_id == tail_id)
                is_turn_tail = true;
            if(is_turn_tail){
                file_id = tail_id;
                file_len = test_db->tail_sge_ptr->cur_offset;
                memcpy(res->msg_buf[3]+20,test_db->tail_sge_ptr->buf.data(),file_len);
                //send_msg_buf_to_extra(res,2,config.node_id,file_id,file_len,fail_id);
                post_send_msg(res,IBV_WR_RDMA_WRITE,fail_id,3,0,file_len,1233);
                poll_completion(res,fail_id,1234);  
                test_db->put_mutex.unlock();
                break;
            }else{
                test_db->put_mutex.unlock();
            }
            target_path = "value_sge_"+std::to_string(cur_recover_vsge_id);
            file_len = read_file(res->msg_buf[3]+20,target_path,11);
            //file_len=MAX_SGE_SIZE;
            test_db->rdma_msg_lock[0].lock();
            post_send_msg(res,IBV_WR_RDMA_WRITE,fail_id,3,0,file_len,1244);
            poll_completion(res,fail_id,1245); 
            test_db->rdma_msg_lock[0].unlock();
            uint32_t flush_tail = 1;
            uint64_t none = 0;
            memcpy(res->msg_buf[3]+20,&flush_tail,4);
            memcpy(res->msg_buf[3]+20+4,&cur_recover_vsge_id,4);
            memcpy(res->msg_buf[3]+20+8,&none,8);
            test_db->rdma_msg_lock[0].lock();
            post_send_msg(res,IBV_WR_SEND,fail_id,3,20,16,1253); 
            poll_completion(res,fail_id,1254);
            post_receive_msg(res,fail_id,3,20,1255);
            poll_completion(res,fail_id,1256);
            test_db->rdma_msg_lock[0].unlock();
            memcpy(&(res->remote_props[fail_id].rkey),res->msg_buf[3],4);
            memcpy(&(res->remote_props[fail_id].addr),res->msg_buf[3]+4,8);
            //std::cout<<"read "<<target_path<<" ,len="<<file_len<<std::endl;
            //send_msg_buf_to_extra(res,6,config.node_id,cur_recover_vsge_id++,file_len,fail_id);
            //std::cout<<"reover "<<target_path<<" finish"<<std::endl; 
            cur_recover_vsge_id++;
        }
    }
    else if(test_db->GC_MODE==2){
        std::cout<<"TODO GC_MODE==2 with backup recovery"<<std::endl;
    }       
    std::cout<<"backup_recover_task finish"<<std::endl;
    test_db->backup_node_avail[fail_id] = true;    
}