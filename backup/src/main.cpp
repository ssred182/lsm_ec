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
#include <map>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <thread>
#include <assert.h>
#include "backup.h"
#include "rdma_connect.h"
#include "define.h"
#define DEBUG 1
#define SEND_INDEX 0

//1--PBR   2--EC
//#define K 4
#define GC_TRIGER_NUM 1024
#define GC_PER_NUM 512
#define KV_NUM  16*1024
#define V_LEN 4*1024

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


void master_caller(struct resources *res);
static int resources_create(struct resources *res,size_t mr_size)
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
    for(int n=0;n<K;n++){
         res->msg_buf[n] = (char *) malloc(MAX_MSG_SIZE);
         fprintf(stdout, "申请内存msg_buf for node:%d\n",n);
        if(!res->buf[n])
        {
            fprintf(stderr, "failed to malloc %Zu bytes to memory buffer for node:%d\n", size,n);
            rc = 1;
            goto resources_create_exit;
        }
        memset(res->buf[n], 0 , size);
         /* only in the server side put the message in the memory buffer */
        if(!config.server_name[n])
        {
             std::string tmp;
            tmp = "test send:server node:"+std::to_string(config.node_id)+" qp id: "+std::to_string(n)+" ";
            //strcpy(res->buf[n],SRV_MSG);
            //res->buf[n][0]='0'+n;
            for(int i=0;i<tmp.size();i++){
                res->buf[n][i] = tmp[i];
            }
            fprintf(stdout, "put the message: '%s' to buf\n", res->buf[n]);
        }
        else
        {
            memset(res->buf[n], 0, size);
        }
    }
    
    
 
   
    
    /* register the memory buffer */
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
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
    int size_c=128*1024;//MAX_KV_LEN_FROM_CLIENT
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



BackupNode backup_node;

void listen_flush_tail(struct resources *res);
void listen_flush_tail_single_qp(struct resources *res,uint32_t poll_id);
int rc = 1;

void write_file(const char *buf,uint32_t len,std::string target_path,uint32_t node_id){
    std::ofstream fout;
    std::string prex = "../rocksdb_lsm_"+std::to_string(node_id)+'/';
    prex.append(target_path);
    fout.open(prex,std::ios::out|std::ios::binary);
    fout.seekp(0,std::ios::beg);
    fout.write(buf,len);
    fout.close();
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
extern int BACKUP_MODE;
int USE_LRU=0;
int put_num=0,get_num=0;
double total_put_time=0,total_get_time=0;
std::unordered_map<std::string,std::string> read_cache;
std::mutex read_cache_lock;
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
            rc = 1;
        }else if(poll_result == 0){
            //
        }else{
            clock_t beg,end;
            double duration;
            int opcode,key_len,value_len,primary_node_id;
            beg = clock();
            memcpy(&opcode,res->client_buf[poll_id],4);
             
             
            // if(primary_node_id>=4){
            //     std::cout<<"error!! primary_node_id = "<<primary_node_id<<" ,task_id = "<<poll_id<<std::endl;
            //     primary_node_id=poll_id/8;
            // }
            //assert(primary_node_id<4);
            //std::cout<<"recv a req, op= "<<opcode<<std::endl;
            int status;
            if(opcode == 1){
               std::cout<<"error!! put operation not allowed in backup node"<<std::endl;
            }
            else if(opcode == 2){
                get_num++;
                static bool is_first_get=true;
                memcpy(&primary_node_id,res->client_buf[poll_id]+4,4);
                memcpy(&key_len,res->client_buf[poll_id]+8,4);
                std::string key(res->client_buf[poll_id]+12,key_len);
                std::string value;
                int value_len=0;
                // if(BACKUP_MODE==2)
                //     std::cout<<"begin deal sub read "<<std::endl;
                std::string meta_value;
                if(BACKUP_MODE==1){
                    auto ret = backup_node.db_list[primary_node_id].rocksdb_ptr->Get(rocksdb::ReadOptions(),key,&meta_value);
                    status = ret.ok()?0:1;
                    uint32_t sge_id = *((uint32_t*)meta_value.data());
                    uint32_t offset_of_sgement = *( (uint32_t*)(meta_value.data()+4 ) );
                    if(status==0){
                        if(sge_id==backup_node.db_list[primary_node_id].tail_sge_ptr->sge_id){
                            backup_node.db_list[primary_node_id].tail_sge_ptr->get_kv(offset_of_sgement,key,value);
                        }
                        else{
                            NAM_SGE::read_kv_from_sge_for_backup(primary_node_id,sge_id,offset_of_sgement,key,value);                           
                        }      
                    }

                }else if(BACKUP_MODE==2 && USE_LRU==1 || read_cache.size()>0){
                    read_cache_lock.lock();
                    auto iter=read_cache.find(key);
                    if(iter==read_cache.end()){
                        status=1;
                    }else{
                        status=0;
                        value=iter->second;
                    }
                    read_cache_lock.unlock();
                }else if(BACKUP_MODE==2 && USE_LRU==0){
                    //sub read
                    auto ret = backup_node.db_list[primary_node_id].rocksdb_ptr->Get(rocksdb::ReadOptions(),key,&value);
                    status = ret.ok()?2:1;
                }else{
                    std::cout<<"BACKUP_MODE ="<<BACKUP_MODE<<" USE_LRU ="<<USE_LRU<<std::endl;
                    assert(0);
                }
                value_len = value.size();
                memcpy(res->client_buf[poll_id],&status,4);
                memcpy(res->client_buf[poll_id]+4,&value_len,4);
                memcpy(res->client_buf[poll_id]+8,value.data(),value_len); 
                post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,8+value_len,780);
                poll_completion_client(res,poll_id,781);
                // if(BACKUP_MODE==2)
                //     std::cout<<"search LSM finish"<<std::endl;
                
                // if(!ret.ok())
                //     fail_count++;
                // if(fail_count>0 && fail_count%100==0)
                //     std::cout<<"sub read in backup node fail **&&$$##"<<std::endl;
                

                // value.clear();
                // uint32_t value_len;
                //     if(status==0){
                //         if(sge_id==backup_node.db_list[primary_node_id].tail_sge_ptr->sge_id){
                //             backup_node.db_list[primary_node_id].tail_sge_ptr->get_kv(offset_of_sgement,key,value);
                //         }
                //         else{
                //             if(BACKUP_MODE==1){
                //                 NAM_SGE::read_kv_from_sge_for_backup(primary_node_id,sge_id,offset_of_sgement,key,value);
                //                 value_len = value.size();
                //                 memcpy(res->client_buf[poll_id],&status,4);
                //                 memcpy(res->client_buf[poll_id]+4,&value_len,4);
                //                 memcpy(res->client_buf[poll_id]+8,value.data(),value_len);
                //             }
                //             else{//BACKUP_MODE==2
                //                 //status=2 means return position information
                //                 // status=2;
                //                 // value.append((const char*)(&sge_id),4);
                //                 // value.append((const char*)(&offset_of_sgement),4);
                //                 std::string &value = read_cache[key];
                //                 value_len = value.size();
                //                 memcpy(res->client_buf[poll_id],&status,4);
                //                 memcpy(res->client_buf[poll_id]+4,&value_len,4);
                //                 memcpy(res->client_buf[poll_id]+8,value.data(),value_len);
                //             }
                //         }
                //     }
                    
                // end = clock();
                // post_send_client(res,IBV_WR_SEND,poll_id,poll_id,0,8+value_len,780);
                // poll_completion_client(res,poll_id,781);
                // if(is_first_get){
                //     std::cout<<"first deal get ^^^^^^^^"<<std::endl;
                //     std::cout<<"key = "<<key<<" node="<<primary_node_id<<std::endl;
                //     std::cout<<"status = "<<status<<"sge_id ="<<sge_id<<" offset_of_segment="<<offset_of_sgement<<std::endl;
                //     std::cout<<"value = "<<value<<std::endl;
                //     is_first_get=false;
                // }
                // duration = (double)(end - beg)/CLOCKS_PER_SEC;
                // total_get_time += duration;
                // if(get_num>0&&get_num%100000==0)
                //     std::cout<<"avg_deal_get_time="<<total_get_time/get_num<<std::endl;
            }
            else if(opcode == 3){
                //evict a set of key
                int key_num;
                memcpy(&key_num,res->client_buf[poll_id]+4,4);
                //std::cout<<"evict a set of key ,num = "<<key_num<<std::endl;
                int cur_offset=8;
                int not_exist_count=0;
                read_cache_lock.lock();
                for(int i=0;i<key_num;++i){
                    int key_len;
                    memcpy(&key_len,res->client_buf[poll_id]+cur_offset,4);
                    cur_offset+=4;
                    std::string key(res->client_buf[poll_id]+cur_offset,key_len);
                    cur_offset+=key_len;
                    auto iter=read_cache.find(key);
                    if(iter!=read_cache.end())
                        read_cache.erase(iter);
                    else{
                        //std::cout<<"evict not exist, key="<<key<<std::endl;
                        not_exist_count++;
                    }
                }
                read_cache_lock.unlock();
                //std::cout<<"evict finish ,not_exist_count="<<not_exist_count<<" now map has element count="<<read_cache.size()<<std::endl;
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
    if(BACKUP_MODE==2)
        read_cache.reserve(500*1024);
    struct resources res;
    uint32_t max_sge_size = MAX_SGE_SIZE;
    
    config.dev_name = NULL;
    config.server_name[0]  = NULL;
    config.server_name[1]  = NULL;
    config.server_name[2]  = NULL;
    config.server_name[3]  = NULL;
    config.tcp_port[0] = 19875;
    config.master_tcp_port = 19991;
    config.client_tcp_port = 19904;
    config.master_server_name = "10.118.0.53";
    config.extra_server_name[0] = "10.118.0.53";
    config.extra_server_name[1] = "10.118.0.53";
    config.ib_port = 1;
    config.gid_idx = 0;
    config.node_id = 0;
    BACKUP_MODE = 2;
    

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
            {.name = "client", .has_arg = 1, .val = 'c' },
            {.name = "backup_mode", .has_arg = 1, .val = 'b' },
            {.name = "master", .has_arg = 1, .val = 'm' },
            {.name = "extra", .has_arg = 1, .val = 'e' },
            {.name = "extra2", .has_arg = 1, .val = 'E' },
            {.name = "lru", .has_arg = 1, .val = 'l' },
            {.name = NULL, .has_arg = 0, .val = '\0'}
        };
        /*关于参数设置，gid必须两端都一样, ib-port必须两端等于0
        */
        c = getopt_long(argc, argv, "p:d:i:g:n:c:m:e:E:b:sl", long_options, NULL);
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
            config.server_name[0] = strdup(optarg);
            K = 2;
            break;
        case 's':
            K = 4;
            std::cout<<"is bakcup node"<<std::endl;
            config.server_name[0] = NULL;
            config.server_name[1] = NULL;
            break;
        case 'b':
            BACKUP_MODE = strtoul(optarg, NULL, 0);
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
        case 'l':
            USE_LRU=1;
        break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    std::cout<<"BACKUP_MODE="<<BACKUP_MODE<<std::endl;
    std::string whether_send_index;
    if(SEND_INDEX)
        whether_send_index="is send index";
    else
        whether_send_index="no send inedx";
    std::cout<<whether_send_index<<std::endl;
 
    /* print the used parameters for info*/
    
    /* init all of the resources, so cleanup will be easy */
    resources_init(&res);
    config.client_tcp_port+=config.node_id;
    print_config();
    //initilize backup_node before create res to prepare sge for buf(MR)
    backup_node.init(config.node_id);
    for(int i=0;i<K;i++){
        res.buf[i]=backup_node.db_list[i].tail_sge_ptr->buf.data();
    }
    /* create resources before using them */
    
    if(resources_create(&res,max_sge_size))
    {
        fprintf(stderr, "failed to create resources\n");
        exit(&res);
        return 1;
    }
   
    std::cout<<"finish res create"<<std::endl;
    /* connect the QPs */
    if(connect_qp(&res))
    {
        fprintf(stderr, "failed to connect QPs\n");
        exit(&res);
        assert(0);
    }
    std::cout <<"finish connect qp"<<std::endl;
    /* let the server post the sr */
    if(config.server_name[0])
	{
        for(int n=0;n<K;n++){
            if(post_send(&res, IBV_WR_SEND,n,n,0,64,611))
            {
                fprintf(stderr, "failed to post sr\n");
                exit(&res);
                assert(0);
            }
        }
        
	}
    for(int n=0;n<K;n++){
        if(poll_completion(&res,n,622))
        {
            fprintf(stderr, "poll completion failed\n");
            exit(&res);
            return 1;
        }
        write_file(res.buf[n],37,"IDENTITY",n);
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
        exit(&res);
        return 1;
    }
    /* in both sides we expect to get a completion */
    
    if(poll_completion(&res,10,630))
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
    // if(BACKUP_MODE==1){
        for(int i=0;i<MAX_QP_NUM;i++){
            std::thread listen_client_th(listen_client_task,&res,i);
            listen_client_th.detach();
        }
    //}
    



    //建立连接的过程结束
    listen_flush_tail(&res);
    // std::thread listen_th0(listen_flush_tail_single_qp,&res,0);
    // std::thread listen_th1(listen_flush_tail_single_qp,&res,1);
    // std::thread listen_th2(listen_flush_tail_single_qp,&res,2);
    // listen_flush_tail_single_qp(&res,3);
    
 
    return rc;
}
void remove_rocks_file(std::string &target_path,uint32_t node_id){
    std::string prex = "../rocksdb_lsm_"+std::to_string(node_id)+'/';
    prex.append(target_path);
    std::remove((const char*)prex.c_str());
}
uint32_t read_file(const char *buf,std::string target_path,uint32_t node_id){
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
void make_sst_path(std::string &target_path,uint32_t id){
    target_path = "000000.sst";
    std::string tmp2 = std::to_string(id);
    uint32_t tmp_size = tmp2.size();
    for(int l=0;l<tmp_size;l++)
        target_path[6-tmp_size+l]=tmp2[l];
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
uint32_t syn_gc_finish_id[4];
uint32_t syn_gc_begin_id[4];
bool is_gc_list[4];
bool is_gc = false;

std::map<uint32_t,uint32_t> current_sst_id_list[4];
static double gc_rdma_time=0;
static double update_parity_time=0;
static double update_parity_io_time=0;
static int update_parity_num=0;
void update_parity_task(int poll_id,int segment_id,struct resources *res){
    clock_t beg,end;
    double duration;
    std::string path;
    update_parity_num++;
    path.append("../parity_sgement/parity_sge_");
    path.append(std::to_string(segment_id));
    beg=clock();
    uint8_t *read_buf = new uint8_t[16*1024*1024];
    std::ifstream fin;
    //std::cout<<"read file"<<std::endl;
    fin.open(path,std::ios::in|std::ios::binary);
    fin.seekg(0,std::ios::beg);
    fin.read((char*)read_buf,16*1024*1024);
    fin.close();
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    update_parity_io_time+=duration;
    beg = clock();
    backup_node.encoder.encode_data_update(backup_node.parity_node_id,(uint8_t *)res->msg_buf[poll_id],poll_id,read_buf);
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    update_parity_time+=duration;
                
    std::ofstream fout;
    beg=clock();
    //std::cout<<"write file"<<std::endl;
    fout.open(path,std::ios::out|std::ios::binary);
    fout.seekp(0,std::ios::beg);
    fout.write((const char*)read_buf,16*1024*1024);
    
    fout.close();
    delete read_buf;
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    update_parity_io_time+=duration;
    if(update_parity_num%40==0){
        std::cout<<"update parity time = "<<update_parity_time<<" ,io time="<<update_parity_io_time<<" ,gc_rdma_time="<<gc_rdma_time<<" ,update_parity_num="<<update_parity_num<<std::endl;
        int max_id = syn_gc_finish_id[0];
        int min_id = syn_gc_finish_id[0];
        for(int i=1;i<4;i++){
            if(max_id<syn_gc_finish_id[i])
                max_id = syn_gc_finish_id[i];
            if(min_id>syn_gc_finish_id[i])
                min_id = syn_gc_finish_id[i];
        }
        //std::cout<<"fragement stripe num = "<<max_id-min_id <<" ,total parity_sgement="<<backup_node.parity_sge_num<<" ,gc_rdma_time="<<gc_rdma_time<<std::endl;
    }
}

std::map<uint32_t,uint32_t> parity_update_map;
void listen_flush_tail(struct resources *res){
    
    double total_flush_tail_time[K] = {0},total_send_index_time[K] = {0};
    
    //map<id,num_has_gc>
    //segemnt gc num for every stripe

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
            rc = 1;
        }else if(poll_result == 0){
            //
        }else{
            if(wc.status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", 
                        wc.status, wc.vendor_err);
                rc = 1;
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
                    if(BACKUP_MODE==2&&backup_node.parity_sge_num>GC_TRIGER_NUM){
                        if(backup_node.syn_gc_lock.try_lock()){
                            #if DEBUG
                            std::cout<<"trigger gc"<<std::endl;
                            #endif
                            syn_gc_end_id = backup_node.min_parity_sge_id + GC_PER_NUM-1; 
                            is_gc = true;
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
                if(is_gc == true && is_gc_list[poll_id] == false){
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
                
                
                // if(BACKUP_MODE==1){
                //     backup_node.db_list[poll_id].rocksdb_ptr->Close();
                //     std::cout<<"close rocksdb"<<std::endl;
                //     rocksdb::DB *dbptr;
                //     rocksdb::Options options;
                //     options.compression = rocksdb::kNoCompression;
                //     options.max_background_flushes = 1;
                //     options.max_background_compactions = 1;
                //     options.create_if_missing = true;
                //     std::string rocks_path = "../rocksdb_lsm_"+std::to_string(poll_id);
                //     std::cout<<"open rocksdb for backupdb node_id: "<<poll_id<<"path= "<<rocks_path<<std::endl;
                //     rocksdb::DB::Open(options, rocks_path, &dbptr);
                //     assert(dbptr != nullptr);
                //     backup_node.db_list[poll_id].rocksdb_ptr = std::shared_ptr<rocksdb::DB>(dbptr);
                // }
                end = clock();
                duration = (double)(end - beg)/CLOCKS_PER_SEC;
                total_send_index_time[poll_id] += duration;
                std::cout<<"total_send_index_time["<<poll_id<<"] = "<<total_send_index_time[poll_id]<<std::endl;
                
            }else if(msg_type==3){
                //syn-gc message
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
                    //std::cout<<"node: "<<poll_id<<" begin_id="<<syn_gc_begin_id[poll_id]<<" ,++"<<std::endl;
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
                        is_gc = false;
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
                //gc message and remove file
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
            else if(msg_type==5){
                //gc-message and parity update
                
                
                
                clock_t beg,end;
                uint32_t remove_num,remove_beg_id;
                
                memcpy(&remove_beg_id,res->msg_buf[poll_id]+4,4);
                memcpy(&remove_num,res->msg_buf[poll_id]+8,4);
                {
                    clock_t beg,end;
                    beg = clock();
                    post_receive_msg(res,poll_id,poll_id,16*1024*1024,984);
                    poll_completion(res,poll_id,985);
                    end = clock();
                    duration = (double)(end - beg)/CLOCKS_PER_SEC;
                    gc_rdma_time +=duration;
                }
                //std::cout<<"node: "<<poll_id <<" msg_type==5    remove_id="<<remove_beg_id<<std::endl;
                // syn_gc_finish_id[poll_id]+=remove_num;
                // std::string path;
                // for(int i=0;i<remove_num;i++){
                //     clock_t beg,end;
                //     double duration;
                //     if((syn_gc_finish_id[0]<remove_beg_id+i) || (syn_gc_finish_id[1]<remove_beg_id+i)||(syn_gc_finish_id[2]<remove_beg_id+i)||(syn_gc_finish_id[3]<remove_beg_id+i)){
                //         std::thread update_parity_th(update_parity_task,poll_id,remove_beg_id+i,res);
                //         update_parity_th.detach();
                //     }
                //     else{
                //         backup_node.remove_sge(remove_beg_id+i);
                //     }    
                // }
                
                std::string path;
                bool parity_still_valid=true;
                if(parity_update_map.count(remove_beg_id)>0 )
                    if(++parity_update_map[remove_beg_id]>=4)
                        parity_still_valid=false;
                else//不存在就插入
                    parity_update_map[remove_beg_id]=1;
                
                if(parity_still_valid){
                    std::thread update_parity_th(update_parity_task,poll_id,remove_beg_id,res);
                    update_parity_th.detach();
                }
                else{
                    backup_node.remove_sge(remove_beg_id);
                }    
                
                //std::cout<<"finish msg_type=5"<<std::endl;
            }else if(msg_type==7){
                //insert new kv pairs into map
               
                int key_num;
                memcpy(&key_num,res->msg_buf[poll_id]+4,4);
                post_receive_msg(res,poll_id,poll_id,MAX_MSG_SIZE,1316);
                poll_completion(res,poll_id,1317);
                //std::cout<<"new push kvs into map,key num= "<<key_num<<std::endl;
                int cur_offset=0;
                for(int i=0;i<key_num;++i){
                    int key_len;
                    //std::cout<<"cur_put num = "<<i<<std::endl;
                    memcpy(&key_len,res->msg_buf[poll_id]+cur_offset,4);
                    cur_offset+=4;
                    std::string key(res->msg_buf[poll_id]+cur_offset,key_len);
                    cur_offset+=key_len;
                    //std::cout<<"debug p1"<<std::endl;
                    int value_len;
                    memcpy(&value_len,res->msg_buf[poll_id]+cur_offset,4);
                    cur_offset+=4;
                    //std::string value(res->msg_buf[poll_id]+cur_offset,value_len);
                   
                    //read_cache[key]=value;
                    read_cache_lock.lock();
                    read_cache[key]=std::string(res->msg_buf[poll_id]+cur_offset,value_len);
                    read_cache_lock.unlock();
                    cur_offset+=value_len;
                }
                //std::cout<<"put finish, now elements num = "<<read_cache.size()<<std::endl;
            }
            else{

                std::cout<<"^^^^^error!!!!!!&&&&&&&&msg_type error********"<<std::endl;
                std::cout<<"msg_type= "<<msg_type<<" from node:"<<poll_id<<std::endl;
                assert(0);
            }
            post_receive_msg(res,poll_id,poll_id,16,922);
        }
        poll_id++;
        if(poll_id >= K)
            poll_id = 0;
    }
}


void send_master_buf(struct resources *res,uint32_t file_id, uint32_t file_len){
    memcpy(res->master_buf,&file_id,4);
    memcpy(res->master_buf+4,&file_len,4);
    post_send(res,IBV_WR_SEND,10,10,0,8+file_len,934);
    poll_completion(res,10,935);
}
void listen_recover_from_master(struct resources *res,uint32_t fail_id){
    uint32_t file_type,file_id,file_len;
     std::cout<<"listen_recover_from_master begin"<<std::endl;
    bool recover_finish = false;
    post_receive(res,10,10,8,940);
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
            std::ifstream fin;
            std::string key;
            uint32_t min_parity_id;
            uint32_t max_parity_id;
            uint32_t max_vsge_id;
            uint32_t gc_num;;
            std::cout<<"request file type= "<<file_type<<" ,file_id= "<<file_id<<std::endl;
            switch(file_type){
                case 1:
                    min_parity_id = syn_gc_finish_id[fail_id] + 1;
                    max_parity_id = backup_node.max_parity_sge_id;
                    max_vsge_id = backup_node.db_list[fail_id].tail_sge_ptr->sge_id-1;
                   
                    if(is_gc) 
                        gc_num = syn_gc_begin_id[fail_id]-syn_gc_finish_id[fail_id];
                    else
                        gc_num = 0;
                    memcpy(res->master_buf+8,&min_parity_id,4);
                    memcpy(res->master_buf+12,&max_parity_id,4);
                    memcpy(res->master_buf+16,&max_vsge_id,4);
                    memcpy(res->master_buf+20,&gc_num,4);
                    //send_master_buf(res,0,12);
                    file_id = 0;
                    file_len = 16;
                break;
                case 2:
                    file_id = backup_node.db_list[fail_id].tail_sge_ptr->sge_id;
                    //file_len = backup_node.db_list[fail_id].tail_sge_ptr->buf.size();
                    for(int offset = 0;offset<MAX_SGE_SIZE;){
                        uint32_t key_len;
                        memcpy(&key_len,backup_node.db_list[fail_id].tail_sge_ptr->buf.data()+offset,4);
                        if(key_len == 0){
                            file_len = offset;
                            break;
                        }
                        uint32_t new_offset = backup_node.db_list[fail_id].tail_sge_ptr->get_kv_for_build(offset,key);
                        offset = new_offset;
                    }
                    memcpy(res->master_buf+8,backup_node.db_list[fail_id].tail_sge_ptr->buf.data(),file_len);
                    //send_master_buf(res,backup_node.db_list[fail_id].tail_sge_ptr->sge_id,);
                break;
                case 3:
                    target_path = "IDENTITY";
                    file_len = read_file(res->master_buf+8,target_path,fail_id);
                    file_id = 0;
                    //send_master_buf(res,0,file_len);
                break;
                case 4:
                    file_id = backup_node.db_list[fail_id].current_mani_id;
                    make_mani_path(target_path,file_id);
                    file_len = read_file(res->master_buf+8,target_path,fail_id);
                    //send_master_buf(res,file_id,file_len);
                break;
                case 5:
                    
                    make_sst_path(target_path,file_id);
                    file_len = read_file(res->master_buf+8,target_path,fail_id);
                    //assert(file_len==MAX_SGE_SIZE);
                    //send_master_buf(res,file_id,file_len);
                break;
                case 6:
                    if(file_id<=backup_node.max_parity_sge_id){
                        target_path ="parity_sge_"+std::to_string(file_id);
                        file_len = read_file(res->master_buf+8,target_path,12);
                        assert(file_len==MAX_SGE_SIZE);
                        //send_master_buf(res,file_id,file_len);
                    }
                    else{
                        target_path.append("../value_sgement/value_sge_");
                        target_path.append(std::to_string(fail_id));
                        target_path.append("_");
                        target_path.append(std::to_string(file_id));
                        
                        fin.open(target_path,std::ios::in|std::ios::binary);
                        if(fin.is_open()== true){
                            fin.seekg(0,std::ios::end);
                            file_len = fin.tellg();
                            assert(file_len==MAX_SGE_SIZE);
                            fin.seekg(0,std::ios::beg);
                            fin.read(res->master_buf+8,file_len);
                            //send_master_buf(res,file_id,file_len);
                        }else{                           
                            
                            int target_position = -1;
                            for(int i=0;i<MAX_MEM_SGE;i++){
                                if(backup_node.db_list[fail_id].in_mem_sges[i].use_count()==0)
                                    continue;
                                if(backup_node.db_list[fail_id].in_mem_sges[i]->sge_id == file_id  ){
                                    target_position = i;
                                }
                            }
                            assert(target_position>0);
                            file_len = backup_node.db_list[fail_id].in_mem_sges[target_position]->buf.size();
                            assert(file_len==MAX_SGE_SIZE);
                            memcpy(res->master_buf+8,backup_node.db_list[fail_id].in_mem_sges[target_position]->buf.data(),file_len);
                            //send_master_buf(res,file_id,file_len);
                        }
                    }
                break;
                case 7:
                    recover_finish = true;
                break;
                case 8:
                    file_len = 4*current_sst_id_list[fail_id].size();
                    int num = 0;
                    uint32_t *id_array = (uint32_t *)(res->master_buf+8);
                    for(auto pair : current_sst_id_list[fail_id]){
                        //int sst_id = pair.first;
                        //memcpy(res->master_buf+8+4*num++,&sst_id,4);
                        id_array[num++] = pair.first;
                    }
                    file_id = 0;
                break;
            }
            if(recover_finish==true)
                break;
            else{
                send_master_buf(res,file_id,file_len);
                post_receive(res,10,10,8,1058);
            }
        }
    }
    std::cout<<"listen from master finish"<<std::endl;
}



void send_extra_buf(struct resources *res,uint32_t file_id, uint32_t file_len,int extra_id){
    memcpy(res->extra_buf[extra_id],&file_id,4);
    memcpy(res->extra_buf[extra_id]+4,&file_len,4);
    post_send(res,IBV_WR_SEND,11+extra_id,11+extra_id,0,8+file_len,688);
    poll_completion(res,11+extra_id,689);
}
std::mutex listen_recover_lock;
void listen_recover_from_extra(struct resources *res,uint32_t fail_id,int extra_id){
    uint32_t file_type,file_id,file_len;
    bool recover_finish = false;
    std::cout<<"listen_recover_from_extra begin"<<std::endl;
    if(extra_id<=2){
        listen_recover_lock.lock();
        post_receive(res,11+extra_id,11+extra_id,8,891);
        listen_recover_lock.unlock();
        while(1){
            int poll_result;
            struct ibv_wc wc;
            listen_recover_lock.lock();
            poll_result = ibv_poll_cq(res->extra_cq[extra_id], 1, &wc);
            listen_recover_lock.unlock();
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
                std::ifstream fin;
                std::string key;
                uint32_t min_parity_id;
                uint32_t max_parity_id;
                uint32_t max_vsge_id,min_vsge_id;
                uint32_t gc_num;;
                std::cout<<"extra: "<<extra_id<<" request file type= "<<file_type<<" ,file_id= "<<file_id<<std::endl;
                switch(file_type){
                    case 1:
                        min_parity_id = syn_gc_finish_id[fail_id] + 1;
                        max_parity_id = backup_node.max_parity_sge_id;
                        if(BACKUP_MODE==1)
                            min_vsge_id = syn_gc_finish_id[fail_id] + 1;
                        else
                            min_vsge_id = max_parity_id+1;
                        max_vsge_id = backup_node.db_list[fail_id].tail_sge_ptr->sge_id-1;
                    
                        if(is_gc) 
                            gc_num = syn_gc_begin_id[fail_id]-syn_gc_finish_id[fail_id];
                        else
                            gc_num = 0;
                        memcpy(res->extra_buf[extra_id]+8,&min_parity_id,4);
                        memcpy(res->extra_buf[extra_id]+12,&max_parity_id,4);
                        memcpy(res->extra_buf[extra_id]+16,&min_vsge_id,4);
                        memcpy(res->extra_buf[extra_id]+20,&max_vsge_id,4);
                        memcpy(res->extra_buf[extra_id]+24,&gc_num,4);
                        std::cout<<"min_parity_id ="<<min_parity_id<<" ,max_parity_id="<<max_parity_id<<"min_vsge_id ="<<min_vsge_id<<" ,max_vsge_id= "<<max_vsge_id<<" ,recover_gc_num="<<gc_num<<std::endl;
                        //send_master_buf(res,0,12);
                        file_id = 0;
                        file_len = 20;
                    break;
                    case 2:
                        file_id = backup_node.db_list[fail_id].tail_sge_ptr->sge_id;
                        //file_len = backup_node.db_list[fail_id].tail_sge_ptr->buf.size();
                        for(int offset = 0;offset<MAX_SGE_SIZE;){
                            uint32_t key_len;
                            memcpy(&key_len,backup_node.db_list[fail_id].tail_sge_ptr->buf.data()+offset,4);
                            if(key_len == 0){
                                file_len = offset;
                                break;
                            }
                            uint32_t new_offset = backup_node.db_list[fail_id].tail_sge_ptr->get_kv_for_build(offset,key);
                            offset = new_offset;
                        }
                        memcpy(res->extra_buf[extra_id]+8,backup_node.db_list[fail_id].tail_sge_ptr->buf.data(),file_len);
                        //send_master_buf(res,backup_node.db_list[fail_id].tail_sge_ptr->sge_id,);
                    break;
                    case 3:
                        target_path = "IDENTITY";
                        file_len = read_file(res->extra_buf[extra_id]+8,target_path,fail_id);
                        file_id = 0;
                        //send_master_buf(res,0,file_len);
                    break;
                    case 4:
                        file_id = backup_node.db_list[fail_id].current_mani_id;
                        make_mani_path(target_path,file_id);
                        file_len = read_file(res->extra_buf[extra_id]+8,target_path,fail_id);
                        //send_master_buf(res,file_id,file_len);
                    break;
                    case 5:
                        
                        make_sst_path(target_path,file_id);
                        file_len = read_file(res->extra_buf[extra_id]+8,target_path,fail_id);
                        std::cout<<"sst_id="<<file_id<<" ,len="<<file_len<<std::endl;
                        //assert(file_len==MAX_SGE_SIZE);
                        //send_master_buf(res,file_id,file_len);
                    break;
                    case 6:
                        if(BACKUP_MODE==2){
                            if(file_id<=backup_node.max_parity_sge_id){
                                target_path ="parity_sge_"+std::to_string(file_id);
                                file_len = read_file(res->extra_buf[extra_id]+8,target_path,12);
                                assert(file_len==MAX_SGE_SIZE);
                                //send_master_buf(res,file_id,file_len);
                            }
                            else{
                                target_path.append("../value_sgement/value_sge_");
                                target_path.append(std::to_string(fail_id));
                                target_path.append("_");
                                target_path.append(std::to_string(file_id));
                                
                                fin.open(target_path,std::ios::in|std::ios::binary);
                                if(fin.is_open()== true){
                                    fin.seekg(0,std::ios::end);
                                    file_len = fin.tellg();
                                    assert(file_len==MAX_SGE_SIZE);
                                    fin.seekg(0,std::ios::beg);
                                    fin.read(res->extra_buf[extra_id]+8,file_len);
                                    //send_master_buf(res,file_id,file_len);
                                }else{                           
                                    
                                    int target_position = -1;
                                    for(int i=0;i<MAX_MEM_SGE;i++){
                                        if(backup_node.db_list[fail_id].in_mem_sges[i].use_count()==0)
                                            continue;
                                        if(backup_node.db_list[fail_id].in_mem_sges[i]->sge_id == file_id  ){
                                            target_position = i;
                                            break;
                                        }
                                    }
                                    assert(target_position>=0);
                                    file_len = backup_node.db_list[fail_id].in_mem_sges[target_position]->buf.size();
                                    assert(file_len==MAX_SGE_SIZE);
                                    memcpy(res->extra_buf[extra_id]+8,backup_node.db_list[fail_id].in_mem_sges[target_position]->buf.data(),file_len);
                                    //send_master_buf(res,file_id,file_len);
                                }
                            }
                        }
                        else if(BACKUP_MODE==1){
                            target_path.append("../value_sgement/value_sge_");
                                target_path.append(std::to_string(fail_id));
                                target_path.append("_");
                                target_path.append(std::to_string(file_id));
                                
                                fin.open(target_path,std::ios::in|std::ios::binary);
                                if(fin.is_open()== true){
                                    fin.seekg(0,std::ios::end);
                                    file_len = fin.tellg();
                                    assert(file_len==MAX_SGE_SIZE);
                                    fin.seekg(0,std::ios::beg);
                                    fin.read(res->extra_buf[extra_id]+8,file_len);
                                    //send_master_buf(res,file_id,file_len);
                                }else{
                                    file_id=0;
                                    file_len=0;
                                }               
                        }
                    break;
                    case 7:
                        recover_finish = true;
                    break;
                    case 8:
                        file_len = 4*current_sst_id_list[fail_id].size();
                        int num = 0;
                        uint32_t *id_array = (uint32_t *)(res->extra_buf[extra_id]+8);
                        for(auto pair : current_sst_id_list[fail_id]){
                            //int sst_id = pair.first;
                            //memcpy(res->master_buf+8+4*num++,&sst_id,4);
                            id_array[num++] = pair.first;
                        }
                        file_id = 0;
                    break;
                }
                if(recover_finish==true)
                    break;
                else{
                    listen_recover_lock.lock();
                    send_extra_buf(res,file_id,file_len,extra_id);
                    post_receive(res,11+extra_id,11+extra_id,8,755);
                    listen_recover_lock.unlock();
                }
            }
        }
    }
    // else{
    //     bool finish_list[2] = {false,false};
    //     post_receive(res,11,11,8,891);
    //     post_receive(res,12,12,8,891);
    //     while(1){
    //         int poll_result;
    //         int poll_id =0;
    //         struct ibv_wc wc;
    //         poll_result = ibv_poll_cq(res->extra_cq[poll_id], 1, &wc);
    //         if(poll_result < 0)
    //         {
    //             /* poll CQ failed */
    //             fprintf(stderr, "poll CQ failed\n");
    //             rc = 1;
    //         }else if(poll_result == 0){
    //             //
    //         }else{
    //             memcpy(&file_type,res->extra_buf[poll_id],4);
    //             memcpy(&file_id,res->extra_buf[poll_id]+4,4);
    //             std::string target_path;
    //             std::ifstream fin;
    //             std::string key;
    //             uint32_t min_parity_id;
    //             uint32_t max_parity_id;
    //             uint32_t max_vsge_id;
    //             uint32_t gc_num;;
    //             std::cout<<"extra: "<<poll_id<<" request file type= "<<file_type<<" ,file_id= "<<file_id<<std::endl;
    //             switch(file_type){
    //                 case 1:
    //                     min_parity_id = syn_gc_finish_id[fail_id] + 1;
    //                     max_parity_id = backup_node.max_parity_sge_id;
    //                     max_vsge_id = backup_node.db_list[fail_id].tail_sge_ptr->sge_id-1;
                    
    //                     if(is_gc) 
    //                         gc_num = syn_gc_begin_id[fail_id]-syn_gc_finish_id[fail_id];
    //                     else
    //                         gc_num = 0;
    //                     memcpy(res->extra_buf[poll_id]+8,&min_parity_id,4);
    //                     memcpy(res->extra_buf[poll_id]+12,&max_parity_id,4);
    //                     memcpy(res->extra_buf[poll_id]+16,&max_vsge_id,4);
    //                     memcpy(res->extra_buf[poll_id]+20,&gc_num,4);
    //                     //send_master_buf(res,0,12);
    //                     file_id = 0;
    //                     file_len = 16;
    //                 break;
    //                 case 2:
    //                     file_id = backup_node.db_list[fail_id].tail_sge_ptr->sge_id;
    //                     //file_len = backup_node.db_list[fail_id].tail_sge_ptr->buf.size();
    //                     for(int offset = 0;offset<MAX_SGE_SIZE;){
    //                         uint32_t key_len;
    //                         memcpy(&key_len,backup_node.db_list[fail_id].tail_sge_ptr->buf.data()+offset,4);
    //                         if(key_len == 0){
    //                             file_len = offset;
    //                             break;
    //                         }
    //                         uint32_t new_offset = backup_node.db_list[fail_id].tail_sge_ptr->get_kv_for_build(offset,key);
    //                         offset = new_offset;
    //                     }
    //                     memcpy(res->extra_buf[poll_id]+8,backup_node.db_list[fail_id].tail_sge_ptr->buf.data(),file_len);
    //                     //send_master_buf(res,backup_node.db_list[fail_id].tail_sge_ptr->sge_id,);
    //                 break;
    //                 case 3:
    //                     target_path = "IDENTITY";
    //                     file_len = read_file(res->extra_buf[poll_id]+8,target_path,fail_id);
    //                     file_id = 0;
    //                     //send_master_buf(res,0,file_len);
    //                 break;
    //                 case 4:
    //                     file_id = backup_node.db_list[fail_id].current_mani_id;
    //                     make_mani_path(target_path,file_id);
    //                     file_len = read_file(res->extra_buf[poll_id]+8,target_path,fail_id);
    //                     //send_master_buf(res,file_id,file_len);
    //                 break;
    //                 case 5:
                        
    //                     make_sst_path(target_path,file_id);
    //                     file_len = read_file(res->extra_buf[poll_id]+8,target_path,fail_id);
    //                     //assert(file_len==MAX_SGE_SIZE);
    //                     //send_master_buf(res,file_id,file_len);
    //                 break;
    //                 case 6:
    //                     if(file_id<=backup_node.max_parity_sge_id){
    //                         target_path ="parity_sge_"+std::to_string(file_id);
    //                         file_len = read_file(res->extra_buf[poll_id]+8,target_path,12);
    //                         assert(file_len==MAX_SGE_SIZE);
    //                         //send_master_buf(res,file_id,file_len);
    //                     }
    //                     else{
    //                         target_path.append("../value_sgement/value_sge_");
    //                         target_path.append(std::to_string(fail_id));
    //                         target_path.append("_");
    //                         target_path.append(std::to_string(file_id));
                            
    //                         fin.open(target_path,std::ios::in|std::ios::binary);
    //                         if(fin.is_open()== true){
    //                             fin.seekg(0,std::ios::end);
    //                             file_len = fin.tellg();
    //                             assert(file_len==MAX_SGE_SIZE);
    //                             fin.seekg(0,std::ios::beg);
    //                             fin.read(res->extra_buf[poll_id]+8,file_len);
    //                             //send_master_buf(res,file_id,file_len);
    //                         }else{                           
                                
    //                             int target_position = -1;
    //                             for(int i=0;i<MAX_MEM_SGE;i++){
    //                                 if(backup_node.db_list[fail_id].in_mem_sges[i].use_count()==0)
    //                                     continue;
    //                                 if(backup_node.db_list[fail_id].in_mem_sges[i]->sge_id == file_id  ){
    //                                     target_position = i;
    //                                     break;
    //                                 }
    //                             }
    //                             assert(target_position>=0);
    //                             file_len = backup_node.db_list[fail_id].in_mem_sges[target_position]->buf.size();
    //                             assert(file_len==MAX_SGE_SIZE);
    //                             memcpy(res->extra_buf[poll_id]+8,backup_node.db_list[fail_id].in_mem_sges[target_position]->buf.data(),file_len);
    //                             //send_master_buf(res,file_id,file_len);
    //                         }
    //                     }
    //                 break;
    //                 case 7:
    //                     finish_list[poll_id] = true;
    //                 break;
    //                 case 8:
    //                     file_len = 4*current_sst_id_list.size();
    //                     int num = 0;
    //                     uint32_t *id_array = (uint32_t *)(res->extra_buf[poll_id]+8);
    //                     for(auto pair : current_sst_id_list){
    //                         //int sst_id = pair.first;
    //                         //memcpy(res->master_buf+8+4*num++,&sst_id,4);
    //                         id_array[num++] = pair.first;
    //                     }
    //                     file_id = 0;
    //                 break;
    //             }
    //             if(finish_list[0]==true && finish_list[1]==true)
    //                 break;
    //             else{
    //                 send_extra_buf(res,file_id,file_len,poll_id);
    //                 post_receive(res,11+poll_id,11+poll_id,8,755);
    //                 poll_id = (poll_id+1)%2;
    //             }
    //         }
    //     }
    // }
    resources_destroy_for_extra(res);
}
// void fragement_num_task(){
//     int counter =0;
//     double frag_rate=0;
//     int frag_num=0;
//     int parity_num=0;
//     while(1){
//         usleep(100*1000);
//         counter++;
        
//         int max_id = syn_gc_finish_id[0];
//         int min_id = syn_gc_finish_id[0];
//         for(int i=1;i<4;i++){
//             if(max_id<syn_gc_finish_id[i])
//                 max_id = syn_gc_finish_id[i];
//             if(min_id>syn_gc_finish_id[i])
//                 min_id = syn_gc_finish_id[i];
//         }
//         int cur_num = max_id-min_id;
//         frag_num+=cur_num;
//         parity_num+=backup_node.parity_sge_num;
//         double cur_rate = cur_num/backup_node.parity_sge_num;
//         frag_rate+=cur_rate;
//         if(counter%10==0){
//             std::cout<<"average_fragement_rate="<<frag_rate/counter<<" ,frag_num="<<frag_num/counter<<" ,parity_num="<<parity_num/counter<<std::endl;
//             std::cout<<"current_frag_num="<<cur_num <<" current_rate=,"<<cur_rate<<std::endl;
//         }
//     }
    
// }

void master_caller(struct resources *res){
    memset(res->master_msg_buf,0,MAX_MASTER_MSG_BUF_SIZE);
    std::cout<<"master_caller begin"<<std::endl;
    char temp_char;
    if(sock_sync_data(res->master_sock, 1, "Q", &temp_char))  /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error while sync master caller\n");
        assert(0);
    }
    std::cout<<"master_caller sync finish"<<std::endl;
    int counter =0;
    double frag_rate=0;
    int frag_num=0;
    int parity_num=0;
    int pid = getpid();
    // std::string cmd_str = "pidstat -p ";
    // cmd_str.append(std::to_string(pid));
    // cmd_str.append(" -d -u -h 500 5 &");
    //system(cmd_str.c_str());
    while(1){
        usleep(100*1000);
        uint32_t msg_finish,msg_type,fail_num,fail_id,fail_id2;
        memcpy(&msg_finish,res->master_msg_buf+4,4);
        memcpy(&msg_type,res->master_msg_buf+8,4);
        memcpy(&fail_num,res->master_msg_buf+12,4);
        memcpy(&fail_id,res->master_msg_buf+16,4);
        memcpy(&fail_id2,res->master_msg_buf+44,4);
        uint32_t name_len,new_tcp,new_tcp2;
        char new_server_name_buf[16];
        memcpy(&name_len,res->master_msg_buf+20,4);
        memcpy(new_server_name_buf,res->master_msg_buf+24,16);
        memcpy(&new_tcp,res->master_msg_buf+40,4);
        memcpy(&new_tcp2,res->master_msg_buf+68,4);
        if(msg_finish == 1){ 
            if(msg_type!=1)
                std::cout<<"recv ret information from master,msg_type is "<<msg_type<<std::endl;
            if(msg_type==4){
                reconnect_qp(res,fail_id);
                // listen_recover_from_master(res,fail_id);
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
                    reconnect_qp(res,fail_id2);
                    config.extra_tcp_port[1] = new_tcp2;
                    resources_create_for_extra(res,1);
                    std::cout<<"create res for extra1 finish"<<std::endl;
                    connect_qp_for_extra(res,1);         
                    listen_recover_from_extra(res,fail_id,1);
                }
                //std::thread listen_from_extra_th(listen_recover_from_extra,res,fail_id2,1);
                //listen_from_extra_th.detach();
                
                
                while(1);
            }
            else if(msg_type==5){
                reconnect_qp(res,fail_id);
                while(1);
            }
            else if(msg_type==2 || msg_type==3){
                while(1);
            }                                         
            memset(res->master_msg_buf,0,MAX_MASTER_MSG_BUF_SIZE);
        }
        uint32_t send_flag =1;
        //std::cout<<"tell master i am fine"<<std::endl;
        memcpy(res->master_msg_buf,&send_flag,4);
        post_send_msg(res,IBV_WR_RDMA_WRITE,10,10,0,4,1096);
        poll_completion_quick(res,10,1097);
      
         if(backup_node.parity_node_id==0){
            counter++;
        //     int max_id = syn_gc_finish_id[0];
        //     int min_id = syn_gc_finish_id[0];
        //     for(int i=1;i<4;i++){
        //         if(max_id<syn_gc_finish_id[i])
        //             max_id = syn_gc_finish_id[i];
        //         if(min_id>syn_gc_finish_id[i])
        //             min_id = syn_gc_finish_id[i];
        //     }
            int cur_num = parity_update_map.size();
            frag_num+=cur_num;
            parity_num+=backup_node.parity_sge_num;
            double cur_rate=0;
            if(backup_node.parity_sge_num>0)
                cur_rate = 1.0*cur_num/backup_node.parity_sge_num;
            frag_rate+=cur_rate;
            if(frag_rate>0&&counter%20==0){
                std::cout<<"current_frag_num="<<cur_num<<" ,parity_num="<<backup_node.parity_sge_num <<" current_rate=,"<<cur_rate<<std::endl;
            }
            if(frag_rate>0&&counter%5000==0){
                std::cout<<"average_fragement_rate="<<frag_rate/counter<<" ,frag_num="<<frag_num/counter<<" ,parity_num="<<parity_num/counter<<std::endl;
            }
        }
        
    }
}