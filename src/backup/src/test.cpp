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
#include "backup.h"
#include "rdma_connect.h"
#define DEBUG 1
#define KV_NUM  1024
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
    if(config.server_name)
    {
        for(int n=0;n<K;n++){
            res->sock[n] = sock_connect(config.server_name, config.tcp_port[n]);
            if(res->sock[n] < 0)
            {
                fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                        config.server_name, config.tcp_port[n]);
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
        if(!config.server_name)
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




BackupNode backup_node;

bool get_from_rocksdb(uint32_t db_id,const std::string &key,uint32_t &sge_id,uint32_t &offset_of_sgement){
    //rocksdb的参数是string *value所以取地址
    std::string value;
    auto status = backup_node.db_list[db_id].rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value);
    sge_id = *((uint32_t*)value.data());
    offset_of_sgement = *( (uint32_t*)(value.data()+4 ) );
    return status.ok();
}

void listen_flush_tail(struct resources *res);
void listen_single_flush_tail(struct resources *res,int target_id);
int rc = 1;
int main(int argc, char *argv[])
{
    struct resources res;
    uint32_t max_sge_size = MAX_SGE_SIZE;
    
    config.dev_name = NULL;
    config.server_name  = NULL;
    config.tcp_port[0] = 19875;
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
            {.name = "client", .has_arg = 1, .val = 'c' },
            {.name = NULL, .has_arg = 0, .val = '\0'}
        };
        /*关于参数设置，gid必须两端都一样, ib-port必须两端等于0
        */
        c = getopt_long(argc, argv, "p:d:i:g:n:c:s", long_options, NULL);
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
            config.server_name = strdup(optarg);
            
            break;
        case 's':
            config.server_name = NULL;
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    
   
 
    /* print the used parameters for info*/
    print_config();
    /* init all of the resources, so cleanup will be easy */
    //resources_init(&res);
    //initilize backup_node before create res to prepare sge for buf(MR)
    backup_node.init(config.node_id);
    for(int i=0;i<4;i++)
        backup_node.db_list[i].tail_sge_ptr->buf.resize(0);
    // for(int i=0;i<K;i++){
    //     res.buf[i]=backup_node.db_list[i].tail_sge_ptr->buf.data();
    // }
    /* create resources before using them */
    std::vector<KV>test_kvs;
    for(int i=0;i<KV_NUM;i++){
        std::string value;
        while(value.size()<V_LEN){
            value.append(std::to_string(i));
        }
        KV tmp_kv(std::to_string(i),value);
        test_kvs.emplace_back(tmp_kv);
    }
    int num = 0;
    std::vector<std::pair<uint32_t,uint32_t>> key_indexs;
    for(int i=0;i<3*4;i++){
        srand(time(nullptr));
    for(auto &kv :test_kvs){
        // std::string key = kv.key;
        // std::string value = kv.value;
        
        int db_id = rand()%4;//num%4;//;
        //std::cout<<"put into node:"<<db_id<<std::endl;
        uint32_t old_sge_id = backup_node.db_list[db_id].tail_sge_ptr->sge_id;
        if(backup_node.db_list[db_id].tail_sge_ptr->get_sge_size() + kv.key.size()+kv.value.size()+8>MAX_SGE_SIZE-4)
        {
            int sge_len = backup_node.db_list[db_id].tail_sge_ptr->get_sge_size();
            memcpy(backup_node.db_list[db_id].tail_sge_ptr->buf.data()+MAX_SGE_SIZE-4,&sge_len,4);
            backup_node.flush_tail(db_id);
            backup_node.db_list[db_id].tail_sge_ptr->buf.resize(0);
        }
        uint32_t offset = backup_node.db_list[db_id].tail_sge_ptr->append(kv.key.data(),kv.key.size(),kv.value.data(),kv.value.size());
        uint32_t seg_id = backup_node.db_list[db_id].tail_sge_ptr->sge_id;
        key_indexs.emplace_back(std::make_pair(seg_id,offset));
        if((num+1)%1000==0)
            std::cout<<"put "<< num+1<<" kvs"<<std::endl;
        num++;
    }
    }
    std::cout<<"put finish"<<std::endl;
    // for(int i=0;i<key_indexs.size();i++){
    //     uint32_t id = key_indexs[i].first;
    //     uint32_t off = key_indexs[i].second;
    //     int db_id = i%4;
    //     std::string key = std::to_string(i);
    //     uint32_t get_id,get_off;
    //     bool ret = get_from_rocksdb(db_id,key,get_id,get_off);
    //     std::cout<< "key = " << key << " ,id=" << id << " ,get_id=" << get_id<<" ,off="<<off<<" ,get_off="<<get_off<<std::endl;
    //     if(!ret)
    //         std::cout<<"!!!!!error:get from rocksd fail."<<std::endl;
    // } 
    std::cout<<"finish all"<<std::endl;
    
 

   
    return rc;
}


