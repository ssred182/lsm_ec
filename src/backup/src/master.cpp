
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
#include <assert.h>
#include <time.h>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
//#include <thread>
#include "master_rdma_connect.h"
//#include "encode.h"
#define DEBUG 1
#define P_K 4;
#define B_K 2;
#define MAX_SGE_SIZE 16*1024*1024
#define MAX_BUF_SIZE 65*1024*1024
#define MAX_MSG_BUF_SIZE 128

extern struct config_t config;



void recover_primary(struct resources *res,uint32_t fail_id);
static int resources_create(struct resources *res)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;
    int rc = 0;
 
    std::cout<<"begin res_create"<<std::endl;
    for(int n=0;n<8;n++){
        std::cout<<"waiting on port "<<config.master_tcp_port[n]<<std::endl;
        //fprintf(stdout, "waiting on port %d for TCP connection\n", config.master_tcp_port[n]);
        res->sock[n] = sock_connect(NULL, config.master_tcp_port[n]);
        if(res->sock[n] < 0)
        {
            fprintf(stderr, "failed to establish TCP connection with client on port %d\n",config.master_tcp_port[n]);
            rc = -1;
            goto resources_create_exit;
        }
        std::cout<<"port "<<config.master_tcp_port[n]<<"connection was established"<<std::endl;
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
    for(int n=0;n<8;n++){
        res->cq[n] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if(!res->cq[n])
        {
            fprintf(stderr, "failed to create CQ with %u entries for node:%d\n", cq_size,n);
            rc = 1;
            goto resources_create_exit;
        }
    }
    
   
    /* allocate the memory buffer that will hold the data */
    for(int n=0;n<8;n++){
        res->msg_buf[n] = (char *) malloc(MAX_MSG_BUF_SIZE);
        fprintf(stdout, "申请内存msg_buf for node:%d\n",n);
        res->buf[n] = (char *) malloc(MAX_BUF_SIZE);
        fprintf(stdout, "申请内存buf for node:%d\n",n); 
        if(!res->buf[n])
        {
            fprintf(stderr, "failed to malloc %Zu bytes to memory buffer for node:%d\n", MAX_BUF_SIZE,n);
            rc = 1;
            goto resources_create_exit;
        }
        memset(res->buf[n], 0 , MAX_BUF_SIZE);     
    }
    /* register the memory buffer */
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    for(int n=0;n<8;n++){
        res->mr[n] = ibv_reg_mr(res->pd, res->buf[n], MAX_BUF_SIZE, mr_flags);
        res->msg_mr[n] = ibv_reg_mr(res->pd, res->msg_buf[n], MAX_MSG_BUF_SIZE, mr_flags);
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
    for(int n=0;n<8;n++){
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
        for(int n=0;n<8;n++){
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


//BackupNode backup_node;

void listen_fail(struct resources *res);
int rc = 1;


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

int main(int argc, char *argv[])
{
    struct resources res;
    uint32_t max_sge_size = MAX_SGE_SIZE;
    
    config.dev_name = NULL;
    config.master_server_name  = NULL;
    config.ib_port = 1;
    config.gid_idx = 0;
    config.master_tcp_port[0]= 19991;
    

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
            {.name = "master", .has_arg = 1, .val = 'm' },
            {.name = NULL, .has_arg = 0, .val = '\0'}
        };
        /*关于参数设置，gid必须两端都一样, ib-port必须两端等于0
        */
        c = getopt_long(argc, argv, "p:d:i:g:m:", long_options, NULL);
        if(c == -1)
        {
            break;
        }
        switch(c)
        {
        case 'p':
            config.master_tcp_port[0] = strtoul(optarg, NULL, 0);
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
        case 'm':
            config.master_server_name = strdup(optarg);     
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    
   
 
    /* print the used parameters for info*/
    print_config();
    /* init all of the resources, so cleanup will be easy */
    resources_init(&res);
    //initilize backup_node before create res to prepare sge for buf(MR)

    /* create resources before using them */
    if(resources_create(&res))
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
        return 1;
    }
    std::cout <<"finish connect qp"<<std::endl;
    /* let the server post the sr */

    for(int n=0;n<8;n++){
        if(post_send(&res, IBV_WR_SEND,n,0,4))
        {
            fprintf(stderr, "failed to post sr\n");
            exit(&res);
            return 1;
        }
    }
        

    /* in both sides we expect to get a completion */
    for(int n=0;n<8;n++){
        if(poll_completion(&res,n))
        {
            fprintf(stderr, "poll completion failed\n");
            exit(&res);
            return 1;
        }
    }
    std::cout<<"connect established"<<std::endl;
    // while(1)
    //     ;
    listen_fail(&res);
    std::cout<<" a reovery finish"<<std::endl;
    while(1)
        ;
    return rc;
}

uint32_t global_new_tcp = 20001;
void listen_fail(struct resources *res){
    // for(int i=0;i<6;i++)
    //     post_receive_msg(res,i,16);
    for(int i=0;i<6;i++){
        memset(res->msg_buf[i],0,4);
    }
    char temp_char;
    for(int i=0;i<6;i++){
        if(sock_sync_data(res->sock[i], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error while sync master caller\n");
            assert(0);
        }
    }
    std::cout<<"master_caller sync finish"<<std::endl;

    while(1){

        usleep(800*1000);
        int fail_num = 0;
        int fail_list[2];
        for(int poll_id=0;poll_id<6;poll_id++){
            uint32_t send_flag;
            memcpy(&send_flag,res->msg_buf[poll_id],4);
            
            //if(send_flag == 0){
                int rc =post_send_msg(res,IBV_WR_RDMA_WRITE,poll_id,0,4);
                int rc2;
                if(rc==0)
                    rc2 = poll_completion_quick(res,poll_id);
                if(rc!=0||rc2!=0){
                //this node fail
                assert(fail_num<2);
                fail_list[fail_num++] = poll_id;
                }
            //}
            // else if(send_flag == 1){
            //     //expected
            //     ;
            // }
            // else if(send_flag == 2){
            //     //recovery finish

            // }
            // else{
            //     std::cout<<"error! node flag error,flag= "<<send_flag<<std::endl;
            //     assert(send_flag<=2);
            // }
            memset(res->msg_buf[poll_id],0,4);
        }
        if(fail_num == 1){
            assert(fail_list[0]<6);
            std::cout<<"node: "<<fail_list[0]<<" fail"<<std::endl;
            if(fail_list[0]<4){
            //primary fail
                uint32_t msg_type = 1;
                uint32_t node_type = 1;//NODE_TYPE_P;
                uint32_t node_id = fail_list[0];
                std::cout<<"inform extra0 to take place of the fail node"<<std::endl;
                memcpy(res->buf[6],&msg_type,4);
                memcpy(res->buf[6]+4,&node_type,4);
                memcpy(res->buf[6]+8,&node_id,4);
                memcpy(res->buf[6]+12,&config.backup_tcp_port[0][node_id],4);
                memcpy(res->buf[6]+16,&config.backup_tcp_port[1][node_id],4);
                uint32_t extra_tcp_port[4];
                for(int i=0;i<4;i++){
                    extra_tcp_port[i] = global_new_tcp++;
                    memcpy(res->buf[6]+28+4*i,&extra_tcp_port[i],4);
                }
                post_send(res,IBV_WR_SEND,6,0,44);
                poll_completion(res,6);
                std::cout<<"inform finish,update the rdma_connect infomartion of the fail node to extra config"<<std::endl;
                res->cq[fail_list[0]]= res->cq[6];
                res->qp[fail_list[0]]= res->qp[6];
                res->mr[fail_list[0]]= res->mr[6];
                res->msg_mr[fail_list[0]]= res->msg_mr[6];
                res->buf[fail_list[0]]= res->buf[6];
                res->msg_buf[fail_list[0]]= res->msg_buf[6];
                res->sock[fail_list[0]]= res->sock[6];
                res->remote_props[fail_list[0]]= res->remote_props[6];
                int extra_tcp_port_for_primary = 1;
                for(int i=0;i<4;i++){
                    if(i == fail_list[0])
                        continue;
                    uint32_t msg_finish =1;
                    uint32_t msg_type = 4;
                    uint32_t fail_num = 1;
                    uint32_t id1 = fail_list[0];
                    memcpy(res->msg_buf[i]+4,&msg_finish,4);
                    memcpy(res->msg_buf[i]+8,&msg_type,4);
                    memcpy(res->msg_buf[i]+12,&fail_num,4);
                    memcpy(res->msg_buf[i]+16,&id1,4);
                    uint32_t new_tcp_port1 = extra_tcp_port[extra_tcp_port_for_primary++];
                    memcpy(res->msg_buf[i]+40,&new_tcp_port1,4);
                    std::cout<<"inform primary node"<<i << " ,msg_type = 4,they need post_recv to provide data we want to recover"<<std::endl;
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,64);
                    poll_completion(res,i);
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                    poll_completion(res,i);
                }
                for(int i=4;i<6;i++){
                    uint32_t msg_finish =1;
                    uint32_t msg_type;
                    if(i==4)
                        msg_type = 4;
                    else
                        msg_type = 5;
                    uint32_t fail_num = 1;
                    uint32_t id1 = fail_list[0];
                    
                    memcpy(res->msg_buf[i]+4,&msg_finish,4);
                    memcpy(res->msg_buf[i]+8,&msg_type,4);
                    memcpy(res->msg_buf[i]+12,&fail_num,4);
                    memcpy(res->msg_buf[i]+16,&id1,4);
                    memcpy(res->msg_buf[i]+40,&extra_tcp_port[0],4);
                    std::cout<<"inform node: "<<i<<" ,msg_type is "<<msg_type <<std::endl;
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,64);
                    poll_completion(res,i);
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                    poll_completion(res,i);
                }
                //recover_primary(res,fail_list[0]);

            }
            else{
            //backup fail
                for(int i=0;i<4;i++)
                    config.backup_tcp_port[fail_list[0]-4][i] = global_new_tcp++;
                uint32_t msg_type = 1;
                uint32_t node_type = 2;//NODE_TYPE_B;
                uint32_t node_id = fail_list[0]-4;
                config.backup_server_name[node_id]="10.118.0.53";
                memcpy(res->buf[6],&msg_type,4);
                memcpy(res->buf[6]+4,&node_type,4);
                memcpy(res->buf[6]+8,&node_id,4);
                for(int i=0;i<4;i++)
                    memcpy(res->buf[6]+12+4*i,&config.backup_tcp_port[node_id][i],4);
                std::cout<<"inform extra0 to take place of the fail node, first new tcp port is "<<config.backup_tcp_port[node_id][0]<<std::endl;
                post_send(res,IBV_WR_SEND,6,0,28);
                poll_completion(res,6);
                res->cq[fail_list[0]]= res->cq[6];
                res->qp[fail_list[0]]= res->qp[6];
                res->mr[fail_list[0]]= res->mr[6];
                res->msg_mr[fail_list[0]]= res->msg_mr[6];
                res->buf[fail_list[0]]= res->buf[6];
                res->msg_buf[fail_list[0]]= res->msg_buf[6];
                res->sock[fail_list[0]]= res->sock[6];
                res->remote_props[fail_list[0]]= res->remote_props[6];
                std::cout<<"inform finish,update the rdma_connect infomartion of the fail node to extra config"<<std::endl;
                for(int i=0;i<4;i++){
                    uint32_t msg_finish =1;
                    uint32_t msg_type = 2;
                    uint32_t fail_num = 1;
                    uint32_t id1 = fail_list[0]-4;
                    char name_buf[16];
                    strcpy(name_buf,config.backup_server_name[id1]);
                    uint32_t name_len = strlen(name_buf);
                    uint32_t new_tcp1 = config.backup_tcp_port[id1][i];
                    memcpy(res->msg_buf[i]+4,&msg_finish,4);
                    memcpy(res->msg_buf[i]+8,&msg_type,4);
                    memcpy(res->msg_buf[i]+12,&fail_num,4);
                    memcpy(res->msg_buf[i]+16,&id1,4);
                    memcpy(res->msg_buf[i]+20,&name_len,4);
                    memcpy(res->msg_buf[i]+24,name_buf,16);
                    memcpy(res->msg_buf[i]+40,&new_tcp1,4);
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,36);
                    poll_completion(res,i);
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                    poll_completion(res,i);
                }
                for(int i=4;i<6;i++){
                    if(i == fail_list[0])
                        continue;
                    uint32_t msg_finish =1;
                    uint32_t msg_type = 3;
                    uint32_t fail_num = 1;
                     uint32_t id1 = fail_list[0] - 4;
                    memcpy(res->msg_buf[i]+4,&msg_finish,4);
                    memcpy(res->msg_buf[i]+8,&msg_type,4);
                    memcpy(res->msg_buf[i]+12,&fail_num,4);
                    memcpy(res->msg_buf[i]+16,&id1,4);
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,12);
                    poll_completion(res,i);
                    post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                    poll_completion(res,i);
                }
            }
            break;
        }
        else if(fail_num == 2){
            assert(fail_list[0]<6);
            assert(fail_list[1]<6);
            std::cout<<"node: "<<fail_list[0]<<" and node: "<<fail_list[1]<<" fail"<<std::endl;
            
            //primary fail
            uint32_t msg_type = 1;
            uint32_t node_type = 1;//NODE_TYPE_P;
            uint32_t node_id = fail_list[0];
            std::cout<<"inform extra0 to take place of the fail node:"<<fail_list[0]<<std::endl;
            memcpy(res->buf[6],&msg_type,4);
            memcpy(res->buf[6]+4,&node_type,4);
            memcpy(res->buf[6]+8,&node_id,4);
            memcpy(res->buf[6]+12,&config.backup_tcp_port[0][node_id],4);
            memcpy(res->buf[6]+16,&config.backup_tcp_port[1][node_id],4);
            uint32_t extra_tcp_port[4];
            for(int i=0;i<4;i++){
                extra_tcp_port[i] = global_new_tcp++;
                memcpy(res->buf[6]+28+4*i,&extra_tcp_port[i],4);
            }
            post_send(res,IBV_WR_SEND,6,0,44);
            poll_completion(res,6);
            std::cout<<"inform finish,update the rdma_connect infomartion of the fail node to extra config"<<std::endl;
            res->cq[fail_list[0]]= res->cq[6];
            res->qp[fail_list[0]]= res->qp[6];
            res->mr[fail_list[0]]= res->mr[6];
            res->msg_mr[fail_list[0]]= res->msg_mr[6];
            res->buf[fail_list[0]]= res->buf[6];
            res->msg_buf[fail_list[0]]= res->msg_buf[6];
            res->sock[fail_list[0]]= res->sock[6];
            res->remote_props[fail_list[0]]= res->remote_props[6];
            int extra_tcp_port_for_primary = 0;
            //--------------------------
            node_id = fail_list[1];
            std::cout<<"inform extra1 to take place of the fail node: "<<fail_list[1]<<std::endl;
            memcpy(res->buf[7],&msg_type,4);
            memcpy(res->buf[7]+4,&node_type,4);
            memcpy(res->buf[7]+8,&node_id,4);
            memcpy(res->buf[7]+12,&config.backup_tcp_port[0][node_id],4);
            memcpy(res->buf[7]+16,&config.backup_tcp_port[1][node_id],4);
            uint32_t extra_tcp_port2[4];
            for(int i=0;i<4;i++){
                extra_tcp_port2[i] = global_new_tcp++;
                memcpy(res->buf[7]+28+4*i,&extra_tcp_port2[i],4);
            }
            post_send(res,IBV_WR_SEND,7,0,44);
            poll_completion(res,7);
            std::cout<<"inform finish,update the rdma_connect infomartion of the fail node to extra config"<<std::endl;
            res->cq[fail_list[1]]= res->cq[7];
            res->qp[fail_list[1]]= res->qp[7];
            res->mr[fail_list[1]]= res->mr[7];
            res->msg_mr[fail_list[1]]= res->msg_mr[7];
            res->buf[fail_list[1]]= res->buf[7];
            res->msg_buf[fail_list[1]]= res->msg_buf[7];
            res->sock[fail_list[1]]= res->sock[7];
            res->remote_props[fail_list[1]]= res->remote_props[7];
            //--------------------------------------------------------
            for(int i=4;i<6;i++){
                uint32_t msg_finish =1;
                uint32_t msg_type;
                msg_type = 4;
                uint32_t fail_num = 2;
                uint32_t id1 = fail_list[0];
                uint32_t id2 = fail_list[1];
                memcpy(res->msg_buf[i]+4,&msg_finish,4);
                memcpy(res->msg_buf[i]+8,&msg_type,4);
                memcpy(res->msg_buf[i]+12,&fail_num,4);
                memcpy(res->msg_buf[i]+16,&id1,4);
                memcpy(res->msg_buf[i]+40,&extra_tcp_port[extra_tcp_port_for_primary],4);
                memcpy(res->msg_buf[i]+44,&id2,4);
                memcpy(res->msg_buf[i]+68,&extra_tcp_port2[extra_tcp_port_for_primary++],4);
                std::cout<<"inform node: "<<i<<" ,msg_type is "<<msg_type <<std::endl;
                post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,64);
                poll_completion(res,i);
                post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                poll_completion(res,i);
            }
            for(int i=0;i<4;i++){
                if(i == fail_list[0]||i==fail_list[1])
                    continue;
                uint32_t msg_finish =1;
                uint32_t msg_type = 4;
                uint32_t fail_num = 2;
                uint32_t id1 = fail_list[0];
                uint32_t id2 = fail_list[1];
                memcpy(res->msg_buf[i]+4,&msg_finish,4);
                memcpy(res->msg_buf[i]+8,&msg_type,4);
                memcpy(res->msg_buf[i]+12,&fail_num,4);
                memcpy(res->msg_buf[i]+16,&id1,4);
                //uint32_t new_tcp_port1 = extra_tcp_port[extra_tcp_port_for_primary];
                memcpy(res->msg_buf[i]+40,&extra_tcp_port[extra_tcp_port_for_primary],4);
                memcpy(res->msg_buf[i]+44,&id2,4);
                memcpy(res->msg_buf[i]+68,&extra_tcp_port2[extra_tcp_port_for_primary++],4);
                std::cout<<"inform primary node"<<i << " ,msg_type = 4,they need post_recv to provide data we want to recover"<<std::endl;
                post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,64);
                poll_completion(res,i);
                post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                poll_completion(res,i);
            }
            break;       
            //recover_primary(res,fail_list[0]);  
        }
        else{
            std::cout<<"no node fail"<<std::endl;
            for(int i =0;i<6;i++){
                memset(res->msg_buf[i]+4,0,40);
                uint32_t msg_type = 1;
                uint32_t msg_finish = 1;
                memcpy(res->msg_buf[i]+4,&msg_finish,4);
                memcpy(res->msg_buf[i]+8,&msg_type,4);
                post_send_msg(res,IBV_WR_RDMA_WRITE,i,8,4);
                poll_completion(res,i);
                post_send_msg(res,IBV_WR_RDMA_WRITE,i,4,4);
                poll_completion(res,i);
            }
            
        }
    }
}
//node_id是master的连接id，不是parity_node_id
void send_buf(struct resources *res,uint32_t file_type,uint32_t file_id,uint32_t *ret_id,uint32_t *ret_len,uint32_t node_id){
    memcpy(res->buf[node_id],&file_type,4);
    memcpy(res->buf[node_id]+4,&file_id,4);
    int rc;
    rc = post_send(res,IBV_WR_SEND,node_id,0,8);
    assert(!rc);
    rc = poll_completion(res,node_id);
    assert(!rc);
    rc = post_receive(res,node_id,MAX_BUF_SIZE);
    assert(!rc);
    rc = poll_completion(res,node_id);
    assert(!rc);
    memcpy(ret_id,res->buf[node_id],4);
    memcpy(ret_len,res->buf[node_id]+4,4);
}
void send_buf_extra(struct resources *res,uint32_t file_type,uint32_t file_id,uint32_t file_len,char *file_content,uint32_t fail_id){
    uint32_t msg_type = 2;
    int rc;
    memcpy(res->buf[fail_id],&msg_type,4);
    memcpy(res->buf[fail_id]+4,&file_type,4);
    memcpy(res->buf[fail_id]+8,&file_id,4);
    memcpy(res->buf[fail_id]+12,&file_len,4);
    memcpy(res->buf[fail_id]+16,file_content,file_len);
    rc = post_send(res,IBV_WR_SEND,fail_id,0,file_len+16);
    assert(!rc);
    rc = poll_completion(res,fail_id);
    assert(!rc);
}
void send_buf_extra_nocpy(struct resources *res,uint32_t file_type,uint32_t file_id,uint32_t file_len,uint32_t fail_id){
    uint32_t msg_type = 2;
    int rc;
    memcpy(res->buf[fail_id],&msg_type,4);
    memcpy(res->buf[fail_id]+4,&file_type,4);
    memcpy(res->buf[fail_id]+8,&file_id,4);
    memcpy(res->buf[fail_id]+12,&file_len,4);
    rc = post_send(res,IBV_WR_SEND,fail_id,0,file_len+16);
    assert(!rc);
    rc = poll_completion(res,fail_id);
    assert(!rc);
}
void recover_primary(struct resources *res,uint32_t fail_id);
// void recover_primary(struct resources *res,uint32_t fail_id){
//     clock_t beg,end;
//     double duration,total_recover_time=0,total_decode_time=0;
//     //get id range from backup node 0
//     std::cout<<"begin recover_primary"<<std::endl;
//     uint32_t file_id,file_len;
//     char *result_buf = new char[MAX_BUF_SIZE];


//     //IDENTITY
//     std::cout<<"get IDENTITY"<<std::endl;
//     beg = clock();
//     send_buf(res,3,0,&file_id,&file_len,4);
//     send_buf_extra(res,3,file_id,file_len,res->buf[4]+8,fail_id);
//     std::cout<<"send IDENTITY"<<std::endl;
//     //MANIFEST
//     std::cout<<"get MANIFEST"<<std::endl;
//     send_buf(res,4,0,&file_id,&file_len,4);
//     std::cout<<"id = "<<file_id<<" ,len = "<<file_len<<std::endl;
//     send_buf_extra(res,4,file_id,file_len,res->buf[4]+8,fail_id);
//     std::cout<<"send MANIFEST"<<std::endl;
//     //sst id list  file_len = 4*id_num
//     std::cout<<"get sst_id list"<<std::endl;
//     send_buf(res,8,0,&file_id,&file_len,4);
//     std::vector<uint32_t> sst_id_list;
//     std::cout<<"id_list: ";
//     for(int i=0;i<file_len;i+=4){
//         uint32_t tmp;
//         memcpy(&tmp,res->buf[4]+8+i,4);
//         std::cout<<tmp<<" ,";
//         sst_id_list.emplace_back(tmp);
//     }
//     std::cout<<std::endl;
//     //every sst file
//     for(uint32_t sst_id : sst_id_list){
//         std::cout<<"get sst "<<sst_id<<std::endl;
//         send_buf(res,5,sst_id,&file_id,&file_len,4);
//         send_buf_extra(res,5,file_id,file_len,res->buf[4]+8,fail_id);
//     }


//     std::cout<<"get id_range"<<std::endl;
//     send_buf(res,1,0,&file_id,&file_len,4);
//     uint32_t min_parity_id,max_parity_id,max_vsge_id;
//     memcpy(&min_parity_id,res->buf[4]+8,4);
//     memcpy(&max_parity_id,res->buf[4]+12,4);
//     memcpy(&max_vsge_id,res->buf[4]+16,4);
//     std::cout<<"min_parity_id ="<<min_parity_id<<" ,max_parity_id="<<max_parity_id<<" ,max_vsge_id= "<<max_vsge_id<<std::endl;
//     //id range
//     send_buf_extra(res,1,file_id,file_len,res->buf[4]+8,fail_id);
//     std::cout<<"send id_range"<<std::endl;
//     //tail sge
//     std::cout<<"get tail_sge"<<std::endl;
//     send_buf(res,2,0,&file_id,&file_len,4);
//     send_buf_extra(res,2,file_id,file_len,res->buf[4]+8,fail_id);
//     std::cout<<"send tail_sge"<<std::endl;
    
//     //every vsge not encoded
//     for(int id=max_parity_id+1;id<=max_vsge_id;id++){
//         std::cout<<"get vsge: "<<id<<std::endl;
//         send_buf(res,6,id,&file_id,&file_len,4);
//         send_buf_extra(res,6,file_id,file_len,res->buf[4]+8,fail_id);
//     }
//     end = clock();
//     duration = (double)(end - beg)/CLOCKS_PER_SEC;
//     total_recover_time += duration;
//     Encoder encoder;
    
//     encoder.init_encode(4,2,MAX_SGE_SIZE);
//     uint8_t error_list[1] = {fail_id};
//     encoder.init_decode(&error_list[0],1);
//     uint8_t *decode_src[6];
//     uint8_t *decode_dest[1];
//     for(int i=0;i<6;i++){
//         decode_src[i]=(uint8_t *)(res->buf[i]+8);
//     }
//     decode_dest[0] = (uint8_t *)(res->buf[fail_id]+16);
//     for(int id=min_parity_id;id<=max_parity_id;id++){
        
//         for(int i=0;i<4;i++){
//             if(i==fail_id)
//                 continue;
//             beg = clock();
//             send_buf(res,6,id,&file_id,&file_len,i);
//             end = clock();
//             duration = (double)(end - beg)/CLOCKS_PER_SEC;
//             total_recover_time += duration;
//             std::cout<<"get sge_id="<<id<<" from node : "<<i<<" file_len="<<file_len<<std::endl;
//             memset(res->buf[i]+8+file_len,0,MAX_SGE_SIZE-file_len);
//         }
//         std::cout<<"get parity_id="<<id<<" from node : 4 "<<std::endl;
//         beg = clock();
//         send_buf(res,6,id,&file_id,&file_len,4);
//         end = clock();
//         duration = (double)(end - beg)/CLOCKS_PER_SEC;
//         total_recover_time += duration;
//         std::cout<<"decode begin"<<std::endl;
//         beg = clock();
//         encoder.decode_data(decode_src,decode_dest);
//         end = clock();
//         duration = (double)(end - beg)/CLOCKS_PER_SEC;
//         total_recover_time += duration;
//         total_decode_time += duration;
//         std::cout<<"decode finish,send to fail node"<<std::endl;
//         // std::ofstream fout;
//         // std::string path = "vsge_"+std::to_string(id);
//         // fout.open(path,std::ios::out|std::ios::binary);
//         // fout.seekp(0,std::ios::beg);
//         // fout.write(res->buf[fail_id]+16,file_len);
//         // fout.close();
//         beg = clock();
//         send_buf_extra_nocpy(res,6,file_id,file_len,fail_id);
//         end = clock();
//         duration = (double)(end - beg)/CLOCKS_PER_SEC;
//         total_recover_time += duration;
//     }





//     std::cout<<"inform node : 4 ,listenr finish"<<std::endl;
//     uint32_t file_type = 7;
//     memcpy(res->buf[4],&file_type,4);
//     post_send(res,IBV_WR_SEND,4,0,8);
//     poll_completion(res,4);
//     for(int i =0;i<4;i++){
//         if(i==fail_id)
//             continue;
//         memcpy(res->buf[i],&file_type,4);
//         std::cout<<"inform node : "<<i <<" ,listenr finish"<<std::endl;
//         post_send(res,IBV_WR_SEND,i,0,8);
//         poll_completion(res,i);
//     }
//     uint32_t msg_type = 3;
//     memcpy(res->buf[fail_id],&msg_type,4);
//     std::cout<<"inform extra, recover finish"<<std::endl;
//     post_send(res,IBV_WR_SEND,fail_id,0,4);
//     poll_completion(res,fail_id);
//     std::cout<<"total recover time = "<<total_recover_time<<std::endl;
//     std::cout<<"total decode time = "<<total_decode_time<<std::endl;
//     delete result_buf;
// }