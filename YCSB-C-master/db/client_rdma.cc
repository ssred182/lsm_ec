#include "client_rdma.h"
#include <unistd.h>
#include <iostream>
#include <assert.h>
#if __BYTE_ORDER == __LITTLE_ENDIAN
inline uint64_t htonll(uint64_t x)
{
    return bswap_64(x);
}
inline uint64_t ntohll(uint64_t x)
{
    return bswap_64(x);
}
#elif __BYTE_ORDER == __BIG_ENDIAN
inline uint64_t htonll(uint64_t x)
{
    return x;
}
inline uint64_t ntohll(uint64_t x)
{
    return x;
}
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
struct config_t config;
struct global_resource *global_res;




int sock_connect(const char *servername, int port)
{
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;
    int tmp;
    struct addrinfo hints =
    {
        .ai_flags    = AI_PASSIVE,
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_STREAM
    };
 
    if(sprintf(service, "%d", port) < 0)
    {
        goto sock_connect_exit;
    }
 
    /* Resolve DNS address, use sockfd as temp storage */
    sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
    if(sockfd < 0)
    {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
        goto sock_connect_exit;
    }
 
    /* Search through results and find the one we want */
    for(iterator = resolved_addr; iterator ; iterator = iterator->ai_next)
    {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        int reuse = 1;
        setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse));
        if(sockfd >= 0)
        {
            if(servername)
			{
                /* Client mode. Initiate connection to remote */

                while((tmp=connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
                {
                    // std::cout<<"sleep 100ms"<<std::endl;
                    // usleep(100*1000);
                    // std::cout<<"wake up"<<std::endl;
                    // fprintf(stdout, "failed connect \n");
                    // close(sockfd);
                    // sockfd = -1;
                }
			}
            else
            {
                /* Server mode. Set up listening socket an accept a connection */
                listenfd = sockfd;
                sockfd = -1;
                if(bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
                {
                    goto sock_connect_exit;
                }
                listen(listenfd, 1);
                sockfd = accept(listenfd, NULL, 0);
            }
        }
    }
 
sock_connect_exit:
    if(listenfd)
    {
        close(listenfd);
        std::cout<<"sock is close,server name = "<<servername<<std::endl;
    }
 
    if(resolved_addr)
    {
        freeaddrinfo(resolved_addr);
    }
 
    if(sockfd < 0)
    {
        if(servername)
        {
            std::cout<<"Couldn't connect to server name = "<<servername<<std::endl;
            fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        }
        else
        {
            std::cout<<"sockfd<0,accept fail"<<std::endl;
            perror("server accept");
            fprintf(stderr, "accept() failed\n");
        }
    }
    char buf[10];
    int read_bytes = read(sockfd,buf,0);
    std::cout<<"try read 0 bytes after socket establish, result = "<<read_bytes<<std::endl;
    int error = -1;
    socklen_t len = sizeof(socklen_t);
    if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, (socklen_t*)&len) < 0) {
        // 错误处理
        std::cout<<"connect fail, error = "<<error<<std::endl;
    } else {
        if (error == 0) {
            // 连接成功
            std::cout<<"socket connect fine, sockfd = "<<sockfd<<std::endl;
        } else {
            // 连接失败
            std::cout<<"connect fail, error = "<<error<<std::endl;
        }
    }
               
    return sockfd;
}
 
/******************************************************************************
* Function: sock_sync_data
* Input:
* sock: socket to transfer data on
* xfer_size: size of data to transfer
* local_data: pointer to data to be sent to remote
*
* Output: remote_data pointer to buffer to receive remote data
*
* Returns: 0 on success, negative error code on failure
*
* Description:
* Sync data across a socket. The indicated local data will be sent to the
* remote. It will then wait for the remote to send its data back. It is
* assumed that the two sides are in sync and call this function in the proper
* order. Chaos will ensue if they are not. :)
*
* Also note this is a blocking function and will wait for the full data to be
* received from the remote.
*
******************************************************************************/
int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data)
{
    //std::cout<<"check status before write, sock="<<sock<<std::endl;
    int error = -1;
    socklen_t len = sizeof(socklen_t);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, (socklen_t*)&len) < 0) {
        // 错误处理
        std::cout<<"connect fail, error = "<<error<<std::endl;
    } else {
        if (error == 0) {
            // 连接成功
            //std::cout<<"socket connect fine"<<std::endl;
        } else {
            // 连接失败
            std::cout<<"connect fail, error = "<<error<<std::endl;
        }
    }
    int rc;
    int read_bytes = 0;
    int total_read_bytes = 0;
    rc = write(sock, local_data, xfer_size);
    // std::cout<<"size of cm_con_data_t = "<<sizeof(struct cm_con_data_t)<<" ,xfer_size="<<xfer_size\
    // <<" rc="<<rc<<std::endl;
    int rep_num=0;
    while(rc <= 0){
        std::cout<<"errno = "<<errno<<std::endl;
        rc = write(sock, local_data, xfer_size);
        if(rep_num++>10)
            break;
    }
    if(rc < xfer_size)
    {
        fprintf(stderr, "Failed writing data during sock_sync_data\n");

        //getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &errlen);
    }
    else
    {
        rc = 0;
    }
    //std::cout<<"r/w size = "<<xfer_size<<" remote addr="<<remote_data<<std::endl;
    while(!rc && total_read_bytes < xfer_size)
    {
        read_bytes = read(sock, remote_data, xfer_size);
        if(read_bytes > 0)
        {
            total_read_bytes += read_bytes;
        }
        else
        {
            rc = read_bytes;
        }
    }
    //std::cout<<"sync_finish"<<std::endl;
    return rc;
}


int poll_completion(struct resources *res,int target_node_id,int qp_id)
{
    struct ibv_wc wc;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;
    int poll_result;
    int rc = 0;
    /* poll the completion for a while before giving up of doing it .. */
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    do
    {
        poll_result = ibv_poll_cq(res->res_list[target_node_id*MAX_QP_NUM+qp_id].cq, 1, &wc);
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    }
    while((poll_result == 0) && ((cur_time_msec - start_time_msec) < 6000));
 
    if(poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if(poll_result == 0)
    {
        /* the CQ is empty */
       printf("node: %d :%d completion wasn't found in the CQ after timeout\n",target_node_id,qp_id);
        rc = 1;
    }
    else
    {
        /* CQE found */
        //fprintf(stdout, "node: %d completion was found in CQ with status 0x%x\n",target_node_id,wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if(wc.status != IBV_WC_SUCCESS)
        {
           printf("got bad completion with status: 0x%x, vendor syndrome: 0x%x \n", 
					wc.status, wc.vendor_err);
            rc = 1;
        }
    }
    return rc;
}

int poll_completion_quick(struct resources *res,int target_node_id,int qp_id)
{
    struct ibv_wc wc;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;
    int poll_result;
    int rc = 0;
    /* poll the completion for a while before giving up of doing it .. */
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    do
    {
        poll_result = ibv_poll_cq(res->res_list[target_node_id*MAX_QP_NUM+qp_id].cq, 1, &wc);
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    }
    while((poll_result == 0) && ((cur_time_msec - start_time_msec) < 50));
 
    if(poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if(poll_result == 0)
    {
        /* the CQ is empty */
       printf("node: %d :%d completion wasn't found in the CQ after timeout\n",target_node_id,qp_id);
        rc = 1;
    }
    else
    {
        /* CQE found */
        //fprintf(stdout, "node: %d completion was found in CQ with status 0x%x\n",target_node_id,wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if(wc.status != IBV_WC_SUCCESS)
        {
           printf("got bad completion with status: 0x%x, vendor syndrome: 0x%x \n", 
					wc.status, wc.vendor_err);
            rc = 1;
        }
    }
    return rc;
}



int post_send(struct resources *res, int opcode,int target_node_id,int qp_id,int offset,int len)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->res_list[target_node_id*MAX_QP_NUM+qp_id].buf+offset;
    sge.lkey = res->res_list[target_node_id*MAX_QP_NUM+qp_id].mr->lkey;
    sge.length = len;
    
 
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = (ibv_wr_opcode)opcode;
    sr.send_flags = IBV_SEND_SIGNALED;
    if(opcode != IBV_WR_SEND)
    {
        sr.wr.rdma.remote_addr = res->res_list[target_node_id*MAX_QP_NUM+qp_id].remote_props.addr+offset;
        sr.wr.rdma.rkey = res->res_list[target_node_id*MAX_QP_NUM+qp_id].remote_props.rkey;    
    }
 
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    rc = ibv_post_send(res->res_list[target_node_id*MAX_QP_NUM+qp_id].qp, &sr, &bad_wr);
    if(rc)
    {
        printf("failed to post SR, node=%d:%d ,rc=%d\n",target_node_id,qp_id,rc);
    }
   
    return rc;
}
int post_receive(struct resources *res,int target_node_id,int qp_id,int len)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->res_list[target_node_id*MAX_QP_NUM+qp_id].buf;
    sge.lkey = res->res_list[target_node_id*MAX_QP_NUM+qp_id].mr->lkey;
    sge.length = len;
 
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
 
    /* post the Receive Request to the RQ */
    rc = ibv_post_recv(res->res_list[target_node_id*MAX_QP_NUM+qp_id].qp, &rr, &bad_wr);
    if(rc)
    {
       fprintf(stderr, "failed to post RR to node:%d:%d \n",target_node_id,qp_id);
    }
    return rc;
}
void resources_init(struct resources *res){
    config.dev_name = "mlx5_0";
    config.ib_port = 1;
    config.gid_idx = 3;
    config.primary_server_name[0] = "10.118.0.52";
    config.primary_server_name[1] = "10.118.0.52";
    config.primary_server_name[2] = "10.118.0.52";
    config.primary_server_name[3] = "10.118.0.52";
    config.backup_server_name[0] = "10.118.0.52";
    config.backup_server_name[1] = "10.118.0.52";
    config.tcp_port[0] = 19900;
    config.tcp_port[1] = 19901;
    config.tcp_port[2] = 19902;
    config.tcp_port[3] = 19903;
    config.backup_tcp_port[0]=19904;
    config.backup_tcp_port[1]=19905;
}

int modify_qp_to_init(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = config.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc)
    {
        fprintf(stderr, "failed to modify QP state to INIT\n");
    }
    return rc;
}

int modify_qp_to_rst(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET ;
    // attr.port_num = config.ib_port;
    // attr.pkey_index = 0;
    // attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc)
    {
        fprintf(stderr, "failed to modify QP state to RESET\n");
    }
    return rc;
} 
/******************************************************************************
* Function: modify_qp_to_rtr
*
* Input:
* qp: QP to transition
* remote_qpn: remote QP number
* dlid: destination LID
* dgid: destination GID (mandatory for RoCEE)
*
* Output: none
*
* Returns: 0 on success, ibv_modify_qp failure code on failure
*
* Description: 
* Transition a QP from the INIT to RTR state, using the specified QP number
******************************************************************************/
int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_256;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = config.ib_port;
    if(config.gid_idx >= 0)
    {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = config.gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
 
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc)
    {
        fprintf(stderr, "failed to modify QP state to RTR\n");
    }
    return rc;
}
 
/******************************************************************************
* Function: modify_qp_to_rts
*
* Input:
* qp: QP to transition
*
* Output: none
*
* Returns: 0 on success, ibv_modify_qp failure code on failure
*
* Description: Transition a QP from the RTR to RTS state
******************************************************************************/
int modify_qp_to_rts(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x15;
    attr.retry_cnt = 6;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc)
    {
        fprintf(stderr, "failed to modify QP state to RTS\n");
    }
    return rc;
}
 
#define K 4
int resources_create(struct resources *res)
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
  
    for(int n=0;n<K;n++){
        std::cout<<"try connect to server: "<<config.primary_server_name[n]<<" : "<<config.tcp_port[n]<<std::endl;
        res->sock[n] = sock_connect(config.primary_server_name[n], config.tcp_port[n]);
        if(res->sock[n] < 0)
        {
            fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                        config.primary_server_name[n], config.tcp_port[n]);
            rc = -1;
            assert(0);
        }
    }
    for(int n=0;n<2;n++){
        std::cout<<"try connect to server: "<<config.backup_server_name[n]<<" : "<<config.backup_tcp_port[n]<<std::endl;
        res->sock[4+n] = sock_connect(config.backup_server_name[n], config.backup_tcp_port[n]);
        if(res->sock[4+n] < 0)
        {
            fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                        config.backup_server_name[n], config.backup_tcp_port[n]);
            rc = -1;
            assert(0);
        }
    }
    //int temp_sock[6];
    for(int i=0;i<6;i++){
        std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
        //temp_sock[i]=res->sock[i];
    }
    std::cout<<"searching for IB devices in host"<<std::endl;
 
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if(!dev_list)
    {
        fprintf(stderr, "failed to get IB devices list\n");
        assert(0);
    }
 
    /* if there isn't any IB device in host */
    if(!num_devices)
    {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        assert(0);
    }
    std::cout<<"find "<<num_devices<<" devices"<<std::endl;
 
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
        assert(0);
    }
 
    /* get device handle */
    global_res->ib_ctx = ibv_open_device(ib_dev);
    if(!global_res->ib_ctx)
    {
        fprintf(stderr, "failed to open device %s\n", config.dev_name);
        rc = 1;
        assert(0);
    }
 
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
 
    /* query port properties */
    if(ibv_query_port(global_res->ib_ctx, config.ib_port, &(global_res->port_attr)))
    {
        fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
        rc = 1;
        assert(0);
    }
 
    /* allocate Protection Domain */
    global_res->pd = ibv_alloc_pd(global_res->ib_ctx);
    if(!global_res->pd)
    {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        assert(0);
    }
 
    /* each side will send only one WR, so Completion Queue with 1 entry is enough */
    cq_size = 1;
    // for(int i=0;i<4;i++)
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    // for(int i=0;i<6;i++)
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    std::cout<<"create cq"<<std::endl;
    for(int n=0;n<12;n++){
        for(int i=0;i<MAX_QP_NUM;i++){
            res->res_list[n*MAX_QP_NUM+i].cq = ibv_create_cq(global_res->ib_ctx, cq_size, NULL, NULL, 0);
            if(!res->res_list[n*MAX_QP_NUM+i].cq)
            {
                fprintf(stderr, "failed to create CQ for node:%d ,qp_id=\n",n,i);
                assert(0);
            }
            //std::cout<<"cq of "<<n<<" : "<<i<<" addr = "<<res->res_list[n*MAX_QP_NUM+i].cq<<std::endl;
        }           
    }
    // for(int i=0;i<6;i++)
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    
    std::cout<<"begin to create qp"<<std::endl;
    for(int n=0;n<K*3;n++){
        for(int j=0;j<MAX_QP_NUM;j++){
            //std::cout<<"try access res->cp[] "<<n<<" : "<<j<<std::endl;
            if(!res->res_list[n*MAX_QP_NUM+j].cq){
                std::cout<<"n="<<n<<" j="<<j<<" not have cq"<<std::endl;
                assert(0);
            }
            memset(&qp_init_attr, 0, sizeof(qp_init_attr));
            qp_init_attr.qp_type = IBV_QPT_RC;
            qp_init_attr.sq_sig_all = 1;
            qp_init_attr.cap.max_send_wr = 1;
            qp_init_attr.cap.max_recv_wr = 1;
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.send_cq = res->res_list[n*MAX_QP_NUM+j].cq;
            qp_init_attr.recv_cq = res->res_list[n*MAX_QP_NUM+j].cq;
            //std::cout<<"n="<<n<<" j="<<j<<" try create qp"<<std::endl;
            res->res_list[n*MAX_QP_NUM+j].qp = ibv_create_qp(global_res->pd, &qp_init_attr);
            if(!res->res_list[n*MAX_QP_NUM+j].qp)
            {
                fprintf(stderr, "failed to create QP for node:%d : %d\n",n,j);
                assert(0);
            }
            //fprintf(stdout, "QP for node:%d : %d was created, QP number=0x%x\n",n,j, res->res_list[n*MAX_QP_NUM+j].qp->qp_num);
        }
    }
    // for(int i=0;i<6;i++){
    //     //res->sock[i]=temp_sock[i];
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    // }
    size = 128*1024;
    /* allocate the memory buffer that will hold the data */
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    std::cout<<"begin create buf and mr"<<std::endl;
    
    for(int i=0;i<K*3;i++){
        for(int j=0;j<MAX_QP_NUM;j++){
            //std::cout<<"create buf"<<std::endl;
            res->res_list[i*MAX_QP_NUM+j].buf = new char[size];
            if(!res->res_list[i*MAX_QP_NUM+j].buf)
                assert(0);
            //std::cout<<"create mr"<<std::endl;
            res->res_list[i*MAX_QP_NUM+j].mr = ibv_reg_mr(global_res->pd, res->res_list[i*MAX_QP_NUM+j].buf, size, mr_flags);
            if(!res->res_list[i*MAX_QP_NUM+j].mr)
                assert(0);
            //std::cout<<"MR for primary: "<<i<<" : "<<j<<" was registered with addr="<<res->res_list[i*MAX_QP_NUM+j].buf<<" lkey= "<<res->res_list[i*MAX_QP_NUM+j].mr->lkey<<" rkey=0x="<<res->res_list[i*MAX_QP_NUM+j].mr->rkey<<"mr_flag="<<mr_flags<<std::endl;
            //fprintf(stdout, "MR for primary: %d : %d was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                    //i,j,res->res_list[i*MAX_QP_NUM+j].buf, res->res_list[i*MAX_QP_NUM+j].mr->lkey, res->res_list[i*MAX_QP_NUM+j].mr->rkey, mr_flags);
        }
    }
    // for(int i=0;i<4;i++)
    //     for(int j=0;j<MAX_QP_NUM;j++)
    //         std::cout<<"cq of "<<i<<" : "<<j<<" addr = "<<res->res_list[i*MAX_QP_NUM+j].cq<<std::endl;
    
    
    
    // for(int i=0;i<4;i++)
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    
    // for(int i=0;i<6;i++)
    //     for(int j=0;j<MAX_QP_NUM;j++)
    //         fprintf(stdout, "MR for primary: %d : %d was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
    //                 i,j,res->res_list[i*MAX_QP_NUM+j].buf, res->res_list[i*MAX_QP_NUM+j].mr->lkey, res->res_list[i*MAX_QP_NUM+j].mr->rkey, mr_flags);
    // std::cout<<"tyr to create qp"<<std::endl;
    
    // for(int i=0;i<6;i++){
    //     res->sock[i]=temp_sock[i];
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    // }
    // std::cout<<"res create finish"<<std::endl;
    // uint32_t rkey;
    // uint64_t addr;
    // uint32_t qp_num;
    // for(int i=0;i<6;i++)
    //     for(int j=0;j<MAX_QP_NUM;j++){
    //         std::cout<<"i="<<i<<" ,j="<<j<<std::endl;
            
    //         addr = (uint64_t)res->res_list[i*MAX_QP_NUM+j].buf;
    //         fprintf(stdout, "address = %p\n",addr);
    //         rkey = res->res_list[i*MAX_QP_NUM+j].mr->rkey;
    //         fprintf(stdout, "rkey = 0x%x\n", rkey);
    //         qp_num = res->res_list[i*MAX_QP_NUM+j].qp->qp_num;
    //         fprintf(stdout, "QP number = 0x%x\n",qp_num);
    //     }
    return rc;
}

int connect_qp(struct resources *res)
{
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    // for(int i=0;i<4;i++)
    //     std::cout<<"sock["<<i<<"]="<<res->sock[i]<<std::endl;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(global_res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
        if(rc)
        {
            fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
            return rc;
        }
    }
    else
    {
        memset(&my_gid, 0, sizeof my_gid);
    }
 
    /* exchange using TCP sockets info required to connect QPs */
    

    for(int n=0;n<K*3;n++){
        for(int j=0;j<MAX_QP_NUM;j++){
            //std::cout<<"change data with node:"<<n<<" :"<<j<<std::endl;
            local_con_data.addr = htonll((uintptr_t)res->res_list[n*MAX_QP_NUM+j].buf);
            //std::cout<<"buf fine"<<std::endl;
            local_con_data.rkey = htonl(res->res_list[n*MAX_QP_NUM+j].mr->rkey); 
            //std::cout<<"rkey fine"<<std::endl;
            local_con_data.qp_num = htonl(res->res_list[n*MAX_QP_NUM+j].qp->qp_num);
            //std::cout<<"qp fine"<<std::endl;
            local_con_data.lid = htons(global_res->port_attr.lid);
            memcpy(local_con_data.gid, &my_gid, 16);
            //std::cout<<" try to sock_sync_data with node "<<n<<" : "<<j<<std::endl;
            int sock_id;
            if(n<4)
                sock_id=n;
            else
                sock_id=3+n/4;
            if(sock_sync_data(res->sock[sock_id], sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
            {
                fprintf(stderr, "failed to exchange connection data with target node:%d :%d\n",n,j);
                assert(0);
            }
            res->res_list[n*MAX_QP_NUM+j].remote_props.addr = ntohll(tmp_con_data.addr);
            res->res_list[n*MAX_QP_NUM+j].remote_props.rkey = ntohl(tmp_con_data.rkey);
            res->res_list[n*MAX_QP_NUM+j].remote_props.qp_num = ntohl(tmp_con_data.qp_num);
            res->res_list[n*MAX_QP_NUM+j].remote_props.lid = ntohs(tmp_con_data.lid);
            memcpy(res->res_list[n*MAX_QP_NUM+j].remote_props.gid, tmp_con_data.gid, 16);
            //fprintf(stdout, "node:%d :%d Remote address = 0x%p\n", n,j,res->res_list[n*MAX_QP_NUM+j].remote_props.addr);
            //fprintf(stdout, "node:%d :%d Remote rkey = 0x%x\n", n,j,res->res_list[n*MAX_QP_NUM+j].remote_props.rkey);
            //fprintf(stdout, "node:%d :%d Remote QP number = 0x%x\n", n,j,res->res_list[n*MAX_QP_NUM+j].remote_props.qp_num);
            //fprintf(stdout, "node:%d :%d Remote LID = 0x%x\n", n,j,res->res_list[n*MAX_QP_NUM+j].remote_props.lid);
            if(config.gid_idx >= 0)
            {
                uint8_t *p = res->res_list[n*MAX_QP_NUM+j].remote_props.gid;
                //fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                        //p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
            }
        }
    }
    
 
    /* modify the QP to init */
    for(int n=0;n<K*3;n++){
        for(int j=0;j<MAX_QP_NUM;j++){
            rc = modify_qp_to_init(res->res_list[n*MAX_QP_NUM+j].qp);
            if(rc)
            {
                fprintf(stderr, "change QP of node:%d : %d state to INIT failed\n",n,j);
                assert(0);
            }
        }  
    }
    /* let the client post RR to be prepared for incoming messages */
    /* modify the QP to RTR */
    for(int n=0;n<K*3;n++){
        for(int j=0;j<MAX_QP_NUM;j++){
            //std::cout<<"modify qp to rtr for "<<n<<" : "<<j<<std::endl;
            rc = modify_qp_to_rtr(res->res_list[n*MAX_QP_NUM+j].qp, res->res_list[n*MAX_QP_NUM+j].remote_props.qp_num, res->res_list[n*MAX_QP_NUM+j].remote_props.lid, res->res_list[n*MAX_QP_NUM+j].remote_props.gid);
            if(rc)
            {
                fprintf(stderr, "failed to modify QP of node:%d :%d state to RTR\n",n,j);
                assert(0);
            }
            /* modify the QP to RTS */
            rc = modify_qp_to_rts(res->res_list[n*MAX_QP_NUM+j].qp);
            if(rc)
            {
                fprintf(stderr, "failed to modify QP of node:%d :%d state to RTS\n",n,j);
                assert(0);
            }
            //fprintf(stdout, "QP state of node:%d :%d was change to RTS\n",n,j);
        }
        
    }
    /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
    for(int n=0;n<K+2;n++){
        if(sock_sync_data(res->sock[n], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error of node:%d after QPs are were moved to RTS\n",n);
            rc = 1;
        }
    }
    
connect_qp_exit:
    return rc;
}



int reconnect_qp(struct resources *res,int target_node_id){

    std::cout<<"reconnect target node id = "<<target_node_id <<std::endl;
    for(int j=0;j<MAX_QP_NUM;j++){
        if(res->res_list[target_node_id*MAX_QP_NUM+j].qp)
        {
            std::cout<<"destroy qp"<<std::endl;
            if(ibv_destroy_qp(res->res_list[target_node_id*MAX_QP_NUM+j].qp))
            {
                fprintf(stderr, "failed to destroy QP[%d*MAX_QP_NUM+%d]\n",target_node_id,j);
                assert(0);
            }
        }
    }
    // if(res->res_list[target_node_id])
    // {
    //     std::cout<<"destroy cq"<<std::endl;
    //     if(ibv_destroy_cq(res->res_list[target_node_id]))
    //     {
    //         fprintf(stderr, "failed to destroy CQ[%d]\n",target_node_id);
    //         assert(0);
    //     }
    // }

    /*
    // std::cout<<"destroy mr"<<std::endl;
    // if(res->res_list[mem_id])
    // {
    //     if(ibv_dereg_mr(res->res_list[mem_id]))
    //     {
    //         fprintf(stderr, "failed to deregister MR[%d]\n",mem_id);
    //         assert(0);
    //     }
    // }
    // std::cout<<"destroy msg_mr"<<std::endl;
    // if(res->msg_mr[mem_id])
    // {
    //     if(ibv_dereg_mr(res->msg_mr[mem_id]))
    //     {
    //         fprintf(stderr, "failed to deregister MR_MSG[%d]\n",mem_id);
    //         assert(0);
    //     }
    // }
    // */
    // std::cout<<"create cq"<<std::endl;
    // int cq_size = 1;
    // res->res_list[target_node_id] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    // if(!res->res_list[target_node_id])
    // {
    //     fprintf(stderr, "failed to create CQ with %u entries for node:%d\n", 1,target_node_id);
    //     assert(0);
    // }
    // std::cout<<"create cq finish"<<std::endl;
    // /*
    // int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    // res->res_list[mem_id] = ibv_reg_mr(res->pd, [mem_id], MAX_SGE_SIZE, mr_flags);
    // res->msg_mr[mem_id] = ibv_reg_mr(res->pd, res->msg_buf[mem_id], MAX_MSG_SIZE, mr_flags);
    // */
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    for(int j=0;j<MAX_QP_NUM;j++){
        qp_init_attr.send_cq = res->res_list[target_node_id*MAX_QP_NUM+j].cq;
        qp_init_attr.recv_cq = res->res_list[target_node_id*MAX_QP_NUM+j].cq;
        res->res_list[target_node_id*MAX_QP_NUM+j].qp = ibv_create_qp(global_res->pd, &qp_init_attr);
        std::cout<<"create new qp for node:"<<target_node_id<<" ,qp_id="<<j<<" ,qp number= "<<res->res_list[target_node_id*MAX_QP_NUM+j].qp->qp_num<<std::endl;
    }
       
    //modify_qp_to_rst(res->res_list[target_node_id]);
    std::cout<<"close sock"<<std::endl;
    int sock_id=target_node_id;
 
    if(close(res->sock[sock_id])){
        fprintf(stderr, "failed to close socket in reconnect\n");
    }
    config.primary_server_name[target_node_id] = config.extra_server_name[0];
    
        std::cout<<"connect to "<<config.primary_server_name[target_node_id]<< " : "<<config.tcp_port[target_node_id]<<std::endl;
        res->sock[target_node_id] = sock_connect(config.primary_server_name[target_node_id], config.tcp_port[target_node_id]);
        std::cout<<"sock_connect finish"<<std::endl;
        if(res->sock[target_node_id] < 0)
        {
            fprintf(stderr, "failed to re_establish TCP connection to server %s, port %d\n",
                    config.primary_server_name[target_node_id], config.tcp_port[target_node_id]);
           
            assert(0);
            return -1;
        }
    
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(global_res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
        if(rc)
        {
            fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
            assert(0);
            return rc;
        }
    }
    else
    {
        memset(&my_gid, 0, sizeof my_gid);
    }
    for(int j=0;j<MAX_QP_NUM;j++){
        local_con_data.addr = htonll((uintptr_t)res->res_list[target_node_id*MAX_QP_NUM+j].buf);
        local_con_data.rkey = htonl(res->res_list[target_node_id*MAX_QP_NUM+j].mr->rkey);
        local_con_data.qp_num = htonl(res->res_list[target_node_id*MAX_QP_NUM+j].qp->qp_num);
        local_con_data.lid = htons(global_res->port_attr.lid);
        memcpy(local_con_data.gid, &my_gid, 16);
        fprintf(stdout, "\nLocal LID = 0x%x\n", global_res->port_attr.lid);
        if(sock_sync_data(res->sock[target_node_id], sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
        {
            fprintf(stderr, "failed to exchange connection data with target node:%d :%d\n",target_node_id,j);
            assert(0);
        }
        res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.addr = ntohll(tmp_con_data.addr);
        res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.rkey = ntohl(tmp_con_data.rkey);
        res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.qp_num = ntohl(tmp_con_data.qp_num);
        res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.lid = ntohs(tmp_con_data.lid);
        memcpy(res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.gid, tmp_con_data.gid, 16);
        
            /* save the remote side attributes, we will need it for the post SR */
            //res->remote_props = remote_con_data;
        fprintf(stdout, "node:%d :%d Remote address = 0x%p\n", target_node_id,j,res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.addr);
        fprintf(stdout, "node:%d :%d Remote rkey = 0x%x\n", target_node_id,j,res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.rkey);
        fprintf(stdout, "node:%d :%d Remote QP number = 0x%x\n", target_node_id,j,res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.qp_num);
        fprintf(stdout, "node:%d :%d Remote LID = 0x%x\n", target_node_id,j,res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.lid);
        if(config.gid_idx >= 0)
        {
            uint8_t *p = res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.gid;
            fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[MAX_QP_NUM], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
        }
    }
    
    for(int j=0;j<MAX_QP_NUM;j++){
         rc = modify_qp_to_init(res->res_list[target_node_id*MAX_QP_NUM+j].qp);
        if(rc)
        {
            fprintf(stderr, "change QP of node:%d :%d state to INIT failed\n",target_node_id,j);
            assert(0);
        }
    
        rc = modify_qp_to_rtr(res->res_list[target_node_id*MAX_QP_NUM+j].qp, res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.qp_num, res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.lid, res->res_list[target_node_id*MAX_QP_NUM+j].remote_props.gid);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d :%d state to RTR\n",target_node_id,j);
            assert(0);
        }
        rc = modify_qp_to_rts(res->res_list[target_node_id*MAX_QP_NUM+j].qp);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d :%d state to RTS\n",target_node_id,j);
            assert(0);
        }
        fprintf(stdout, "QP state of node:%d :%d was change to RTS\n",target_node_id,j);    
    }
    
    if(sock_sync_data(res->sock[target_node_id], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error of node:%d after QPs are were moved to RTS\n",target_node_id);
        assert(0);
    }
    // if(poll_completion(res,target_node_id,1022))
    // {
    //     fprintf(stderr, "poll completion failed\n");
    //     //assert(0);
    // }
    //printf("recv msg from extra is: %s",[mem_id]);
    uint32_t opcode = 4;
    for(int j=0;j<MAX_QP_NUM;j++){
        memcpy(res->res_list[target_node_id*MAX_QP_NUM+j].buf,&opcode,4);
        post_send(res,IBV_WR_SEND,target_node_id,j,0,4);
        poll_completion_quick(res,target_node_id,j);
    }
    std::cout<<"reconnect finish"<<std::endl;
    return rc;   
}