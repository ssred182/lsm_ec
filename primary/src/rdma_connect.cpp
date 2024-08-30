
#include "rdma_connect.h"
#include <iostream>
/* poll CQ timeout in millisec (2 seconds) */
#include <assert.h>
#include "define.h"
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
 
/* structure of test parameters */

 
/* structure to exchange data which is needed to connect the QPs */


 
/******************************************************************************
Socket operations:
For simplicity, the example program uses TCP sockets to exchange control
information. If a TCP/IP stack/connection is not available, connection manager
(CM) may be used to pass this information. Use of CM is beyond the scope of
this example
******************************************************************************/
/******************************************************************************
* Function: sock_connect
* Input:
* servername: URL of server to connect to (NULL for server mode)
* port: port of service
*
* Output:none
*
* Returns: socket (fd) on success, negative error code on failure
*
* Description:
* Connect a socket. If servername is specified a client connection will be
* initiated to the indicated server and port. Otherwise listen on the
* indicated port for an incoming connection.
*
******************************************************************************/
struct config_t config;
int K;
//#define K 4
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
                    ;
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
                    std::cout<<"bind fail"<<std::endl;
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
    }
 
    if(resolved_addr)
    {
        freeaddrinfo(resolved_addr);
    }
 
    if(sockfd < 0)
    {
        if(servername)
        {
            fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        }
        else
        {
            perror("server accept");
            fprintf(stderr, "accept() failed\n");
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
    int rc;
    int read_bytes = 0;
    int total_read_bytes = 0;
    rc = write(sock, local_data, xfer_size);
 
    if(rc < xfer_size)
    {
        fprintf(stderr, "Failed writing data during sock_sync_data\n");
        std::cout<<"error!! fail to write in sock_sync_data, rc="<<rc<<std::endl;
    }
    else
    {
        rc = 0;
    }
 
    while(!rc && total_read_bytes < xfer_size)
    {
        read_bytes = read(sock, remote_data, xfer_size);
        if(read_bytes > 0)
        {
            total_read_bytes += read_bytes;
        }
        else
        {
            std::cout<<"read in sock_sync_data fail"<<std::endl;
            rc = read_bytes;
        }
    }
    return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/
 
/* poll_completion */
/******************************************************************************
* Function: poll_completion
*
* Input:
* res: pointer to resources structure
*
* Output: none
*
* Returns: 0 on success, 1 on failure
*
* Description:
* Poll the completion queue for a single event. This function will continue to
* poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
*
******************************************************************************/
int poll_completion(struct resources *res,int target_node_id,int line)
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
        if(target_node_id==10)
            poll_result = ibv_poll_cq(res->master_cq, 1, &wc);
        else if(target_node_id>=11)
            poll_result = ibv_poll_cq(res->extra_cq[target_node_id-11], 1, &wc);
        else
            poll_result = ibv_poll_cq(res->cq[target_node_id], 1, &wc);
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    }
    while((poll_result == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
 
    if(poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if(poll_result == 0)
    {
        /* the CQ is empty */
       printf("node: %d completion wasn't found in the CQ after timeout,line= %d\n",target_node_id,line);
        rc = 1;
    }
    else
    {
        /* CQE found */
        //fprintf(stdout, "node: %d completion was found in CQ with status 0x%x\n",target_node_id,wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if(wc.status != IBV_WC_SUCCESS)
        {
           printf("got bad completion with status: 0x%x, vendor syndrome: 0x%x ,line= %d\n", 
					wc.status, wc.vendor_err,line);
            rc = 1;
        }
    }
    return rc;
}

int poll_completion_quick(struct resources *res,int target_node_id,int line)
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
        if(target_node_id==10)
            poll_result = ibv_poll_cq(res->master_cq, 1, &wc);
        else if(target_node_id>=11)
            poll_result = ibv_poll_cq(res->extra_cq[target_node_id-11], 1, &wc);
        else
            poll_result = ibv_poll_cq(res->cq[target_node_id], 1, &wc);
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
       printf("node: %d completion wasn't found in the CQ after quick timeout,line= %d\n",target_node_id,line);
        rc = 1;
    }
    else
    {
        /* CQE found */
        //fprintf(stdout, "node: %d completion was found in CQ with status 0x%x\n",target_node_id,wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if(wc.status != IBV_WC_SUCCESS)
        {
           fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x ,line= %d\n", 
					wc.status, wc.vendor_err,line);
            rc = 1;
        }
    }
    return rc;
}


/******************************************************************************
* Function: post_send
*
* Input:
* res: pointer to resources structure
* opcode: IBV_WR_SEND, IBV_WR_RDMA_READ or IBV_WR_RDMA_WRITE
*
* Output: none
*
* Returns: 0 on success, error code on failure
*
* Description: This function will create and post a send work request
******************************************************************************/
int post_send(struct resources *res, int opcode,int target_node_id,int buf_id,int offset,int len,int line)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    if(target_node_id==10){
        sge.addr = (uintptr_t)res->master_buf+offset;
        sge.lkey = res->master_mr->lkey;
    }
    else if(target_node_id>=11){
        sge.addr = (uintptr_t)res->extra_buf[target_node_id-11]+offset;
        sge.lkey = res->extra_mr[target_node_id-11]->lkey;
    }
    else{
        sge.addr = (uintptr_t)res->buf[buf_id]+offset;
        sge.lkey = res->mr[buf_id]->lkey;
    }
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
        if(target_node_id==10){
            sr.wr.rdma.remote_addr = res->master_remote_props.addr+offset;
            sr.wr.rdma.rkey = res->master_remote_props.rkey;
        }
        else if(target_node_id>=11){
            sr.wr.rdma.remote_addr = res->extra_remote_props[target_node_id-11].addr+offset;
            sr.wr.rdma.rkey = res->extra_remote_props[target_node_id-11].rkey;
        }
        else{
            sr.wr.rdma.remote_addr = res->remote_props[target_node_id].addr+offset;
            sr.wr.rdma.rkey = res->remote_props[target_node_id].rkey;
        }
    }
 
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    if(target_node_id == 10)
        rc = ibv_post_send(res->master_qp, &sr, &bad_wr);
    else if(target_node_id >= 11)
        rc = ibv_post_send(res->extra_qp[target_node_id-11], &sr, &bad_wr);
    else
        rc = ibv_post_send(res->qp[target_node_id], &sr, &bad_wr);
    if(rc)
    {
        printf("failed to post SR, line= %d,node=%d,rc=%d\n",line,target_node_id,rc);
    }
   
    return rc;
}

int post_send_msg(struct resources *res, int opcode,int target_node_id,int buf_id,int offset,int len,int line)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    if(target_node_id==10){
        sge.addr = (uintptr_t)res->master_msg_buf+offset;
        sge.lkey = res->master_msg_mr->lkey;
    }
    else if(target_node_id==11){
        sge.addr = (uintptr_t)res->gc_msg_buf+offset;
        sge.lkey = res->gc_msg_mr->lkey;
    }else
    {
        sge.addr = (uintptr_t)res->msg_buf[buf_id]+offset;
        sge.lkey = res->msg_mr[buf_id]->lkey;
    }
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
        if(target_node_id==10){
            sr.wr.rdma.remote_addr = res->master_remote_props.addr+offset;
            sr.wr.rdma.rkey = res->master_remote_props.rkey;
        }else{
            sr.wr.rdma.remote_addr = res->remote_props[target_node_id].addr+offset;
            sr.wr.rdma.rkey = res->remote_props[target_node_id].rkey;
        }
        
    }
 
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    if(target_node_id == 10)
        rc = ibv_post_send(res->master_qp, &sr, &bad_wr);
    else if(target_node_id ==11)
        rc = ibv_post_send(res->qp[0], &sr, &bad_wr);
    else
        rc = ibv_post_send(res->qp[target_node_id], &sr, &bad_wr);
    if(rc)
    {
        printf( "failed to post_msg SR, line= %d,node=%d,rc=%d\n",line,target_node_id,rc);
    }
 
    return rc;
}
/******************************************************************************
* Function: post_receive
*
* Input:
* res: pointer to resources structure
*
* Output: none
*
* Returns: 0 on success, error code on failure
*
* Description: post RR to be prepared for incoming messages
*
******************************************************************************/
int post_send_gc_msg(struct resources *res, int opcode,int target_node_id,uint32_t offset,uint32_t len,int line)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(res->gc_msg_buf+offset);
    sge.length = len;
    sge.lkey = res->gc_msg_mr->lkey;
 
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
        sr.wr.rdma.remote_addr = res->remote_props[target_node_id].addr+offset;
        sr.wr.rdma.rkey = res->remote_props[target_node_id].rkey;
    }
 
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    if(target_node_id == 10)
        rc = ibv_post_send(res->master_qp, &sr, &bad_wr);
    else
        rc = ibv_post_send(res->qp[target_node_id], &sr, &bad_wr);
    if(rc)
    {
        fprintf(stderr, "failed to post gc SR,rc = %d , line= %d\n",rc,line);
    }
    return rc;
}
int post_receive_gc_msg(struct resources *res,int target_node_id,int len,int line)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->gc_msg_buf;
    sge.length = len;
    sge.lkey = res->gc_msg_mr->lkey;
 
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
 
    /* post the Receive Request to the RQ */
    rc = ibv_post_recv(res->qp[target_node_id], &rr, &bad_wr);
    if(rc)
    {
        fprintf(stderr, "failed to post RR to node:%d ,line= %d\n",target_node_id,line);
    }
    return rc;
}




int post_receive(struct resources *res,int target_node_id,int buf_id,int len,int line)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    if(target_node_id==10){
        sge.addr = (uintptr_t)res->master_buf;
        sge.lkey = res->master_mr->lkey;
    }
    else if(target_node_id>=11){
        sge.addr = (uintptr_t)res->extra_buf[target_node_id-11];
        sge.lkey = res->extra_mr[target_node_id-11]->lkey;
    }
    else{
        sge.addr = (uintptr_t)res->buf[buf_id];
        sge.lkey = res->mr[buf_id]->lkey;
    }
    sge.length = len;
 
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
 
    /* post the Receive Request to the RQ */
    if(target_node_id==10)
        rc = ibv_post_recv(res->master_qp, &rr, &bad_wr);
    else if(target_node_id>=11){
        rc = ibv_post_recv(res->extra_qp[target_node_id-11], &rr, &bad_wr);
    }
    else
        rc = ibv_post_recv(res->qp[target_node_id], &rr, &bad_wr);
    if(rc)
    {
       fprintf(stderr, "failed to post RR to node:%d ,line= %d\n",target_node_id,line);
    }
    return rc;
}

int post_receive_msg(struct resources *res,int target_node_id,int buf_id,int len,int line)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    if(target_node_id==10){
        sge.addr = (uintptr_t)res->master_msg_buf;
        sge.lkey = res->master_msg_mr->lkey;
    }else if(target_node_id==11){
        sge.addr = (uintptr_t)res->gc_msg_buf;
        sge.lkey = res->gc_msg_mr->lkey;
    }
    else{
        sge.addr = (uintptr_t)res->msg_buf[buf_id];
        sge.lkey = res->msg_mr[buf_id]->lkey;
    }
    sge.length = len;
 
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
 
    /* post the Receive Request to the RQ */
    if(target_node_id==10)
        rc = ibv_post_recv(res->master_qp, &rr, &bad_wr);
    else if(target_node_id==11){
        rc = ibv_post_recv(res->qp[0], &rr, &bad_wr);
    }else
        rc = ibv_post_recv(res->qp[target_node_id], &rr, &bad_wr);
    if(rc)
    {
        fprintf(stderr, "failed to post RR to node:%d ,line= %d\n",target_node_id,line);
    }

    return rc;
}
 
/******************************************************************************
* Function: resources_init
*
* Input:
* res: pointer to resources structure
*
* Output: res is initialized
*
* Returns: none
*
* Description: res is initialized to default values
******************************************************************************/
void resources_init(struct resources *res)
{
    memset(res, 0, sizeof *res);
    for(int i=0;i<K;i++)
        res->sock[i] = -1;
    if(config.server_name[0]){
        for(int i=1;i<K;i++)
            config.tcp_port[i]=config.tcp_port[i-1]+4;
        for(int i=0;i<K;i++)
            config.tcp_port[i]+=config.node_id;
        config.master_tcp_port += config.node_id;
    }
    else{
        for(int i=1;i<K;i++)
            config.tcp_port[i]=config.tcp_port[i-1]+1;
        for(int i=0;i<K;i++)
            config.tcp_port[i]+=K*config.node_id;
        config.master_tcp_port += config.node_id+4;
    }
    
}
 
/*
client:
for(int i=1;i<K;i++)
        config.tcp_port[i]=config.tcp_port[i-1]+K;
for(int i=0;i<K;i++)
        config.tcp_port[i]+=config.node_id;
server:
for(int i=1;i<K;i++)
        config.tcp_port[i]=config.tcp_port[i-1]+1;
for(int i=0;i<K;i++)
        config.tcp_port[i]+=K*config.node_id;
*/

/******************************************************************************
* Function: resources_create
*
* Input: res pointer to resources structure to be filled in
*
* Output: res filled in with resources
*
* Returns: 0 on success, 1 on failure
*
* Description:
* This function creates and allocates all necessary system resources. These
* are stored in res.
*****************************************************************************/

 
/******************************************************************************
* Function: modify_qp_to_init
*
* Input:
* qp: QP to transition
*
* Output: none
*
* Returns: 0 on success, ibv_modify_qp failure code on failure
*
* Description: Transition a QP from the RESET to INIT state
******************************************************************************/
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
    attr.timeout = 0x18;
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
 
/******************************************************************************
* Function: connect_qp
*
* Input:
* res: pointer to resources structure
*
* Output: none
*
* Returns: 0 on success, error code on failure
*
* Description: 
* Connect the QP. Transition the server side to RTR, sender side to RTS
******************************************************************************/
int reconnect_qp(struct resources *res,int target_node_id){

    std::cout<<"target node id = "<<target_node_id <<std::endl;
    
    if(res->qp[target_node_id])
    {
        std::cout<<"destroy qp"<<std::endl;
        if(ibv_destroy_qp(res->qp[target_node_id]))
        {
            fprintf(stderr, "failed to destroy QP[%d]\n",target_node_id);
            assert(0);
        }
    }
    
    // if(res->cq[target_node_id])
    // {
    //     std::cout<<"destroy cq"<<std::endl;
    //     if(ibv_destroy_cq(res->cq[target_node_id]))
    //     {
    //         fprintf(stderr, "failed to destroy CQ[%d]\n",target_node_id);
    //         assert(0);
    //     }
    // }
    uint32_t mem_id;
    if(!config.server_name[0])
        mem_id = target_node_id;
    else
        mem_id = 0;
    /*
    // std::cout<<"destroy mr"<<std::endl;
    // if(res->mr[mem_id])
    // {
    //     if(ibv_dereg_mr(res->mr[mem_id]))
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
    // res->cq[target_node_id] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    // if(!res->cq[target_node_id])
    // {
    //     fprintf(stderr, "failed to create CQ with %u entries for node:%d\n", 1,target_node_id);
    //     assert(0);
    // }
    // std::cout<<"create cq finish"<<std::endl;
    // /*
    // int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
    // res->mr[mem_id] = ibv_reg_mr(res->pd, res->buf[mem_id], MAX_SGE_SIZE, mr_flags);
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
    qp_init_attr.send_cq = res->cq[target_node_id];
    qp_init_attr.recv_cq = res->cq[target_node_id];
    std::cout<<"create qp"<<std::endl;
    res->qp[target_node_id] = ibv_create_qp(res->pd, &qp_init_attr);

    //modify_qp_to_rst(res->qp[target_node_id]);
    std::cout<<"close sock"<<std::endl;
    if(close(res->sock[target_node_id])){
        fprintf(stderr, "failed to close socket in reconnect\n");
    }
    if(config.server_name[0])
    {
        std::cout<<"connect to tcp_port: "<<config.tcp_port[target_node_id]<<std::endl;
        res->sock[target_node_id] = sock_connect(config.server_name[target_node_id], config.tcp_port[target_node_id]);
        std::cout<<"sock_connect finish"<<std::endl;
        if(res->sock[target_node_id] < 0)
        {
            fprintf(stderr, "failed to re_establish TCP connection to server %s, port %d\n",
                    config.server_name[target_node_id], config.tcp_port[target_node_id]);
           
            assert(0);
            return -1;
        }
    }
    else
    { 
        fprintf(stdout, "waiting on port %d for TCP connection\n", config.tcp_port[target_node_id]);
        res->sock[target_node_id] = sock_connect(NULL, config.tcp_port[target_node_id]);
        std::cout<<"sock_connect finish"<<std::endl;
        if(res->sock[target_node_id] < 0)
        {
            fprintf(stderr, "failed to re_establish TCP connection with client on port %d\n",
                        config.tcp_port[target_node_id]);
            
            assert(0);
            return -1;
        }
        fprintf(stdout, "TCP port: %d connection was established \n",config.tcp_port[target_node_id]);
               
    }
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
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
    local_con_data.addr = htonll((uintptr_t)res->buf[target_node_id]);
    local_con_data.rkey = htonl(res->mr[target_node_id]->rkey);
    local_con_data.qp_num = htonl(res->qp[target_node_id]->qp_num);
    local_con_data.lid = htons(res->port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);
    fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
    if(sock_sync_data(res->sock[target_node_id], sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
    {
        fprintf(stderr, "failed to exchange connection data with target node:%d\n",target_node_id);
        rc = 1;
        assert(0);
        return -1;
    }
    res->remote_props[target_node_id].addr = ntohll(tmp_con_data.addr);
    res->remote_props[target_node_id].rkey = ntohl(tmp_con_data.rkey);
    res->remote_props[target_node_id].qp_num = ntohl(tmp_con_data.qp_num);
    res->remote_props[target_node_id].lid = ntohs(tmp_con_data.lid);
    memcpy(res->remote_props[target_node_id].gid, tmp_con_data.gid, 16);
    
        /* save the remote side attributes, we will need it for the post SR */
        //res->remote_props = remote_con_data;
    fprintf(stdout, "node:%d Remote address = 0x%"PRIx64"\n", target_node_id,res->remote_props[target_node_id].addr);
    fprintf(stdout, "node:%d Remote rkey = 0x%x\n", target_node_id,res->remote_props[target_node_id].rkey);
    fprintf(stdout, "node:%d Remote QP number = 0x%x\n", target_node_id,res->remote_props[target_node_id].qp_num);
    fprintf(stdout, "node:%d Remote LID = 0x%x\n", target_node_id,res->remote_props[target_node_id].lid);
    if(config.gid_idx >= 0)
    {
        uint8_t *p = res->remote_props[target_node_id].gid;
        fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
    }
    rc = modify_qp_to_init(res->qp[target_node_id]);
    if(rc)
    {
        fprintf(stderr, "change QP of node:%d state to INIT failed\n",target_node_id);
        assert(0);
        return -1;
    }
    if(!config.server_name[0])
        rc = post_receive(res,target_node_id,mem_id,37,745);   
    std::cout<<"post receive to node: "<<mem_id<<std::endl;
    if(rc)
    {
        fprintf(stderr, "fail to post recv to node:%d\n",target_node_id);
        assert(0);
        return -1;
    }
    rc = modify_qp_to_rtr(res->qp[target_node_id], res->remote_props[target_node_id].qp_num, res->remote_props[target_node_id].lid, res->remote_props[target_node_id].gid);
    if(rc)
    {
        fprintf(stderr, "failed to modify QP of node:%d state to RTR\n",target_node_id);
        assert(0);
        return -1;
    }
    
        /* modify the QP to RTS */
    rc = modify_qp_to_rts(res->qp[target_node_id]);
    if(rc)
    {
        fprintf(stderr, "failed to modify QP of node:%d state to RTS\n",target_node_id);
        assert(0);
        return -1;
    }
    fprintf(stdout, "QP state of node:%d was change to RTS\n",target_node_id);     
    if(sock_sync_data(res->sock[target_node_id], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error of node:%d after QPs are were moved to RTS\n",target_node_id);
        rc = 1;
        assert(0);
    }
    // if(poll_completion(res,target_node_id,1022))
    // {
    //     fprintf(stderr, "poll completion failed\n");
    //     //assert(0);
    // }
    //printf("recv msg from extra is: %s",res->buf[mem_id]);
    std::cout<<"reconnect finish"<<std::endl;
    return rc;   
}
int connect_qp(struct resources *res)
{
    struct cm_con_data_t local_con_data[K];
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
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
    

    for(int n=0;n<K;n++){
        if(config.server_name[n]){
            local_con_data[n].addr = htonll((uintptr_t)res->buf[0]);
            local_con_data[n].rkey = htonl(res->mr[0]->rkey);
        }
        else{
            local_con_data[n].addr = htonll((uintptr_t)res->buf[n]);
            local_con_data[n].rkey = htonl(res->mr[n]->rkey);
            
        }
        local_con_data[n].qp_num = htonl(res->qp[n]->qp_num);
        local_con_data[n].lid = htons(res->port_attr.lid);
        memcpy(local_con_data[n].gid, &my_gid, 16);
        if(n==0)
            fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
        if(sock_sync_data(res->sock[n], sizeof(struct cm_con_data_t), (char *) &local_con_data[n], (char *) &tmp_con_data) < 0)
        {
            fprintf(stderr, "failed to exchange connection data with target node:%d\n",n);
            rc = 1;
            goto connect_qp_exit;
        }
    
        res->remote_props[n].addr = ntohll(tmp_con_data.addr);
        res->remote_props[n].rkey = ntohl(tmp_con_data.rkey);
        res->remote_props[n].qp_num = ntohl(tmp_con_data.qp_num);
        res->remote_props[n].lid = ntohs(tmp_con_data.lid);
        memcpy(res->remote_props[n].gid, tmp_con_data.gid, 16);
    
        /* save the remote side attributes, we will need it for the post SR */
        //res->remote_props = remote_con_data;
        fprintf(stdout, "node:%d Remote address = 0x%p\n", n,res->remote_props[n].addr);
        fprintf(stdout, "node:%d Remote rkey = 0x%x\n", n,res->remote_props[n].rkey);
        fprintf(stdout, "node:%d Remote QP number = 0x%x\n", n,res->remote_props[n].qp_num);
        fprintf(stdout, "node:%d Remote LID = 0x%x\n", n,res->remote_props[n].lid);
        if(config.gid_idx >= 0)
        {
            uint8_t *p = res->remote_props[n].gid;
            fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
        }
    }
    
 
    /* modify the QP to init */
    for(int n=0;n<K;n++){
        rc = modify_qp_to_init(res->qp[n]);
        if(rc)
        {
            fprintf(stderr, "change QP of node:%d state to INIT failed\n",n);
            goto connect_qp_exit;
        }
    }
    
 
    /* let the client post RR to be prepared for incoming messages */
    if(!config.server_name[0])
    {
        for(int n=0;n<K;n++){
            rc = post_receive(res,n,n,37,853);
            
            if(rc)
            {
                fprintf(stderr, "failed to post RR to node:%d\n",n);
                goto connect_qp_exit;
            }
        } 
    }
 
    /* modify the QP to RTR */
    for(int n=0;n<K;n++){
        rc = modify_qp_to_rtr(res->qp[n], res->remote_props[n].qp_num, res->remote_props[n].lid, res->remote_props[n].gid);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d state to RTR\n",n);
            goto connect_qp_exit;
        }
    
        /* modify the QP to RTS */
        rc = modify_qp_to_rts(res->qp[n]);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d state to RTS\n",n);
            goto connect_qp_exit;
        }
        fprintf(stdout, "QP state of node:%d was change to RTS\n",n);
    }
    
 
    /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
    for(int n=0;n<K;n++){
        if(sock_sync_data(res->sock[n], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error of node:%d after QPs are were moved to RTS\n",n);
            rc = 1;
        }
    }
    
 
connect_qp_exit:
    return rc;
}




int connect_qp_for_master(struct resources *res)
{
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
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
        local_con_data.addr = htonll((uintptr_t)res->master_msg_buf);
        local_con_data.rkey = htonl(res->master_msg_mr->rkey);
        local_con_data.qp_num = htonl(res->master_qp->qp_num);
        local_con_data.lid = htons(res->port_attr.lid);
        memcpy(local_con_data.gid, &my_gid, 16);
        fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
        if(sock_sync_data(res->master_sock, sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
        {
            fprintf(stderr, "failed to exchange connection data with master\n");
            rc = 1;
            goto connect_qp_exit;
        }
    
        res->master_remote_props.addr = ntohll(tmp_con_data.addr);
        res->master_remote_props.rkey = ntohl(tmp_con_data.rkey);
        res->master_remote_props.qp_num = ntohl(tmp_con_data.qp_num);
        res->master_remote_props.lid = ntohs(tmp_con_data.lid);
        memcpy(res->master_remote_props.gid, tmp_con_data.gid, 16);
        /* save the remote side attributes, we will need it for the post SR */
        //res->remote_props = remote_con_data;
        fprintf(stdout, "node:%d Remote address = 0x%"PRIx64"\n", 10,res->master_remote_props.addr);
        fprintf(stdout, "node:%d Remote rkey = 0x%x\n", 10,res->master_remote_props.rkey);
        fprintf(stdout, "node:%d Remote QP number = 0x%x\n", 10,res->master_remote_props.qp_num);
        fprintf(stdout, "node:%d Remote LID = 0x%x\n", 10,res->master_remote_props.lid);
        if(config.gid_idx >= 0)
        {
            uint8_t *p = res->master_remote_props.gid;
            fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
        }
    /* modify the QP to init */
        rc = modify_qp_to_init(res->master_qp);
        if(rc)
        {
            fprintf(stderr, "change QP of master state to INIT failed\n");
            goto connect_qp_exit;
        }
    /* let the client post RR to be prepared for incoming messages */
    if(config.master_server_name)
    {
        
        rc = post_receive(res,10,10,4,964);
        if(rc)
        {
            fprintf(stderr, "failed to post RR to master\n");
            goto connect_qp_exit;
        }
        
    }
 
    /* modify the QP to RTR */
    
        rc = modify_qp_to_rtr(res->master_qp, res->master_remote_props.qp_num, res->master_remote_props.lid, res->master_remote_props.gid);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d state to RTR\n",10);
            goto connect_qp_exit;
        }
    
        /* modify the QP to RTS */
        rc = modify_qp_to_rts(res->master_qp);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of master state to RTS\n");
            goto connect_qp_exit;
        }
        fprintf(stdout, "QP state of master was change to RTS\n");
    
    
 
    /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
    
        if(sock_sync_data(res->master_sock, 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error of master after QPs are were moved to RTS\n");
            rc = 1;
        }
    
    
 
connect_qp_exit:
    return rc;
}


int connect_qp_for_client(struct resources *res)
{
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
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
    for(int i=0;i<MAX_QP_NUM;i++){
    /* exchange using TCP sockets info required to connect QPs */
        local_con_data.addr = htonll((uintptr_t)res->client_buf[i]);
        local_con_data.rkey = htonl(res->client_mr[i]->rkey);
        local_con_data.qp_num = htonl(res->client_qp[i]->qp_num);
        local_con_data.lid = htons(res->port_attr.lid);
        memcpy(local_con_data.gid, &my_gid, 16);
        fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
        if(sock_sync_data(res->client_sock, sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
        {
            std::cout<<"failed to exchange connection data with client["<<i<<std::endl;
            assert(0);
        }
        res->client_remote_props[i].addr = ntohll(tmp_con_data.addr);
        res->client_remote_props[i].rkey = ntohl(tmp_con_data.rkey);
        res->client_remote_props[i].qp_num = ntohl(tmp_con_data.qp_num);
        res->client_remote_props[i].lid = ntohs(tmp_con_data.lid);
        memcpy(res->client_remote_props[i].gid, tmp_con_data.gid, 16);
        /* save the remote side attributes, we will need it for the post SR */
        //res->remote_props = remote_con_data;
        fprintf(stdout, "qp_id:%d Remote address = 0x%"PRIx64"\n", i,res->client_remote_props[i].addr);
        fprintf(stdout, "qp_id:%d Remote rkey = 0x%x\n", i,res->client_remote_props[i].rkey);
        fprintf(stdout, "qp_id:%d Remote QP number = 0x%x\n", i,res->client_remote_props[i].qp_num);
        fprintf(stdout, "qp_id:%d Remote LID = 0x%x\n", i,res->client_remote_props[i].lid);
        if(config.gid_idx >= 0)
        {
            uint8_t *p = res->client_remote_props[i].gid;
            fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
        }
    }
    std::cout<<"finish change data and begin to modify qp"<<std::endl;
    for(int i=0;i<MAX_QP_NUM;i++){
         rc = modify_qp_to_init(res->client_qp[i]);
        if(rc)
        {
            fprintf(stderr, "change QP[%d] of client state to INIT failed\n",i);
            assert(0);
        }
    /* let the client post RR to be prepared for incoming messages */
    /* modify the QP to RTR */   
        rc = modify_qp_to_rtr(res->client_qp[i], res->client_remote_props[i].qp_num, res->client_remote_props[i].lid, res->client_remote_props[i].gid);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP[%d] state to RTR\n",i);
            assert(0);
        }
    
        /* modify the QP to RTS */
        rc = modify_qp_to_rts(res->client_qp[i]);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP[%d] of client state to RTS\n",i);
            assert(0);
        }
        fprintf(stdout, "QP state of client was change to RTS\n");
    }
    /* modify the QP to init */
    //for(int i=0;i<7;i++){
        if(sock_sync_data(res->client_sock, 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error of client after QPs are were moved to RTS\n");
            assert(0);
        }
    //} 
connect_qp_exit:
    return rc;
}



int connect_qp_for_extra(struct resources *res,int extra_id)
{
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;
    if(config.gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
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
    if(config.extra_server_name[extra_id]){
        local_con_data.addr = htonll((uintptr_t)res->extra_buf[extra_id]);
        local_con_data.rkey = htonl(res->extra_mr[extra_id]->rkey);
        local_con_data.qp_num = htonl(res->extra_qp[extra_id]->qp_num);
        local_con_data.lid = htons(res->port_attr.lid);
        memcpy(local_con_data.gid, &my_gid, 16);
        fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
        if(sock_sync_data(res->extra_sock[extra_id], sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
        {
            fprintf(stderr, "failed to exchange connection data with extra: %d\n",extra_id);
            rc = 1;
            assert(0);
        }
        res->extra_remote_props[extra_id].addr = ntohll(tmp_con_data.addr);
        res->extra_remote_props[extra_id].rkey = ntohl(tmp_con_data.rkey);
        res->extra_remote_props[extra_id].qp_num = ntohl(tmp_con_data.qp_num);
        res->extra_remote_props[extra_id].lid = ntohs(tmp_con_data.lid);
        memcpy(res->extra_remote_props[extra_id].gid, tmp_con_data.gid, 16);
        /* save the remote side attributes, we will need it for the post SR */
        //res->remote_props = remote_con_data;
        fprintf(stdout, "node:%d Remote address = 0x%"PRIx64"\n", 11+extra_id,res->extra_remote_props[extra_id].addr);
        fprintf(stdout, "node:%d Remote rkey = 0x%x\n", 11+extra_id,res->extra_remote_props[extra_id].rkey);
        fprintf(stdout, "node:%d Remote QP number = 0x%x\n", 11+extra_id,res->extra_remote_props[extra_id].qp_num);
        fprintf(stdout, "node:%d Remote LID = 0x%x\n", 11+extra_id,res->extra_remote_props[extra_id].lid);
    }
    else{
        for(int i=0;i<4;i++){
            std::cout<<"change information between extra_recover_node :"<<i<<std::endl;
            local_con_data.addr = htonll((uintptr_t)res->extra_buf[i]);
            std::cout<<"debug p1"<<std::endl;
            local_con_data.rkey = htonl(res->extra_mr[i]->rkey);
            std::cout<<"debug p2"<<std::endl;
            local_con_data.qp_num = htonl(res->extra_qp[i]->qp_num);
            std::cout<<"debug p3"<<std::endl;
            local_con_data.lid = htons(res->port_attr.lid);
            std::cout<<"debug p4"<<std::endl;
            memcpy(local_con_data.gid, &my_gid, 16);
            std::cout<<"debug p5"<<std::endl;
            fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
            if(sock_sync_data(res->extra_sock[i], sizeof(struct cm_con_data_t), (char *) &local_con_data, (char *) &tmp_con_data) < 0)
            {
                fprintf(stderr, "failed to exchange connection data with extra:  %d\n",i);
                rc = 1;
                assert(0);
            }
            std::cout<<"debug p6"<<std::endl;
            res->extra_remote_props[i].addr = ntohll(tmp_con_data.addr);
            std::cout<<"debug p7"<<std::endl;
            res->extra_remote_props[i].rkey = ntohl(tmp_con_data.rkey);
            std::cout<<"debug p8"<<std::endl;
            res->extra_remote_props[i].qp_num = ntohl(tmp_con_data.qp_num);
            std::cout<<"debug p9"<<std::endl;
            res->extra_remote_props[i].lid = ntohs(tmp_con_data.lid);
            std::cout<<"debug p10"<<std::endl;
            memcpy(res->extra_remote_props[i].gid, tmp_con_data.gid, 16);
            /* save the remote side attributes, we will need it for the post SR */
            //res->remote_props = remote_con_data;
            printf("node:%d Remote address = 0x%"PRIx64"\n", i,res->extra_remote_props[i].addr);
            printf("node:%d Remote rkey = 0x%x\n", i,res->extra_remote_props[i].rkey);
            printf("node:%d Remote QP number = 0x%x\n", i,res->extra_remote_props[i].qp_num);
            printf("node:%d Remote LID = 0x%x\n", i,res->extra_remote_props[i].lid);
            if(config.gid_idx >= 0)
            {
                uint8_t *p = res->extra_remote_props[i].gid;
                fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
            }
        }
    }
        
    if(config.gid_idx >= 0)
    {
        uint8_t *p = res->extra_remote_props[extra_id].gid;
        fprintf(stdout, "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
    }
    /* modify the QP to init */
    if(config.extra_server_name[extra_id]){
        std::cout<<"modify_qp_to_init"<<std::endl;
        rc = modify_qp_to_init(res->extra_qp[extra_id]);
        if(rc)
        {
            fprintf(stderr, "change QP of master state to INIT failed\n");
            assert(0);
        }
    /* let the client post RR to be prepared for incoming messages */
        if(config.extra_server_name[extra_id])
        {
            
            rc = post_receive(res,11+extra_id,11+extra_id,4,964);
            if(rc)
            {
                fprintf(stderr, "failed to post RR to extra\n");
                assert(0);
            }
            
        }
 
    /* modify the QP to RTR */
    std::cout<<"modify_qp_to_rtr"<<std::endl;
        rc = modify_qp_to_rtr(res->extra_qp[extra_id], res->extra_remote_props[extra_id].qp_num, res->extra_remote_props[extra_id].lid, res->extra_remote_props[extra_id].gid);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d state to RTR\n",11+extra_id);
            assert(0);
        }
    
        /* modify the QP to RTS */
    std::cout<<"modify_qp_to_rts"<<std::endl;
        rc = modify_qp_to_rts(res->extra_qp[extra_id]);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of master state to RTS\n");
            assert(0);
        }
        fprintf(stdout, "QP state of master was change to RTS\n");
    }
    else{
        for(int i=0;i<4;i++){
            std::cout<<"modify_qp_to_init"<<std::endl;
            rc = modify_qp_to_init(res->extra_qp[i]);
            if(rc)
            {
                fprintf(stderr, "change QP of master state to INIT failed\n");
                assert(0);
            }
        /* modify the QP to RTR */
            std::cout<<"modify_qp_to_rtr"<<std::endl;
            rc = modify_qp_to_rtr(res->extra_qp[i], res->extra_remote_props[i].qp_num, res->extra_remote_props[i].lid, res->extra_remote_props[i].gid);
            if(rc)
            {
                fprintf(stderr, "failed to modify QP of node:%d state to RTR\n",i);
                assert(0);
            }
        
            /* modify the QP to RTS */
            std::cout<<"modify_qp_to_rts"<<std::endl;
            rc = modify_qp_to_rts(res->extra_qp[i]);
            if(rc)
            {
                fprintf(stderr, "failed to modify QP of master state to RTS\n");
                assert(0);
            }
            fprintf(stdout, "QP state of master was change to RTS\n");
        }
    }
        
    if(config.extra_server_name[extra_id]){
        if(sock_sync_data(res->extra_sock[extra_id], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error of master after QPs are were moved to RTS\n");
            assert(0);
        }
        std::cout<<"syn finish"<<std::endl;
    }else{
        for(int i=0;i<4;i++){
            if(sock_sync_data(res->extra_sock[i], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
            {
                fprintf(stderr, "sync error of master after QPs are were moved to RTS\n");
                assert(0);
            }
        }
        std::cout<<"syn finish"<<std::endl;
    }
    if(config.extra_server_name[extra_id]){
        poll_completion(res,11+extra_id,1469);
        std::cout<<"poll finish"<<std::endl;
    }else{
        for(int i=0;i<4;i++){
            std::cout<<"post_send to node: "<<i<<std::endl;
            post_send(res,IBV_WR_SEND,11+i,11+i,0,4,1472);
            std::cout<<"post_send succeed"<<std::endl;
            poll_completion(res,11+i,1469);
            std::cout<<"poll finish"<<std::endl;
        }
    }
    std::cout<<"connect qp for extra:"<<extra_id <<" finish"<<std::endl;
    
 
connect_qp_exit:
    return rc;
}










/******************************************************************************
* Function: resources_destroy
*
* Input:
* res: pointer to resources structure
*
* Output: none
*
* Returns: 0 on success, 1 on failure
*
* Description: Cleanup and deallocate all resources used
******************************************************************************/
int resources_destroy(struct resources *res)
{
    int rc = 0;
    
        
    for(int n=0;n<K;n++){
        if(res->qp[n])
        {
            if(ibv_destroy_qp(res->qp[n]))
            {
                fprintf(stderr, "failed to destroy QP[%d]\n",n);
                rc = 1;
            }
        }
    
        if(res->mr[n])
        {
            if(ibv_dereg_mr(res->mr[n]))
            {
                fprintf(stderr, "failed to deregister MR[%d]\n",n);
                rc = 1;
            }
        }
        if(res->msg_mr[n])
        {
            if(ibv_dereg_mr(res->msg_mr[n]))
            {
                fprintf(stderr, "failed to deregister MR_MSG[%d]\n",n);
                rc = 1;
            }
        }
    
        if(res->buf[n])
        {
            free(res->buf[n]);
        }
        if(res->msg_buf[n])
        {
            free(res->msg_buf[n]);
        }
        if(res->cq[n])
        {
            if(ibv_destroy_cq(res->cq[n]))
            {
                fprintf(stderr, "failed to destroy CQ[%d]\n",n);
                rc = 1;
            }
        }
        if(res->sock[n] >= 0)
        {
            if(close(res->sock[n]))
            {
                fprintf(stderr, "failed to close socket[%d]\n",n);
                rc = 1;
            }
        }
    }
    
 
    if(res->pd)
	{
        if(ibv_dealloc_pd(res->pd))
        {
            fprintf(stderr, "failed to deallocate PD\n");
            rc = 1;
        }
	}
 
    if(res->ib_ctx)
	{
        if(ibv_close_device(res->ib_ctx))
        {
            fprintf(stderr, "failed to close device context\n");
            rc = 1;
        }
	}
 
    
    return rc;
}
 
/******************************************************************************
* Function: print_config
*
* Input: none
*
* Output: none
*
* Returns: none
*
* Description: Print out config information
******************************************************************************/
void print_config(void)
{
    fprintf(stdout, " ------------------------------------------------\n");
    fprintf(stdout, " Device name : \"%s\"\n", config.dev_name);
    fprintf(stdout, " IB port : %u\n", config.ib_port);
    if(config.server_name[0])
    {
        fprintf(stdout, " IP : %s\n", config.server_name[0]);
    }
    fprintf(stdout, " TCP port : %u\n", config.tcp_port[0]);
    if(config.server_name[1])
    {
        fprintf(stdout, " IP : %s\n", config.server_name[1]);
    }
    fprintf(stdout, " TCP port : %u\n", config.tcp_port[1]);
    if(config.gid_idx >= 0)
    {
        fprintf(stdout, " GID index : %u\n", config.gid_idx);
    }
    fprintf(stdout, " ------------------------------------------------\n\n");
}
 
/******************************************************************************
* Function: usage
*
* Input:
* argv0: command line arguments
*
* Output: none
*
* Returns: none
*
* Description: print a description of command line syntax
******************************************************************************/
void usage(const char *argv0)
{
    fprintf(stdout, "Usage:\n");
    fprintf(stdout, " %s start a server and wait for connection\n", argv0);
    fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
    fprintf(stdout, "\n");
    fprintf(stdout, "Options:\n");
    fprintf(stdout, " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
    fprintf(stdout, " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
    fprintf(stdout, " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
    fprintf(stdout, " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}
 
/******************************************************************************
* Function: main
*
* Input:
* argc: number of items in argv
* argv: command line parameters
*
* Output: none
*
* Returns: 0 on success, 1 on failure
*
* Description: Main program code
******************************************************************************/


int resources_create_for_extra(struct resources *res,int extra_id)
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
    if(config.extra_server_name[extra_id]){
        std::cout<<"try connect to server_name: "<< config.extra_server_name[extra_id]<<" port : "<<config.extra_tcp_port[extra_id]<<std::endl;
        res->extra_sock[extra_id] = sock_connect(config.extra_server_name[extra_id], config.extra_tcp_port[extra_id]);
        std::cout<<"socket connect finish"<<std::endl;
    }
    else{
        for(int i=0;i<4;i++){
            std::cout<<"waiting on tcp_port "<<config.extra_tcp_port[i]<<std::endl;
            res->extra_sock[i] = sock_connect(NULL, config.extra_tcp_port[i]);
            std::cout<<"socket connect finish"<<std::endl;
        }
    }
    if(res->extra_sock[extra_id] < 0)
    {
        fprintf(stderr, "failed to establish TCP connection to extra_server %s\n",config.extra_server_name[extra_id]);
        rc = -1;
        assert(0);
    }
    /* each side will send only one WR, so Completion Queue with 1 entry is enough */
    if(config.extra_server_name[extra_id]){
        cq_size = 1;    
        res->extra_cq[extra_id] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if(!res->extra_cq[extra_id])
        {
            fprintf(stderr, "failed to create master CQ with %u entries for extra\n", cq_size);
            rc = 1;
            assert(0);
        }
        /* allocate the memory buffer that will hold the data */
        res->extra_buf[extra_id] = (char *) malloc(65*1024*1024);
        fprintf(stdout, "申请内存extra_buf\n");
        /* register the memory buffer */
        mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
        res->extra_mr[extra_id] = ibv_reg_mr(res->pd, res->extra_buf[extra_id], MAX_MASTER_BUF_SIZE, mr_flags);
        fprintf(stdout, "注册extra_buf内存到pd\n");
        if(!res->extra_mr[extra_id])
        {
            fprintf(stderr, "ibv_reg_mrfailed with mr_flags=0x%x\n", mr_flags);
            assert(0);
        }
        fprintf(stdout, "extra_MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                    res->extra_buf[extra_id], res->extra_mr[extra_id]->lkey, res->extra_mr[extra_id]->rkey, mr_flags);
    }
    else{
        for(int i=0;i<4;i++){
            cq_size = 1;    
            res->extra_cq[i] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
            if(!res->extra_cq[i])
            {
                fprintf(stderr, "failed to create master CQ with %u entries for extra\n", cq_size);
                rc = 1;
                assert(0);
            }
            /* allocate the memory buffer that will hold the data */
            res->extra_buf[i] = (char *) malloc(65*1024*1024);
            fprintf(stdout, "申请内存extra_buf\n");
            /* register the memory buffer */
            mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE ;
            res->extra_mr[i] = ibv_reg_mr(res->pd, res->extra_buf[i], MAX_MASTER_BUF_SIZE, mr_flags);
            fprintf(stdout, "注册extra_buf内存到pd\n");
            if(!res->extra_mr[i])
            {
                fprintf(stderr, "ibv_reg_mrfailed with mr_flags=0x%x\n", mr_flags);
                assert(0);
            }
            fprintf(stdout, "extra_MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
                        res->extra_buf[i], res->extra_mr[i]->lkey, res->extra_mr[i]->rkey, mr_flags);
        }    
    }
    if(config.extra_server_name[extra_id]){
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.sq_sig_all = 1;
        qp_init_attr.cap.max_send_wr = 1;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.send_cq = res->extra_cq[extra_id];
        qp_init_attr.recv_cq = res->extra_cq[extra_id];
        res->extra_qp[extra_id] = ibv_create_qp(res->pd, &qp_init_attr);
        if(!res->extra_qp[extra_id])
        {
                fprintf(stderr, "failed to create extra_QP\n");
                assert(0);
        }
        fprintf(stdout, "extra_QP was created, QP number=0x%x\n",res->extra_qp[extra_id]->qp_num);
        
    }
    else{
        for(int i=0;i<4;i++){
            memset(&qp_init_attr, 0, sizeof(qp_init_attr));
            qp_init_attr.qp_type = IBV_QPT_RC;
            qp_init_attr.sq_sig_all = 1;
            qp_init_attr.cap.max_send_wr = 1;
            qp_init_attr.cap.max_recv_wr = 1;
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.send_cq = res->extra_cq[i];
            qp_init_attr.recv_cq = res->extra_cq[i];
            res->extra_qp[i] = ibv_create_qp(res->pd, &qp_init_attr);
            if(!res->extra_qp[i])
            {
                    fprintf(stderr, "failed to create extra_QP\n");
                    assert(0);
            }
            fprintf(stdout, "extra_QP was created, QP number=0x%x\n",res->extra_qp[i]->qp_num);
        }
    }
        /* create the Queue Pair */   
    return rc;
}
int resources_destroy_for_extra(struct resources *res){
    for(int n=0;n<4;n++){
        if(res->extra_qp[n])
        {
            if(ibv_destroy_qp(res->extra_qp[n]))
            {
                fprintf(stderr, "failed to destroy extar_QP[%d]\n",n);
            }
        }
    
        if(res->extra_mr[n])
        {
            if(ibv_dereg_mr(res->extra_mr[n]))
            {
                fprintf(stderr, "failed to deregister extra_MR[%d]\n",n);
            }
        }
    
        if(res->extra_buf[n])
        {
            free(res->extra_buf[n]);
        }

        if(res->extra_cq[n])
        {
            if(ibv_destroy_cq(res->extra_cq[n]))
            {
                fprintf(stderr, "failed to destroy extra_CQ[%d]\n",n);
            }
        }
        if(res->extra_sock[n] >= 0)
        {
            if(close(res->extra_sock[n]))
            {
                fprintf(stderr, "failed to close extra_socket[%d]\n",n);
            }
        }
    }
}

int poll_completion_client(struct resources *res,int qp_id,int line)
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
        poll_result = ibv_poll_cq(res->client_cq[qp_id], 1, &wc);
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    }
    while((poll_result == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
 
    if(poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if(poll_result == 0)
    {
        /* the CQ is empty */
       printf("client[%d]  completion wasn't found in the CQ after timeout,line= %d\n",qp_id,line);
        rc = 1;
    }
    else
    {
        /* CQE found */
        //fprintf(stdout, "node: %d completion was found in CQ with status 0x%x\n",target_node_id,wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if(wc.status != IBV_WC_SUCCESS)
        {
           printf("got bad completion with status: 0x%x, vendor syndrome: 0x%x ,line= %d\n", 
					wc.status, wc.vendor_err,line);
            rc = 1;
        }
    }
    return rc;
}

int post_send_client(struct resources *res, int opcode,int qp_id,int buf_id,int offset,int len,int line)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->client_buf[qp_id]+offset;
    sge.lkey = res->client_mr[qp_id]->lkey;
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
        sr.wr.rdma.remote_addr = res->client_remote_props[qp_id].addr+offset;
        sr.wr.rdma.rkey = res->client_remote_props[qp_id].rkey;
    }
    rc = ibv_post_send(res->client_qp[qp_id], &sr, &bad_wr);
    if(rc)
    {
        printf("failed to post SR to client, line= %d,qp_id=%d,rc=%d\n",line,qp_id,rc);
    }
    return rc;
}

int post_receive_client(struct resources *res,int qp_id,int buf_id,int len,int line)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
  
    sge.addr = (uintptr_t)res->client_buf[qp_id];
    sge.lkey = res->client_mr[qp_id]->lkey;
    sge.length = len;
 
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    rc = ibv_post_recv(res->client_qp[qp_id], &rr, &bad_wr);
    if(rc)
    {
       fprintf(stderr, "failed to post RR to client qp_id:%d ,line= %d\n,rc=%d",qp_id,line,rc);
    }
    return rc;
}