#include <assert.h>
#include "rdma_connect.h"
/* poll CQ timeout in millisec (2 seconds) */

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
        if(sockfd >= 0)
        {
            if(servername)
			{
                /* Client mode. Initiate connection to remote */
                if((tmp=connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
                {
                    fprintf(stdout, "failed connect \n");
                    close(sockfd);
                    sockfd = -1;
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
int poll_completion(struct resources *res,int target_node_id,uint32_t line)
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
        if(target_node_id == 10)
            poll_result = ibv_poll_cq(res->master_cq, 1, &wc);
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
        fprintf(stderr, "node: %d completion wasn't found in the CQ after timeout,line= %d\n",target_node_id,line);
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
    assert(poll_result);
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
int post_send(struct resources *res, int opcode,int target_node_id,uint32_t offset,uint32_t len,uint32_t line)
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
    else{
        sge.addr = (uintptr_t)res->buf[0]+offset;
        sge.lkey = res->mr[0]->lkey;
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
        fprintf(stderr, "failed to post SR, line= %d\n",line);
    }
    // else
    // {
    //     switch(opcode)
    //     {
    //     case IBV_WR_SEND:
    //         fprintf(stdout, "Send Request to node:%d was posted\n",target_node_id);
    //         break;
    //     case IBV_WR_RDMA_READ:
    //         fprintf(stdout, "RDMA Read Request to node:%d was posted\n",target_node_id);
    //         break;
    //     case IBV_WR_RDMA_WRITE:
    //         //fprintf(stdout, "RDMA Write Request to node:%d was posted\n",target_node_id);
    //         break;
    //     default:
    //         fprintf(stdout, "Unknown Request to node:%d was posted\n",target_node_id);
    //         break;
    //     }
    //}
    return rc;
}

int post_send_msg(struct resources *res, int opcode,int target_node_id,uint32_t offset,uint32_t len,uint32_t line)
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
    else{
        sge.addr = (uintptr_t)res->msg_buf[0]+offset;
        sge.lkey = res->msg_mr[0]->lkey;
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
        fprintf(stderr, "failed to post SR, line= %d\n",line);
    }
    // else
    // {
    //     switch(opcode)
    //     {
    //     case IBV_WR_SEND:
    //         fprintf(stdout, "Send Request to node:%d was posted\n",target_node_id);
    //         break;
    //     case IBV_WR_RDMA_READ:
    //         fprintf(stdout, "RDMA Read Request to node:%d was posted\n",target_node_id);
    //         break;
    //     case IBV_WR_RDMA_WRITE:
    //         fprintf(stdout, "RDMA Write Request to node:%d was posted\n",target_node_id);
    //         break;
    //     default:
    //         fprintf(stdout, "Unknown Request to node:%d was posted\n",target_node_id);
    //         break;
    //     }
    //}
    return rc;
}











int post_send_msg_for_recover(struct resources *res, int opcode,int target_node_id,int msg_buf_id,uint32_t offset,uint32_t len,uint32_t line)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    
    sge.addr = (uintptr_t)res->msg_buf[msg_buf_id]+offset;
    sge.lkey = res->msg_mr[msg_buf_id]->lkey;
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
        fprintf(stderr, "failed to post SR, line= %d\n",line);
    }
  
    return rc;
}















int post_send_gc_msg(struct resources *res, int opcode,int target_node_id,uint32_t offset,uint32_t len,uint32_t line)
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
    //else
        //fprintf(stderr, "send gc_msg wr, line= %d\n",line);
    // else
    // {
    //     switch(opcode)
    //     {
    //     case IBV_WR_SEND:
    //         fprintf(stdout, "Send Request to node:%d was posted\n",target_node_id);
    //         break;
    //     case IBV_WR_RDMA_READ:
    //         fprintf(stdout, "RDMA Read Request to node:%d was posted\n",target_node_id);
    //         break;
    //     case IBV_WR_RDMA_WRITE:
    //         fprintf(stdout, "RDMA Write Request to node:%d was posted\n",target_node_id);
    //         break;
    //     default:
    //         fprintf(stdout, "Unknown Request to node:%d was posted\n",target_node_id);
    //         break;
    //     }
    //}
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
int post_receive(struct resources *res,int target_node_id,int len ,uint32_t line)
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
    else{
        sge.addr = (uintptr_t)res->buf[0];
        sge.lkey = res->mr[0]->lkey;
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
    else
        rc = ibv_post_recv(res->qp[target_node_id], &rr, &bad_wr);
    if(rc)
    {
        fprintf(stderr, "failed to post RR to node:%d ,line= %d\n",target_node_id,line);
    }
    // else
    // {
    //     fprintf(stdout, "Receive Request to node:%d was posted\n",target_node_id);
    // }
    return rc;
}

int post_receive_msg(struct resources *res,int target_node_id,int len,uint32_t line)
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
    }
    else{
        sge.addr = (uintptr_t)res->msg_buf[0];
        sge.lkey = res->msg_mr[0]->lkey;
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
    else
        rc = ibv_post_recv(res->qp[target_node_id], &rr, &bad_wr);
    if(rc)
    {
        fprintf(stderr, "failed to post RR to node:%d ,line= %d\n",target_node_id,line);
    }
    // else
    // {
    //     fprintf(stdout, "Receive Request to node:%d was posted\n",target_node_id);
    // }
    return rc;
}


int post_receive_gc_msg(struct resources *res,int target_node_id,int len,uint32_t line)
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
    // else
    // {
    //     fprintf(stdout, "Receive Request to node:%d was posted\n",target_node_id);
    // }
    return rc;
}
 





int post_receive_msg_for_recover(struct resources *res,int target_node_id,int msg_buf_id,int len,uint32_t line)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
 
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
   
    sge.addr = (uintptr_t)res->msg_buf[msg_buf_id];
    sge.lkey = res->msg_mr[msg_buf_id]->lkey; 
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
    else
        rc = ibv_post_recv(res->qp[target_node_id], &rr, &bad_wr);
    if(rc)
    {
        fprintf(stderr, "failed to post RR to node:%d ,line= %d\n",target_node_id,line);
    }
    // else
    // {
    //     fprintf(stdout, "Receive Request to node:%d was posted\n",target_node_id);
    // }
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
    if(config.server_name){
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
    attr.rnr_retry = 3;
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
        local_con_data[n].addr = htonll((uintptr_t)res->buf[0]);
        local_con_data[n].rkey = htonl(res->mr[0]->rkey);
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
        fprintf(stdout, "node:%d Remote address = 0x%"PRIx64"\n", n,remote_con_data.addr);
        fprintf(stdout, "node:%d Remote rkey = 0x%x\n", n,remote_con_data.rkey);
        fprintf(stdout, "node:%d Remote QP number = 0x%x\n", n,remote_con_data.qp_num);
        fprintf(stdout, "node:%d Remote LID = 0x%x\n", n,remote_con_data.lid);
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
    if(!config.server_name)
    {
        for(int n=0;n<K;n++){
            rc = post_receive(res,n,64,727);
            
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

int reconnect_qp(struct resources *res,int target_node_id){
    if(close(res->sock[target_node_id])){
        fprintf(stderr, "failed to close socket in reconnect\n");
    }
    if(config.server_name)
    {
        res->sock[target_node_id] = sock_connect(config.server_name, config.tcp_port[target_node_id]);
        if(res->sock[target_node_id] < 0)
        {
            fprintf(stderr, "failed to re_establish TCP connection to server %s, port %d\n",
                    config.server_name, config.tcp_port[target_node_id]);
            rc = -1;
            assert(0);
            return -1;
        }
    }
    else
    { 
        fprintf(stdout, "waiting on port %d for TCP connection\n", config.tcp_port[target_node_id]);
        res->sock[target_node_id] = sock_connect(NULL, config.tcp_port[target_node_id]);
        if(res->sock[target_node_id] < 0)
        {
            fprintf(stderr, "failed to re_establish TCP connection with client on port %d\n",
                        config.tcp_port[target_node_id]);
            rc = -1;
            assert(0);
            return -1;
        }
        fprintf(stdout, "TCP port: %d connection was established \n",config.tcp_port[n]);
               
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
        fprintf(stderr, "failed to exchange connection data with target node:%d\n",n);
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
    fprintf(stdout, "node:%d Remote address = 0x%"PRIx64"\n", target_node_id,remote_con_data.addr);
    fprintf(stdout, "node:%d Remote rkey = 0x%x\n", target_node_id,remote_con_data.rkey);
    fprintf(stdout, "node:%d Remote QP number = 0x%x\n", target_node_id,remote_con_data.qp_num);
    fprintf(stdout, "node:%d Remote LID = 0x%x\n", target_node_id,remote_con_data.lid);
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
    if(!config.server_name)
    {
        rc = post_receive(res,target_node_id,37);
    }        
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
        memcpy(local_con_data[n].gid, &my_gid, 16);
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
        fprintf(stdout, "node:%d Remote address = 0x%"PRIx64"\n", n,remote_con_data.addr);
        fprintf(stdout, "node:%d Remote rkey = 0x%x\n", n,remote_con_data.rkey);
        fprintf(stdout, "node:%d Remote QP number = 0x%x\n", n,remote_con_data.qp_num);
        fprintf(stdout, "node:%d Remote LID = 0x%x\n", n,remote_con_data.lid);
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
        
        rc = post_receive(res,10,4);
        if(rc)
        {
            fprintf(stderr, "failed to post RR to master\n");
            goto connect_qp_exit;
        }
        
    }
 
    /* modify the QP to RTR */
    
        rc = modify_qp_to_rtr(res->qp[n], res->remote_props[n].qp_num, res->remote_props[n].lid, res->remote_props[n].gid);
        if(rc)
        {
            fprintf(stderr, "failed to modify QP of node:%d state to RTR\n",n);
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
    
        if(sock_sync_data(res->sock[n], 1, "Q", &temp_char))  /* just send a dummy char back and forth */
        {
            fprintf(stderr, "sync error of master after QPs are were moved to RTS\n");
            rc = 1;
        }
    
    
 
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
    
        if(res->mr)
        {
            if(ibv_dereg_mr(res->mr))
            {
                fprintf(stderr, "failed to deregister MR[%d]\n",n);
                rc = 1;
            }
        }
        if(res->msg_mr)
        {
            if(ibv_dereg_mr(res->msg_mr))
            {
                fprintf(stderr, "failed to deregister MR_MSG[%d]\n",n);
                rc = 1;
            }
        }
    
        if(res->buf)
        {
            free(res->buf);
        }
        if(res->msg_buf)
        {
            free(res->msg_buf);
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
    if(config.server_name)
    {
        fprintf(stdout, " IP : %s\n", config.server_name);
    }
    fprintf(stdout, " TCP port : %u\n", config.tcp_port[0]);
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
