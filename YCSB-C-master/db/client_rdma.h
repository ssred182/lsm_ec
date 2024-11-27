#ifndef CLIENT_RDMA
#define CLIENT_RDMA
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
#include <string>


#define MAX_KV_LEN_FROM_CLIENT 1024*8
#define MAX_QP_NUM 8
struct config_t
{
    const char *dev_name; /* IB device name */
    char *primary_server_name[4];
    char *backup_server_name[2];
    char *extra_server_name[2];
    uint32_t tcp_port[4];    /* server TCP port */
    uint32_t backup_tcp_port[2];
    int ib_port;          /* local IB port to work with */
    int gid_idx;          /* gid index to use */
};
 
struct cm_con_data_t
{
    uint64_t addr;        /* Buffer address */
    uint32_t rkey;        /* Remote key */
    uint32_t qp_num;      /* QP number */
    uint16_t lid;         /* LID of the IB port */
    uint8_t gid[16];      /* gid */
} __attribute__((packed));
 
/* structure of system resources */
struct local_res{
    struct ibv_cq *cq;   
    struct ibv_qp *qp; 
    struct ibv_mr *mr;
    char *buf;    
    struct cm_con_data_t remote_props;
};
struct resources
{
    struct local_res res_list[16*MAX_QP_NUM];
    int sock[8];
    
    
    // struct ibv_cq *cq[4*MAX_QP_NUM];                  /* CQ handle */
    // struct ibv_qp *qp[4*MAX_QP_NUM];                  /* QP handle */
    // struct ibv_mr *mr[4*MAX_QP_NUM];                  /* MR handle for buf */
    // char *buf[4*MAX_QP_NUM];                          /* memory buffer pointer, used for RDMA and send ops */
    // struct cm_con_data_t remote_props[4*MAX_QP_NUM];                    /* TCP socket file descriptor */
    
};
struct global_resource
{
    struct ibv_device_attr device_attr; /* Device attributes */
    struct ibv_port_attr port_attr;     /* IB port attributes */
      /* values to connect to remote side */
    struct ibv_context *ib_ctx;         /* device handle */
    struct ibv_pd *pd;                  /* PD handle */
};
 


inline uint64_t htonll(uint64_t x);
inline uint64_t ntohll(uint64_t x);
int sock_connect(const char *servername, int port);
int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data);
int poll_completion(struct resources *res,int target_node_id,int qp_id);
int post_send(struct resources *res, int opcode,int target_node_id,int qp_id,int offset,int len);
int post_receive(struct resources *res,int target_node_id,int qp_id,int len);
int modify_qp_to_init(struct ibv_qp *qp);
int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
int modify_qp_to_rts(struct ibv_qp *qp);
void resources_init(struct resources *res);
int resources_create(struct resources *res);
int connect_qp(struct resources *res);
int reconnect_qp(struct resources *res,int target_node_id);
int poll_completion_quick(struct resources *res,int target_node_id,int qp_id);

#endif