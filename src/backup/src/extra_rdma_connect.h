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
/*
* BUILD COMMAND:
* gcc -Wall -O0 -g -o RDMA_RC_example RDMA_RC_example.c -libverbs
*server：
*./RDMA_RC_example  -d mlx5_0 -i 1 -g 3
*client：
*./RDMA_RC_example 192.169.31.53 -d mlx5_0 -i 1 -g 3
*/


/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 2000
#define SRV_MSG " Server's message "
#define RDMAMSGR "RDMA read operation "
#define RDMAMSGW "RDMA write operation"
#define MSG_SIZE 64


/* structure of test parameters */
struct config_t
{
    const char *dev_name; /* IB device name */
    char *server_name;    /* server host name */
    char *master_server_name;
    uint32_t tcp_port[4];    /* server TCP port */
    int ib_port;          /* local IB port to work with */
    int gid_idx;          /* gid index to use */
    int node_id;
    int master_tcp_port;
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
struct resources
{
    struct ibv_device_attr device_attr; /* Device attributes */
    struct ibv_port_attr port_attr;     /* IB port attributes */
    struct cm_con_data_t* remote_props;  /* values to connect to remote side */
    struct ibv_context *ib_ctx;         /* device handle */
    struct ibv_pd *pd;                  /* PD handle */
    struct ibv_cq **cq;                  /* CQ handle */
    struct ibv_qp **qp;                  /* QP handle */
    struct ibv_mr **mr;                  /* MR handle for buf */
    struct ibv_mr **msg_mr;  
    char **buf;                          /* memory buffer pointer, used for RDMA and send ops */
    char **msg_buf;
    int *sock;                           /* TCP socket file descriptor */
    char *master_buf;
    char *master_msg_buf;
    struct ibv_cq *master_cq;                 
    struct ibv_qp *master_qp;                 
    struct ibv_mr *master_mr;                 
    struct ibv_mr *master_msg_mr; 
    int master_sock;
    struct cm_con_data_t master_remote_props;
};
 


inline uint64_t htonll(uint64_t x);
inline uint64_t ntohll(uint64_t x);


int sock_connect(const char *servername, int port);
int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data);
int poll_completion(struct resources *res,int target_node_id);
int post_send(struct resources *res, int opcode,int target_node_id,int offset,int len);
int post_send_msg(struct resources *res, int opcode,int target_node_id,int offset,int len);
int post_receive(struct resources *res,int target_node_id,int len);
int post_receive_msg(struct resources *res,int target_node_id,int len);
void resources_init(struct resources *res);
int resources_create(struct resources *res);
int modify_qp_to_init(struct ibv_qp *qp);
int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
int modify_qp_to_rts(struct ibv_qp *qp);
int connect_qp(struct resources *res);
int connect_qp_for_master(struct resources *res);
int resources_destroy(struct resources *res);
void print_config(void);
void usage(const char *argv0);