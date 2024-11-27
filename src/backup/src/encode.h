#include <vector>
#include <stdint.h>
#include "erasure_code.h"
class Chunk{
    public:
        uint8_t chunk_id;
        uint8_t *chunk_data;
};
typedef uint8_t u8;
static int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index, u8 * frag_err_list, int nerrs, int k,
				       int m);
class Encoder{
    public:
        void init_encode(int k,int p,int len_);
        void encode_data(int backup_node_id,uint8_t **src,uint8_t* dest);
        void encode_data_update(int backup_node_id,uint8_t *src,int src_id,uint8_t* dest);
        void init_decode(uint8_t* frag_err_list,int nerrs_);
        void decode_data(uint8_t **frag_ptrs,uint8_t **recover_outp);
        int len;
        int encoder_k;
        int encoder_p;
        int encoder_m;
        uint8_t* encode_matrix;
        uint8_t *g_tbls;
        uint8_t *decode_matrix;
        uint8_t *invert_matrix;
        uint8_t *temp_matrix;
        uint8_t err_list[10];
        int nerrs;
        uint8_t decode_index[10];
};
