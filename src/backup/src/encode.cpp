#include <stdlib.h>
#include <string.h>
#include "encode.h"
#include "erasure_code.h"
#define SINGLE_LINE_ENCODE 1
void Encoder::init_encode(int k,int p,int len_){
  len = len_;
    encoder_k = k;
    encoder_p = p;
    encoder_m = k+p;
    encode_matrix = new uint8_t[encoder_m * k];
    decode_matrix = new uint8_t[encoder_m * k];
	invert_matrix = new uint8_t[encoder_m * k];
	temp_matrix = new uint8_t[encoder_m * k];
    g_tbls = new uint8_t[k * p * 32];
    gf_gen_cauchy1_matrix(encode_matrix, encoder_m, k);
    ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);
}
void Encoder::encode_data(int backup_node_id,uint8_t **src,uint8_t* dest){
  #if SINGLE_LINE_ENCODE 
    //直接使用p=1来只进行一行的encode运算，需要再g_tbls上做偏移即可
    ec_encode_data(len, encoder_k, 1, g_tbls+backup_node_id*encoder_k*32, src, &dest);
  #else
  uint8_t* dests[5];//原本一个backup结点只要算一个Pi就行，但是接口是都要算所以其余不填
  //之后要优化需要改动ec_encode_data接口内部的逻辑，比较麻烦留待以后~~~~~~~~
  for(int i=0;i<p;i++){
    if(i!=backup_node_id)
      dests[i]=new uint8_t[len];
  }
  dests[backup_node_id] = dest;
  ec_encode_data(len, k, p, g_tbls, src, dests);
  for(int i=0;i<p;i++){
    if(i!=backup_node_id)
      delete[] dests[i];
  }
  #endif
}
void Encoder::encode_data_update(int backup_node_id,uint8_t *src,int src_id,uint8_t* dest){
	ec_encode_data_update(len, encoder_k, 1,src_id, g_tbls+backup_node_id*encoder_k*32, src, &dest);
}

void Encoder::init_decode(uint8_t* frag_err_list,int nerrs_){
  nerrs = nerrs_;
  for(int i=0;i<nerrs_;i++){
    err_list[i] = frag_err_list[i];
  }
  gf_gen_decode_matrix_simple(encode_matrix, decode_matrix,
					  invert_matrix, temp_matrix, decode_index,
					  err_list, nerrs, encoder_k, encoder_m);
  ec_init_tables(encoder_k, nerrs, decode_matrix, g_tbls);
}
void Encoder::decode_data(uint8_t **frag_ptrs,uint8_t **recover_outp){
  uint8_t* recover_srcs[10];
  //decode_index是参与decode的k项在总src_id_list里的id
  //frag_ptrs是所有data_ptr和parity_ptr的数组，长度是k+p(m)，要恢复的block位置随便填即可
  for (int i = 0; i < encoder_k; i++){
		recover_srcs[i] = frag_ptrs[decode_index[i]];
  }
  ec_encode_data(len, encoder_k, nerrs, g_tbls, recover_srcs, recover_outp);
}


static int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index, u8 * frag_err_list, int nerrs, int k,
				       int m)
{
	int i, j, p, r;
	int nsrcerrs = 0;
	u8 s, *b = temp_matrix;
	u8 frag_in_err[10];

	memset(frag_in_err, 0, sizeof(frag_in_err));

	// Order the fragments in erasure for easier sorting
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)
			nsrcerrs++;
		frag_in_err[frag_err_list[i]] = 1;
	}

	// Construct b (matrix that encoded remaining frags) by removing erased rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (frag_in_err[r])
			r++;
		for (j = 0; j < k; j++)
			b[k * i + j] = encode_matrix[k * r + j];
		decode_index[i] = r;
	}

	// Invert matrix to get recovery matrix
	if (gf_invert_matrix(b, invert_matrix, k) < 0)
		return -1;

	// Get decode matrix with only wanted recovery rows
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)	// A src err
			for (j = 0; j < k; j++)
				decode_matrix[k * i + j] =
				    invert_matrix[k * frag_err_list[i] + j];
	}

	// For non-src (parity) erasures need to multiply encode matrix * invert
	for (p = 0; p < nerrs; p++) {
		if (frag_err_list[p] >= k) {	// A parity err
			for (i = 0; i < k; i++) {
				s = 0;
				for (j = 0; j < k; j++)
					s ^= gf_mul(invert_matrix[j * k + i],
						    encode_matrix[k * frag_err_list[p] + j]);
				decode_matrix[k * p + i] = s;
			}
		}
	}
	return 0;
}