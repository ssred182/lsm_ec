#include "my_client.h"
class MurMurHash {
public:
   static uint32_t MurMur3_32(std::vector<uint8_t>& input);
   static inline uint32_t rotl32(uint32_t x, int8_t r) {
      return (x << r) | (x >> (32 - r));
   }
private:
   static const uint32_t c1 = 0xcc9e2d51;
   static const uint32_t c2 = 0x1b873593;
   static const uint32_t m = 5;
   static const uint32_t n = 0xe6546b64;
   static const uint32_t final1 = 0x85ebca6b;
   static const uint32_t final2 = 0xc2b2ae35;
};
uint32_t MurMurHash::MurMur3_32(std::vector<uint8_t>& input) {
   if (input.empty()) {
      return 0;
   }
   
   const int nBlocks = input.size() / 4;
   const uint32_t* blocks = reinterpret_cast<const uint32_t *>(&input[0]);
   const uint8_t* tail = &input[nBlocks*4];
   
   uint32_t hash = 0;

   uint32_t k;
   for (int i = 0; i < nBlocks ; ++i) {
      k = blocks[i];
      
      k *= c1;
      k = rotl32(k,15);
      k *= c2;
      
      hash ^= k;
      hash = rotl32(hash,13);
      hash = (hash * m) + n;
   }
   
   k = 0;
   switch (input.size() & 3) {
      case 3: 
         k ^= tail[2] << 16;
      case 2: // intentionally inclusive of above
         k ^= tail[1] << 8;
      case 1: // intentionally inclusive of above
         k ^= tail[0];
         k *= c1;
         k = rotl32(k,15);
         k *= c2;
         hash ^= k;
   }
   
   hash ^= input.size();
   hash ^= hash >> 16;
   hash *= final1;
   hash ^= hash >> 13;
   hash *= final2;
   hash ^= hash  >> 16;
           
   return hash;
}
my_client::my_client(){
   resources_init(&res);
   resources_create(&res);
   connect_qp(&res);
    
}

int my_client::put(const std::string &key, const std::string &value){
    std::vector<uint8_t> key_vector(key.begin(),key.end());
    uint32_t hash_id = MurMurHash::MurMur3_32(key_vector)%4;
    primary_connect_lock_list[hash_id].lock();
    int op_code = 1;
    int key_len = key.size();
    int value_len = value.size();
    memcpy(res.buf[hash_id],&op_code,4);
    memcpy(res.buf[hash_id]+4,&key_len,4);
    memcpy(res.buf[hash_id]+8,&value_len,4);
    memcpy(res.buf[hash_id]+12,key.data(),key_len);
    memcpy(res.buf[hash_id]+12+key_len,value.data(),value_len);
    post_send(&res,IBV_WR_SEND,hash_id,0,12+key_len+value_len);
    poll_completion(&res,hash_id);
    post_receive(&res,hash_id,4);
    poll_completion(&res,hash_id);
    int status;
    memcpy(&status,res.buf[hash_id],4);
    primary_connect_lock_list[hash_id].unlock();
    return status;
}
int my_client::get(const std::string &key, std::string &value){
    std::vector<uint8_t> key_vector(key.begin(),key.end());
    uint32_t hash_id = MurMurHash::MurMur3_32(key_vector)%4;
    primary_connect_lock_list[hash_id].lock();
    int op_code = 2;
    int key_len = key.size();
    memcpy(res.buf[hash_id],&op_code,4);
    memcpy(res.buf[hash_id]+4,&key_len,4);
    memcpy(res.buf[hash_id]+8,key.data(),key_len);
    post_send(&res,IBV_WR_SEND,hash_id,0,8+key_len);
    poll_completion(&res,hash_id);
    post_receive(&res,hash_id,MAX_KV_LEN_FROM_CLIENT);
    poll_completion(&res,hash_id);
    int status;
    memcpy(&status,res.buf[hash_id],4);
    int value_len;
    memcpy(&value_len,res.buf[hash_id]+4,4);
    value.assign(res.buf[hash_id]+8,value_len);
    primary_connect_lock_list[hash_id].unlock();
    return status;
}