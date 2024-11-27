#include <thread>
#include "my_client.h"
#include <unistd.h>



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
void real_time_task(my_client *target_client){
   usleep(10*1000*1000);
   while(1){
      //std::cout<<" I am real_time_task"<<std::endl;
      usleep(1000*1000);
      uint32_t ops=0;
      double lat=0;
      for(int i=0;i<4;i++){
         ops+=target_client->req_times[i].load();
         lat+=target_client->total_latency[i].load();
      }
      std::cout<<ops <<std::endl;
      //std::cout<<"ops= "<<ops <<" ,lat= "<<lat/ops <<std::endl;
      for(int i=0;i<4;i++){
         target_client->req_times[i].store(0);
         target_client->total_latency[i].store(0);
      }
      target_client->total_lat_in_final += lat;
      target_client->total_req_times += ops;
      //std::cout<<"average_lat = "<<target_client->total_lat_in_final/target_client->total_req_times<<std::endl;
   }
}
extern struct config_t config;
extern struct global_resource *global_res;

void my_client::init(utils::Properties &props){
   
   
   global_res=(struct global_resource*)malloc(sizeof(struct global_resource));
   resources_init(&res);
   config.primary_server_name[0] = (char *)props["primary0"].c_str();
   config.primary_server_name[1] = (char *)props["primary1"].c_str();
   config.primary_server_name[2] = (char *)props["primary2"].c_str();
   config.primary_server_name[3] = (char *)props["primary3"].c_str();
   config.backup_server_name[0] = (char *)props["backup0"].c_str();
   config.backup_server_name[1] = (char *)props["backup1"].c_str();
   config.extra_server_name[0] = (char *)props["extra0"].c_str();
   config.extra_server_name[1] =(char *)props["extra1"].c_str();
   USE_LRU= stoi(props.GetProperty("lru", "0"));
   if(props["backup_mode"] == "slimKV")
      BACKUP_MODE=2;
   else
      BACKUP_MODE=1;
   std::cout<<"BACKUP_MODE="<<BACKUP_MODE<<std::endl;
   std::cout<<"set server_name in creat function"<<std::endl;
   std::cout<<"begin connect with all nodes"<<std::endl;
   resources_create(&res);
   connect_qp(&res);
   std::cout<<"connect_qp finish"<<std::endl;
   for(int i=0;i<4;i++){
      connect_status[i] = true;
      this->req_times[i].store(0);
      read_times[i].store(0);
      this->total_latency[i].store(0);
   }
   for(int i=4;i<12;++i)
      connect_status[i]=true;
   write_times.store(0);
   total_lat_in_final = 0;
   total_req_times = 0;
   write_times.store(0);
   
   degraded_read_times.store(0);
   total_write_wait_latency.store(0);
   total_write_com_latency.store(0);
   total_read_wait_latency.store(0);
   total_read_com_latency.store(0);
   total_degraded_read_wait_latency.store(0);
   total_degraded_read_com_latency.store(0);
   std::thread real_time_th(real_time_task,this);
   real_time_th.detach();
   std::cout<<"init finish"<<std::endl;
   if(USE_LRU==1)
      lru_fit_count.store(0);
   
}

bool exist_fail = false;
int my_client::put(const std::string &key, const std::string &value){
     //std::cout<<"put first"<<std::endl;
     //std::cout<<"key_len = "<<key.size()<<" ,vlaue_len="<<value.size()<<std::endl;
   //  std::cout<<"put: key="<<key<<std::endl;//" ,value="<<value<<std::endl;
   
   uint32_t hash_id;
   int qp_id=-1;
   bool need_reput = false;
   std::vector<uint8_t> key_vector(key.begin(),key.end());
    hash_id = MurMurHash::MurMur3_32(key_vector)%4;
   if(BACKUP_MODE==2 && USE_LRU==1){
      bool need_delete=false;
      int backup_node_id=0;
      lru_cache.put(key,need_delete,hash_id,backup_node_id);
      if(need_delete){
         //std::cout<<"&##@@!!&& backup_node_id = "<<backup_node_id<<" hash_id ="<<hash_id<<" need delete lru_cache"<<std::endl;
         int clear_delete_hash_id = hash_id+4*(backup_node_id+1);
         while(connect_status[clear_delete_hash_id]==false)
         ;
         for(int i=0;;){
            if(primary_connect_lock_list[clear_delete_hash_id*MAX_QP_NUM+i].try_lock()==false)
               i = (i+1)%MAX_QP_NUM;
            else{
               qp_id = i;
               break;
            }
         }
      
         int op_code=3;
         memcpy(res.res_list[clear_delete_hash_id*MAX_QP_NUM+qp_id].buf,&op_code,4);
         int key_num=0;
         int message_len=lru_cache.clear_delete(backup_node_id,hash_id,res.res_list[clear_delete_hash_id*MAX_QP_NUM+qp_id].buf+12,key_num);
         
         memcpy(res.res_list[clear_delete_hash_id*MAX_QP_NUM+qp_id].buf+4,&hash_id,4);
         memcpy(res.res_list[clear_delete_hash_id*MAX_QP_NUM+qp_id].buf+8,&key_num,4);
         if(message_len>0){
            message_len+=12;
            //std::cout<<"message len = "<<message_len<<" key_num="<<key_num<<std::endl;
            post_send(&res,IBV_WR_SEND,clear_delete_hash_id,qp_id,0,message_len);
            poll_completion(&res,clear_delete_hash_id,qp_id);
         }
         primary_connect_lock_list[clear_delete_hash_id*MAX_QP_NUM+qp_id].unlock();
      }
   }
   reconnect:
   if(need_reput){
      connect_status[hash_id] =false;
      primary_connect_lock_list[hash_id*MAX_QP_NUM+qp_id].unlock();
      if(recover_lock[hash_id].try_lock()){
         clock_t beg,end;
         double duration;
         std::cout<<"fail occurs load_is_finish="<<load_finish<<std::endl;
         std::cout<<"total write times="<<write_times.load()<<std::endl;
         beg = clock();
         std::thread::id id = std::this_thread::get_id();
         std::cout<<"my thread is "<<id<<std::endl;
         reconnect_qp(&res,hash_id);
         while(1){
            bool no_req_wait_for_completion=true;
            for(int i=0;i<MAX_QP_NUM;i++){
               if(primary_connect_lock_list[hash_id*MAX_QP_NUM+i].try_lock()==false)
                  no_req_wait_for_completion=false;
               else
                  primary_connect_lock_list[hash_id*MAX_QP_NUM+i].unlock();
            }
            if(no_req_wait_for_completion)
               break;
         }
         connect_status[hash_id] =true;
         end = clock();
         duration = (double)(end - beg)/CLOCKS_PER_SEC;
         std::cout<<"primary recover time = "<<duration<<std::endl;
         recover_lock[hash_id].unlock();
      }
      
   }
   clock_t beg,end;
   double duration;
   beg = clock();
    
   while(connect_status[hash_id]==false)
      ;
    for(int i=0;;){
      if(primary_connect_lock_list[hash_id*MAX_QP_NUM+i].try_lock()==false)
         i = (i+1)%MAX_QP_NUM;
      else{
         qp_id = i;
         break;
      }
    }
   end = clock();
   duration = (double)(end - beg)/CLOCKS_PER_SEC;
   total_write_wait_latency.store(total_write_wait_latency.load()+duration);
   
   beg = clock();
    int op_code = 1;
    int key_len = key.size();
    int value_len = value.size();
    int rc;
    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&op_code,4);
    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,&key_len,4);
    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,&value_len,4);
    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+12,key.data(),key_len);
    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+12+key_len,value.data(),value_len);
    
    rc = post_send(&res,IBV_WR_SEND,hash_id,qp_id,0,12+key_len+value_len);
    if(rc){
      need_reput = true;
      std::cout<<"send fail"<<std::endl;
      goto reconnect;
    }
    rc = poll_completion(&res,hash_id,qp_id);
    if(rc){
      need_reput = true;
      std::cout<<"send time out"<<std::endl;
       rc = post_send(&res,IBV_WR_RDMA_READ,hash_id,qp_id,0,4);
      if(rc==0)
         rc = poll_completion_quick(&res,hash_id,qp_id);
      if(rc)
         goto reconnect;
    }
     if(exist_fail)
       std::cout<<"^^^put to node:"<<hash_id<<"finish"<<std::endl;
    memset(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,0,8);
    
    //int finish_flag=0;
   //  while(1){
   //    rc = post_send(&res,IBV_WR_RDMA_READ,hash_id,0,8);
   //    if(rc)
   //       goto exit;
   //    rc = poll_completion(&res,hash_id);
   //    if(rc)
   //       goto exit;
   //    memcpy(&finish_flag,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,4);
   //    if(finish_flag==1)
   //       break;
   //    else if(finish_flag ==0)
   //       continue;
   //    else 
   //       goto exit;
   //  }
   //  finish_flag = 0;
   //  memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&finish_flag,4);
   //  rc = post_send(&res,IBV_WR_RDMA_WRITE,hash_id,0,8);
   //  if(rc)
   //    goto exit;
   //  rc = poll_completion(&res,hash_id);
   //  if(rc)
   //    goto exit;
   rc = post_receive(&res,hash_id,qp_id,4);
    if(rc){
      need_reput = true;
      std::cout<<"recv fail in put"<<std::endl;
      goto reconnect;
    }
    rc = poll_completion(&res,hash_id,qp_id);
    if(rc){
      need_reput = true;
      std::cout<<"recv time out in put"<<std::endl;
       rc = post_send(&res,IBV_WR_RDMA_READ,hash_id,qp_id,0,4);
      if(rc==0)
         rc = poll_completion_quick(&res,hash_id,qp_id);
      if(rc)
         goto reconnect;
    }
    int status;
    memcpy(&status,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,4);
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    primary_connect_lock_list[hash_id*MAX_QP_NUM+qp_id].unlock();
    write_times++;
    req_times[hash_id]++;
    total_write_com_latency.store(total_write_com_latency.load()+duration);
    if(write_times.load()%1000000==0)
         std::cout<<"$$$write_wait_lat="<<total_write_wait_latency.load()/write_times.load()<<" com_lat= "<<total_write_com_latency.load()/write_times.load()<<std::endl;
    //std::cout<<"node:"<<hash_id<<"put finish"<<std::endl;
    return status;
}
int my_client::get(const std::string &key, std::string &value){
   // static std::atomic<double> total_backup_get_time;
   // static std::atomic<uint32_t> total_backup_get_num;
   int qp_id;
   static bool is_first_get =true;
   
   //std::cout<<"get key = "<<key<<std::endl;
   load_finish = true;
   bool need_reput = false;
   uint32_t hash_id;
   std::string real_key(key,5,key.size()-5);
   std::vector<uint8_t> key_vector(real_key.begin(),real_key.end());
    hash_id = MurMurHash::MurMur3_32(key_vector)%4;
   uint32_t init_hash_id = hash_id;
   reconnect:
   if(need_reput){
      connect_status[hash_id] =false;
      primary_connect_lock_list[hash_id*MAX_QP_NUM+qp_id].unlock();
      if(recover_lock[hash_id].try_lock()){
         clock_t beg,end;
         double duration;
         std::cout<<"fail occurs load_is_finish="<<load_finish<<std::endl;
         std::cout<<"total write times="<<write_times.load()<<std::endl;
         beg = clock();
         std::thread::id id = std::this_thread::get_id();
         std::cout<<"my thread is "<<id<<std::endl;
         reconnect_qp(&res,hash_id);
         while(1){
            bool no_req_wait_for_completion=true;
            for(int i=0;i<MAX_QP_NUM;i++){
               if(primary_connect_lock_list[hash_id*MAX_QP_NUM+i].try_lock()==false)
                  no_req_wait_for_completion=false;
               else
                  primary_connect_lock_list[hash_id*MAX_QP_NUM+i].unlock();
            }
            if(no_req_wait_for_completion)
               break;
         }
         connect_status[hash_id] =true;
         end = clock();
         duration = (double)(end - beg)/CLOCKS_PER_SEC;
         std::cout<<"primary recover time = "<<duration<<std::endl;
         recover_lock[hash_id].unlock();
      }
      
   }
   clock_t beg,end;
   double duration;
   beg = clock();

   int backup_node_id=0;
   bool push=false;
   bool exist=false;
   // if(BACKUP_MODE==1){
      if (BACKUP_MODE==2 && USE_LRU==1){
        
         exist=lru_cache.exist(real_key,init_hash_id,backup_node_id,push);
         if(exist){
            hash_id=init_hash_id+4*(backup_node_id+1);
            lru_fit_count++;
            int count=lru_fit_count.load();            
            if(count%100000==0)
               std::cout<<"lru_fit_count = "<<count<<std::endl;
         }
         //std::cout<<"exist = "<<exist<<"  push="<<push<<std::endl;
      }
      else if(BACKUP_MODE==1 || BACKUP_MODE==2 && USE_LRU==2){
         //PBR or subread
       hash_id=init_hash_id+4*(req_times[init_hash_id].load()%3);
      }
   // }
   // if(hash_id>=4)
   //    std::cout<<"redirect to hash_id = "<<hash_id<<std::endl;
   while(connect_status[hash_id]==false)
      ;
    for(int i=0;;){
      if(primary_connect_lock_list[hash_id*MAX_QP_NUM+i].try_lock()==false){
         if(i==MAX_QP_NUM-1){
            i=0;
         }
         else
            i+=1;
      }
      else{
         qp_id = i;
         break;
      }
    }
    

   if(BACKUP_MODE==2 && USE_LRU==1){
      if(push&&exist==false){
         int op_code = 4;
         int key_num;
         // {
         //    int opcode=2;
         //    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&opcode,4);
         //    lru_cache.insert_cache_list_lock[hash_id][backup_node_id].lock();
         //    std::string key = insert_cache_list[hash_id][backup_node_id][0];
         //    int key_len=key.size();
         //    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,&key_len,4);
         //    memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,&key.data(),key_len);

         //    lru_cache.insert_cache_list_lock[hash_id][backup_node_id].unlock();
         // }
         memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&op_code,4);
         memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,&backup_node_id,4);
         int message_len = lru_cache.push(hash_id,backup_node_id,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+12,key_num);
         memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,&key_num,4);
         if(message_len>0){
         message_len+=12;
         //std::cout<<"need push lru in pid : "<<hash_id<<" qp_id="<<qp_id<<" to bid = "<<backup_node_id<<" msg_len="<<message_len<<std::endl;
         post_send(&res,IBV_WR_SEND,hash_id,qp_id,0,message_len);
         poll_completion(&res,hash_id,qp_id);
         }
      }
   }
   end = clock();
   duration = (double)(end - beg)/CLOCKS_PER_SEC;
   double wait_time=duration;
   beg = clock();
    int op_code = 2;
    int key_len = real_key.size();
    if(init_hash_id>4)
      std::cout<<"fatal error with init_hash_id = "<<init_hash_id<<std::endl;
    if(hash_id>=4){
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&op_code,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,&init_hash_id,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,&key_len,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+12,real_key.data(),key_len);
    }
    else{
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&op_code,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,&key_len,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,real_key.data(),key_len);
    }
    //std::cout<<"node "<<hash_id<<" : "<<qp_id <<" key = "<<real_key<<std::endl;
    int rc;
    rc = post_send(&res,IBV_WR_SEND,hash_id,qp_id,0,12+key_len);
    if(rc){
      need_reput = true;
      
      goto reconnect;
    }
    rc = poll_completion(&res,hash_id,qp_id);
    if(rc){
      need_reput = true;

      goto reconnect;
    }
    rc = post_receive(&res,hash_id,qp_id,MAX_KV_LEN_FROM_CLIENT);
    if(rc){
      need_reput = true;

      goto reconnect;
    }
    rc = poll_completion(&res,hash_id,qp_id);
    
    if(rc){
      need_reput = true;

      goto reconnect;
    }

   

    primary_connect_lock_list[hash_id*MAX_QP_NUM+qp_id].unlock();
    //std::cout<<"get finish"<<std::endl;
    int status;
    memcpy(&status,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,4);
    if(BACKUP_MODE==2 && USE_LRU==2 && status==2&&hash_id>=4){
     
      int sge_id,offset;
      memcpy(&sge_id,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,4);
      memcpy(&offset,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+12,4);
      //std::cout<<"sub read from backup node, sge_id = "<<sge_id<<" offset = "<<offset<<std::endl;
      hash_id=init_hash_id;
      while(connect_status[hash_id]==false)
      ;
      for(int i=0;;){
         if(primary_connect_lock_list[hash_id*MAX_QP_NUM+i].try_lock()==false){
            if(i==MAX_QP_NUM-1){
               i=0;
            }
            else
               i+=1;
         }
         else{
            qp_id = i;
            break;
         }
      }
      int op_code=3;
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,&op_code,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,&sge_id,4);
      memcpy(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,&offset,4);
      
      rc = post_send(&res,IBV_WR_SEND,hash_id,qp_id,0,12);
      if(rc){
         need_reput = true;
         
         goto reconnect;
      }
      rc = poll_completion(&res,hash_id,qp_id);
      if(rc){
         need_reput = true;

         goto reconnect;
      }
      rc = post_receive(&res,hash_id,qp_id,MAX_KV_LEN_FROM_CLIENT);
      if(rc){
         need_reput = true;

         goto reconnect;
      }
      rc = poll_completion(&res,hash_id,qp_id);
      if(rc){
         need_reput = true;

         goto reconnect;
      }
      memcpy(&status,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf,4);
      primary_connect_lock_list[hash_id*MAX_QP_NUM+qp_id].unlock();
      
      //std::cout<<"sub read transfer to primary node finish "<<std::endl;
    }
    static int backup_get_fail=0;
    
    if(status==1&&hash_id>=4){
      backup_get_fail++;
       if(backup_get_fail%500==0)
          std::cout<<"back_get_fail_count="<<backup_get_fail<<std::endl;
    }
    int value_len;
    memcpy(&value_len,res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+4,4);
    value.assign(res.res_list[hash_id*MAX_QP_NUM+qp_id].buf+8,value_len);
    
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
   
    total_latency[init_hash_id].store(duration+total_latency[hash_id].load());
    req_times[init_hash_id]++;
    read_times[init_hash_id]++;
   //std::cout<<"get value = "<<value<<std::endl;
   if(is_first_get){
      is_first_get=false;
      std::cout<<"^^^^^^load finish^^^^"<<std::endl;
      // total_backup_get_num.store(0);
      // total_backup_get_time.store(0.0);
   }
   // if(read_times.load()%300000==0){
   //    std::cout<<"key= "<<real_key<<"  ,hash_id="<<hash_id<<std::endl;
   //    std::cout<<"status="<<status<<std::endl;
   //    std::cout<<"value= "<<value<<std::endl;
   // }
   static int num_get=0;
   static double degraded_time=0;
   if(status ==3){
      status =0;
      degraded_read_times++;
      total_degraded_read_wait_latency.store(total_degraded_read_wait_latency.load()+wait_time);
      total_degraded_read_com_latency.store(total_degraded_read_com_latency.load()+duration);
      // if(degraded_read_times.load()%10000==0)
      //    std::cout<<"***degraded_read__wait_lat="<<total_degraded_read_wait_latency.load()/degraded_read_times.load()<<" com_lat= "<<total_degraded_read_com_latency.load()/degraded_read_times.load()<<std::endl;
   }
   else{
      total_read_wait_latency.store(total_read_wait_latency.load()+wait_time);
      total_read_com_latency.store(total_read_com_latency.load()+duration);
      int total_read_times=read_times[0].load()+read_times[1].load()+read_times[2].load()+read_times[3].load();
      // if(total_read_times%100000==0)
      //    std::cout<<"&&&read_wait_lat="<<total_read_wait_latency.load()/total_read_times<<" com_lat= "<<total_read_com_latency.load()/total_read_times<<std::endl;
   }
    return status;
}