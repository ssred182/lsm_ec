#include "client_rdma.h"
#include "core/properties.h"
#include <time.h>
#include <vector>
#include <mutex>
#include <string>
#include <iostream>
#include <atomic>
#include <list>
#include <map>
#include <unordered_map>
#include "oneapi/tbb/concurrent_hash_map.h"
#define USE_CON_MAP 1
#define PUT_DELETE_LRU 1
using namespace std;
struct MyHashCompare {
    static size_t hash( const string& x ) {
        size_t h = 0;
        for( const char* s = x.c_str(); *s; ++s )
            h = (h*17)^*s;
        return h;
    }
    //! True if strings are equal
    static bool equal( const string& x, const string& y ) {
        return x==y;
    }
};
using namespace oneapi::tbb;

class LruCache{
   public:
      using list_node = std::pair<std::string,int>;
      using list_node_ptr = std::list<list_node>::iterator;
      std::vector<std::string> insert_cache_list[4][2];
      std::mutex insert_cache_list_lock[4][2];
      std::vector<std::string> delete_cache_list[2][4];
      std::mutex delete_cache_list_lock[2][4];
      int capacity;
      int out_put_count;
      std::mutex lru_lock[4];
      std::list<list_node> data_list[4];
      typedef concurrent_hash_map<std::string,list_node_ptr,MyHashCompare> ConReadCache;
      #if USE_CON_MAP==1
      vector<ConReadCache> con_search_map;
      #else
      std::unordered_map<std::string,list_node_ptr> search_map[4];
      #endif
      #if USE_CON_MAP==1
      LruCache(){
         for(int i=0;i<4;++i)
            con_search_map.emplace_back(ConReadCache(200*1024));
      #else
      LruCache(){
      #endif
         capacity=200*1000;
         out_put_count=1;
         //search_map.reserve(500*1024);
         for(int i=0;i<4;++i){
            for(int j=0;j<2;++j){
                insert_cache_list[i][j].reserve(1000);
                delete_cache_list[j][i].reserve(500);
            }
         }
      }
      bool exist(const std::string &key,int primary_node_id,int &_backup_node_id,bool &full){
         //use this function every get,return whether to push
         //std::cout<<"debug p1"<<std::endl;
         //std::cout<<"key = "<<key<<std::endl;
         #if USE_CON_MAP==0
         lru_lock[primary_node_id].lock();
         auto map_iter=search_map[primary_node_id].find(key);
         //std::cout<<"debug p2"<<std::endl;
         if(map_iter==search_map[primary_node_id].end()){
            //std::cout<<"not exist"<<std::endl;
            lru_lock[primary_node_id].unlock();
            //std::cout<<"debug p3"<<std::endl;
            int backup_node_id = key.back()%2;
            _backup_node_id=backup_node_id;
            //std::cout<<"pid= "<<primary_node_id<<" bid="<<backup_node_id<<std::endl;
            //std::cout<<"debug p4"<<std::endl;
            
            insert_cache_list_lock[primary_node_id][backup_node_id].lock();
            insert_cache_list[primary_node_id][backup_node_id].emplace_back(key);
            //std::cout<<"debug p5"<<std::endl;
            bool ret;
            if(insert_cache_list[primary_node_id][backup_node_id].size()>=1000)
               full=true;
            else
               full = false;
            insert_cache_list_lock[primary_node_id][backup_node_id].unlock();
            return false;
         }
         //std::cout<<"exist"<<std::endl;
         //std::cout<<"debug p6"<<std::endl;
         list_node_ptr iter = map_iter->second;
         _backup_node_id=iter->second;
         data_list[primary_node_id].erase(iter);
         //std::cout<<"debug p7"<<std::endl;
         data_list[primary_node_id].push_front({key,_backup_node_id});
         map_iter->second=data_list[primary_node_id].begin();
         lru_lock[primary_node_id].unlock();
         //std::cout<<"debug p8"<<std::endl;
         return true;
         #else
         ConReadCache::accessor a;
         if(con_search_map[primary_node_id].find(a,key) ){
            lru_lock[primary_node_id].lock();
            list_node_ptr iter = a->second;
            _backup_node_id=iter->second;
            data_list[primary_node_id].erase(iter);
            data_list[primary_node_id].push_front({key,_backup_node_id});
            a->second=data_list[primary_node_id].begin();
            lru_lock[primary_node_id].unlock();
            a.release();
            return true;
         }
         a.release();
         int backup_node_id = key.back()%2;
            _backup_node_id=backup_node_id;
            insert_cache_list_lock[primary_node_id][backup_node_id].lock();
            insert_cache_list[primary_node_id][backup_node_id].emplace_back(key);
            bool ret;
            if(insert_cache_list[primary_node_id][backup_node_id].size()>=1000)
               full=true;
            else
               full = false;
            insert_cache_list_lock[primary_node_id][backup_node_id].unlock();
            return false;
         #endif
      }
      void put(const std::string &key,bool &need_delete,int primary_node_id,int &backup_node_id){
         // std::cout<<"debug p1"<<std::endl;
         //std::cout<<"key = "<<key<<std::endl;
         // std::cout<<"debug p1.5"<<std::endl;
         // std::cout<<"search_map.size="<<search_map.size()<<std::endl;
         
         //backup_node_id=0;
         #if USE_CON_MAP==0
          lru_lock[primary_node_id].lock();
         // //std::cout<<"debug p1.8"<<std::endl;
         auto map_iter = search_map[primary_node_id].find(key);
         if(map_iter==search_map[primary_node_id].end()){
            //std::cout<<"debug p2"<<std::endl;
            lru_lock[primary_node_id].unlock();
            return ;
         }
         // //std::cout<<"debug p3"<<std::endl;
          list_node_ptr iter = map_iter->second;//search_map[key];
         // //backup_node_id=iter->second;
         
          data_list[primary_node_id].erase(iter);
         // // //std::cout<<"debug p4"<<std::endl;
         search_map[primary_node_id].erase(map_iter);
        
         // //std::cout<<"debug p5"<<std::endl;
          lru_lock[primary_node_id].unlock();
         
         #else
            //cout<<"try find in con_search_map_list.con_search_map"<<endl;
             ConReadCache::accessor a;
            if(!con_search_map[primary_node_id].find(a,key)){
               a.release();
               //cout<<"not exist in con_search_map"<<endl;
               return ;
            }
             lru_lock[primary_node_id].lock();
            // //cout<<"debug p1"<<endl;
             list_node_ptr iter = a->second;//search_map[key];
            // //cout<<"debug p2"<<endl;
             backup_node_id=iter->second;
            
             data_list[primary_node_id].erase(iter);    
             lru_lock[primary_node_id].unlock();           
             con_search_map[primary_node_id].erase(a);
             a.release();
         #endif
         
          delete_cache_list_lock[backup_node_id][primary_node_id].lock();
          delete_cache_list[backup_node_id][primary_node_id].emplace_back(key);
         // //std::cout<<"debug p6"<<std::endl;
         backup_node_id = key.back()%2;
         if(delete_cache_list[backup_node_id][primary_node_id].size()>=1000){
            need_delete=true;     
         }
         delete_cache_list_lock[backup_node_id][primary_node_id].unlock();

         // insert_cache_list_lock[primary_node_id][backup_node_id].lock();
         // insert_cache_list[primary_node_id][backup_node_id].emplace_back(key);
         // insert_cache_list_lock[primary_node_id][backup_node_id].unlock();
      }
      int clear_delete(int backup_node_id,int primary_node_id,char* addr,int &key_num){
         if(delete_cache_list_lock[backup_node_id][primary_node_id].try_lock()==false)
            return 0;
         if(delete_cache_list[backup_node_id][primary_node_id].size()<1000){
            delete_cache_list_lock[backup_node_id][primary_node_id].unlock();
            return 0;
         }
         int offset=0;
         key_num=0;
            for(auto &node:delete_cache_list[backup_node_id][primary_node_id]){
               key_num++;
               int key_len= node.size();
               memcpy(addr+offset,&key_len,4);
               offset+=4;
               memcpy(addr+offset,(const char*)node.data(),key_len);
               offset+=key_len;
            }
            delete_cache_list[backup_node_id][primary_node_id].clear();
            delete_cache_list_lock[backup_node_id][primary_node_id].unlock();
            return offset;
      }
      int push(int primary_node_id,int backup_node_id,char *addr,int &key_num){
         int offset=0;
         if(insert_cache_list_lock[primary_node_id][backup_node_id].try_lock()==false )
            return 0;
         key_num=0;
         bool is_first_delete=true;
         int inlist_notinmap_count=0;
         #if USE_CON_MAP==0
         std::vector<std::string> tmp_cache_list(insert_cache_list[primary_node_id][backup_node_id]);
         insert_cache_list[primary_node_id][backup_node_id].clear();
         insert_cache_list_lock[primary_node_id][backup_node_id].unlock();
        
         
         for(auto &node:tmp_cache_list){
            key_num++;
            int key_len= node.size();
            memcpy(addr+offset,&key_len,4);
            offset+=4;
            memcpy(addr+offset,(const char*)node.data(),key_len);
            offset+=key_len;
            list_node p;
            p.first=node;
            p.second=backup_node_id;
            lru_lock[primary_node_id].lock();
            data_list[primary_node_id].emplace_front(p);
            auto map_iter=search_map[primary_node_id].find(node);
            if(map_iter!=search_map[primary_node_id].end()){
               data_list[primary_node_id].erase(map_iter->second);
            }
            search_map[primary_node_id][node]=data_list[primary_node_id].begin();
            
            if(data_list[primary_node_id].size()>capacity){
               if(is_first_delete){
                  is_first_delete=false;
                  //std::cout<<"lru full pop elements"<<std::endl;
               }
               auto &node=data_list[primary_node_id].back();
               data_list[primary_node_id].pop_back(); 
               auto iter = search_map[primary_node_id].find(node.first);
               if(iter!=search_map[primary_node_id].end()){
                  
                  search_map[primary_node_id].erase(iter);
                  int backup_node_id = node.second;

                  delete_cache_list_lock[backup_node_id][primary_node_id].lock();
                  delete_cache_list[backup_node_id][primary_node_id].emplace_back(node.first);
                  delete_cache_list_lock[backup_node_id][primary_node_id].unlock();    
               }else{
                  inlist_notinmap_count++;
               }
            }
            lru_lock[primary_node_id].unlock();
         }
         // if(is_first_delete==false){
         //    std::cout<<"data_list ele not exist in search map count = "<<inlist_notinmap_count<<std::endl;
         // }
         // if(is_first_delete==false)
         //    std::cout<<"push full finish"<<std::endl;
         // if(++out_put_count%200==0)
         //    std::cout<<"data_list_size="<<data_list[primary_node_id].size()<<" search_map_size="<<search_map[primary_node_id].size()<<std::endl;
         
         insert_cache_list[primary_node_id][backup_node_id].clear();
         insert_cache_list_lock[primary_node_id][backup_node_id].unlock();
         #else
            std::vector<std::string> tmp_cache_list(insert_cache_list[primary_node_id][backup_node_id]);
            insert_cache_list[primary_node_id][backup_node_id].clear();
            insert_cache_list_lock[primary_node_id][backup_node_id].unlock();
            //cout<<"insert num = "<<tmp_cache_list.size()<<" size of search_map = "<<con_search_map[primary_node_id].size()<<" size of data_list = "<<data_list[primary_node_id].size()<<endl;
            for(auto &node:tmp_cache_list){
               key_num++;
               int key_len= node.size();
               memcpy(addr+offset,&key_len,4);
               offset+=4;
               memcpy(addr+offset,(const char*)node.data(),key_len);
               offset+=key_len;
               list_node p;
               p.first=node;
               p.second=backup_node_id;
               
               ConReadCache::accessor a;
               if(con_search_map[primary_node_id].find(a,node)){
                  lru_lock[primary_node_id].lock();
                  data_list[primary_node_id].erase(a->second);
                  data_list[primary_node_id].emplace_front(p);
                  a->second=data_list[primary_node_id].begin();
                  lru_lock[primary_node_id].unlock();
               }else{
                  a.release();
                  con_search_map[primary_node_id].insert(a,node);
                  lru_lock[primary_node_id].lock();
                  data_list[primary_node_id].emplace_front(p);
                  a->second=data_list[primary_node_id].begin();
                  lru_lock[primary_node_id].unlock();
               }
               a.release();                        
               if(data_list[primary_node_id].size()>capacity){
                  if(is_first_delete){
                     is_first_delete=false;
                     //std::cout<<"lru full pop elements"<<std::endl;
                  }
                  lru_lock[primary_node_id].lock();
                  auto &node=data_list[primary_node_id].back();
                  data_list[primary_node_id].pop_back();
                  lru_lock[primary_node_id].unlock();
                  ConReadCache::accessor a;
                  if(con_search_map[primary_node_id].find(a,node.first)){
                     con_search_map[primary_node_id].erase(a);
                     a.release();
                     int backup_node_id = node.second;
                     delete_cache_list_lock[backup_node_id][primary_node_id].lock();
                     delete_cache_list[backup_node_id][primary_node_id].emplace_back(node.first);
                     delete_cache_list_lock[backup_node_id][primary_node_id].unlock();   
                  }else{
                     inlist_notinmap_count++;
                  }
               }
            }
            //cout<<"push finish"<<endl;
         #endif
         
         return offset;
      }
};
class my_client{
    public:
        int put(const std::string &key, const std::string &value);
        int get(const std::string &key, std::string &value);
    public:
        int BACKUP_MODE;
        std::mutex primary_connect_lock_list[12*MAX_QP_NUM];
        std::mutex recover_lock[12];
        bool connect_status[12];
        struct resources res;
        std::atomic<double> total_latency[4];
        std::atomic<uint32_t> req_times[4];
        std::atomic<uint32_t> write_times;
        std::atomic<double> total_write_wait_latency;
        std::atomic<double> total_write_com_latency;
        std::atomic<uint32_t> read_times[4];
        std::atomic<double> total_read_wait_latency;
        std::atomic<double> total_read_com_latency;
        std::atomic<int> degraded_read_times;
        std::atomic<double> total_degraded_read_wait_latency;
        std::atomic<double> total_degraded_read_com_latency;
        double total_lat_in_final;
        int total_req_times;
        bool load_finish = false;
        int USE_LRU=0; 
        LruCache lru_cache;
        std::atomic<int> lru_fit_count;
       
        //std::mutex reconnect_lock;
        // char *primary_server_name[4];
        // char *extra_server_name[2];
    public:
      
      my_client(){};
        void init(utils::Properties &props);
};