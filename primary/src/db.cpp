#include <thread>
#include <time.h>
#include "db.h"
#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include "rdma_connect.h"
#include "define.h"
#define TEST_MODE 3
//1--no rep   2--WAL   3--PBR     4--ec
extern int K;
int BACKUP_MODE;
//1--PBR 2--sync-gc     3---parity_update
bool isFileExists_stat(std::string& name) {
  struct stat buffer;   
  return (stat(name.c_str(), &buffer) == 0); 
}
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

extern struct config_t config;






void get_manipath(std::string &manifest_path){
    std::string current_path  ="../rocksdb_lsm/CURRENT";
    std::ifstream fin;
    fin.open(current_path,std::ios::in|std::ios::binary);
    fin.seekg(0,std::ios::end);
    uint32_t file_size = fin.tellg();
    fin.seekg(0,std::ios::beg);
    //为了略去结尾的换行符
    manifest_path.resize(file_size-1);
    fin.read((char*)manifest_path.data(),file_size-1);
    fin.close();
}
void DB::get_new_ssts(){
    int target_id = max_rocks_id + 1;
    
    std::string target_path(11,'0');
    target_path[6]='.';
    target_path[7]='s';
    target_path[8]='s';
    target_path[9]='t';
    target_path[10]='\0';
    std::string tmp;
    for(int not_find_num=0;not_find_num<10;){
        tmp = std::to_string(target_id);
        for(int i=0;i<tmp.size();i++){
            target_path[6-tmp.size()+i] = tmp[i];
        }
        std::string prex = "../rocksdb_lsm/";
        prex.append(target_path);
        if(access(prex.data(),F_OK)==0){
            max_rocks_id = target_id;
            struct stat stat_buf;
            stat(prex.c_str(),&stat_buf);          
            rocks_id_list[target_id] = stat_buf.st_size;
            not_find_num = 0;  
            new_sst_list.emplace_back(target_id);
        }
        else{
            not_find_num++;
        }
        target_id++;
    }
    // for(auto id_pair : rocks_id_list){
    //     new_sst_list.emplace_back(id_pair.first);
    // }
} 
void DB::get_delete_ssts(std::vector<uint32_t> &delete_sst_list){
    std::string target_path(11,'0');
    target_path[6]='.';
    target_path[7]='s';
    target_path[8]='s';
    target_path[9]='t';
    target_path[10]='\0';
    std::string tmp;
    uint32_t target_id;
    for(auto iter=rocks_id_list.begin();iter!=rocks_id_list.end();){
        target_id = iter->first;
        tmp = std::to_string(target_id);
        for(int i=0;i<tmp.size();i++){
            target_path[6-tmp.size()+i] = tmp[i];
        }
        std::string prex = "../rocksdb_lsm/";
        prex.append(target_path);
        if(access(prex.data(),F_OK)==0){ 
            struct stat stat_buf;
            stat(prex.c_str(),&stat_buf);         
            if(rocks_id_list[target_id] != stat_buf.st_size){
                rocks_id_list[target_id] = stat_buf.st_size;
                new_sst_list.emplace_back(target_id);
            } 
            
            iter++;
        }
        else{
            rocks_id_list.erase(iter++);
            delete_sst_list.emplace_back(target_id);
        }
    }
}

static uint32_t read_file(const char* buf,const std::string &target_path){
    std::ifstream fin;
    std::string prex = "../rocksdb_lsm/";
    prex.append(target_path);
    fin.open(prex,std::ios::in|std::ios::binary);
    fin.seekg(0,std::ios::end);
    uint32_t file_size = fin.tellg();
    fin.seekg(0,std::ios::beg);
    fin.read((char*)buf,file_size);
    fin.close();
    return file_size;
}
uint32_t get_file_size(const std::string &target_path){
    std::ifstream fin;
    std::string prex = "../rocksdb_lsm/";
    prex.append(target_path);
    // fin.open(prex,std::ios::in|std::ios::binary);
    // fin.seekg(0,std::ios::end);
    // uint32_t file_size = fin.tellg();
    // fin.close();

    struct stat target_stat;
    stat(prex.c_str(), &target_stat);
    uint32_t file_size = target_stat.st_size;
    return file_size;
}


static int offset_of_wal = 0;
char wal_buf[MAX_SGE_SIZE];
void erase_wal(){
    
    std::ofstream fout;
    fout.open("tail_sge.log",std::ios::in|std::ios::out|std::ios::binary);
    fout.seekp(0,std::ios::beg);
    
    memset(wal_buf,0,MAX_SGE_SIZE);
    fout.write((char *)wal_buf,MAX_SGE_SIZE);
    fout.close();
    offset_of_wal = 0;
    //std::cout<<"erase wal finish"<<std::endl;
}
int write_wal(const std::string &key,const std::string &value){
    // clock_t beg,end;
    // double duration;
    // beg = clock();
    std::ofstream fout("tail_sge.log",std::ios::in|std::ios::out|std::ios::binary);
    fout.seekp(offset_of_wal,std::ios::beg);
    //char buf[MAX_SGE_SIZE];
    uint32_t key_len,value_len;
    key_len = key.size();
    value_len = value.size();
    memcpy(wal_buf,&key_len,4);
    memcpy(wal_buf+4,&value_len,4);
    memcpy(wal_buf+8,key.data(),key_len);
    memcpy(wal_buf+8+key_len,value.data(),value_len);
    fout.write((char *)wal_buf,8+key_len+value_len);
    fout.close();
    offset_of_wal += key_len+value_len+8;
    // end = clock();
    // duration = (double)(end - beg)/CLOCKS_PER_SEC;
    //std::cout<<"wal time is "<<duration<<std::endl;
}
DB::DB(std::string & path,struct resources *res_ptr_,uint32_t node_id_){
    res_ptr = res_ptr_;
    node_id = node_id_;
    max_sge_size = MAX_SGE_SIZE;
    num_flush_thread = 4;
    max_sge_num_trigger_gc = 1024;
    sge_num_per_gc = 128;
    init(path);
    #if TEST_MODE==2
    erase_wal();
    #endif
}
void DB::init(std::string &path){

    //打开一个rocksDB
    rocksdb::DB *dbptr;
    rocksdb::Options options;
    options.write_buffer_size = 8<<20;
    options.compression = rocksdb::kNoCompression;
    options.max_background_flushes = 1;
    options.max_background_compactions = 1;
     /// options.max_write_buffer_number = 2;
    options.create_if_missing = true;
    rocksdb::DB::Open(options, path, &dbptr);
    assert(dbptr != nullptr);
    rocksdb_ptr = std::shared_ptr<rocksdb::DB>(dbptr);
    gc_sge_map_ptr=&rocksdb::gc_sge_map;
    gc_sge_map_lock_ptr=&rocksdb::gc_sge_map_lock;
    get_manipath(last_manifest_path);
    //stat(last_manifest_path.c_str(), &manifest_stat);
    last_mani_size = 0;
    max_rocks_id = 5;
    
    //tp = std::make_shared<mq_cache::ThreadPool>(num_flush_thread);
    cur_sge_id.store(1);
    head_sge_id.store(1);
    //TODO : here link the gc_sge_map_ptr with the rocksdb

    create_new_sgement();

    for(int i=0;i<PUTTING_HASH_SIZE;i++){
        is_putting_hash_map[i].store(0);
    }
}
void DB::create_new_sgement(){
    tail_sge_ptr=  std::make_shared<NAM_SGE::ValueSGE>(cur_sge_id++,max_sge_size);
}
bool is_gc = false;
void flush_task (DB *db,ValueSGE_ptr target_sge_ptr,uint32_t gc_num){
        target_sge_ptr->flush();
        #if DEBUG
        std::cout<<"flush complete"<<std::endl;
        #endif
        db->flushing_mutex.lock();
        for(bool finish=false;finish==false;){
            for(auto iter = db->flushing_sges.begin();iter!=db->flushing_sges.end();iter++){
                if((*iter)->sge_id==target_sge_ptr->sge_id){
                    db->flushing_sges.erase(iter);
                    finish = true;
                    break;
                }
            }
        }
        db->flushing_mutex.unlock();
        //if(db->cur_sge_id.load()-db->head_sge_id.load()>db->max_sge_num_trigger_gc){
        if(gc_num >0){
            //std::cout<<"i am flush task,gc_num= "<<gc_num<<std::endl;
            if(db->gc_lock.try_lock()){
                std::cout<<"enter gc function"<<std::endl;
                is_gc = true;
                db->gc(gc_num);
                db->gc_lock.unlock();
                is_gc = false;
            }  
        }
}
void DB::generate_flush_task(ValueSGE_ptr target_sge_ptr,uint32_t gc_num){
 
    //flush task text
    std::thread flush_th(flush_task,this,target_sge_ptr,gc_num);
    flush_th.detach();
    std::lock_guard<std::mutex> lock(flushing_mutex);
    flushing_sges.emplace_back(target_sge_ptr);
}
double flush_tail_time=0,send_index_time=0,rdma_write_time=0,local_put_sge_time=0,local_put_rocks_time=0,gc_local_time=0,gc_rdma_time=0;

#if TEST_MODE==3||TEST_MODE==4

bool DB::put_impl(const std::string &key, const std::string &value){
    uint32_t kv_len = key.size()+value.size()+8;
    clock_t beg,end;
    double duration;
    static int put_num=0;
    put_mutex.lock();
    put_num++;
    //tail sge的满全部由db判断
    if(tail_sge_ptr->get_sge_size()+ kv_len>max_sge_size){
        
        beg = clock();
        //std::cout<<"&&&&&&&flush tail !!!!!!!"<<std::endl;
        //uint32_t tail_len;
        //memcpy(&tail_len,tail_sge_ptr->buf.data()+max_sge_size-4,4);
        //std::cout<<"tail len ="<<tail_len<<std::endl;
        //std::cout<<"^^^^flush sge id="<< tail_sge_ptr->sge_id << " len= "<<tail_sge_ptr->get_sge_size()<<std::endl;
        // post_send(res_ptr,IBV_WR_RDMA_READ,0,max_sge_size-4,4);
        // poll_completion(res_ptr,0);
        // uint32_t read_len;
        // memcpy(&read_len,res_ptr->buf+max_sge_size-4,4);
        // std::cout<<"while inform flush tail node0:read_len ="<<read_len<<std::endl;
        // post_send(res_ptr,IBV_WR_RDMA_READ,1,max_sge_size-4,4);
        // poll_completion(res_ptr,1);
        // memcpy(&read_len,res_ptr->buf+max_sge_size-4,4);
        // std::cout<<"while inform flush tail node1:read_len ="<<read_len<<std::endl;
        uint32_t old_id = tail_sge_ptr->sge_id;
        
        //TODO:检查rocksdb文件变化，以约定好的消息格式发送给backup通知flush tail并且同步文件
        uint32_t flush_tail = 1;
        uint64_t none = 0;
        rdma_msg_lock[0].lock();
        memcpy(res_ptr->msg_buf[0]+20,&flush_tail,4);
        memcpy(res_ptr->msg_buf[0]+20+4,&old_id,4);
        memcpy(res_ptr->msg_buf[0]+20+8,&none,8);
        uint32_t gc_flag,ret_num;
        int status;
        for(int i=0;i<2;i++){
            if(backup_node_avail[i])
                status = post_send_msg(res_ptr,IBV_WR_SEND,i,0,20,16,277);
            else
                continue;
            if(status){
                backup_node_avail[i] = false; std::cout<<"error connect drop node :"<<i<<std::endl; 
            }
            status = poll_completion(res_ptr,i,282);
            if(status){
                backup_node_avail[i] = false; std::cout<<"error connect drop node :"<<i<<std::endl;
            }
            if(backup_node_avail[i])
                status = post_receive_msg(res_ptr,i,0,20,286);
            if(status){
                backup_node_avail[i] = false; std::cout<<"error connect drop node :"<<i<<std::endl;
            }
            status = poll_completion(res_ptr,i,278);
            if(status){
                backup_node_avail[i] = false; std::cout<<"error connect drop node :"<<i<<std::endl;
            }
            memcpy(&(res_ptr->remote_props[i].rkey),res_ptr->msg_buf[0],4);
            memcpy(&(res_ptr->remote_props[i].addr),res_ptr->msg_buf[0]+4,8);
            if(i==0){
            memcpy(&gc_flag,res_ptr->msg_buf[0]+12,4);
            memcpy(&ret_num,res_ptr->msg_buf[0]+16,4);
            }
            //std::cout<<"server:new addr: "<<res_ptr->remote_props[i].addr <<" new rkey: "<<res_ptr->remote_props[i].rkey <<std::endl;
        }
        rdma_msg_lock[0].unlock();
        end = clock();
        duration = (double)(end - beg)/CLOCKS_PER_SEC;
        flush_tail_time += duration;
           
        uint32_t gc_num = 0;
        if(BACKUP_MODE==2 && backup_node_avail[0]==true){
            if(gc_flag==0){
                ;
            }
            else if(gc_flag==1 && backup_node_avail[0]){
                
                gc_num = ret_num;
                //std::cout<<"gc_num= "<<gc_num<<"generate flush task"<<std::endl;
            }else{
                std::cout<<"error!illegal gc_flag"<<std::endl;
            }
        }
        if(BACKUP_MODE==1||BACKUP_MODE==3){
            bool whether_gc=false;
            if(GC_MODE==1 && is_gc==false && (tail_sge_ptr->sge_id - head_sge_id.load() )>1024)
                whether_gc=true;
            else if(GC_MODE==2 && is_gc==false &&gc_sge_map_ptr->size() > 1024)
                whether_gc=true;
            if(whether_gc){
                    gc_num = 512;
                    std::cout<<"trigger gc"<<std::endl;
            }
                
        }
        ibv_dereg_mr(res_ptr->mr[0]);  
        generate_flush_task(tail_sge_ptr,gc_num);
        create_new_sgement();
        res_ptr->buf[0] = tail_sge_ptr->buf.data();
        int mr_flags = mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        res_ptr->mr[0] = ibv_reg_mr(res_ptr->pd, res_ptr->buf[0], MAX_SGE_SIZE, mr_flags);


        #if SEND_INDEX
        static uint32_t flush_count = 0;
        flush_count++;
        if(flush_count%2==0 && backup_node_avail[0] && backup_node_avail[1] &&BACKUP_MODE!=1){
            beg = clock();
            //std::cout<<"check LSM change"<<std::endl;
            bool change = true;
            std::string manifest_path;
            get_manipath(manifest_path);
            if(last_manifest_path == manifest_path){
                auto new_mani_size = get_file_size(manifest_path);
                if(last_mani_size == new_mani_size){
                    change = false;
                    //std::cout<<"not change,mini_size is "<<new_mani_size<<std::endl;
                }
                last_mani_size = new_mani_size;      
            }
            else{
                last_manifest_path = manifest_path;
            }
            if(change){
                //std::cout<<"MANIFEST changed^^^^^SEND INDEX"<<std::endl;
                uint32_t file_size;
                
                std::vector<uint32_t>delete_sst_list;
                rocks_id_list_lock.lock();
                new_sst_list.clear();
                get_delete_ssts(delete_sst_list);
                //std::vector<uint32_t>new_sst_list;
                
                get_new_ssts();
                rocks_id_list_lock.unlock();
                uint32_t new_sst_num = new_sst_list.size();
                uint32_t delete_sst_num = delete_sst_list.size();
                // for(int i=0;i<new_sst_num;i++)
                //     std::cout<<"new sst id:"<<new_sst_list[i]<<std::endl;
                // for(int i=0;i<delete_sst_num;i++)
                //     std::cout<<"delete sst id:"<<delete_sst_list[i]<<std::endl;
                //int mr_flags = mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
                uint32_t lazy_backup = 2;
                uint32_t mani_size = get_file_size(manifest_path);
                rdma_msg_lock[0].lock();
                memcpy(res_ptr->msg_buf[0],&lazy_backup,4);
                memcpy(res_ptr->msg_buf[0]+4,&new_sst_num,4);
                memcpy(res_ptr->msg_buf[0]+8,&delete_sst_num,4);
                memcpy(res_ptr->msg_buf[0]+12,&mani_size,4);
                
                for(int n=0;n<K;n++){
                    if(backup_node_avail[n])
                        status = post_send_msg(res_ptr,IBV_WR_SEND,n,0,0,16,355);
                    else
                        continue;
                    if(status){
                        backup_node_avail[n] = false;std::cout<<"error connect drop node :"<<n<<std::endl;
                    }
                    status = poll_completion(res_ptr,n,356);
                    if(status){
                        backup_node_avail[n] = false;std::cout<<"error connect drop node :"<<n<<std::endl;
                    }
                }
                
                
                file_size = read_file(res_ptr->msg_buf[0]+4,manifest_path);
                //memcpy(res_ptr->msg_buf+4,&file_size,4);
                uint32_t id;
                id = atoi(manifest_path.data()+9);
                memcpy(res_ptr->msg_buf[0],&id,4);
                for(int n=0;n<K;n++){
                    if(backup_node_avail[n])
                        status = post_send_msg(res_ptr,IBV_WR_SEND,n,0,0,4+mani_size,366);
                    else
                        continue;
                    if(status){
                        backup_node_avail[n] = false;;
                    }
                    status = poll_completion(res_ptr,n,367);
                    if(status){
                        backup_node_avail[n] = false;;
                    }
                }

                
                //struct ibv_mr **new_sst_mrs = (struct ibv_mr **)malloc(sizeof(struct ibv_mr *)*new_sst_num);
                std::string target_path(11,'0');
                target_path[6]='.';
                target_path[7]='s';
                target_path[8]='s';
                target_path[9]='t';
                target_path[10]='\0';
                std::string tmp;
                uint32_t target_id;
                for(int k=0;k<new_sst_num;k++){
                    target_id = new_sst_list[k];
                    tmp = std::to_string(target_id);
                    for(int i=0;i<tmp.size();i++){
                        target_path[6-tmp.size()+i] = tmp[i];
                    }    
                    file_size = read_file(res_ptr->msg_buf[0]+8,target_path);
                    memcpy(res_ptr->msg_buf[0]+4,&file_size,4);
                    memcpy(res_ptr->msg_buf[0],&target_id,4);
                    for(int n=0;n<K;n++){
                        if(backup_node_avail[n])
                            status = post_send_msg(res_ptr,IBV_WR_SEND,n,0,0,file_size+8,390);
                        else
                            continue;
                        if(status){
                            backup_node_avail[n] = false;std::cout<<"error connect drop node :"<<n<<std::endl;
                        }
                        status = poll_completion(res_ptr,n,391);
                        if(status){
                            backup_node_avail[n] = false;std::cout<<"error connect drop node :"<<n<<std::endl;
                        }
                    }
                }
                for(int k=0;k<delete_sst_num;k++){
                    target_id = delete_sst_list[k];
                    memcpy(res_ptr->msg_buf[0]+4*k,&target_id,4);
                }
                if(delete_sst_num>0){
                    for(int n=0;n<K;n++){
                        if(backup_node_avail[n])
                            status = post_send_msg(res_ptr,IBV_WR_SEND,n,0,0,4*delete_sst_num,400);
                        else
                            continue;
                        if(status){
                            backup_node_avail[n] = false;std::cout<<"error connect drop node :"<<n<<std::endl;
                        }
                        status = poll_completion(res_ptr,n,401);
                        if(status){
                            backup_node_avail[n] = false;std::cout<<"error connect drop node :"<<n<<std::endl;
                        }
                    } 
                
                }
                rdma_msg_lock[0].unlock();  
            }        
            end = clock();
            duration = (double)(end - beg)/CLOCKS_PER_SEC;
            send_index_time += duration;    
                
        }
        #endif
    }
    //append到tail sgement并且得到offset和sge_id来作为key index插入lsm
    beg = clock();
    uint32_t offset_of_sgement=tail_sge_ptr->append(key.data(),key.size(),value.data(),value.size());
    uint32_t sge_id = tail_sge_ptr->sge_id;
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    local_put_sge_time += duration;
        
    beg = clock();
    rdma_msg_lock[0].lock();
    for(int i=0;i<2;i++){
        int status;
        if(backup_node_avail[i]){
           // std::cout<<"rdma_write: cur_len= "<< tail_sge_ptr->cur_offset<<std::endl;
            status = post_send(res_ptr,IBV_WR_RDMA_WRITE,i,0,offset_of_sgement,kv_len,422);
        }
        else
            continue;
        if(status){
            backup_node_avail[i] = false; std::cout<<"error connect drop node : "<<i<<std::endl; 
        }
        //memcpy(res_ptr->buf+max_sge_size-4,&(tail_sge_ptr->cur_offset),4);
        //std::cout<<"cur_len ="<<*(int *)(res_ptr->buf+max_sge_size-4)<<std::endl;
        //post_send(res_ptr,IBV_WR_RDMA_WRITE,i,max_sge_size-4,4);
        //poll_completion(res_ptr,i);
        // post_send(res_ptr,IBV_WR_RDMA_READ,i,max_sge_size-4,4);
        // poll_completion(res_ptr,i);
        // uint32_t read_len;
        // memcpy(&read_len,res_ptr->buf+max_sge_size-4,4);
        // std::cout<<"read_len ="<<read_len<<std::endl;
    }
    for(int i=0;i<2;i++){
        int status;
        if(backup_node_avail[i])
            status = poll_completion_quick(res_ptr,i,435);
        else
            continue;
        if(status){
            backup_node_avail[i] = false;std::cout<<"error connect drop node :"<<i<<std::endl;
        }
    }
    rdma_msg_lock[0].unlock();
    put_mutex.unlock();
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    rdma_write_time += duration;
    
    //直接使用rocksdb的write batch 存储的value为4字节的sge_id和4字节的offset
    rocksdb::WriteOptions options;
    //rocksdb::WriteBatch batch;
    beg = clock();
    options.disableWAL = true;
    std::string lsm_index;
    lsm_index.append((const char*)&sge_id,4);
    lsm_index.append((const char*)&offset_of_sgement,4);
    //batch.Put(key, lsm_index);
    //auto status = rocksdb_ptr->Write(options, &batch);
    auto status = rocksdb_ptr->Put(options,key,lsm_index);
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    local_put_rocks_time += duration;
    if(put_num%100000==0){
         std::cout<<"flush_tail_time for 100k put is "<<flush_tail_time<<" ,put_num="<<put_num<<std::endl;
        std::cout<<"send_time for 100k put is "<<send_index_time<<" ,put_num="<<put_num<<std::endl;
        std::cout<<"put_segment_time for 100k put is "<<local_put_sge_time<<" ,put_num="<<put_num<<std::endl;
        std::cout<<"rdma_write_time for 100k put is "<<rdma_write_time<<" ,put_num="<<put_num<<std::endl;
        std::cout<<"put_lsm_time for 100k put is "<<local_put_rocks_time<<" ,put_num="<<put_num<<std::endl;
    }
        
    return status.ok();
}
#elif TEST_MODE ==1
bool DB::put_impl(const std::string &key, const std::string &value){
    uint32_t kv_len = key.size()+value.size()+8;
    put_mutex.lock();
    if(tail_sge_ptr->get_sge_size()+ kv_len>max_sge_size){
        uint32_t old_id = tail_sge_ptr->sge_id;
        uint32_t gc_num = 0;
        generate_flush_task(tail_sge_ptr,gc_num);
        create_new_sgement();
    }
    uint32_t offset_of_sgement=tail_sge_ptr->append(key.data(),key.size(),value.data(),value.size());
    uint32_t sge_id = tail_sge_ptr->sge_id;
    put_mutex.unlock();
    rocksdb::WriteOptions options;
    options.disableWAL = true;
    std::string lsm_index;
    lsm_index.append((const char*)&sge_id,4);
    lsm_index.append((const char*)&offset_of_sgement,4);
    auto status = rocksdb_ptr->Put(options,key,lsm_index);
    return status.ok();
}
#elif TEST_MODE ==2


bool DB::put_impl(const std::string &key, const std::string &value){
    clock_t beg,end;
    double duration;
    beg = clock();
    uint32_t kv_len = key.size()+value.size()+8;
    put_mutex.lock();
    //tail sge的满全部由db判断
    if(tail_sge_ptr->get_sge_size()+ kv_len>max_sge_size){
        uint32_t old_id = tail_sge_ptr->sge_id;
        //TODO:检查rocksdb文件变化，以约定好的消息格式发送给backup通知flush tail并且同步文件
        uint32_t gc_num = 0;
        generate_flush_task(tail_sge_ptr,gc_num);
        create_new_sgement();
        erase_wal();
    }
    //append到tail sgement并且得到offset和sge_id来作为key index插入lsm
    //std::cout<<"put into wal"<<std::endl;
    write_wal(key,value);
    uint32_t offset_of_sgement=tail_sge_ptr->append(key.data(),key.size(),value.data(),value.size());
    uint32_t sge_id = tail_sge_ptr->sge_id;
    put_mutex.unlock();
    rocksdb::WriteOptions options;
    //rocksdb::WriteBatch batch;
    options.disableWAL = true;
    std::string lsm_index;
    lsm_index.append((const char*)&sge_id,4);
    lsm_index.append((const char*)&offset_of_sgement,4);
    auto status = rocksdb_ptr->Put(options,key,lsm_index);
    
     end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    //std::cout<<"put impl time = "<<duration<<std::endl;
    return status.ok();
}
#endif
bool DB::put(const std::string &key, const std::string &value){
    std::string key_cur_gc_;
    key_cur_gc.get(key_cur_gc_);
    int gc_status = 0;
    bool try_gc_lock = gc_lock.try_lock();
    if(try_gc_lock)
        gc_lock.unlock();
    //0无gc，1冲突, 2无关 ,3gc两个kv的间隙
    if(try_gc_lock)
        gc_status = 0;
    else if(key_cur_gc_.size()==0)
        gc_status = 3;
    else if(key_cur_gc_ == key){
        gc_status = 1;
        #if DEBUG
        std::cout<<"try put key equal with the gc reput,key is "<<key <<std::endl;
        #endif
    }       
    else
        gc_status = 2;

    if(gc_status==1 || gc_status ==3)
        gc_reput_lock.lock();
    std::vector<uint8_t> key_vector(key.begin(),key.end() );
    uint32_t hash_id = MurMurHash::MurMur3_32(key_vector)%PUTTING_HASH_SIZE;
    is_putting_hash_map[hash_id]++;
    //std::cout<<"execute put_impl"<<std::endl;
    bool ret = put_impl(key,value);
    is_putting_hash_map[hash_id]--;
    if(gc_status==1 || gc_status==3)
        gc_reput_lock.unlock();
    return ret;
}
bool del(const std::string &key){
    return false;
}
bool DB::gc_reput(const std::string &key, const std::string &value){
    return put_impl(key,value);
}

bool DB::get_from_rocksdb(const std::string &key,uint32_t &sge_id,uint32_t &offset_of_sgement){
    //rocksdb的参数是string *value所以取地址
    std::string value;
    auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value);
    sge_id = *((uint32_t*)value.data());
    offset_of_sgement = *( (uint32_t*)(value.data()+4 ) );
    return status.ok();
}
bool DB::get(const std::string &key, std::string &value){
    //因为没有自己的memtable所以直接从rocksdb读
    static int get_num=0;
    get_num++;
    uint32_t sge_id;
    uint32_t offset_of_sgement;
    clock_t beg,end;
    double duration;
    static double get_from_lsm_time=0,get_from_sge_time=0;
    beg = clock();
    bool res = get_from_rocksdb(key,sge_id,offset_of_sgement);
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    get_from_lsm_time += duration;
    if(get_num%100000==0)
        std::cout<<"get_lsm_time for 100000 get is "<<get_from_lsm_time<<" ,get_num="<<get_num<<std::endl;


    if(res==false){
        //std::cout<<"get from rocks fail"<< std::endl;
        return false;
    }
    //std::cout<<"get from rocksdb succ,key="<<key<<" value="<<sge_id<<" "<<offset_of_sgement<<std::endl;
    //利用索引信息从value sgement里读
    //先查看内存里的sgement
    ValueSGE_ptr sge_to_read_ptr;
    bool sge_in_mem = false;
    beg = clock();
    //此处是否需要上锁? $$$$$$$$
    put_mutex.lock();
    if(sge_id ==tail_sge_ptr->sge_id){
        sge_to_read_ptr = tail_sge_ptr;
        put_mutex.unlock();
        sge_in_mem = true;
        //std::cout<<"get in tail!!!"<<std::endl;
    }
    else{
        put_mutex.unlock();
        flushing_mutex.lock();
        for(auto flushing_sge : flushing_sges){
            if(flushing_sge->sge_id == sge_id){
                sge_to_read_ptr = flushing_sge;
                sge_in_mem = true;
                #if DEBUG
                std::cout<<"get in flushing!!!"<<std::endl;
                #endif
                break;
            }
        }
        flushing_mutex.unlock();
    }
    if(sge_in_mem){
        sge_to_read_ptr->get_kv(offset_of_sgement,key,value);
    }
    else{
    NAM_SGE::read_kv_from_sge(sge_id,offset_of_sgement,key,value);
    }
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    get_from_sge_time += duration;
    if(get_num%100000==0)
        std::cout<<"get_sge_time for 100000 get is "<<get_from_sge_time<<" ,get_num="<<get_num<<std::endl;

    return true;
}

//Wisc原始gc + 选取无效kv对最多的一系列sgement
class Sgemeta_pair{
    public:
        uint32_t id;
        uint32_t invalid_num;
};
class cmp{
    public:
        bool operator()(Sgemeta_pair p1,Sgemeta_pair p2){
            return p1.invalid_num<p2.invalid_num;
        }
};
void DB::gc(uint32_t gc_num){
    head_sge_id_after_gc = head_sge_id.load()+gc_num;
    clock_t beg,end;
    double duration;
    uint32_t gc_sge_count=0;
    ValueSGE_ptr gc_sge_ptr = std::make_shared<NAM_SGE::ValueSGE>(0,max_sge_size);
    std::string key;
    std::string value;
    std::string path;
    uint32_t sge_id;
    uint32_t offset_of_sgement;

//use the gc_sge_map to build a priority_queue to take gc_num segment_id with the most invalid kv pairs
    
    std::priority_queue<Sgemeta_pair,std::vector<Sgemeta_pair>,cmp> sge_heap;
    if(GC_MODE==2){
        gc_sge_map_lock_ptr->lock();
        for(auto pair:*gc_sge_map_ptr){
            Sgemeta_pair tmp;
            tmp.id=pair.first;
            tmp.invalid_num=pair.second;
            sge_heap.push(tmp);
        }
        gc_sge_map_lock_ptr->unlock();
    }

    //std::cout<<"gc begin id "<<head_sge_id.load() <<" gc sge num is "<<gc_num<<std::endl;
    for(;gc_sge_count<gc_num;gc_sge_count++){
        beg = clock();
        uint32_t gc_sge_id;
        if(GC_MODE==1)
            gc_sge_id=head_sge_id++;
        else if(GC_MODE==2){
            gc_sge_id=sge_heap.top().id;
            sge_heap.pop();
            gc_sge_map_lock_ptr->lock();
            auto iter=gc_sge_map_ptr->find(gc_sge_id);
            if(iter!=gc_sge_map_ptr->end())
                gc_sge_map_ptr->erase(iter);
            gc_sge_map_lock_ptr->unlock();
        }
        std::cout<<"gc select id = "<<gc_sge_id<<std::endl;
        gc_sge_ptr->read_sge(gc_sge_id,max_sge_size);
        
        //std::cout<<"gc target sge id is "<<gc_sge_ptr->sge_id<<std::endl;
        //遍历一个sgement的每一条kv
        for(uint32_t offset=0;offset<gc_sge_ptr->buf.size();){
            uint32_t new_offset = gc_sge_ptr->get_kv_for_gc(offset,key,value);
            key_cur_gc.set(key);
            std::vector<uint8_t> key_vector(key.begin(),key.end() );
            uint32_t hash_id = MurMurHash::MurMur3_32(key_vector)%PUTTING_HASH_SIZE;
            while(is_putting_hash_map[hash_id].load()>0){
                #if DEBUG
                std::cout<<"gc is waiting for possible contention with new put"<<std::endl;
                #endif
                ;
            }            
            gc_reput_lock.lock();
            get_from_rocksdb(key,sge_id,offset_of_sgement);
            #if DEBUG
            std::cout<<"gc:key="<<key<<" sge_offset="<<offset<<" rocks_offset="<<offset_of_sgement<<" id_sge="<<gc_sge_ptr->sge_id<<" rocks_id"<<sge_id<<std::endl;
            #endif
            if(sge_id==gc_sge_ptr->sge_id &&offset_of_sgement==offset){
            //数据仍然是有效的，作为新记录添加回key index和tail sgement
            //不采用修改key index而是直接加新的，主要是因为需要改rocksdb
                #if DEBUG
                std::cout<<"valid and put_back"<<std::endl;
                #endif
                gc_reput(key,value);
            } 
            gc_reput_lock.unlock();
            key_cur_gc.clear();
            //无效数据则丢掉
            offset = new_offset;
            #if DEBUG
            std::cout<<"new_offset="<<new_offset<<" buf.size="<<gc_sge_ptr->buf.size()<<std::endl;
            #endif
        }
        end = clock();
        duration = (double)(end - beg)/CLOCKS_PER_SEC;
        gc_local_time += duration; 
        
        //完成一整个sgement后删除硬盘里的文件
        //path.clear();
        // path.append("../value_sgement/value_sge_");
        // path.append(std::to_string(gc_sge_ptr->sge_id));
        static int parity_update_num=0;
        parity_update_num++;
        if(BACKUP_MODE==2){
            beg = clock();
            rdma_msg_lock[0].lock();
            uint32_t gc_finish_flag = 3;
            uint32_t finish_num=1;
            uint64_t none=0;
            memcpy(res_ptr->gc_msg_buf,&gc_finish_flag,4);
            memcpy(res_ptr->gc_msg_buf+4,&finish_num,4);
            memcpy(res_ptr->gc_msg_buf+8,&none,8);
            //std::cout<<"finish gc of a sge, gc_finish_flag= "<<gc_finish_flag<<" ,finish_num= "<<finish_num<<std::endl;
            post_send_gc_msg(res_ptr,IBV_WR_SEND,0,0,16,609);
            //post_send_msg(res_ptr,IBV_WR_SEND,11,11,0,16,668);
            poll_completion(res_ptr,0,610);
            uint32_t remove_beg_id=0,remove_num = 0;
            post_receive_gc_msg(res_ptr,0,8,612);
            //post_receive_msg(res_ptr,11,11,8,672);
            poll_completion(res_ptr,0,613);
            rdma_msg_lock[0].unlock();
            memcpy(&remove_beg_id,res_ptr->gc_msg_buf,4);
            memcpy(&remove_num,res_ptr->gc_msg_buf+4,4);
            end = clock();
            duration = (double)(end - beg)/CLOCKS_PER_SEC;
            gc_rdma_time += duration; 
            if(parity_update_num%10==0){
                std::cout<<"gc rdma_time = "<<gc_rdma_time<<" ,update_parity_num="<<parity_update_num<<std::endl;
            }
            //std::cout<<"remove_begin_id= "<<remove_beg_id<<" ,remove_num= "<<remove_num<<std::endl;
            std::string path;
            beg = clock();
            for(int i=0;i<remove_num;i++){
                path.clear();
                path.append("../value_sgement/value_sge_");
                path.append(std::to_string(remove_beg_id+i));
                std::remove((const char*)path.c_str());
                
                //std::cout<<"remove old sge file id "<<remove_beg_id+i<<std::endl;
                
            }
            end = clock();
            duration = (double)(end - beg)/CLOCKS_PER_SEC;
            gc_local_time += duration; 
        }
        else if(BACKUP_MODE==1){
            std::string path;
            path.clear();
            path.append("../value_sgement/value_sge_");
            path.append(std::to_string(gc_sge_ptr->sge_id));
            std::remove((const char*)path.c_str());
            rdma_msg_lock[0].lock();
            uint32_t gc_finish_flag = 4;
            uint32_t remove_beg_id=gc_sge_ptr->sge_id;
            uint32_t remove_num=1;
            memcpy(res_ptr->gc_msg_buf,&gc_finish_flag,4);
            memcpy(res_ptr->gc_msg_buf+4,&remove_beg_id,4);
            memcpy(res_ptr->gc_msg_buf+8,&remove_num,4);
            //std::cout<<"finish gc of a sge, gc_finish_flag= "<<gc_finish_flag<<" ,finish_num= "<<finish_num<<std::endl;
            post_send_gc_msg(res_ptr,IBV_WR_SEND,0,0,16,891);
            //post_send_msg(res_ptr,IBV_WR_SEND,11,11,0,16,668);
            poll_completion(res_ptr,0,893);
            rdma_msg_lock[0].unlock();
        }
        else if(BACKUP_MODE==3){
            
            clock_t beg,end;
            double duration;
            
            beg = clock();
            rdma_msg_lock[0].lock();
            uint32_t gc_finish_flag = 5;
            uint32_t remove_beg_id=gc_sge_ptr->sge_id;
            uint32_t remove_num=1;
            memcpy(res_ptr->gc_msg_buf,&gc_finish_flag,4);
            memcpy(res_ptr->gc_msg_buf+4,&remove_beg_id,4);
            memcpy(res_ptr->gc_msg_buf+8,&remove_num,4);
            //std::cout<<"remove_beg_id="<<remove_beg_id<<std::endl;
            //std::cout<<"finish gc of a sge, gc_finish_flag= "<<gc_finish_flag<<" ,finish_num= "<<finish_num<<std::endl;
            post_send_gc_msg(res_ptr,IBV_WR_SEND,0,0,16,918);
            poll_completion(res_ptr,0,919);
           //std::cout<<"copy segment"<<std::endl;
            memcpy(res_ptr->gc_msg_buf,gc_sge_ptr->buf.data(),16*1024*1024);
            post_send_gc_msg(res_ptr,IBV_WR_SEND,0,0,16*1024*1024,922);
            poll_completion(res_ptr,0,923);

            // memcpy(res_ptr->gc_msg_buf,&gc_finish_flag,4);
            // memcpy(res_ptr->gc_msg_buf+4,&remove_beg_id,4);
            // memcpy(res_ptr->gc_msg_buf+8,&remove_num,4);

            // post_send_gc_msg(res_ptr,IBV_WR_SEND,1,0,16,925);
            // poll_completion(res_ptr,0,926);
            // memcpy(res_ptr->gc_msg_buf,gc_sge_ptr->buf.data(),16*1024*1024);
            // post_send_gc_msg(res_ptr,IBV_WR_SEND,1,0,16*1024*1024,927);
            // poll_completion(res_ptr,0,928);
            //std::cout<<"send finish"<<std::endl;
            rdma_msg_lock[0].unlock();
            end = clock();
            duration = (double)(end - beg)/CLOCKS_PER_SEC;
            gc_rdma_time += duration; 
            if(parity_update_num%10==0){
                std::cout<<"gc rdma_time = "<<gc_rdma_time<<" ,update_parity_num="<<parity_update_num<<std::endl;
            }
            std::string path;
            path.clear();
            path.append("../value_sgement/value_sge_");
            path.append(std::to_string(gc_sge_ptr->sge_id));
            std::remove((const char*)path.c_str());
        }
        //std::remove((const char*)path.c_str());
        
    }
    head_sge_id_after_gc = 0;
}

/*my design of gc
DB::gc(){

}
*/

bool DB::manual_gc(uint32_t sge_num){
    if(gc_lock.try_lock()){
        gc(sge_num);
        gc_lock.unlock();
        return true;
    }
    else{
    //已经在gc了
        return false;
    }
}
void DB::flush_tail(){
    put_mutex.lock();
    auto target_sge_ptr = tail_sge_ptr;
    put_mutex.unlock();
    target_sge_ptr->flush();
}