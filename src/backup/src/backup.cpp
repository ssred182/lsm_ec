#include "backup.h"
#include <stdio.h>
#include <thread>
#include <iostream>
#include <fstream>
#define K           4
#define SEND_INDEX 1
//#define BACKUP_MODE 2
int BACKUP_MODE;
//1--PRB  2--EC and gc  3-- EC without gc
#define DEBUG 0
static double total_build_time[K]={0};
double total_vsge_io_time = 0,total_parity_io_time=0;
void BackupDB::init(uint32_t node_id_,std::string &rocks_path){
    node_id = node_id_;
    current_mani_id = 0;
    max_sge_size = MAX_SGE_SIZE;
    num_free_mem_sge = MAX_MEM_SGE;
    min_ssd_sge_id = 0;
    max_ssd_sge_id = 0;
    ssd_sge_num = 0;
    //backup的sge id不自己维护
    tail_sge_ptr=  std::make_shared<NAM_SGE::ValueSGE>(1,max_sge_size);
    tail_sge_ptr->buf.resize(max_sge_size,0);
    for(int i=0;i<MAX_MEM_SGE;i++)
        in_mem_sges[i].reset();
    //#if !SEND_INDEX
    rocksdb::DB *dbptr;
    rocksdb::Options options;
    options.compression = rocksdb::kNoCompression;
    options.max_background_flushes = 1;
    options.max_background_compactions = 1;
    options.create_if_missing = true;
    
    std::cout<<"create rocksdb for backupdb node_id: "<<node_id_<<"path= "<<rocks_path<<std::endl;
    
    rocksdb::DB::Open(options, rocks_path, &dbptr);
    assert(dbptr != nullptr);
    rocksdb_ptr = std::shared_ptr<rocksdb::DB>(dbptr);
    //#endif
}
void flush_task (BackupDB *db,ValueSGE_ptr target_sge_ptr){
    clock_t beg,end;
    double duration;
    beg = clock();
    target_sge_ptr->flush(db->node_id); 
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    total_vsge_io_time += duration;
}

void put_sge_into_rocks (BackupDB *db,ValueSGE_ptr target_sge_ptr){

    
    clock_t beg,end;
    double duration;
    //std::cout<<"put into rocks begin"<<std::endl;
    beg = clock();
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    options.disableWAL = true;
    std::string key;
    std::string lsm_index;
    std::string rkey;
    uint32_t rid,roff;
    uint32_t sge_id =target_sge_ptr->sge_id;
    //uint32_t sge_size;
    //sge_size = *((uint32_t *)(target_sge_ptr->buf.data()+db->max_sge_size -4));//target_sge_ptr->buf.size();
    uint32_t offset=0;
    uint32_t key_len;
    
    while(1)
    {   
        if(offset>db->max_sge_size-4)
            break;
        memcpy(&key_len,target_sge_ptr->buf.data()+offset,4);
        if(key_len==0)
            break;
        uint32_t new_offset = target_sge_ptr->get_kv_for_build(offset,key);
        lsm_index.clear();
        lsm_index.append((const char*)&sge_id,4);
        lsm_index.append((const char*)&offset,4);
        batch.Put(key,lsm_index);
        // ConReadCache::accessor a;
        // if(db->con_read_cache.find(a,key)){
        //     std::string value;
        //     target_sge_ptr->get_kv_for_gc(offset,key,value);
        //     a->second=value;
        // }
        // a.release();
        #if DEBUG
        if(offset < 2048){
            rkey = key;
            rid = sge_id;
            roff = offset;
        }
        std::cout<<"put batch: key="<<key<<" ,id="<<sge_id<<" ,offset="<<offset<<std::endl;
        #endif
        offset = new_offset;
    }
    
    #if DEBUG
        std::cout<<"finish batch build"<<std::endl;
    #endif
    auto status = db->rocksdb_ptr->Write(options, &batch);
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    total_build_time[db->node_id] += duration;
    static int build_num[4]={0};
    if(build_num[db->node_id]++%100==0)
        std::cout<<"total_build_time["<<db->node_id<<"] = "<<total_build_time[db->node_id]<<std::endl;
    #if DEBUG
        if(status.ok())
            std::cout<<"!!!!!!write batch succeed!!!!"<<std::endl;
        else
            std::cout<<"error************* write batch fail"<<std::endl;
        std::string value;
        for(uint32_t offset=0;offset<sge_size;){
        uint32_t new_offset = target_sge_ptr->get_kv_for_build(offset,rkey);
        roff = offset;
        rid = sge_id;;
        auto r_status =db->rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value);
        if(r_status.ok())
        std::cout<<"r_status = true"<<std::endl;
        else
        std::cout<<"r_status = false"<<std::endl;
        uint32_t id = *((uint32_t*)value.data());
        uint32_t off = *( (uint32_t*)(value.data()+4 ) );
        printf("test read after write batch\n");
        printf("key=%s,rid=%d,id=%d,roff=%d,off=%d\n",rkey.c_str(),rid,id,roff,off);
        offset = new_offset;
        }
        //std::cout<<""<<rkey<<",rid="<<rid<<" ,id="<<id<<" ,roff="<<roff<<" ,off="<<off<<std::endl;
        
    #endif
}
static void write_file(const char *buf,uint32_t len,std::string target_path,uint32_t node_id){
    std::ofstream fout;
    std::string prex = "../rocksdb_lsm_"+std::to_string(node_id)+'/';
    prex.append(target_path);
    fout.open(prex,std::ios::out|std::ios::binary);
    fout.seekp(0,std::ios::beg);
    fout.write(buf,len);
    fout.close();
}
void BackupDB::flush_tail(){
    //if(BACKUP_MODE==1){
        std::thread put_into_rocks_th(put_sge_into_rocks,this,tail_sge_ptr);
        put_into_rocks_th.detach();
    //} 
    // std::string tmp_path = "vsge_"+std::to_string(tail_sge_ptr->sge_id);
    // write_file(tail_sge_ptr->buf.data(),MAX_SGE_SIZE,tmp_path,node_id);
    if(BACKUP_MODE==1){
        std::thread flush_th(flush_task,this,tail_sge_ptr);
        flush_th.detach();
    }
    else{
        in_mem_sge_lock.lock();
        if(num_free_mem_sge==0){
        //没有空闲位置把已有的一个挤到硬盘里去
            uint32_t min_id =0;
            uint32_t min_position;
            #if DEBUG
            #endif
            for(int i=0;i<MAX_MEM_SGE;i++){
                if(in_mem_sges[i].use_count()==0)
                    std::cout<<"in_mem_sges count error!!"<<std::endl;
                if(min_id==0||in_mem_sges[i]->sge_id <min_id){
                    min_id = in_mem_sges[i]->sge_id;
                    min_position = i;
                }
            }
            ValueSGE_ptr target_sge_ptr = in_mem_sges[min_position];
            #if DEBUG
            std::cout<<"^^^node_id= "<<node_id <<" sge_id = "<<tail_sge_ptr->sge_id <<" ,sge with smallest id is replace to disk,id= "<<target_sge_ptr->sge_id<<std::endl;
            #endif
            in_mem_sges[min_position]=tail_sge_ptr;
            in_mem_sge_lock.unlock();
            std::thread flush_th(flush_task,this,target_sge_ptr);
            flush_th.detach();
            id_list_lock.lock();

            if(ssd_sge_num ==0)
            {
                min_ssd_sge_id=target_sge_ptr->sge_id;
                max_ssd_sge_id = min_ssd_sge_id-1;
            }
            max_ssd_sge_id++;
            ssd_sge_num++;
            id_list_lock.unlock();
            
        }
        else{
        num_free_mem_sge--;
        int free_position=-1;
        //in_mem_sge_lock.lock();
        for(int i=0;i<MAX_MEM_SGE;i++){
            if(in_mem_sges[i].use_count()==0){
                free_position = i;
                break;
            }
        }
        #if DEBUG
        std::cout<<"^^^node_id= "<<node_id<<" ,sge_id= "<<tail_sge_ptr->sge_id <<" ,free position of sge_in_mem is "<<free_position<<std::endl;
        #endif
        if(free_position==-1)
            std::cout<<"node_id="<<node_id <<"error!!!backup.cpp:46"<<std::endl;
        in_mem_sges[free_position] = tail_sge_ptr;
        
        in_mem_sge_lock.unlock();
        
     
    }
    }
    tail_sge_ptr=  std::make_shared<NAM_SGE::ValueSGE>(tail_sge_ptr->sge_id+1,max_sge_size);
    tail_sge_ptr->buf.resize(max_sge_size,0);  
    #if DEBUG
        std::cout<<"create a new tail_sge,id="<<tail_sge_ptr->sge_id<<std::endl;
    #endif
}
bool BackupDB::remove_sge(uint32_t sge_id_){
    in_mem_sge_lock.lock();
    bool in_mem =false;
    for(int i=0;i<MAX_MEM_SGE;i++){
        if(in_mem_sges[i]->sge_id==sge_id_){
            in_mem_sges[i].reset();
            num_free_mem_sge++;
            in_mem = true;
            break;
        }
    }
    in_mem_sge_lock.unlock();
    if(in_mem){
        return true;
    }
    else{
        id_list_lock.lock();
        if(ssd_sge_num<=0)
            std::cout<<"node_id="<<node_id <<"error!!!backup.cpp:139"<<std::endl;
        ValueSGE_ptr target_sge_ptr;
        uint32_t target_id = sge_id_;
        if(min_ssd_sge_id==sge_id_)
            min_ssd_sge_id++;
        ssd_sge_num--;
        id_list_lock.unlock();
        std::string path;
        path.append("../value_sgement/value_sge_");
        path.append(std::to_string(node_id));
        path.append("_");
        path.append(std::to_string(target_id));
        std::remove((const char*)path.c_str());
        return true;
    }
}
bool BackupDB::has_sges_notcoded(){
    if(num_free_mem_sge<MAX_MEM_SGE)
        return true;
    id_list_lock.lock();
    uint32_t num = ssd_sge_num;
    id_list_lock.unlock();
    if(num>0)
        return true;
    return false;
}
bool BackupDB::get_sgement_to_code(ValueSGE_ptr &target_sge_ptr){
    id_list_lock.lock();
    if(ssd_sge_num>0){
        id_list_lock.unlock();
        return false;
    }
    id_list_lock.unlock();
    uint32_t min_id =0;
    uint32_t min_position;
    in_mem_sge_lock.lock();
    //std::cout<<"debug p2"<<std::endl;
    for(int i=0;i<MAX_MEM_SGE;i++){
        if(in_mem_sges[i].use_count()==0)
            continue;
        if(min_id==0||(in_mem_sges[i]->sge_id <min_id ) ){
            min_id = in_mem_sges[i]->sge_id;
            min_position = i;
        }
        //std::cout<<"find min :in_mem_sges["<<i<<"] checked"<<std::endl;
    }
    if(min_id==0)
        std::cout<<"error!!!not find sge from in_mem_sge[]"<<std::endl;
    target_sge_ptr = in_mem_sges[min_position];
    #if DEBUG
        std::cout<<"node_id="<<node_id <<"get sge in mem!!,id = "<<target_sge_ptr->sge_id<<std::endl;
    #endif
    in_mem_sges[min_position].reset();
    num_free_mem_sge++;
    in_mem_sge_lock.unlock();
    
    return true;
}
void read_vsge_file(uint32_t node_id,uint32_t sge_id,ValueSGE_ptr &target_sge_ptr){
    target_sge_ptr = std::make_shared<NAM_SGE::ValueSGE>(sge_id,MAX_SGE_SIZE);
    std::string path;
    path.append("../value_sgement/value_sge_");
    path.append(std::to_string(node_id));
    path.append("_");
    path.append(std::to_string(target_sge_ptr->sge_id));
    std::ifstream fin;
    while(fin.is_open()== false){
            //std::cout<<"try open file: "<<path<<std::endl;
            fin.open(path,std::ios::in|std::ios::binary);
    }
    fin.seekg(0,std::ios::end);
    uint32_t file_size = fin.tellg();
    fin.seekg(0,std::ios::beg);
    target_sge_ptr->buf.resize(file_size);
    fin.read((char*)target_sge_ptr->buf.data(),file_size);
    fin.close();
    std::remove((const char*)path.c_str());
}
uint32_t BackupDB::get_sgement_from_ssd_to_code(){
    id_list_lock.lock();
    if(ssd_sge_num>0){
        
        uint32_t target_id = min_ssd_sge_id++;
        ssd_sge_num--;
        id_list_lock.unlock();
        
        #if DEBUG
        std::cout<<"node_id="<<node_id <<"get sge from disk!!,sge_id = "<<target_id<<std::endl;
        #endif
        return target_id;
    }
    id_list_lock.unlock();
    std::cout<<"error!!!!!no sge from ssd"<<std::endl;
    return 0;
}

void BackupNode::init(int parity_node_id_){
    parity_node_id = parity_node_id_;
    max_sge_size = MAX_SGE_SIZE;
    min_parity_sge_id = 0;
    max_parity_sge_id = 0;
    parity_sge_num = 0;
    encoder.init_encode(K,M,max_sge_size);
    std::string path = "../rocksdb_lsm_";
    for(int i=0;i<K;i++){
        std::string path_of_i = path + std::to_string(i)+'/';
        db_list[i].init(i,path_of_i);
    }

}
int encode_num=0;
void encode_task (BackupNode *db_node,std::vector<ValueSGE_ptr> encode_sges,std::vector<uint32_t> sge_node_id){
    encode_num++;
    clock_t beg,end,beg_e,end_e;
    double duration;
    beg_e = clock();
    #if DEBUG
    std::cout<<"begin a encode task,then get segs"<<std::endl;
    #endif
    ValueSGE_ptr tmp;
    #if DEBUG
        uint32_t last_id = 0;
        std::cout<<"sge_node_id_list: ";
        for(int i=0;i<K;i++){
            std::cout<<sge_node_id[i]<<" ";
        }
        std::cout<<std::endl;
        for(int i=0;i<K;i++){
            if(last_id == 0)
                last_id = sge_node_id[i];
            else{
                if(last_id!=sge_node_id[i])
                    std::cout<<"error!!!id not equal with same stripe"<<std::endl;
                last_id = sge_node_id[i];
            }
        }
    #endif  
    for(int i=0;i<K;i++){
        if(encode_sges[i]==nullptr){
            beg = clock();
            read_vsge_file(i,sge_node_id[i],tmp);
            end = clock();
            duration = (double)(end - beg)/CLOCKS_PER_SEC;
            total_vsge_io_time += duration;          
            encode_sges[i]=tmp;
            #if DEBUG
            std::cout<<"db_list["<<i<<"] put a sge with id= "<<tmp->sge_id <<" to encode from ssd" <<std::endl;
            #endif
        }
    }
    #if DEBUG
        std::cout<<"get from ssd finish"<<std::endl;
    #endif
    std::string path = "../parity_sgement/parity_sge_";
    ValueSGE_ptr code_sge;
    uint32_t target_parity_id;
    db_node->parity_id_lock.lock();
    if(db_node->min_parity_sge_id ==0)
    {
        //db_node->min_parity_sge_id=1;
        db_node->min_parity_sge_id=sge_node_id[0];
    }
    //db_node->max_parity_sge_id++;
    db_node->max_parity_sge_id=sge_node_id[0];
    target_parity_id = db_node->max_parity_sge_id;
    db_node->parity_sge_num++;
    db_node->parity_id_lock.unlock();
    #if DEBUG
    std::cout<<"finish parity_id update"<<std::endl;
    #endif
    code_sge =std::make_shared<NAM_SGE::ValueSGE>(target_parity_id,db_node->max_sge_size);
    code_sge->buf.resize(db_node->max_sge_size);
    uint8_t *encode_src[10];
    for(int i=0;i<encode_sges.size();i++){
        encode_src[i]=(uint8_t*)encode_sges[i]->buf.data();
    }
    #if DEBUG
    std::cout<<"$$$$use encoder function"<<std::endl;
    #endif
    
    static double total_encode_time = 0;
    beg = clock();
    db_node->encoder.encode_data(db_node->parity_node_id,encode_src,(uint8_t*)code_sge->buf.data());
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    
    //std::cout<<"total_encode_time: "<<total_encode_time<<std::endl; 
    #if DEBUG
    std::cout<<"ISA-L use succeed!!!!!!!!!!!! sge_id = "<< target_parity_id<<std::endl;
    #endif
    std::ofstream fout;
    path.append(std::to_string(target_parity_id));
    //用open创建文件
    beg = clock();
    fout.open(path,std::ios::out);
    fout << code_sge->buf;
    //如果是使用二进制自己读文件就不要加endl
    fout.close();
    end = clock();
    duration = (double)(end - beg)/CLOCKS_PER_SEC;
    total_parity_io_time += duration;
    end_e = clock();
    duration = (double)(end_e - beg_e)/CLOCKS_PER_SEC;
    total_encode_time += duration;
    if(encode_num%10==0){
        std::cout<<"encode time = "<<total_encode_time<<" ,num="<<encode_num<<std::endl;
        std::cout<<"io time = "<<total_parity_io_time+total_vsge_io_time<<std::endl;
    }
    
}



void BackupNode::flush_tail(uint32_t lsm_id){
    db_list[lsm_id].flush_tail();
    if (BACKUP_MODE==2||BACKUP_MODE==3){
    if(encode_lock.try_lock()){
        if(get_encode_group_sges()==true){
            std::vector<ValueSGE_ptr> mem_encode_sges;
            std::vector<uint32_t> mem_sge_node_id;
            for(int i=0;i<K;i++){
                ValueSGE_ptr target_sge_ptr;
                //std::cout<<"debug p1"<<std::endl;
                if(db_list[i].get_sgement_to_code(target_sge_ptr)==true){
                    mem_encode_sges.emplace_back(target_sge_ptr);
                    mem_sge_node_id.emplace_back(target_sge_ptr->sge_id);
                    //std::cout<<"node: "<<i<<" from mem"<<std::endl;

                }
                else{
                    uint32_t id = db_list[i].get_sgement_from_ssd_to_code();
                    mem_encode_sges.emplace_back(nullptr);
                    mem_sge_node_id.emplace_back(id);
                    //std::cout<<"node: "<<i<<" from ssd****"<<std::endl;
                }
            }
            std::thread encode_th(encode_task,this,mem_encode_sges,mem_sge_node_id);
            encode_th.detach();
        }    
        encode_lock.unlock();
    }
    }
}
void BackupNode::remove_sge(uint32_t sge_id_){
    //假设收到这个函数已经gc同步了
    //如果sge是parity就直接删，如果还没encode也直接删
    
        // if(sge_id_>min_parity_sge_id)
        //     std::cout<<"error!not continous parity id remove"<<std::endl;
        //std::cout<<"remove parity_sge_"<<sge_id_<<std::endl;
        std::string path = "../parity_sgement/parity_sge_";
        path.append(std::to_string(sge_id_));
        if(std::remove((const char*)path.c_str())==0 ){
            //std::cout<<"remove succeed"<<std::endl;
            parity_id_lock.lock();
            parity_sge_num--;
            min_parity_sge_id++;
            parity_id_lock.unlock();
        }          
}

bool BackupNode::get_encode_group_sges(){
    for(int i=0;i<K;i++){
        if(db_list[i].has_sges_notcoded()==false){
            #if DEBUG
            std::cout<<"db_list["<<i<<"]has no sges to encode"<<std::endl;
            #endif
            return false;
        }
            
    }
    #if DEBUG
    std::cout<<"fit the condition to encode a group"<<std::endl;
    #endif
    return true;
}

