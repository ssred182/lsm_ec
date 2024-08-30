#include "encode.h"
#include <list>
#include <mutex>
#include <time.h>
#include <rocksdb/db.h>
#include "value_sgement.h"
#include "define.h"
#ifndef BACKUP_H
#define BACKUP_H
#define MAX_MEM_SGE 6
#define M           2

class BackupDB{
    public:
        void init(uint32_t node_id_,std::string &rocks_path);
    public:
        void flush_tail();
        bool remove_sge(uint32_t sge_id_);
    public:
        uint32_t node_id;
        RocksDBPtr rocksdb_ptr =nullptr;
        uint32_t current_mani_id;
        uint32_t max_sge_size;
        ValueSGE_ptr tail_sge_ptr;
        ValueSGE_ptr in_mem_sges[MAX_MEM_SGE];
        std::mutex in_mem_sge_lock;
        uint32_t num_free_mem_sge;
        uint32_t min_ssd_sge_id;
        uint32_t max_ssd_sge_id;
        uint32_t ssd_sge_num;
        std::mutex id_list_lock;
    
    public:
        
        bool has_sges_notcoded();
        bool get_sgement_to_code(ValueSGE_ptr &target_sge_ptr);
        uint32_t get_sgement_from_ssd_to_code();
};
void flush_task (BackupDB *db,ValueSGE_ptr target_sge_ptr);
void put_sge_into_rocks (BackupDB *db,ValueSGE_ptr target_sge_ptr);

class BackupNode{
    public:
        void init(int parity_node_id_);
    public:
        int parity_node_id;
        uint32_t max_sge_size;
        BackupDB db_list[4];
        std::mutex encode_lock;

        uint32_t min_parity_sge_id;
        uint32_t max_parity_sge_id;
        uint32_t parity_sge_num;
        std::mutex parity_id_lock;
        std::mutex syn_gc_lock;
        //uint32_t max_sge_id_list[K];
        //uint32_t min_sge_id_list[K];
    public:
        void flush_tail(uint32_t lsm_id);
        void remove_sge(uint32_t sge_id_);
        bool get_encode_group_sges();
       
    public:
        Encoder encoder;
};
void encode_task (BackupNode *db_node,std::vector<ValueSGE_ptr> encode_sges,std::vector<uint32_t> sge_node_id);


#endif