#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <map>
#include <queue>
#include <shared_mutex>
#include <rocksdb/db.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
//#include "third/rocksdb-main/include/rocksdb/db.h"
//#include "common/ThreadPool.h"
//#include <kv/lsmkv.h>
#include "value_sgement.h"
//#include "rdma_connect.h"
using ValueSGE_ptr = std::shared_ptr<NAM_SGE::ValueSGE>;
using RocksDBPtr = std::shared_ptr<rocksdb::DB>;

#define SEND_INDEX 1
class KEY_CUR_GC{
    public:
        std::string key;
        std::shared_mutex lock;
    public:
        void set(std::string &key_){
            lock.lock();
            key.assign(key_);
            lock.unlock();
        }
        void get(std::string &key_){
            lock.lock_shared();
            key_.assign(key);
            lock.unlock_shared();
        }
        void clear(void){
            lock.lock();
            key.clear();
            lock.unlock();
        }
};
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
#define PUTTING_HASH_SIZE 1024*12

class DB
{
    public:
        DB(std::string &path,struct resources *res_ptr_,uint32_t node_id_);
        bool put(const std::string &key, const std::string &value);
        bool put_impl(const std::string &key, const std::string &value);
        bool gc_reput(const std::string &key, const std::string &value);
        bool get(const std::string &key, std::string &value);
        bool del(const std::string &key);
        //bool manual_gc(uint32_t value_sge_id);
        //gc指定文件导致不连续，就需要一个新的链表来存储sgement id
        bool manual_gc(uint32_t sge_num);
        void flush_tail();
    public:
        uint32_t max_sge_size ;
        uint32_t num_flush_thread;
        //mq_cache::ThreadPoolPtr tp;
        //暂时先不用复杂的线程池
        
        
        
        RocksDBPtr rocksdb_ptr = nullptr;
        std::string last_manifest_path;
        uint32_t last_mani_size;
        //struct stat manifest_stat;//MANIFEST修改状态
        uint32_t max_rocks_id;
        std::map<uint32_t,uint32_t> rocks_id_list;
        std::vector<uint32_t>new_sst_list;
        std::mutex rocks_id_list_lock;
        //LSMKVPtr lsmkv_index_ptr = nullptr;
        std::map<uint32_t,uint32_t> *gc_sge_map_ptr;
        std::mutex *gc_sge_map_lock_ptr;
        std::atomic<uint32_t> cur_sge_id;
        int GC_MODE;
        //GC_MODE==1 means seq      GC_MODE==2 means select from gc_sge_map 
        ValueSGE_ptr tail_sge_ptr;
        std::mutex put_mutex;
        std::vector<ValueSGE_ptr> flushing_sges;
        //在读flushing_sge和更新flushing_sges元数据时上锁
        std::mutex flushing_mutex;

        std::atomic<uint32_t> head_sge_id;
        int head_sge_id_after_gc;
        std::mutex gc_lock;
        std::mutex gc_reput_lock;
        std::mutex rdma_msg_lock[2];
        std::shared_mutex gc_rw_lock;
        std::atomic<uint8_t> is_putting_hash_map[PUTTING_HASH_SIZE];
        KEY_CUR_GC key_cur_gc;

        uint32_t max_sge_num_trigger_gc;
        uint32_t sge_num_per_gc;
        uint32_t node_id;
        struct resources *res_ptr;
        bool backup_node_avail[2] = {true,true};
    public:
        void init(std::string &path);
        void create_new_sgement();
        bool get_from_rocksdb(const std::string &key,uint32_t &sge_id,uint32_t &offset_of_sgement);
        void generate_flush_task(ValueSGE_ptr target_sge_ptr,uint32_t gc_num);
        //void flush_task (ValueSGE_ptr target_sge_ptr);
        void gc(uint32_t gc_num);
        void get_new_ssts();
        void get_delete_ssts(std::vector<uint32_t> &delete_sst_list);
        
};
void get_manipath(std::string &manifest_path);