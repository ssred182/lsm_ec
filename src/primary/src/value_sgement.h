

//#include <mutex>
#include <string>

namespace NAM_SGE{
    class ValueSGE{
        public:
            ValueSGE(uint32_t sge_id_,uint32_t max_size_);
            uint32_t get_sge_size();
            // return the offset of the appended kv
            uint32_t append(const char *key, uint32_t key_length, const char *value, uint32_t value_length);
            void flush();
            void flush(uint32_t node_id_);
            //get_kv是从还没有flush的sgement里读kv
            bool get_kv(const uint32_t offset, const std::string &key, std::string &value);
            uint32_t get_kv_for_gc(const uint32_t offset,std::string &key, std::string &value);
            uint32_t get_kv_for_build(const uint32_t offset, std::string &key);
            bool read_sge(uint32_t sge_id_,uint32_t max_size_);


            uint32_t sge_id=0;
            std::string buf;
            //std::mutex sge_mutex;  
            uint32_t cur_offset;
            
    };
    bool read_kv_from_sge(const uint32_t sge_id_, const uint32_t offset, const std::string &key, std::string &value);
};
