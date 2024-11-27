#pragma once
#include <memory>
#include <string>
#include <rocksdb/db.h>

namespace cloud_kv
{
    class LSMKV
    {
    public:
        LSMKV(std::string path);//, uint32_t num_partition);

        //void write(uint32_t partition_id, rocksdb::WriteBatch & write_batch);
        void write(rocksdb::WriteBatch & write_batch);

        //void put(uint32_t partition_id, const std::string &key, const std::string &value);
        void put(const std::string &key, const std::string &value);

        //bool get(uint32_t partition_id, const std::string &key, std::string *value);
        bool get(const std::string &key, std::string *value);

    private:
        //uint32_t num_partition;

        using RocksDBPtr = std::shared_ptr<rocksdb::DB>;
        RocksDBPtr rocksdb_ptr = nullptr;
        //std::vector<rocksdb::ColumnFamilyHandle *> vec_cfh;
    };
    using LSMKVPtr = std::shared_ptr<LSMKV>;

};
