#include <kv/lsmkv.h>

namespace cloud_kv
{
    /// onlyyyyyyyyyyyyyy use one partition index!
    /// ^_^
    LSMKV::LSMKV(std::string path)//, uint32_t num_partition_)
        //: num_partition(num_partition_)
    {
        rocksdb::DB *dbptr;
        rocksdb::Options options;
        options.max_background_flushes = 1;
        options.max_background_compactions = 1;
        /// options.max_write_buffer_number = 2;
        options.create_if_missing = true;

        rocksdb::DB::Open(options, path, &dbptr);
        assert(dbptr != nullptr);
        rocksdb_ptr = std::shared_ptr<rocksdb::DB>(dbptr);
        /// support more paritition index
        /*
        for (int i = 0; i < num_partition; i++)
            {
                rocksdb::ColumnFamilyHandle *handler;
                auto cf_path = "cf_" + std::to_string(i);
                auto cf_ok = rocksdb_ptr->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf_path, &handler);
                assert(cf_ok.ok());
                vec_cfh.emplace_back(handler);
            }
        */
    }

    //void LSMKV::write(uint32_t partition_id, rocksdb::WriteBatch &write_batch)
    void LSMKV::write(rocksdb::WriteBatch &write_batch)
    {
        rocksdb::WriteOptions options;
        options.disableWAL = true;
        /// auto cf = vec_cfh[partition_id % num_partition];
        auto status = rocksdb_ptr->Write(options, &write_batch);
        assert(status.ok());
    }

    //void LSMKV::put(uint32_t partition_id, const std::string &key, const std::string &value)
    void LSMKV::put( const std::string &key, const std::string &value)
    {
        rocksdb::WriteOptions options;
        options.disableWAL = true;
        /// auto cf = vec_cfh[partition_id % num_partition];
        /// auto status = rocksdb_ptr->Put(options, cf, key, value);
        auto status = rocksdb_ptr->Put(options, key, value);
        assert(status.ok());
    }

    //bool LSMKV::get(uint32_t partition_id, const std::string &key, std::string *value)
    bool LSMKV::get(const std::string &key, std::string *value)
    {
        /// auto cf = vec_cfh[partition_id % num_partition];
        /// auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), cf, key, value);
        auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, value);
        return status.ok();
    }

};
