#include "kv/db.h"
#include <iostream>
#include <vector>

#define KV_NUM  1000
#define V_LEN 1000
int main(){
    DB *test_db = new DB("../rocksdb_lsm");
    
    std::vector<std::pair<std::string,std::string> >test_kvs;
    for(int i=0;i<KV_NUM;i++){
        std::string value;
        while(value.size()<V_LEN){
            value.append(std::to_string(i));
        }
        test_kvs.emplace_back(std::make_pair<std::string,std::string>(std::to_string(i),value) );
    }
    for(auto kv :test_kvs){
        std::string key = kv.first();
        std::string value = kv.second();
        test_db->put(key,value);
    }
    for(auto kv :test_kvs){
        std::string key = kv.first();
        std::string exp_value = kv.second();
        std::string get_value;
        test_db->get(key,get_value);
    }
}