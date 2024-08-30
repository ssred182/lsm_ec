#include "my_client.h"
#define KV_NUM  10*1024
#define V_LEN 1024
class KV{
    public:
        std::string key;
        std::string value;
        KV(const std::string &key_,const std::string &value_){
            key = key_;
            value = value_;
        }
};
int main(){
    std::cout<<"build kvs"<<std::endl;
    std::vector<KV>test_kvs;
    for(int i=0;i<KV_NUM;i++){
        std::string value;
        while(value.size()<V_LEN){
            value.append(std::to_string(i));
        }
        KV tmp_kv(std::to_string(i),value);
        test_kvs.emplace_back(tmp_kv);
    }
    std::cout<<"put begin"<<std::endl;
    my_client test_client;
    int num = 1;
    for(int i=0;i<1024;i++){
        for(auto kv :test_kvs){
            // std::string key = kv.key;
            // std::string value = kv.value;
            test_client.put(kv.key,kv.value);
            if(num%10000==0)
                std::cout<<"put" << num << "kvs"<<std::endl;
            num++;
        }
    }
    std::cout<<"test finish"<<std::endl;
    getchar();
}