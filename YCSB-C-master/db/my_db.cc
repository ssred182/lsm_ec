#include "my_db.h"

namespace ycsbc{
int MyDB::Read(const std::string &table, const std::string &key,const std::vector<std::string> *fields,std::vector<KVPair> &result){
    if(!fields){
        std::cout<<"invalid read"<<std::endl;
        return 1;
    }
    else{
        //std::cout<<"read a group of "<<fields->size()<<" kvs"<<std::endl;
        for(auto &field : *fields){
            std::string value;
            if(client_object->get(field,value))
                result.push_back(std::make_pair(field, value));
        }
    }
    return 0;
}
int MyDB::Scan(const std::string &table, const std::string &key,
                   int record_count, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result){
    return 1;
}
int MyDB::Update(const std::string &table, const std::string &key,std::vector<KVPair> &values){
    //std::cout<<"vector<kv>.size= "<<values.size()<<std::endl;
    //std::cout<<"Update"<<std::endl;
    for(KVPair &field_pair : values){
        
        client_object->put(field_pair.first,field_pair.second);
        
    }
    return 0;
}
int MyDB::Insert(const std::string &table, const std::string &key,std::vector<KVPair> &values){
    std::cout<<"Insert"<<std::endl;
    for(KVPair &field_pair : values){
        
        client_object->put(field_pair.first,field_pair.second);
        
    }
    return 0;
}
int MyDB::Delete(const std::string &table, const std::string &key){
    return 1;
}
// void MyDB::Init(){
//     return ;
// }
// void MyDB::Close(){
//     return ;
// }
MyDB::MyDB(utils::Properties &props){
    client_object = new my_client();
    client_object->init(props);
    std::cout<<"create MyDB finish"<<std::endl;
}
}
