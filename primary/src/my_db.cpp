#include "my_db.h"


int MyDB::Read(const std::string &table, const std::string &key,const std::vector<std::string> *fields,std::vector<KVPair> &result){
    if(!fields){
        return 1;
    }
    else{
        for(auto field : *fields){
            std::string value;
            if(client_object.get(field,value))
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

    for(KVPair &field_pair : values){
        if(!client_object.put(field_pair.first,field_pair.second))
            return 1;
    }
    return 0;
}
int MyDB::Insert(const std::string &table, const std::string &key,std::vector<KVPair> &values){
    return Update(table,key,value);
}
int MyDB::Delete(const std::string &table, const std::string &key){
    return 1;
}
void MyDB::Init(){
    return ;
}
void MyDB::Close(){
    return ;
}
