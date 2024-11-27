#include "value_sgement.h"
#include <stdlib.h>
#include <fstream>
#include <iostream>
namespace NAM_SGE{
    ValueSGE::ValueSGE(uint32_t sge_id_,uint32_t max_size_){
        sge_id = sge_id_;
        cur_offset = 0;
        buf.reserve(max_size_);
        //memset(buf.data(),0,max_size_);
    }

    uint32_t ValueSGE::get_sge_size(){
        return cur_offset;
    }

    uint32_t ValueSGE::append(const char *key, uint32_t key_length, const char *value, uint32_t value_length){
        //sge_mutex.lock();
        uint32_t ret_offset = cur_offset;
        //uint64_t kv_length = ((key_length << 32) | value_length);
        //此处之前出现了bug，只能读到key_length是两个length的和，value_len直接为0.奇怪
        buf.append(reinterpret_cast<const char *>(&key_length), sizeof(key_length));
        buf.append(reinterpret_cast<const char *>(&value_length), sizeof(value_length));
        cur_offset += 8;
        buf.append(key, key_length);
        cur_offset += key_length;
        buf.append(value, value_length);
        cur_offset += value_length;
        //sge_mutex.unlock();
        return ret_offset;
    }
    bool ValueSGE::get_kv(const uint32_t offset, const std::string &key, std::string &value){
        uint32_t key_length;
        uint32_t value_length;
        key_length = *( (uint32_t*)(buf.data()+offset ) );
        value_length = *( (uint32_t*)(buf.data()+offset+4 ) );
        value.assign(buf,offset+8+key_length,value_length);
        //std::cout<<"get from vsge key="<<key<<" kl="<<key_length<<" vl="<<value_length<<"val= "<<value<<std::endl;
        return true;
    }
    uint32_t ValueSGE::get_kv_for_build(const uint32_t offset,std::string &key){
        uint32_t key_length;
        uint32_t value_length;
        key_length = *( (uint32_t*)(buf.data()+offset ) );
        value_length = *( (uint32_t*)(buf.data()+offset+4 ) );
        key.assign(buf,offset+8,key_length);
        uint32_t next_offset = offset+ 8+key_length+value_length;
        return next_offset;
    }
    uint32_t ValueSGE::get_kv_for_gc(const uint32_t offset,std::string &key, std::string &value){
        uint32_t key_length;
        uint32_t value_length;
        key_length = *( (uint32_t*)(buf.data()+offset ) );
        value_length = *( (uint32_t*)(buf.data()+offset+4 ) );
        value.assign(buf,offset+8+key_length,value_length);
        key.assign(buf,offset+8,key_length);
        uint32_t next_offset = offset+ 8+key_length+value_length;
        return next_offset;
    }
    /*
    value sgement没有采用block的设计
    */
    void ValueSGE::flush(){
        std::ofstream fout;
        std::string path;
        path.append("../value_sgement/value_sge_");
        //这里使用to_string而不是直接&sge_id加长度是因为需要转换成assci
        path.append(std::to_string(sge_id));

        //用open创建文件
        fout.open(path,std::ios::out);
        fout << buf;//<<std::endl;
        //如果是使用二进制自己读文件就不要加endl
        fout.close();
    }
    void ValueSGE::flush(uint32_t node_id_){
        std::ofstream fout;
        std::string path;
        path.append("../value_sgement/value_sge_");
        //这里使用to_string而不是直接&sge_id加长度是因为需要转换成assci
        path.append(std::to_string(node_id_));
        path.append("_");
        path.append(std::to_string(sge_id));
        //用open创建文件
        fout.open(path,std::ios::out);
        fout << buf;//<<std::endl;
        //如果是使用二进制自己读文件就不要加endl
        fout.close();
    }

    //读取一整个segment,此函数在GC的时候使用
    bool ValueSGE::read_sge(uint32_t sge_id_,uint32_t max_size_){
        std::string path;
        path.append("../value_sgement/value_sge_");
        path.append(std::to_string(sge_id_));
        std::ifstream fin;
        fin.open(path,std::ios::in|std::ios::binary);
        while(fin.is_open()== false)
            fin.open(path,std::ios::in|std::ios::binary);
        if(buf.size()<max_size_)
            buf.reserve(max_size_);
        sge_id = sge_id_;
        //buf << fin <<std::endl;
        fin.seekg(0,std::ios::end);
        uint32_t file_size = fin.tellg();
        #if DEBUG
        std::cout<<"file_size of id "<<sge_id_<<" is"<<file_size<<std::endl;
        #endif
        fin.seekg(0,std::ios::beg);
        //char* read_buf = new char[file_size];
        buf.resize(file_size);
        fin.read((char*)buf.data(),file_size);
        //buf.assign(read_buf,file_size);
        //delete []read_buf;
        fin.close();
        return true;
    }
    //随机读
    bool read_kv_from_sge_for_backup(const uint32_t node_id,const uint32_t sge_id_, const uint32_t offset, const std::string &key, std::string &value){
        //std::cout<<"read from sge file sge id ="<<sge_id_<<" offset="<<offset<<std::endl;
        std::string path;
        path.append("../value_sgement/value_sge_");
        path.append(std::to_string(node_id));
        path.append("_");
        path.append(std::to_string(sge_id_));
        std::ifstream fin;
        fin.open(path,std::ios::in|std::ios::binary);
        for(int i=0;fin.is_open() == false;++i){
            if(i>=10)
                return false;
            //std::cout<<"cannot find sge file with id ="<<sge_id_<<std::endl;
            //return false;
        }
        //移动文件指针到kvlen-key-value的头部
        fin.seekg(offset,std::ios::beg);
        //  uint64_t kv_length;
        //  fin>>kv_length;
        //  uint32_t value_length = *((uint32_t*)( ((uint64_t)&kv_length)+4));
        //  uint32_t key_length = *((uint32_t*)&kv_length);
        uint32_t key_length;
        uint32_t value_length;
        //不能直接用<<流来得到长度，因为文件是2进制，有太多特殊字符会被识别成一定的含义
        //fin>>key_length>>value_length;
        fin.read((char*)&key_length,4);
        fin.read((char*)&value_length,4);
        //std::cout <<"key_l="<<key_length<<" value_length"<<value_length<<std::endl;
        char* buf = new char[value_length];//+1];
        //移动到value,不需要多移动+8因为读取kv_length本事会自动移动指针
        fin.seekg(key_length,std::ios::cur);
        fin.read(buf,value_length);
        //保持尾端是\0,文件里的kv应该都是以\0结尾所以暂不需要
        //buf[value_length]='\0';
        value.assign(buf,value_length);
        delete[] buf;
        fin.close();
        return true;
    }
    bool read_kv_from_sge_for_primary(const uint32_t sge_id_, const uint32_t offset, const std::string &key, std::string &value){
        //std::cout<<"read from sge file sge id ="<<sge_id_<<" offset="<<offset<<std::endl;
        std::string path;
        path.append("../value_sgement/value_sge_");
        path.append(std::to_string(sge_id_));
        std::ifstream fin;
        fin.open(path,std::ios::in|std::ios::binary);
        for(int i=0;fin.is_open() == false;i++){
           
            if(i>100){
                 //std::cout<<"cannot find sge file with id ="<<sge_id_<<std::endl;
                 return false;
            }
                
        }
        //移动文件指针到kvlen-key-value的头部
        fin.seekg(offset,std::ios::beg);
        //  uint64_t kv_length;
        //  fin>>kv_length;
        //  uint32_t value_length = *((uint32_t*)( ((uint64_t)&kv_length)+4));
        //  uint32_t key_length = *((uint32_t*)&kv_length);
        uint32_t key_length;
        uint32_t value_length;
        //不能直接用<<流来得到长度，因为文件是2进制，有太多特殊字符会被识别成一定的含义
        //fin>>key_length>>value_length;
        fin.read((char*)&key_length,4);
        fin.read((char*)&value_length,4);
        //std::cout <<"key_l="<<key_length<<" value_length"<<value_length<<std::endl;
        char* buf = new char[value_length];//+1];
        //移动到value,不需要多移动+8因为读取kv_length本事会自动移动指针
        fin.seekg(key_length,std::ios::cur);
        fin.read(buf,value_length);
        //保持尾端是\0,文件里的kv应该都是以\0结尾所以暂不需要
        //buf[value_length]='\0';
        value.assign(buf,value_length);
        delete[] buf;
        fin.close();
        return true;
    }
};