#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <unistd.h>

void get_manipath(std::string &manifest_path){
    std::string current_path  ="../rocksdb_lsm/CURRENT";
    std::ifstream fin;
    fin.open(current_path,std::ios::in|std::ios::binary);
    fin.seekg(0,std::ios::end);
    uint32_t file_size = fin.tellg();
    fin.seekg(0,std::ios::beg);
    //为了略去结尾的换行符
    manifest_path.resize(file_size-1);
    fin.read((char*)manifest_path.data(),file_size-1);
    fin.close();
}
static uint32_t read_file(const char* buf,const std::string &target_path){
    std::ifstream fin;
    std::string prex = "../rocksdb_lsm/";
    prex.append(target_path);
    fin.open(prex,std::ios::in|std::ios::binary);
    fin.seekg(0,std::ios::end);
    uint32_t file_size = fin.tellg();
    fin.seekg(0,std::ios::beg);
    fin.read((char*)buf,file_size);
    fin.close();
    return file_size;
}
uint32_t get_file_size(const std::string &target_path){
    std::ifstream fin;
    std::string prex = "../rocksdb_lsm/";
    prex.append(target_path);
    // fin.open(prex,std::ios::in|std::ios::binary);
    // fin.seekg(0,std::ios::end);
    // uint32_t file_size = fin.tellg();
    // fin.close();

    struct stat target_stat;
    stat(prex.c_str(), &target_stat);
    uint32_t file_size = target_stat.st_size;
    return file_size;
}