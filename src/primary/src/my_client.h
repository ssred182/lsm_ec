#include "client_rdma.h"
#include <vector>
#include <mutex>
#include <string>
#include <iostream>



class my_client{
    public:
        int put(const std::string &key, const std::string &value);
        int get(const std::string &key, std::string &value);
    public:
        std::mutex primary_connect_lock_list[4];
        struct resources res;
    public:
        my_client();
};