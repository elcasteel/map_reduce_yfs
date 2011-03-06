// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include <pthread.h>

using namespace std;

struct extent_t{
   
   extent_protocol::extentid_t id;
   string buf;
   extent_protocol::attr attr;
};

class extent_server {

 protected:
  
  pthread_mutex_t mu;
  map<extent_protocol::extentid_t,extent_t> ext_map;

 public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







