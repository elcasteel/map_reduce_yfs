#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <queue>
#include <pthread.h>

class lock_server_cache {
 private:
  int nacquire;

  enum state{
    LOCKED = 0x3001,
    FREE,
    ACQ, //acquiring
    REL //releasing
  };

  struct lock_entry{
    std::queue<std::string> waiting;
    std::string owner;
    state local_state;

    lock_entry(){
      local_state = FREE;
    }

  };

  map<lock_protocol::lockid_t,lock_entry> lock_map;
  pthread_mutex_t mu;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
  int retry_helper(lock_protocol::lockid_t,std::string id,bool wait);
  int revoke_helper(lock_protocol::lockid_t,std::string id,int &);

};

#endif
