// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>

using namespace std;

struct lock_info{
  int lock;
  pthread_cond_t condition;
};

class lock_server {


 protected:
  int nacquire;
  pthread_mutex_t mu;
  map<lock_protocol::lockid_t,lock_info> semaphores;  
  pthread_cond_t condition;
    
 public:
  lock_server();
  // destroy mutex;
  ~lock_server() {pthread_mutex_destroy(&mu);};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(lock_protocol::lockid_t lid, int &);
};

#endif 







