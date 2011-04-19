#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"


#include <map>
#include <pthread.h>
#include "threaded_queue.h"

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  int nacquire;
  class rsm *rsm;

 enum state{
    LOCKED = 0x3001,
    FREE,
    ACQ, //acquiring
    REL //releasing
  };

  enum rpc_enum{ REVOKE, RETRY, RETRY_WAIT };
  struct rpc_call{
        rpc_enum rpc;
        lock_protocol::lockid_t lock_id;
        std::string name;
	lock_protocol::xid_t xid;
  };

  struct lock_entry{
    std::deque<std::string> waiting;
    std::string owner;
    state local_state;
    lock_protocol::xid_t xid;

    lock_entry(){
      local_state = FREE;
    }

  };

  threaded_queue<rpc_call> rpc_queue;

  map<lock_protocol::lockid_t,lock_entry> lock_map;
  pthread_mutex_t mu;


 public:
  lock_server_cache_rsm(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void outgoing();
  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, 
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
  int retry_helper(lock_protocol::lockid_t,std::string id,bool wait,lock_protocol::xid_t xid);
  int revoke_helper(lock_protocol::lockid_t,std::string id,lock_protocol::xid_t xid,int &);
};

#endif
