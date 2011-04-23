// lock client interface.

#ifndef lock_client_cache_rsm_h

#define lock_client_cache_rsm_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"
#include <pthread.h>     
#include <map>          
#include "threaded_queue.h"
#include <time.h>
#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};


class lock_client_cache_rsm;

// Clients that caches locks.  The server can revoke locks using 
// lock_revoke_server.
class lock_client_cache_rsm : public lock_client {
 private:
  rsm_client *rsmc;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  lock_protocol::xid_t xid;
 
enum state{
        FREE = 0x2001,
        LOCKED,
        UNK,
        REL,
        ACQ,
        IDLE
  };

  //struct for lock_info
  struct lock_entry{
        state local_state;
        state rpc_state;
        int waiting;
        pthread_cond_t cond;
        lock_protocol::xid_t xid;
	

    lock_entry(){
       local_state = UNK;
       rpc_state = IDLE;
       waiting = 0;
       pthread_cond_init (&cond, NULL);
    }

  };
  enum rpc_enum{ ACQUIRE, RELEASE };
  struct rpc_call{
        rpc_enum rpc;
        lock_protocol::lockid_t lock_id;
	lock_protocol::xid_t xid;
  };

  threaded_queue<rpc_call> rpc_queue;
  std::map<lock_protocol::lockid_t,lock_entry> lock_map;
  pthread_mutex_t mu;

  //for reducing the number of aqcuire rpcs
  pthread_cond_t a_cond;
  bool releasing;
  long delay;
  struct timespec tp;
  //for repeating acquire rpcs...
  long acquire_delay;
  struct timespec acquire_tp;

public:
  static int last_port;
  lock_client_cache_rsm(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache_rsm() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  virtual lock_protocol::status release(lock_protocol::lockid_t);
  void outgoing();
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
				        lock_protocol::xid_t, int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
				       lock_protocol::xid_t, bool,int &);
};


#endif
