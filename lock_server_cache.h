#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

class lock_server_cache : public rsm_state_transfer {
 private:
  int nacquire;
  class rsm *rsm;
 public:
  lock_server_cache(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, 
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

#endif
