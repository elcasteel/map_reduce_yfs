// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


static void *
outgoingthread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->outgoing();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &outgoingthread, (void *) this);
  VERIFY (r == 0);
  //init mutex
  pthread_mutex_init(&mu, NULL);
}

void
lock_server_cache_rsm::outgoing()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  //just handles the thread safe blocking rpc queue and makes all of the rpc calls
  tprintf("\nstarted outgoing thread.\n");
  while(1){
    rpc_call rp = rpc_queue.dequeue();
    //if not primary dont make rpc call
    if(!rsm->amiprimary()){
      tprintf("\nnot primary so wont make rpc call to client\n");
      return;
    }
    
    int ret;
    tprintf("\nI am the primary, so making rpc call\n");
    if(rp.rpc == lock_server_cache_rsm::REVOKE){
        tprintf("\nmaking revoke rpc call for lock %llu\n",rp.lock_id);
        int r;
        ret = revoke_helper(rp.lock_id,rp.name,rp.xid,r);

    }else if(rp.rpc == lock_server_cache_rsm::RETRY){
         tprintf("\nmaking retry rpc call for lock %llu\n",rp.lock_id);
         ret = retry_helper(rp.lock_id,rp.name,false,rp.xid);
    }else{
        //theres a waiting line for this lock
        tprintf("\nmaking retry with wait rpc call for lock %llu\n",rp.lock_id);
        ret = retry_helper(rp.lock_id,rp.name,true,rp.xid);
    }

    if(ret!=rlock_protocol::OK){
        tprintf("rpc call did not return OK");
    }

  }

}




int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             lock_protocol::xid_t xid, int &)
{
  ScopedLock sl(&mu);
  tprintf("\nacquiring lock on server for client %s\n",id.c_str());
  nacquire++;

  lock_entry le = lock_map[lid];

  if(le.local_state==FREE){
    //le.queue should be empty...
    tprintf("\nlock was free, queueing retry call\n");
    lock_map[lid].owner = id;
    lock_map[lid].local_state = LOCKED;
    lock_map[lid].xid = xid;
    //queue retry call
    rpc_call call;
    call.rpc = lock_server_cache_rsm::RETRY;
    call.lock_id=lid;
    call.name = id;
    rpc_queue.enqueue(call);


  }

  else if(le.local_state == LOCKED){
    //TODO need to do something with xid here
    tprintf("\nthrowing away this xid...\n");
    //le.queue should be empty
    tprintf("\nlock is taken by %s, queueing revoke call\n",le.owner.c_str());
    lock_map[lid].waiting.push(id);
    lock_map[lid].local_state = ACQ;
    //queue revoke call
    rpc_call call;
    call.rpc = lock_server_cache_rsm::REVOKE;
    call.name = le.owner;
    call.lock_id = lid;
    rpc_queue.enqueue(call);

  }

  else if(le.local_state == ACQ){
    //le.queue should be non-empty
    tprintf("\nserver is already attempting to acquire lock, queueing to waiting list\n");
    lock_map[lid].waiting.push(id);

  }else{
    //shouldnt get here
    tprintf("\nacquire got to an unexpected state----------------------\n");
  }

  //return RETRY so client will wait for a queued rpc retry call
  return lock_protocol::RETRY;


}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         lock_protocol::xid_t xid, int &r)
{
  ScopedLock sl(&mu);
  tprintf("\nreleasing lock %llu on server from %s\n",lid,id.c_str());
  lock_protocol::status ret = lock_protocol::OK;
  nacquire--;
  //TODO do we need to do anything with xid here?
  if(lock_map[lid].waiting.empty()){
    //state should be LOCKED
    tprintf("\nno wait on the lock, freeing it\n");
    lock_map[lid].local_state = FREE;
  }else{
    //queue is not empty, send lock to next in line
    std::string next = lock_map[lid].waiting.front();
    tprintf("\nsending lock to next in line: %s\n",next.c_str());
    lock_map[lid].waiting.pop();
    lock_map[lid].owner = next;
    rpc_call call;
    call.name = next;
    call.lock_id = lid;
    if(lock_map[lid].waiting.empty()){
      tprintf("\nno more wait after %s\n",next.c_str());
      lock_map[lid].local_state = LOCKED;
      call.rpc = lock_server_cache_rsm::RETRY;
    }else{
       tprintf("\nstill a wait so need lock back\n");
       //state should stay acquiring
       call.rpc = lock_server_cache_rsm::RETRY_WAIT;
    }
    rpc_queue.enqueue(call);
  }

  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  std::ostringstream ost;
  std::string r;
  return r;
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

int
lock_server_cache_rsm::retry_helper(lock_protocol::lockid_t lid,std::string id,bool wait,lock_protocol::xid_t xid){

   int ret = rlock_protocol::OK;
   tprintf("\nmaking a retry rpc call to %s\n",id.c_str());
   if(wait) {
     tprintf("\nThere is a wait on the server\n");        
   }
   else {
     tprintf("\nthere is no wait on the server\n");        
   }
   
   handle h(id);
   rpcc *cl = h.safebind();
   if(cl){
     int r;
     ret = cl->call(rlock_protocol::retry,lid,xid,wait,r);
   } else {
     tprintf("\nretry helper failed!\n");
   }
   return ret;
}

int
lock_server_cache_rsm::revoke_helper(lock_protocol::lockid_t lid,std::string id,lock_protocol::xid_t xid,int &r){

   int ret = rlock_protocol::OK;
   tprintf("\nmaking a revoke rpc call to %s\n",id.c_str());

   handle h(id);
   rpcc *cl = h.safebind();
   if(cl){
     ret = cl->call(rlock_protocol::revoke,lid,xid,r);
   } else {
     tprintf("\nrevoke helper failed!\n");
   }
   return ret;
}
