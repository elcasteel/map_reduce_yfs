// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"
#include "rpc/slock.h"

#include <unistd.h>
#include <sys/types.h>


lock_server_cache::lock_server_cache()
{
  //init mutex
  pthread_mutex_init(&mu, NULL);
  nacquire = 0;

  pthread_t th_out;
  int rc = pthread_create(&th_out,NULL,dedicated,(void*)this);
  if(rc){
    tprintf("\nfailed to create outgoing thread\n");
    exit(-1);
  }
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
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
    //queue retry call
    rpc_call call;
    call.rpc = lock_server_cache::RETRY;
    call.lock_id=lid;
    call.name = id;
    rpc_queue.enqueue(call);
    

  }

  else if(le.local_state == LOCKED){
    //le.queue should be empty 
    tprintf("\nlock is taken, queueing revoke call\n");
    lock_map[lid].waiting.push(id);
    lock_map[lid].local_state = ACQ;
    //queue revoke call
    rpc_call call;
    call.rpc = lock_server_cache::REVOKE;
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
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  ScopedLock sl(&mu);
  tprintf("\nreleasing lock on server\n");
  lock_protocol::status ret = lock_protocol::OK;
  nacquire--;

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
      call.rpc = lock_server_cache::RETRY;
    }else{
       tprintf("\nstill a wait so need lock back\n");
       //state should stay acquiring
       call.rpc = lock_server_cache::RETRY_WAIT;
    }
    rpc_queue.enqueue(call);
  }

  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("\nstat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

int
lock_server_cache::retry_helper(lock_protocol::lockid_t lid,std::string id,bool wait){

   int ret = rlock_protocol::OK;
   tprintf("\nmaking a retry rpc call to %s\n",id.c_str());
   handle h(id);
   rpcc *cl = h.safebind();
   if(cl){
     int r;
     ret = cl->call(rlock_protocol::retry,lid,wait,r);
   } else {
     tprintf("\nretry helper failed!\n");
   }
   return ret;
}

int
lock_server_cache::revoke_helper(lock_protocol::lockid_t lid,std::string id,int &r){

   int ret = rlock_protocol::OK;
   tprintf("\nmaking a revoke rpc call to %s\n",id.c_str());
      
   handle h(id);
   rpcc *cl = h.safebind();
   if(cl){
     ret = cl->call(rlock_protocol::revoke,lid,r);
   } else {
     tprintf("\nrevoke helper failed!\n");
   }
   return ret;
}

void
lock_server_cache::outgoing(){
  //just handles the thread safe blocking rpc queue and makes all of the rpc calls
  tprintf("\nstarted outgoing thread.\n");
  while(1){
    rpc_call rp = rpc_queue.dequeue();
    tprintf("\nmaking rpc call\n");
    if(rp.rpc == lock_server_cache::REVOKE){
        tprintf("\nmaking revoke rpc call\n");
	int r;
	int ret = revoke_helper(rp.lock_id,rp.name,r);

    }else if(rp.rpc == lock_server_cache::RETRY){
         tprintf("\nmaking retry rpc call\n");
         int ret = retry_helper(rp.lock_id,rp.name,0);
    }else{
	//theres a waiting line for this lock
        tprintf("\nmaking retry with wait rpc call\n");
	int ret = retry_helper(rp.lock_id,rp.name,1);	
    }

  }

}

void *
dedicated(void * lsc){
  lock_server_cache * lc = (lock_server_cache*) lsc;
  lc->outgoing();
}
