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

lock_server_cache::lock_server_cache()
{
  //init mutex
  pthread_mutex_init(&mu, NULL);
  nacquire = 0;
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  
  ScopedLock sl(&mu);
  tprintf("\nacquiring lock on server\n");
  lock_protocol::status ret = lock_protocol::RETRY;
  nacquire++;

  lock_entry le = lock_map[lid];
  
  if(le.local_state==FREE){
   tprintf("\nlock was free\n");
   lock_map[lid].local_state = LOCKED;
    lock_map[lid].owner = id;
    ret = lock_protocol::OK;

  }
  else if(le.local_state == LOCKED){
    //going to revoke from owner
    tprintf("\nlock was taken, going to revoke\n");
    int r;
    ret = revoke_helper(lid,le.owner,r);
    if(ret==rlock_protocol::OK){
      tprintf("\nsent revoke\n");
      if(r){
	tprintf("\nrevoke returned the lock, giving it to new client\n");
	lock_map[lid].local_state=LOCKED;
	lock_map[lid].owner = id;
	ret = lock_protocol::OK;
      }else{
	tprintf("\nsending back RETRY\n");
        lock_map[lid].local_state = ACQ;
        lock_map[lid].waiting.push(id);
        ret = lock_protocol::RETRY;
      }
    }


  }
  else{
    tprintf("\nlock is being acquired, putting on queue\n");

    //should have acq state, maybe i can assert
    lock_map[lid].waiting.push(id);
  }
  
  return ret;
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
    //means no one wants the lock, should never happen?
    tprintf("\nno wants the lock, freeing it\n");

    lock_map[lid].local_state = FREE;
    
  }
  else{
    tprintf("\ngiving lock to next client in queue\n");

    //get the next in line
    std::string next = lock_map[lid].waiting.front();
    lock_map[lid].waiting.pop();
    bool wait = !lock_map[lid].waiting.empty();
    ret = retry_helper(lid,next,wait);
    if(ret==lock_protocol::OK){
      tprintf("\nlock sent\n");
      lock_map[lid].local_state = LOCKED;
      lock_map[lid].owner = next;
    }    

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
     ret = rlock_protocol::RPCERR;
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
     ret = rlock_protocol::RPCERR;
   }
  return ret;
}
