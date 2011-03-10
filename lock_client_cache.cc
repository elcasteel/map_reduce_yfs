// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include "rpc/slock.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  //init mutex
  pthread_mutex_init(&mu, NULL);

}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
//  tprintf("\nacquire: attempting to acquire mutex\n");
  ScopedLock sl(&mu);
  tprintf("\nrecieved an acquire for lock %llu\n",lid);
  tprintf("\nacquire is for client with id %s\n",id.c_str());
  //get lock entry and add count to waiting threads
  lock_map[lid].waiting++;

  while(1){
//        tprintf("\nin acquire while loop\n");
        lock_entry ent = lock_map[lid];
	if(ent.rpc_state==ACQ ){
	  //acquiring so go to wait on cond
          tprintf("\nclient is already acquiring the lock\n");
	}

	else if(ent.local_state == UNK){
          tprintf("\nneed to get lock from server\n");

	  //need to get from server
	  lock_map[lid].rpc_state = ACQ;
	  int r;
	  int ret = cl->call(lock_protocol::acquire, lid,id,r);
	  if(ret==lock_protocol::OK){
	    //we got the lock
            tprintf("\ngot lock from server\n");
	    lock_map[lid].local_state = LOCKED;
	    lock_map[lid].rpc_state = IDLE;
	    lock_map[lid].waiting--;
	    return ret;

	  }
	  else if(ret != lock_protocol::RETRY){
	    //means an error
	    tprintf("\nrpc client error during acquire call\n");
	    lock_map[lid].waiting--;
	    return ret;
	  }

	}

	else if(ent.local_state==LOCKED){
	  //just go to wait, here for debugging
          tprintf("\nlock is taken locally, wait for it\n");
	}
	else if(ent.local_state==FREE){
	  //free
          tprintf("\nlock is locally free, acquired it\n");
          lock_map[lid].local_state = LOCKED;
	  lock_map[lid].waiting--;
	  return lock_protocol::OK;
	}

	//just wait on condition var to eventually acquire
        tprintf("\nwaiting for lock\n");
	pthread_cond_wait(&(lock_map[lid].cond), &mu);
        tprintf("\nwoke up, going to try again\n");

  }
  //should never get here
  tprintf("\nshould never get here---------------------\n");
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
//  tprintf("\nrelease: attempting to acquire mutex\n");
  ScopedLock sl(&mu);
  tprintf("\nrecieved a release for lock %llu on client %s\n",lid,id.c_str());
  lock_entry ent = lock_map[lid];
  
  //see if we need to return it, and if we should now
  if(ent.rpc_state == REL && ent.waiting==0){
        tprintf("\nreturning to server\n");
	int r;
	//make sure no one grabs it, rpc state is REL
	lock_map[lid].local_state = LOCKED;
	pthread_mutex_unlock(&mu);
  	lock_protocol::status ret = cl->call(lock_protocol::release,lid,id,r);
	pthread_mutex_lock(&mu);
	if(ret!=lock_protocol::OK) {
		tprintf("\nrpc release failed!\n");
		return ret;
	}
	lock_map[lid].local_state = UNK;
	lock_map[lid].rpc_state = IDLE;	
	//check if anyone is waiting, if so wake up
	if(lock_map[lid].waiting>0){
	    pthread_cond_signal(&(lock_map[lid].cond));
	}
	return ret;
  }  
  else{
        tprintf("\nkeeping lock, %d threads are waiting\n",lock_map[lid].waiting);
	lock_map[lid].local_state=FREE;
	if(lock_map[lid].waiting>0){
	  tprintf("\nsignaling to wake up any threads\n");
	  pthread_cond_signal(&(lock_map[lid].cond));
	  tprintf("\nsignaled to wake up any threads\n");
	}
        return lock_protocol::OK;
  }

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &r)
{
  tprintf("\nrevoke: attempting to acquire mutex on client %s\n",id.c_str());
  //sets rpc state as releasing, so release will know to let return it.

  ScopedLock sl(&mu);
  tprintf("\nrecieved a revoke for lock %llu with %d threads waiting\n",lid,lock_map[lid].waiting);
  
  if(lock_map[lid].local_state == UNK || lock_map[lid].rpc_state == REL){
     tprintf("\ngot a bad revoke,return ok?\n");
     //if(lock_map[lid].rpc_state==REL) tprintf("\nit is because of the release state\n");
     r=0;
     return rlock_protocol::OK;
  }

  int ret = rlock_protocol::OK;
  lock_map[lid].rpc_state = REL;
  r = 0;
  //if lock is free, then give it back using r
  if(lock_map[lid].local_state==FREE && lock_map[lid].waiting==0){
    //means we freed it
    tprintf("\nwe freed the lock in revoke\n");
    r = 1;
    lock_map[lid].local_state==UNK;
    lock_map[lid].rpc_state = IDLE;
  }

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,bool wait, 
                                 int &)
{
  tprintf("\nretry: attempting to acquire mutex\n");
  //sets the lock as free locally and signals a waiting thread
  ScopedLock sl(&mu);
  tprintf("\nrecieved a retry for lock %llu, %d threads are waiting\n",lid,lock_map[lid].waiting);
   
  if(lock_map[lid].local_state != UNK || lock_map[lid].rpc_state == REL){
     tprintf("\ngot a bad retry,return ok?\n");
     return rlock_protocol::OK;
  }

  int ret = rlock_protocol::OK;
  lock_map[lid].local_state = FREE;
  //is there a wait on the server?
  lock_map[lid].rpc_state = wait ? REL : IDLE;
  pthread_cond_signal(&(lock_map[lid].cond));
  return ret;
}



