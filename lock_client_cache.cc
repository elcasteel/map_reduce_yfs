// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include "rpc/slock.h"
#include <time.h>

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
  //init rpc addons
  pthread_cond_init(&a_cond,NULL);
  releasing = false;
  delay = 10000; //nanosecs
  //start outgoing thread
  pthread_t th_out;
  int rc = pthread_create(&th_out,NULL,dedicated,(void*)this);
  if(rc){
    tprintf("\nfailed to create outgoing thread\n");
    exit(-1);
  }

}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
//  tprintf("\nacquire: attempting to acquire mutex\n");
  ScopedLock sl(&mu);
  tprintf("\nrecieved an acquire for lock %llu\n",lid);
  tprintf("\nacquire is for client with id %s\n",id.c_str());
  lock_map[lid].waiting++;
  
  while(1){
    if(lock_map[lid].local_state == FREE){
	tprintf("\nlock was locally, free\n");
        //give to thread
	lock_map[lid].local_state = LOCKED;
	lock_map[lid].waiting--;
	return lock_protocol::OK;

    }else if(lock_map[lid].rpc_state == ACQ || lock_map[lid].rpc_state == REL){
	//go to wait
	tprintf("\nclient is already acquiring or letting it go\n");

   }else if(lock_map[lid].local_state == UNK){
	//assert rpc_state = IDLE
	tprintf("\nclient needs to get it from the server\n");
        lock_map[lid].rpc_state = ACQ;
	rpc_call call;
	call.rpc = lock_client_cache::ACQUIRE;
	call.lock_id = lid;
	rpc_queue.enqueue(call);
	tprintf("\nacquire call queued for later\n");
   }
  //enter wait
  if(releasing){
    tprintf("\nsignaling release call\n");
    pthread_cond_signal(&a_cond);
  }

  tprintf("\nwaiting for lock\n");
  pthread_cond_wait(&(lock_map[lid].cond),&mu);
  tprintf("\nwaking up...\n");
  }

}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
//  tprintf("\nrelease: attempting to acquire mutex\n");
  ScopedLock sl(&mu);
  tprintf("\nrecieved a release for lock %llu on client %s\n",lid,id.c_str());

  tprintf("\ngoing to delay a bit to see if anyone else wants it\n");
  releasing = true;
  clock_gettime(CLOCK_REALTIME, &tp);
  tp.tv_nsec += delay;
  int rc = 0;
  while(lock_map[lid].waiting == 0 && rc == 0){
     rc = pthread_cond_timedwait(&a_cond,&mu,&tp);
  }    

  if(lock_map[lid].waiting>0){
    //some threads want the lock
    lock_map[lid].local_state = FREE;
    pthread_cond_signal(&(lock_map[lid].cond));
    return lock_protocol::OK;

  }else if(lock_map[lid].rpc_state == REL){
    //no one wants it, but the server so return it
    lock_map[lid].local_state = UNK;
    lock_map[lid].rpc_state = IDLE;
    rpc_call call;
    call.rpc = lock_client_cache::RELEASE;
    call.lock_id = lid;
    rpc_queue.enqueue(call);
    return lock_protocol::OK;
    
  }else{
    //assert state is locked and idle
    lock_map[lid].local_state = FREE;
    return lock_protocol::OK;

  }


}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &r)
{
  tprintf("\nrevoke: attempting to acquire mutex %llu  on client %s\n",lid,id.c_str());
  //sets rpc state as releasing, so release will know to let return it.

  ScopedLock sl(&mu);
  tprintf("\nrecieved a revoke for lock %llu with %d threads waiting\n",lid,lock_map[lid].waiting);

  //state should not be unknown (we have the lock)
  if(lock_map[lid].local_state==LOCKED){
    tprintf("\nit was locked, so it will return later\n");
    //we are not done, just set state to releasing
    lock_map[lid].rpc_state=REL;
  }else{
    tprintf("\nlock is free, so we'll return it.\n");
    //we are done so return it
    lock_map[lid].local_state = UNK;
    lock_map[lid].rpc_state = IDLE;
    rpc_call call;
    call.rpc = lock_client_cache::RELEASE;
    call.lock_id = lid;
    rpc_queue.enqueue(call);
  }
  return rlock_protocol::OK;  

}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,bool wait, 
                                 int &)
{
  tprintf("\nretry: attempting to acquire mutex\n");
  //sets the lock as free locally and signals a waiting thread
  ScopedLock sl(&mu);
  tprintf("\nrecieved a retry for lock %llu on client %s, %d threads are waiting\n",lid,id.c_str(),lock_map[lid].waiting);
  //lock state should be unknown
  lock_map[lid].rpc_state = wait ? REL : IDLE;
  lock_map[lid].local_state = FREE;
  pthread_cond_signal(&(lock_map[lid].cond));
  return rlock_protocol::OK;
}

void 
lock_client_cache::outgoing(){
//handles the thread safe queue and makes rpc calls
  tprintf("\nstarted outgoing thread.\n");
  int r;
  while(1){
    rpc_call rp = rpc_queue.dequeue();
    tprintf("\nrpc call to be made\n");
    if(rp.rpc == lock_client_cache::ACQUIRE){
      tprintf("\nmaking acquire rpc call\n");
      lock_protocol::status ret = cl->call(lock_protocol::acquire,rp.lock_id,id,r);
      if(ret==lock_protocol::RETRY){
	tprintf("\nacquire returned RETRY\n");
      }else{
	 tprintf("\nacquire didnt return RETRY, error...\n");
	}
    }else{
        //we have a release call to make
       tprintf("\nmaking release rpc call\n");
       //flushing extent cache
       lu->dorelease(rp.lock_id);
       lock_protocol::status ret = cl->call(lock_protocol::release,rp.lock_id,id,r);
    }

  }


}

void *
dedicated(void *lcc){
  lock_client_cache* lc = (lock_client_cache*) lcc;
  lc->outgoing();

}
