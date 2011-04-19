// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "rsm_client.h"

static void *
outgoingthread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->outgoing();
  return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
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
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  xid = 0;
  // You fill this in Step Two, Lab 7
  // - Create a rsm_client object, and use the rsm_client object to do RPC 
  //   calls instead of the rpcc object of lock_client
  pthread_t th;
  int r = pthread_create(&th, NULL, &outgoingthread, (void *) this);
  VERIFY (r == 0);
  //init mutex
  pthread_mutex_init(&mu, NULL);
  //init rpc addons
  pthread_cond_init(&a_cond,NULL);
  releasing = false;
  delay = 10000; //nanosecs

  //init rsm client
  //rsmc = new rsm_client(xdst);

}


void
lock_client_cache_rsm::outgoing()
{

//handles the thread safe queue and makes rpc calls
  tprintf("\nstarted outgoing thread.\n");
  int r;
  while(1){
    rpc_call rp = rpc_queue.dequeue();
    tprintf("\nrpc call to be made\n");
    if(rp.rpc == lock_client_cache_rsm::ACQUIRE){
      tprintf("\nmaking acquire rpc call\n");
      lock_protocol::status ret = cl->call(lock_protocol::acquire,rp.lock_id,id,rp.xid,r);
      if(ret==lock_protocol::RETRY){                  
        tprintf("\nacquire returned RETRY\n");
      }else{                  
         tprintf("\nacquire didnt return RETRY, error...\n");
        }
    }else{          
        //we have a release call to make
       tprintf("\nmaking release rpc call for lock %llu from %s\n",rp.lock_id,id.c_str());
       //flushing extent cache
       if(lu!=NULL)
         lu->dorelease(rp.lock_id);
       lock_protocol::status ret = cl->call(lock_protocol::release,rp.lock_id,id,rp.xid,r);
    }

  }

}


lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
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
	//update xid for self and lock entry
	lock_map[lid].xid = xid++;
        tprintf("\nclient needs to get it from the server\n");
        lock_map[lid].rpc_state = ACQ;
        rpc_call call;
        call.rpc = lock_client_cache_rsm::ACQUIRE;
        call.lock_id = lid;
	call.xid = lock_map[lid].xid;
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
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
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
    tprintf("\nSomeone needs the lock\n");
    lock_map[lid].local_state = FREE;
    pthread_cond_signal(&(lock_map[lid].cond));
    return lock_protocol::OK;
  }else if(lock_map[lid].rpc_state == REL){
    //no one wants it, but the server so return it
    tprintf("\nNo one needs the lock, but the server so returning it\n");
    lock_map[lid].local_state = UNK;
    lock_map[lid].rpc_state = IDLE;
    rpc_call call;
    call.rpc = lock_client_cache_rsm::RELEASE;
    call.xid = lock_map[lid].xid;
    call.lock_id = lid;
    rpc_queue.enqueue(call);
    return lock_protocol::OK;

  }else{
    //assert state is locked and idle
    tprintf("\nNo one needs the lock so keeping it\n");
    lock_map[lid].local_state = FREE;
    return lock_protocol::OK;

  }

}


rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
			          lock_protocol::xid_t xid, int &)
{
 //sets rpc state as releasing, so release will know to let return it.

  ScopedLock sl(&mu);  
  tprintf("\nrecieved a revoke for lock %llu with %d threads waiting\n",lid,lock_map[lid].waiting);

  //TODO make sure xid matches

  //state should not be unknown (we have the lock)
  VERIFY(lock_map[lid].local_state!=UNK);

  if(lock_map[lid].local_state==LOCKED){
    tprintf("\nit was locked, so it will return later\n");
    //we are not done, just set state to releasing
    lock_map[lid].rpc_state=REL;
  }else if(lock_map[lid].local_state==FREE){
    tprintf("\nlock is free, so we'll return it.\n");
    //we are done so return it
    lock_map[lid].local_state = UNK;
    lock_map[lid].rpc_state = IDLE;
    rpc_call call;
    call.rpc = lock_client_cache_rsm::RELEASE;
    call.lock_id = lid;
    rpc_queue.enqueue(call);
  }else{
	tprintf("\nGot a revoke when we shouldnt have...\n");
  }
  return rlock_protocol::OK;

}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid,lock_protocol::xid_t xid, bool wait, int &r)
{
  tprintf("\nretry: attempting to acquire mutex\n");
  //sets the lock as free locally and signals a waiting thread         
  ScopedLock sl(&mu);
  tprintf("\nrecieved a retry for lock %llu on client %s, %d threads are waiting\n",lid,id.c_str(),lock_map[lid].waiting);
  
  //TODO make sure xids match

  //lock state should be unknown
  VERIFY(lock_map[lid].local_state==UNK);
  lock_map[lid].rpc_state = wait ? REL : IDLE;
  if(wait) {
    tprintf("\nThere is a wait on the server\n");
  }
  else {
    tprintf("\nthere is no wait on the server\n");
  }
  tprintf("\nwait equals %d\n",wait);
  lock_map[lid].local_state = FREE; 
  pthread_cond_signal(&(lock_map[lid].cond));
  return rlock_protocol::OK;
}


