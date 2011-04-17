// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <map>
#include <pthread.h>

using namespace std;
// with the use of semaphores, this could be extended to have locks for read and write.


lock_server::lock_server():
  nacquire (0)
{
 // here is our pthreads mutex
  pthread_mutex_init(&mu,NULL);
  pthread_cond_init(&condition,NULL);
 // cout << "lock server constructed";  
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(lock_protocol::lockid_t lid, int &r)
{
 //cout << "attempting to acquire " << lid; 

 //locks the semaphores;
 // cout << "   waiting for lock...";
  pthread_mutex_lock(&mu);
  // means we need to add id to the map
  if(semaphores.find(lid)==semaphores.end() )
  {
   //cout << "adding new mutex to list";
    lock_info * t = new lock_info;
    t->lock = 1;
    pthread_cond_init(&(t->condition),NULL);
    semaphores[lid] = *t;
   //unlock since we are done with semaphores
    pthread_mutex_unlock(&mu);
    return lock_protocol::OK;

  }
  
 //unlocks the semaphores;
 // pthread_mutex_unlock(&mu);
  
 // cout << "entering blocking";

  // blocking loop
  while(1)
  {
  //pthread_mutex_lock(&mu);
  if(semaphores[lid].lock == 0)
    break;

  //cout << "waiting on condition for " << lid;
  pthread_cond_wait(&(semaphores[lid].condition),&mu); 
  //cout << "woke up";
  
  //pthread_mutex_unlock(&mu);

  }
 //here we have mu lock and can aqcuire semaphore lid
  semaphores[lid].lock = 1;
  nacquire++;
  pthread_mutex_unlock(&mu);
 
  //cout << "\nacquired";
  return lock_protocol::OK;
}


lock_protocol::status
lock_server::release(lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&mu);
  semaphores[lid].lock = 0;
  nacquire--;
  //pthread_mutex_unlock(&mu);
 // cout << "\nsignal for "<< lid;
  pthread_cond_signal(&(semaphores[lid].condition));
  pthread_mutex_unlock(&mu);  

  return lock_protocol::OK;

}
