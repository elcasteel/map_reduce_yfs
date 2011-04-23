#ifndef __THREAD_QUEUE__
#define __THREAD_QUEUE__

#include <pthread.h>
#include <queue>
#include "rpc/slock.h"
#include "tprintf.h"
using namespace std;

template <class T>
class threaded_queue {
	private:
		pthread_mutex_t mu;
		pthread_cond_t cond;
		queue<T> inner;
		int q_wait;
	public:
		threaded_queue() {
			tprintf("\nconstructing queue\n");
			q_wait = 0;
			pthread_mutex_init(&mu,NULL);
			pthread_cond_init(&cond,NULL);
		}
		~threaded_queue() {}
		void enqueue(T item);
		T dequeue();
	

};

template <class T>
void threaded_queue<T>::enqueue(T item){
  ScopedLock sl(&mu);
  tprintf("\nenqueueing item in blocking queue, waiting is %d\n",q_wait);
  inner.push(item);
//  if(q_wait){
    tprintf("\ngoing to signal waiting dequeue\n");
    pthread_cond_signal(&cond);
//  }
}

template<class T>
T threaded_queue<T>::dequeue(){
  ScopedLock sl(&mu);
  q_wait = 1;
  tprintf("\ncalled dequeue, waiting is %d\n",q_wait);
  while(inner.empty()){
    tprintf("\ngoing to block on queue\n");
    pthread_cond_wait(&cond,&mu);
    tprintf("\nwas signaled to get up\n");
  }
  tprintf("\nqueue not empty so returning item\n");
  q_wait = 0;
  T item = inner.front();
  inner.pop();
  return item;
}

#endif  /*__THREAD_QUEUE__*/
