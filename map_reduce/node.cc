#include "node.h"


node::node(std::string first,std::string me){
  //init config
  myid = me;
  primary = first;
  cfg = new config(first,me,this);

  //init listener thread
  
  //init outgoing thread for async map and reduce rpcs

  //init mutexes, etc
  pthread_mutex_init(&view_mutex,NULL);
}
