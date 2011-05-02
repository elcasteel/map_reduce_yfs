#include "master.h"
#include <string>
#include "node.h"

master::master(class config *c){
  cfg = c;
  pthread_mutex_init(&map_m);

}

void 
master::map_reduce(std::string input_file, std::string output_file){
  //get input reader
  input_reader reader = get_input_reader(input_file);

  //iterate through files while calling mappers and filling in mapper table
  std::string inp_file;
  unsigned job_id = 0;
  pthread_mutex_lock(&map_m);  
  while((inp_file=reader.get_next_file())!=NULL){
    mapper_nodes[job_id] = inp_file;
    job_id++;

  }
  
  //start mappers
  start_mappers();
  //wait on jobs to finish
  

  //start reducers


  //wait on reducers to finish


  //return



}
