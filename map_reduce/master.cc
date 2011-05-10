#include "master.h"
#include <string>
#include "node.h"
#include "dirent.h"
#include "../handle.h"
#include <stdlib.h>
master::master(class config *c, unsigned _master_id){
  cfg = c;
  my_master_id = _master_id;
  pthread_mutex_init(&map_m, NULL);
  pthread_cond_init(&all_mappers_done, NULL);
  pthread_cond_init(&all_reducers_done, NULL);
}

void 
master::map_reduce(std::string input_file, std::string output_file){
  //get input reader
  input_reader reader = get_input_reader(input_file);

  //iterate through files while calling mappers and filling in mapper table
  std::string inp_file;
  unsigned job_id = 0;
  pthread_mutex_lock(&map_m);  
  while((inp_file=reader.get_next_file())!=""){
    mapper_nodes[job_id] = inp_file;
    job_id++;

  }
  
  int acquire_delay = 30;
  while(mapper_nodes.size())
  {
     //start mappers
     start_mappers();
     //wait on jobs to finish
     timespec tp;
     clock_gettime(CLOCK_REALTIME, &tp);
     tp.tv_sec += acquire_delay;
     acquire_delay = acquire_delay *2;
     pthread_cond_timedwait(&all_mappers_done,&map_m,&tp);
  
  }
  //start reducers
  acquire_delay = 30;
  while(reducer_nodes.size())
  {
     //start reducers
     start_reducers();
     //wait on jobs to finish
     timespec tp;
     clock_gettime(CLOCK_REALTIME, &tp);
     tp.tv_sec += acquire_delay;
     acquire_delay = acquire_delay *2;
     pthread_cond_timedwait(&all_reducers_done,&map_m,&tp);
  
  }

  
   pthread_mutex_unlock(&map_m);

  //return



}

void 
master::start_mappers()
{
    //assume we are already holding map_mutex
    std::map <unsigned,std::string> ::iterator it;
    for(it=mapper_nodes.begin(); it!= mapper_nodes.end();it++)
    {
         //choose a random node
         std::string node_id =getMember();

         //ask the node to start a new map job 
         handle h(node_id);
         rpcc *cl = h.safebind();
         unsigned key =it->first;
         std::string file = mapper_nodes[key];
         pthread_mutex_unlock(&map_m);
         if(cl){
            int a;
            int ret = cl->call(node::MAP, file,key, my_master_id,a);
         } else {
             printf("bind() failed\n");
          }
         pthread_mutex_lock(&map_m);

    }
}
std::string 
master::getMember()
{
   std::vector<std::string> members = cfg->get_view(vid);
   int index = rand()%(members.size()-1) +1;
   std::string member= members[index];
   return member;
}
void 
master::start_reducers()
{
    //assume we are already holding map_mutex
    std::map <std::string,std::string> ::iterator it;
    for(it=reducer_nodes.begin(); it!= reducer_nodes.end();it++)
    {
         //choose a random node
         std::string node_id =getMember();

         //ask the node to start a new map job 
         handle h(node_id);
         rpcc *cl = h.safebind();
         std::string key =it->first;
         std::string file_list = reducer_nodes[key];
         pthread_mutex_unlock(&map_m);
         if(cl){
            int a; 
           int ret = cl->call(node::REDUCE, file_list,key, my_master_id,a);
         } else {
             printf("bind() failed\n");
          }
         pthread_mutex_lock(&map_m);

    }
}
int 
master::mapper_done(unsigned job_id, std::string intermediate_dir)
{
    pthread_mutex_lock(&map_m);
    if(mapper_nodes.find(job_id) != mapper_nodes.end())
    {
         //look up the directory this mapper was writing to
         DIR *Dir;
         struct dirent *DirEntry;
         Dir = opendir(intermediate_dir.c_str());
 
         //for each file in the directory, add the file to the list of files for the reducer working on that key
         while(DirEntry= readdir(Dir))
         {
            reducer_nodes[DirEntry->d_name].append(intermediate_dir+"/"+DirEntry->d_name+"\n");
         }

	//remove this job from the mapper_nodes map
        mapper_nodes.erase(job_id);
    }
    if(mapper_nodes.empty())
        pthread_cond_signal(&all_mappers_done); 
    pthread_mutex_unlock(&map_m);
    return 0;
}
//called by node whenever a reducer finishes
int 
master::reducer_done(std::string job_id,std::string output_file)
{
     pthread_mutex_lock(&map_m);
     if(reducer_nodes.find(job_id) != reducer_nodes.end())
     {
	 reducer_outputs[atoi(job_id.c_str())] = output_file;
         reducer_nodes.erase(job_id);
     }
     if(reducer_nodes.empty())
         pthread_cond_signal(&all_reducers_done);
     pthread_mutex_unlock(&map_m);
     return 0;
}

input_reader
sort_master::get_input_reader(std::string input_dir){
  return linesplit_input_reader(input_dir,10);
}


void 
sort_master::merge(std::string output_file){

  //open output file
  std::fstream outfile(output_file.c_str());

  //iterate through reducer_outputs map
  std::map <unsigned, std::string>::iterator it;
  //TODO ensure that the keys are sorted
  for(it = reducer_outputs.begin();it!=reducer_outputs.end();it++){
    //write each file to the output file
    std::ifstream ifs ( reducer_outputs[it->first].c_str()  );
    char *buf;
    while(ifs.good()){
      ifs.read(buf,256);
      outfile << buf;
    }
    ifs.close();
  }




  //close and return
  outfile.close();

}

