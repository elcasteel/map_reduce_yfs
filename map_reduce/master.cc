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
  tprintf("****MASTER****\nmaster starting a map job \n");
  my_dir=input_file; 
  //create directory for input_reader files
  std::string input_reader_dir = my_dir+"/input_reader";
  //leave actual mkdir call to input reader as it expects to only find input files in dir
  //int r= mkdir(input_reader_dir.c_str(), 0777);
  //VERIFY(r == 0);

  input_reader* reader = get_input_reader(my_dir,input_reader_dir);
  tprintf("****MASTER****\ninput reader constructor returns \n");
  //iterate through files while calling mappers and filling in mapper table
  std::string inp_file;
  unsigned job_id = 0;
  pthread_mutex_lock(&map_m); 
  tprintf("****MASTER****\n got map_m lock\n"); 
  while((inp_file=reader->get_next_file())!=""){
   printf("****MASTER**** got a file\n"); 
    mapper_nodes[job_id] = inp_file;
    job_id++;

  }
  delete reader;
  tprintf("****MASTER****\nmaster got %u jobs \n",job_id);

  int acquire_delay = 5;
  //create dir for mappers
  std::string mapper_dir = my_dir+"/mapper_data";
  int r= mkdir(mapper_dir.c_str(), 0777);
  VERIFY(r == 0);
  
  while(mapper_nodes.size())
  {
     tprintf("****MASTER****\nstarting new mappers \n");
     //start mappers
     start_mappers(mapper_dir);
     //wait on jobs to finish
     timespec tp;
     clock_gettime(CLOCK_REALTIME, &tp);
     tp.tv_sec += acquire_delay;
     acquire_delay = acquire_delay *2;
     tprintf("****MASTER****\nwaiting for mappers to finish \n");

     pthread_cond_timedwait(&all_mappers_done,&map_m,&tp);
      
  }
  tprintf("****MASTER****\nmapping done \n");

    //create dir for reducers
    std::string reducer_dir = my_dir+"/reducer_data";
     r= mkdir(reducer_dir.c_str(), 0777);
    VERIFY(r == 0);

  //start reducers
  acquire_delay = 20;
  while(reducer_nodes.size())
  { 
    printf("starting reducers\n");
     //start reducers
     start_reducers(reducer_dir);
     //wait on jobs to finish
     timespec tp;
     clock_gettime(CLOCK_REALTIME, &tp);
     tp.tv_sec += acquire_delay;
     acquire_delay = acquire_delay *2;
     pthread_cond_timedwait(&all_reducers_done,&map_m,&tp);
     printf("****MASTER**** woke up \n");
  }

   merge(output_file);
   pthread_mutex_unlock(&map_m);

  //return



}

void 
master::start_mappers(std::string mapper_dir)
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
            int ret = cl->call(node::MAP, file,key, my_master_id,mapper_dir,a,rpcc::to(500));
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
master::start_reducers(std::string reducer_dir)
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
           int ret = cl->call(node::REDUCE, file_list,key, my_master_id,reducer_dir,a);
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
    printf("****MASTER**** mapper %d has returned \n",job_id);
    if(mapper_nodes.find(job_id) != mapper_nodes.end())
    {
         //look up the directory this mapper was writing to
         DIR *Dir;
         struct dirent *DirEntry;
         Dir = opendir(intermediate_dir.c_str());
 	 printf("****MASTER**** opened directory from mapper \n");
         //for each file in the directory, add the file to the list of files for the reducer working on that key
         while((DirEntry= readdir(Dir)))
         {
            
	    reducer_nodes[DirEntry->d_name].append(intermediate_dir+"/"+DirEntry->d_name+"\n");
            printf("****MASTER**** added new file for reducer %s\n",reducer_nodes[DirEntry->d_name].c_str());
         }

	//remove this job from the mapper_nodes map
        mapper_nodes.erase(job_id);
    }
    else
        printf("****MASTER duplicate job!\n");
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
     printf("****MASTER**** reducer done\n");
     if(reducer_nodes.find(job_id) != reducer_nodes.end())
     {
	 reducer_outputs[atoi(job_id.c_str())] = output_file;
         reducer_nodes.erase(job_id);
     }
     if(reducer_nodes.empty()){
         printf("****MASTER**** signaling reducers done\n");
         pthread_cond_signal(&all_reducers_done);
     }
     printf("****MASTER**** removed reducer reducer_nodes left %d\n",reducer_nodes.size());
     pthread_mutex_unlock(&map_m);
     return 0;
}

input_reader*
sort_master::get_input_reader(std::string input_dir,std::string output_dir){
  return new linesplit_input_reader(input_dir,10,output_dir);
}


void 
sort_master::merge(std::string output_file){

  //open output file
  printf("****MASTER**** merging to %s\n",output_file.c_str());
  fflush(stdout);
  std::ofstream outfile(output_file.c_str());

  //iterate through reducer_outputs map
  std::map <unsigned, std::string>::iterator it;
  //TODO ensure that the keys are sorted
  for(it = reducer_outputs.begin();it!=reducer_outputs.end();it++){
    //write each file to the output file
    printf("reading from %s \n",reducer_outputs[it->first].c_str());
    std::ifstream ifs ( reducer_outputs[it->first].c_str()  );
    //ifs.seekg(0,std::ios::end);
     //int size = ifs.tellg();
    //char *buf=new char[size];
    //ifs.seekg(0,std::ios::beg);
     std::string line;
     while(ifs.good()){
      //ifs.read(buf,size);
      std::getline(ifs,line);
      printf("****MASTER**** read buf %s \n",line.c_str());
      
      outfile << line.c_str();
      outfile << "\n";
    }
    ifs.close();
  }
  printf("****MASTER**** done merging \n");




  //close and return
  outfile.close();

}

