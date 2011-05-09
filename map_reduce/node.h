#ifndef NODE
#define NODE

#include "../rpc/rpc.h"
#include "../tprintf.h"
#include "../config.h"
#include <string>
#include <pthread.h>
#include "../threaded_queue.h"
#include "../paxos.h"
#include "mapper.h"
#include "reducer.h"
#include "master.h"

class node:config_view_change{

private:
config *cfg;
std::string primary;
std::string myid;
pthread_mutex_t view_mutex;
rpcs* rpc_server;

public:
//rpc enums
//enum CALL {JOB };
enum job_type{ MAP=0x10001,REDUCE, MAP_REDUCE,MAP_DONE,REDUCE_DONE};
 
struct rpc_call{
        
        job_type type;
        std::string input;
        std::string master_id;
        unsigned job_id;
  };

threaded_queue<rpc_call> rpc_queue;


unsigned last_master_id;
pthread_mutex_t map_mutex; 

virtual mapper get_mapper() = 0;
virtual reducer get_reducer() =0;
node(std::string first,std::string me);
void commit_change(unsigned vid);

//rpc handlers

void start_map(std::string initial_data, unsigned job_id,unsigned master_id, int &a);
void start_reduce(std::string file_list, std::string job_id, unsigned master_id, int &a);
void start_map_reduce(std::string input_file, std::string output_file,int &a);

void mapper_done(unsigned master_id, std::string dir,unsigned job_id, int &a);
void reducer_done(unsigned master_id,std::string job_id, std::string output_file, int &a); 
//rpc handler
//int new_job(job_type t, std::string input,unsigned job_id, std::string master_id);

void do_map(void* args);
void do_reduce(void *args);

std::map<unsigned,class *master m> master_map;
//bool amiprimary();
};

struct do_map_args
{
   mapper *m; 
   unsigned job_id; 
   unsigned master_id;
   std::string output_dir;
};
struct do_reduce_args
{
   reducer *r;
   std::string job_id;
   unsigned master_id;
   std::string output_file; 
   std::string file_list;
};

#endif
