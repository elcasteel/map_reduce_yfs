#include <string>
#include <time>
#include <map>
#include "input_reader.h"

class master{

protected:
class config *cfg;
unsigned my_master_id;
virtual void merge(std::string output_file);
unsigned calls;
time_t start_time;
void start_mappers();
void start_reducers();
//receive done message from a mapper
int mapper_done(unsigned job_id, std::string intermediate_dir);
//receive done message from a reducer
int reducer_done(std::string job_id,std::string output_file);
pthread_cond_t all_mappers_done;
pthread_cond_t all_reducers_done;
//map job_id -> input_dir
std::map <unsigned,std::string>mapper_nodes;
//map job_id -> input_file (key)
std::map <std::string,std::string>reducer_nodes;
//map key -> output file
std::map <unsigned, std::string> reducer_outputs;
input_reader reader;
virtual input_reader get_input_reader(std::string input_dir);
pthread_mutex_t map_m;

public:
void map_reduce(std::string input_file, std::string output_file);
master(class config *c,std::string _master_id);
std::string getMember();
};

class sort_master{
protected:
   input_reader get_input_reader(std::string input_dir);
   void merge(std::string output_file);
    
};
