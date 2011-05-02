#include <string>
#include <time>
#include <map>
#include "input_reader.h"

class master{

protected:
class config *cfg;
virtual void merge(std::string input_dir, std::string output_file);
unsigned calls;
time_t start_time;
void start_mappers();
void start_reducers();
//receive done message from a mapper
int mapper_done(unsigned job_id);
//receive done message from a reducer
int reducer_done(unsigned job_id);
pthread_cond_t all_mappers_done;
pthread_cond_t all_reducers_done;
//map job_id -> input_dir
std::map <unsigned,std::string>mapper_nodes;
//map job_id -> input_file (key)
std::map <unsigned,std::string>reducer_nodes;
input_reader reader;
virtual input_reader get_input_reader(std::string input_dir);
pthread_mutex_t map_m;

public:
void map_reduce(std::string input_file, std::string output_file);
master(class config *c);
std::string getMember();
}
