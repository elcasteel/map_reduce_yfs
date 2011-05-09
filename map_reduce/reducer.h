#include <string>
#include <vector> 
class reducer{

public:
virtual void reduce(std::string, std::vector<std::string> values,std::string output_file);
virtual void parse_intermediate(std::string filename);
void start_reduce(std::string input, std::string output);
std::vector<std::string> values;
void parse_file_list(std::string file_list);
}

class sort_reducer:reducer
{
void reduce(std::string, std::vector<std::string> values,std::string output_file);
void parse_intermediate(std::string filename);
  
}
