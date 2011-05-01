#include <string>
#include <vector> 
class reducer{

public:
virtual void reduce(std::string, std::vector<std::string> values,std::string output_file);
virtual std::vector<std::string> parse_intermediate(std::string filename);
void start_reduce(std::string input, std::string output);
}

class sort_reducer:reducer
{
void reduce(std::string, std::vector<std::string> values,std::string output_file);
std::vector<std::string> parse_intermediate(std::string filename);
  
}
