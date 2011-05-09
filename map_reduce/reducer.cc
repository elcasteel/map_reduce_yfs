#include "reducer.h"
#include <list>

void
reducer::start_reduce(std::string input, std::string output)
{
    parse_file_list(input);
    reduce(values,output);
    
}
void
reducer::parse_file_list(std::string file_list)
{
    std::stringstream files(file_list);
    while(files.good())
    {
	std::string input_file;
	std::getline(files,input_file );
        parse_intermediate(input_file);
    }
}
void
sort_reducer::parse_intermediate(std::string input_file)
{
  std::ifstream ifs(input_file.c_str());
 // std::vector<std::string> values;
  while( ifs.good() )
  {
      int cur;
      ifs >> cur;
      std::ostringstream value_stream; 
      value_stream << cur;
      values.push_back(value_stream.str());
  }
  ifs.close();
  
}
void 
sort_reducer::reduce(std::vector<std::string> values,std::string output_file)
{
  //convert the strings to ints and sort them   
  std::list<int> int_values;
  for(int i = 0; i < values.size(); i++)
  {
     std::istringstream value_stream(values[i].c_str());
     int value_int;
     value_stream >> value_int;
     int_values.push_back(value_int);
  }
  //sort
  int_values.sort();
  //write to the output file

  std::ofstream output_stream(output_file.c_str());
  while(int_values.size())
  { 
     output_stream << int_values.front();
     int_values.pop_front();
  }
  output_stream.close();
} 

