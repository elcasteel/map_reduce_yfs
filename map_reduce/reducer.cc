#include "reducer.h"

void
reducer::start_reduce(std::string input, std::string output)
{
    parse_file_list(input);
    reduce(values,output);
    
}
void
reducer::parse_file_list(std::string file_list)
{
    stringstream files(file_list);
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
  ifstream ifs(input_file);
 // std::vector<std::string> values;
  while( ifs.good() )
  {
      int cur;
      ifs >> cur;
      ostringstream value_stream; 
      value_stream << cur;
      values.append(value_stream.str());
  }
  ifs.close();
  return values;
}
void 
sort_reducer::reduce(std::string, std::vector<std::string> values,std::string output_file)
{
  //convert the strings to ints and sort them   
  std::list<int> int_values;
  for(int i = 0; i < values.size(); i++)
  {
     istringstream value_stream(values[i]);
     int value_int;
     value_stream >> value_int;
     int_values.append(value_int);
  }
  //sort
  int_values.sort();
  //write to the output file

  ofstream output_stream(output_file);
  while(int_values.size())
  { 
     output_stream << int_values.front();
     int_values.pop_front();
  }
  ofstream.close();
} 

