#include "reducer.h"

void
reducer::start_reduce(std::string input, std::string output)
{
    std::vector<std::string> values = parse_intermediate(input);
    reduce(values,output);
    
}

std::vector<std::string>
sort_reducer::parse_intermediate(std::string filename)
{
  ifstream ifs(input_file,ifstream::in);
  std::vector<std::string> values;
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

