#include <string>

class input_reader{
private:
std::string mydir;
//keep track of how many pieces
unsigned index, num_pieces;

public:
input_reader(std::string input_dir,unsigned pieces);
virtual std::string get_next_file();


}
