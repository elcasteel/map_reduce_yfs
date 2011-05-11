#include <string>
#include "node.h"
#include "../handle.h"
#include "../rpc/rpc.h"
int
main(int argc, char *argv[]){

  if(argc != 4){
    fprintf(stderr, " JOB Usage: %s [master:]port inputfile outputfile\n", argv[0]);
    exit(1);
  }

  std::string primary = string(argv[1]);
  std::string input = string(argv[2]);
  std::string output = string(argv[3]);

  handle h(primary);
  rpcc *cl = h.safebind();
  if(cl){
     int r;
     int ret = cl->call(node::MAP_REDUCE,input, output, r);
   } else {
     tprintf("\failed to start map reduce!\n");
     return 1;
   }


  return 0;
}
