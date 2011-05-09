#include "node.h"
#include <stdio.h>
#include <stdlib.h>

int
main(int argc,char *argv[])
{
  //extract first and me ports

  if(argc<3){
    fprintf(stderr, "Usage: %s [first:]port [me:]port\n", argv[0]);
    exit(1);

  }

  //init node
  node *n = new node(argv[1],argv[2]);
  //sleep
  while(1)
    sleep(1000);



}
