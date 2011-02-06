// RPC stubs for clients to talk to lock_server

#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

using namespace std;

lock_client::lock_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("lock_client: call bind\n");
  }
 // cout << "\nok is " << lock_protocol::OK;
 // cout << "\nretry is " << lock_protocol::RETRY;
 // cout << "\nrpcerr is " << lock_protocol::RPCERR;
 // cout << "\nnoent is " << lock_protocol::NOENT;
 // cout << "\nioerr is " << lock_protocol::IOERR << "\n";

}

int
lock_client::stat(lock_protocol::lockid_t lid)
{
  int r;
  int ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
  assert (ret == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client::acquire(lock_protocol::lockid_t lid)
{
  //make rpc call to acquire lock
  //cout << "\nattempting to acquire";
  int r;
  int ret = cl->call(lock_protocol::acquire, lid,r);
  //cout << "\n" << ret << "\n";
  return ret;
}

lock_protocol::status
lock_client::release(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret = cl->call(lock_protocol::release,lid,r);
  return ret;

}

