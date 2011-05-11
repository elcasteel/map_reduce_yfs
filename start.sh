#!/usr/bin/env bash

ulimit -c unlimited

LOSSY=$1
NUM_LS=$2
NUM_N=5
if [ -z $NUM_LS ]; then
    NUM_LS=0
fi

BASE_PORT=$RANDOM
BASE_PORT=$[BASE_PORT+2000]
EXTENT_PORT=$BASE_PORT
YFS1_PORT=$[BASE_PORT+2]
YFS2_PORT=$[BASE_PORT+4]
YFS3_PORT=$[BASE_PORT+6]
YFS4_PORT=$[BASE_PORT+8]
YFS5_PORT=$[BASE_PORT+10]


LOCK_PORT=$[BASE_PORT+12]

NODE_PORT=$[BASE_PORT+14]


YFSDIR1=$PWD/yfs1
YFSDIR2=$PWD/yfs2
YFSDIR3=$PWD/yfs3
YFSDIR4=$PWD/yfs4
YFSDIR5=$PWD/yfs5

if [ "$LOSSY" ]; then
    export RPC_LOSSY=$LOSSY
fi

if [ $NUM_LS -gt 1 ]; then
    x=0
    rm config
    while [ $x -lt $NUM_LS ]; do
      port=$[LOCK_PORT+2*x]
      x=$[x+1]
      echo $port >> config
    done
    x=0
    while [ $x -lt $NUM_LS ]; do
      port=$[LOCK_PORT+2*x]
      x=$[x+1]
      echo "starting ./lock_server $LOCK_PORT $port > lock_server$x.log 2>&1 &"
      ./lock_server $LOCK_PORT $port > lock_server$x.log 2>&1 &
      sleep 1
    done
else
    echo "starting ./lock_server $LOCK_PORT > lock_server.log 2>&1 &"
    ./lock_server $LOCK_PORT > lock_server.log 2>&1 &
    sleep 1
fi

unset RPC_LOSSY

echo "starting ./extent_server $EXTENT_PORT > extent_server.log 2>&1 &"
./extent_server $EXTENT_PORT > extent_server.log 2>&1 &
sleep 1

rm -rf $YFSDIR1
mkdir $YFSDIR1 || exit 1
sleep 1
echo "starting ./yfs_client $YFSDIR1 $EXTENT_PORT $LOCK_PORT > yfs_client1.log 2>&1 &"
./yfs_client $YFSDIR1 $EXTENT_PORT $LOCK_PORT > yfs_client1.log 2>&1 &
sleep 1

rm -rf $YFSDIR2
mkdir $YFSDIR2 || exit 1
sleep 1
echo "starting ./yfs_client $YFSDIR2 $EXTENT_PORT $LOCK_PORT > yfs_client2.log 2>&1 &"
./yfs_client $YFSDIR2 $EXTENT_PORT $LOCK_PORT > yfs_client2.log 2>&1 &

rm -rf $YFSDIR3
mkdir $YFSDIR3 || exit 1
sleep 1
echo "starting ./yfs_client $YFSDIR3 $EXTENT_PORT $LOCK_PORT > yfs_client3.log 2>&1 &"
./yfs_client $YFSDIR3 $EXTENT_PORT $LOCK_PORT > yfs_client3.log 2>&1 &
rm -rf $YFSDIR4
mkdir $YFSDIR4 || exit 1
sleep 1
echo "starting ./yfs_client $YFSDIR4 $EXTENT_PORT $LOCK_PORT > yfs_client4.log 2>&1 &"
./yfs_client $YFSDIR4 $EXTENT_PORT $LOCK_PORT > yfs_client4.log 2>&1 &
rm -rf $YFSDIR5
mkdir $YFSDIR5 || exit 1
sleep 1
echo "starting ./yfs_client $YFSDIR5 $EXTENT_PORT $LOCK_PORT > yfs_client5.log 2>&1 &"
./yfs_client $YFSDIR5 $EXTENT_PORT $LOCK_PORT > yfs_client5.log 2>&1 &

sleep 2

# make sure FUSE is mounted where we expect
pwd=`pwd -P`
if [ `mount | grep "$pwd/yfs1" | grep -v grep | wc -l` -ne 1 ]; then
    sh stop.sh
    echo "Failed to mount YFS properly at ./yfs1"
    exit -1
fi

# make sure FUSE is mounted where we expect
if [ `mount | grep "$pwd/yfs2" | grep -v grep | wc -l` -ne 1 ]; then
    sh stop.sh
    echo "Failed to mount YFS properly at ./yfs2"
    exit -1
fi

# make sure FUSE is mounted where we expect
if [ `mount | grep "$pwd/yfs3" | grep -v grep | wc -l` -ne 1 ]; then
    sh stop.sh
    echo "Failed to mount YFS properly at ./yfs3"
    exit -1
fi
# make sure FUSE is mounted where we expect
if [ `mount | grep "$pwd/yfs4" | grep -v grep | wc -l` -ne 1 ]; then
    sh stop.sh
    echo "Failed to mount YFS properly at ./yfs4"
    exit -1
fi
# make sure FUSE is mounted where we expect
if [ `mount | grep "$pwd/yfs5" | grep -v grep | wc -l` -ne 1 ]; then
    sh stop.sh
    echo "Failed to mount YFS properly at ./yfs5"
    exit -1
fi
echo "num n = " 
echo $NUM_N
if [ $NUM_N -gt 1 ]; then
    x=0
    rm config
    while [ $x -lt $NUM_N ]; do
      port=$[NODE_PORT+2*x]
      x=$[x+1]
      echo $port >> config
    done
    x=0
    while [ $x -lt $NUM_N ]; do
      port=$[NODE_PORT+2*x]
      x=$[x+1]
      echo "starting ./map_reduce/node $NODE_PORT $port > node$x.log 2>&1 &"
      ./map_reduce/node $NODE_PORT $port > node$x.log 2>&1 &
      sleep 1
    done
else
    echo "starting ./map_reduce/node $NODE_PORT > node.log 2>&1 &"
    ./map_reduce/node $NODE_PORT > node.log 2>&1 &
    sleep 1
fi
sleep 1
python map_reduce/sort_gen.py
sleep 2
map_reduce/job_main $NODE_PORT yfs1 yfs1/output
