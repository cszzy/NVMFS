
# Create a fileset of 50,000 entries ($nfiles), where each file's size is set
# via a gamma distribution with the median size of 16KB ($filesize).
# Fire off 16 threads ($nthreads), where each thread stops after
# deleting 1000 ($count) files.
#
# filesize=256, 1k, 4k, 16k, 64k
#

set $dir=/mnt/pmem0/zzy/fs/mnt
set $count=1000000
set $filesize=1k
set $nfiles=1000000
set $meandirwidth=1000000
set $nthreads=1

set mode quit firstdone

define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$meandirwidth,prealloc=100,paralloc

define process name=filedelete,instances=1
{
  thread name=filedeletethread,memsize=10m,instances=$nthreads
  {
    flowop deletefile name=deletefile1,filesetname=bigfileset
    flowop opslimit name=limit
    flowop finishoncount name=finish,value=$count
  }
}

echo  "FileMicro-Delete Version 2.4 personality successfully loaded"
run 20
