
#
# iosize=256, 1k, 4k, 16k, 64k
#

set $dir=/mnt/pmem0/zzy/fs/mnt
set $nfiles=100000
set $meandirwidth=10
set $filesize=1k
set $iosize=256
set $nthreads=1

define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$meandirwidth,prealloc=80

define process name=filereader,instances=1
{
  thread name=filereaderthread,memsize=10m,instances=$nthreads
  {
    flowop openfile name=openfile1,filesetname=bigfileset,fd=1
    flowop readwholefile name=readfile1,fd=1,iosize=$iosize
    flowop closefile name=closefile1,fd=1
  }
}

echo  "filereader Version 1.0 personality successfully loaded"
run 10