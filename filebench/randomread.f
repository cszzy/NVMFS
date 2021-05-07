
#
# iosize=256, 1k, 4k, 16k, 64k
#

set $dir=/mnt/pmem0/zzy/fs/mnt
set $filesize=2g
set $iosize=4k
set $nthreads=1
set $workingset=0
set $directio=1

define file name=largefile1,path=$dir,size=$filesize,prealloc,reuse,paralloc

define process name=rand-read,instances=1
{
  thread name=rand-thread,memsize=5m,instances=$nthreads
  {
    flowop read name=rand-read1,filename=largefile1,iosize=$iosize,random,workingset=$workingset,directio=$directio
  }
}

echo "Random Read Version 3.0 personality successfully loaded"
run 20
