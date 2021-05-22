
#
# iosize=256, 1k, 4k, 16k, 64k
#

set $dir=/mnt/pmem0/zzy/fs/mnt
set $nfiles=50000
set $meandirwidth=10
set $filesize=64k
set $iosize=64k
set $nthreads=1

define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$meandirwidth,prealloc=80

define process name=filewrite,instances=1
{
  thread name=filewriterthread,memsize=10m,instances=$nthreads
  {
    flowop openfile name=openfile1,filesetname=bigfileset,fd=1
    flowop writewholefile name=writefile1,fd=1,iosize=$iosize,dsync
    flowop closefile name=closefile1,fd=1
  }
}

echo  "filewrite Version 1.0 personality successfully loaded"
run 10