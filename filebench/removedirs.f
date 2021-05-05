#
# Creates a fileset with $ndirs empty leaf directories then rmdir's all of them
#
# depth = 3, 5, 7, 9,
# meandirwidth = 100, 16, 7, 5   ($nfiles=100 0000)
# meandirwidth = 46, 10, 5, 3   ($nfiles=10 0000)
#

set $dir=/mnt/pmem0/zzy/fs/mnt
set $ndirs=100000
set $meandirwidth=46
set $nthreads=1

set mode quit firstdone

define fileset name=bigfileset,path=$dir,size=0,leafdirs=$ndirs,dirwidth=$meandirwidth,prealloc

define process name=remdir,instances=1
{
  thread name=removedirectory,memsize=1m,instances=$nthreads
  {
    flowop removedir name=dirremover,filesetname=bigfileset
  }
}

echo  "RemoveDir Version 1.0 personality successfully loaded"
run 20