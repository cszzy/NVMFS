#
# Creates a fileset with $nfiles empty files, then proceeds to open each one
# and then close it.
#
# depth = 3, 5, 7, 9
# meandirwidth = 100, 16, 7, 5   ($nfiles=100 0000)
# meandirwidth = 46, 10, 5, 3   ($nfiles=10 0000)
#先创建好文件，然后open ， close

set $dir=/mnt/pmem0/zzy/fs/mnt
set $nfiles=1000000
set $meandirwidth=100
set $nthreads=1

define fileset name=bigfileset,path=$dir,size=0,entries=$nfiles,dirwidth=$meandirwidth,prealloc

define process name=fileopen,instances=1
{
  thread name=fileopener,memsize=1m,instances=$nthreads
  {
    flowop openfile name=open1,filesetname=bigfileset,fd=1
    flowop closefile name=close1,fd=1
  }
}

echo  "Openfiles Version 1.0 personality successfully loaded"
run 20