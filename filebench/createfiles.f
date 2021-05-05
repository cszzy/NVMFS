#
# 创建 $nfiles 个文件，每个文件写$meanfilesize ，每次写 $iosize 大小，创建完 $nfiles ，结束
#
# meanfilesize=256, 1k, 4k, 16k, 64k
#

set $dir=/mnt/pmem0/zzy/fs/mnt
set $nfiles=100000
set $meandirwidth=10
set $meanfilesize=1k
set $iosize=1k
set $nthreads=1

set mode quit firstdone

define fileset name=bigfileset,path=$dir,size=$meanfilesize,entries=$nfiles,dirwidth=$meandirwidth

define process name=filecreate,instances=1
{
  thread name=filecreatethread,memsize=10m,instances=$nthreads
  {
    flowop createfile name=createfile1,filesetname=bigfileset,fd=1
    flowop writewholefile name=writefile1,fd=1,iosize=$iosize,dsync
    flowop closefile name=closefile1,fd=1
  }
}

echo  "Createfiles Version 3.0 personality successfully loaded"
run 20
