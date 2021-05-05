#! /bin/sh

mountdir="/mnt/pmem0/zzy/fs/mnt"
metadir="/mnt/pmem0/zzy/fs/meta"
datadir="/mnt/pmem0/zzy/fs/data"
pool="8ac46e8b-59f1-4f9d-829d-51de0eabe61e"
container="72ad41b1-abe4-4e39-9e87-30e7aa5a6c17"

if [ -n "$metadir" ];then
    rm -f $metadir/*
fi

if [ -n "$datadir" ];then
    rm -f $datadir/*
fi

cmd="./nsfs_main -mount_dir $mountdir -meta_dir $metadir -data_dir $datadir -pool $pool -container $container"

echo $cmd
eval $cmd
