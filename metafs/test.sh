#! /bin/sh

mountdir="/mnt/pmem0/zzy/fs/mnt"
metadir="/mnt/pmem0/zzy/fs/meta"
datadir="/mnt/pmem0/zzy/fs/data"
pool="2ec422c7-012c-40bf-b10f-540fc3840079"
container="a1cb222a-3b1c-4cdf-887c-5c2af25960e7"

if [ -n "$metadir" ];then
    rm -f $metadir/*
fi

if [ -n "$datadir" ];then
    rm -f $datadir/*
fi

cmd="./metafs_main -mount_dir $mountdir -meta_dir $metadir -data_dir $datadir -pool $pool -container $container"

echo $cmd
eval $cmd
