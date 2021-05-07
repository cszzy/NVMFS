#! /bin/sh

mountdir="/mnt/pmem0/zzy/fs/mnt"
metadir="/mnt/pmem0/zzy/fs/meta"
datadir="/mnt/pmem0/zzy/fs/data"
pool="36a25cf7-2211-4962-8ec3-c171128bc081"
container="c3d176b0-78d2-4425-ab5d-a3cae8fb380a"

if [ -n "$metadir" ];then
    rm -f $metadir/*
fi

if [ -n "$datadir" ];then
    rm -f $datadir/*
fi

cmd="./rockfs_main -mount_dir $mountdir -meta_dir $metadir -data_dir $datadir -pool $pool -container $container"

echo $cmd
eval $cmd