#! /bin/sh

mountdir="/mnt/pmem0/zzy/fs/mnt"

umount $mountdir
killall metafs_main