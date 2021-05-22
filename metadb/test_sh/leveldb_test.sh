#! /bin/sh

db="/home/lzw/test/"

value_size="8"
benchmarks="dir_fillrandom" 
#benchmarks="dir_fillrandom,stats,dir_readrandom,stats,dir_deleterandom,stats" 
#benchmarks="inode_fillrandom,stats,inode_readrandom,stats,inode_deleterandom,stats"
#benchmarks="dir_rangewrite,stats,dir_rangeread,stats"

nums="1000000"  #
#nums="10000000"  #1千万，0.32G

reads="1000000"  #
deletes="1000000" #
updates="1000000" #
threads="1"

histogram="1"

#write_buffer_size=""
#max_file_size=""
#sync=""

const_params=""

function FILL_PARAMS() {
    if [ -n "$db" ];then
        const_params=$const_params"--db=$db "
    fi

    if [ -n "$value_size" ];then
        const_params=$const_params"--value_size=$value_size "
    fi

    if [ -n "$benchmarks" ];then
        const_params=$const_params"--benchmarks=$benchmarks "
    fi

    if [ -n "$nums" ];then
        const_params=$const_params"--nums=$nums "
    fi

    if [ -n "$reads" ];then
        const_params=$const_params"--reads=$reads "
    fi

    if [ -n "$deletes" ];then
        const_params=$const_params"--deletes=$deletes "
    fi

    if [ -n "$updates" ];then
        const_params=$const_params"--updates=$updates "
    fi

    if [ -n "$range_len" ];then
        const_params=$const_params"--range_len=$range_len "
    fi

    if [ -n "$threads" ];then
        const_params=$const_params"--threads=$threads "
    fi

    if [ -n "$histogram" ];then
        const_params=$const_params"--histogram=$histogram "
    fi

    if [ -n "$write_buffer_size" ];then
        const_params=$const_params"--write_buffer_size=$write_buffer_size "
    fi

    if [ -n "$max_file_size" ];then
        const_params=$const_params"--max_file_size=$max_file_size "
    fi

    if [ -n "$sync" ];then
        const_params=$const_params"--sync=$sync "
    fi
}


bench_file_path="$(dirname $PWD )/leveldb_bench"

if [ ! -f "${bench_file_path}" ];then
bench_file_path="$PWD/leveldb_bench"
fi

if [ ! -f "${bench_file_path}" ];then
echo "Error:${bench_file_path} or $(dirname $PWD )/leveldb_bench not find!"
exit 1
fi

FILL_PARAMS 

cmd="$bench_file_path $const_params "

if [ -n "$db" ];then
    rm -f $db/*
fi

if [ -n "$1" ];then    #后台运行
cmd="nohup $bench_file_path $const_params >>out.out 2>&1 &"
echo $cmd >out.out
fi

echo $cmd
eval $cmd
