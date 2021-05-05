#! /bin/sh

values_array=(128 256 512 1024 4096)
test_all_size=40960000000   #40G

db="/pmem0/test/"

value_size="16"
#benchmarks="dir_fillrandom,stats" 
#benchmarks="dir_fillrandom,stats,dir_readrandom,stats,dir_deleterandom,stats" 
benchmarks="inode_fillrandom,stats,inode_readrandom,stats,inode_deleterandom,stats"
#benchmarks="dir_rangewrite,stats,dir_rangeread,stats"

nums="100000000"  #
#nums="100000000"  #

reads="10000000"  #
deletes="10000000" #
updates="1000000" #
threads="1"

histogram="1"

#write_buffer_size=""
#max_file_size=""
#sync=""


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

CLEAN_DB() {
    if [ -n "$db" ];then
        rm -f $db/*
    fi
}

RUN_ONE_TEST() {
    const_params=""
    FILL_PARAMS
    cmd="$bench_file_path $const_params >>out_values_$value_size.out 2>&1"
    if [ "$1" == "numa" ];then
        cmd="numactl -N 1 $bench_file_path $const_params >>out_values_$value_size.out 2>&1"
    fi

    echo $cmd >out_values_$value_size.out
    echo $cmd
    eval $cmd

    if [ $? -ne 0 ];then
        exit 1
    fi
}

RUN_ALL_TEST() {
    for temp in ${values_array[@]}; do
        value_size="$temp"
        nums="`expr $test_all_size / $value_size`"

        CLEAN_DB
        RUN_ONE_TEST $1
        if [ $? -ne 0 ];then
            exit 1
        fi
        sleep 5
    done
}

RUN_ALL_TEST $1


