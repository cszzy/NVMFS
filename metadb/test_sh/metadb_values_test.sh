#! /bin/sh

values_array=(128 256 512 1024 4096)
test_all_size=40960000000   #40G
#db="/home/lzw/ceshi"

value_size="16"
#benchmarks="dir_fillrandom,stats" 
#benchmarks="dir_fillrandom,stats,dir_readrandom,stats,dir_deleterandom,stats" 
benchmarks="inode_fillrandom,stats,inode_readrandom,stats,inode_deleterandom,stats"
#benchmarks="dir_rangewrite,stats,dir_rangeread,stats"

k_node_allocator_path="/pmem0/test/node.pool"
k_file_allocator_path="/pmem0/test/file.pool"

#nums="1000"  #
nums="100000000"  #

reads="10000000"  #
deletes="10000000" #
updates="1000000" #
threads="1"

histogram="1"

#k_thread_pool_count="4"


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

    if [ -n "$k_DIR_FIRST_HASH_MAX_CAPACITY" ];then
        const_params=$const_params"--k_DIR_FIRST_HASH_MAX_CAPACITY=$k_DIR_FIRST_HASH_MAX_CAPACITY "
    fi

    if [ -n "$k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM" ];then
        const_params=$const_params"--k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM=$k_DIR_LINKNODE_TRAN_SECOND_HASH_NUM "
    fi

    if [ -n "$k_DIR_SECOND_HASH_INIT_SIZE" ];then
        const_params=$const_params"--k_DIR_SECOND_HASH_INIT_SIZE=$k_DIR_SECOND_HASH_INIT_SIZE "
    fi

    if [ -n "$k_DIR_SECOND_HASH_TRIG_REHASH_TIMES" ];then
        const_params=$const_params"--k_DIR_SECOND_HASH_TRIG_REHASH_TIMES=$k_DIR_SECOND_HASH_TRIG_REHASH_TIMES "
    fi

    if [ -n "$k_INODE_MAX_ZONE_NUM" ];then
        const_params=$const_params"--k_INODE_MAX_ZONE_NUM=$k_INODE_MAX_ZONE_NUM "
    fi

    if [ -n "$k_INODE_HASHTABLE_INIT_SIZE" ];then
        const_params=$const_params"--k_INODE_HASHTABLE_INIT_SIZE=$k_INODE_HASHTABLE_INIT_SIZE "
    fi

    if [ -n "$k_node_allocator_path" ];then
        const_params=$const_params"--k_node_allocator_path=$k_node_allocator_path "
    fi

    if [ -n "$k_node_allocator_size" ];then
        const_params=$const_params"--k_node_allocator_size=$k_node_allocator_size "
    fi

    if [ -n "$k_file_allocator_path" ];then
        const_params=$const_params"--k_file_allocator_path=$k_file_allocator_path "
    fi

    if [ -n "$k_file_allocator_size" ];then
        const_params=$const_params"--k_file_allocator_size=$k_file_allocator_size "
    fi

    if [ -n "$k_thread_pool_count" ];then
        const_params=$const_params"--k_thread_pool_count=$k_thread_pool_count "
    fi

}


bench_file_path="$(dirname $PWD )/db_bench"

if [ ! -f "${bench_file_path}" ];then
bench_file_path="$PWD/db_bench"
fi

if [ ! -f "${bench_file_path}" ];then
echo "Error:${bench_file_path} or $(dirname $PWD )/db_bench not find!"
exit 1
fi

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

        RUN_ONE_TEST $1
        if [ $? -ne 0 ];then
            exit 1
        fi
        sleep 5
    done
}

RUN_ALL_TEST $1




