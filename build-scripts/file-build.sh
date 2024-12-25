#!/usr/bin/env bash

############################  GLOBAL VARIABLES

g_stor="fs"
g_list=0
g_depend=0
g_target=""
g_release=0
g_build_rocksdb=0
g_build_opts=(
    "--define=with_glog=true"
    "--define=libunwind=true"
    "--copt -DHAVE_ZLIB=1"
    "--copt -DGFLAGS_NS=google"
    "--copt -DUSE_BTHREAD_MUTEX"
)

g_os="rocky9"

############################  BASIC FUNCTIONS
get_version() {
    #get tag version
    tag_version=`git status | grep -w "HEAD detached at" | awk '{print $NF}' | awk -F"v" '{print $2}'`

    # get git commit version
    commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
    if [ $g_release -eq 1 ]
    then
        debug="+release"
    else
        debug="+debug"
    fi
    if [ -z ${tag_version} ]
    then
        echo "not found version info"
        curve_version=${commit_id}${debug}
    else
        curve_version=${tag_version}+${commit_id}${debug}
    fi
    echo "version: ${curve_version}"
}

msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "\33[32m[✔]\33[0m ${1}${2}"
}

die() {
    msg "\33[31m[✘]\33[0m ${1}${2}"
    exit 1
}

print_title() {
    local delimiter=$(printf '=%.0s' {1..20})
    msg "$delimiter [$1] $delimiter\n"
}

############################ FUNCTIONS
usage () {
    cat << _EOC_
Usage:
    file-build.sh --list
    file-build.sh --only=target
Examples:
    file-build.sh --only=//src/metaserver:metaserver
    file-build.sh --only=src/*
    file-build.sh --only=test/*
    file-build.sh --only=test/metaserver
_EOC_
}

get_options() {
    local args=`getopt -o ldorh --long stor:,list,dep:,only:,os:,release:,build_rocksdb: -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -s|--stor)
                g_stor=$2
                shift 2
                ;;
            -l|--list)
                g_list=1
                shift 1
                ;;
            -d|--dep)
                g_depend=$2
                shift 2
                ;;
            -o|--only)
                g_target=$2
                shift 2
                ;;
            -r|--release)
                g_release=$2
                shift 2
                ;;
            --os)
                g_os=$2
                shift 2
                ;;
            --build_rocksdb)
                g_build_rocksdb=$2
                shift 2
                ;;
            -h)
                usage
                exit 1
                ;;
            --)
                shift
                break
                ;;
            *)
                exit 1
                ;;
        esac
    done
}

build_target() {
    (rm -rf build && mkdir build && cd build && cmake .. && make -j 32)

    if [ $? -eq 0 ]; then
        success "build dingofs success\n"
    else
        die "build dingofs failed\n"
    fi
    
    # build tools-v2
    g_toolsv2_root="tools-v2"
    if [ $g_release -eq 1 ]
    then
        (cd ${g_toolsv2_root} && make build version=${curve_version})
    else
        (cd ${g_toolsv2_root} && make debug version=${curve_version})
    fi
    if [ $? -eq 0 ]; then
        success "build tools-v2 success\n"
    else
        die "build tools-v2 failed\n"
    fi
}


build_requirements() {
    git submodule sync && git submodule update --init --recursive
    if [ $? -ne 0 ]; then
        echo "Error: Failed to update git submodules"
        exit 1
    fi
	# (cd third-party && rm -rf build installed && cmake -S . -B build && cmake --build build -j 16)
	(cd dingo-eureka && rm -rf build && mkdir build && cd build && cmake .. && make -j 32)
    g_etcdclient_root="thirdparties/etcdclient"
    (cd ${g_etcdclient_root} && make clean && make all)
}

main() {
    echo "file build args: $@"
    get_options "$@"
    get_version

    if [[ "$g_target" == "" && "$g_depend" -ne 1 ]]; then
        die "must not disable both only option or dep option\n"
    else
        if [ "$g_depend" -eq 1 ]; then
            build_requirements
        fi
        if [ -n "$g_target" ]; then
            build_target
        fi
    fi
}

############################  MAIN()
main "$@"
