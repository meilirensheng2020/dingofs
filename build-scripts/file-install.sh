#!/usr/bin/env bash

############################  GLOBAL VARIABLES
g_stor="fs"
g_prefix=""
g_only=""
g_project_name=""
g_etcd_version="v3.4.10"
g_util_dir="$(dirname $(realpath $0))" # /path/to/project/build-scripts
g_dingo_dir="$(dirname $g_util_dir)" # /path/to/project
g_build_dir="$g_dingo_dir/build/bin" # /path/to/project/build/bin
g_deploy_script_dir="$g_dingo_dir/deploy-scripts" # /path/to/project/deploy-scripts
g_build_release=0
tools_v2_dingo_file="https://github.com/dingodb/dingofs-tools/releases/download/main/dingo"

g_color_yellow=`printf '\033[33m'`
g_color_red=`printf '\033[31m'`
g_color_orange_red=`printf '\033[38;5;202m'`
g_color_normal=`printf '\033[0m'`

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "$g_color_yellow[✔]$g_color_normal [$g_project_name] ${1}${2}"
}

die() {
    msg "$g_color_red[✘]$g_color_normal [$g_project_name] ${1}${2}"
    rm -rf $g_prefix
    exit 1
}

warn() {
    msg "$g_color_orange_red[!]$g_color_normal [$g_project_name] ${1}${2}"
}


############################ FUNCTIONS
usage () {
    cat << _EOC_
Usage:
    install.sh --stor=fs --prefix=PREFIX --only=TARGET

Examples:
    install.sh --stor=fs --prefix=/usr/local/dingofs --only=metaserver
_EOC_
}

get_options() {
    local long_opts="stor:,prefix:,only:,etcd_version:,help"
    local args=`getopt -o povh --long $long_opts -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -p|--prefix)
                g_prefix=$2 # /path/to/dingofs/docker/rocky9/dingofs
                shift 2
                ;;
            -o|--only)
                g_only=$2 # etcd, monitor, or dingofs
                shift 2
                ;;
            -v|--etcd_version)
                g_etcd_version=$2
                shift 2
                ;;
            -h|--help)
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

get_build_mode() {
    grep "release" .BUILD_MODE > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        g_build_release=1
    fi
}

strip_debug_info() {
    if [ $g_build_release -eq 1 ]; then
        # binary file generated by bazel isn't writable by default
        chmod +w $1
        objcopy --only-keep-debug $1 $1.dbg-sym
        objcopy --strip-debug $1
        objcopy --add-gnu-debuglink=$1.dbg-sym $1
        chmod -w $1
    fi
}

create_project_dir() {
    mkdir -p $1/{conf,logs,sbin,lib,data}
    if [ $? -eq 0 ]; then
        success "create project directory $1 success\n"
    else
        die "create directory $1 failed\n"
    fi
}

copy_file() {
    cp -rf "$1" "$2"
    if [ $? -eq 0 ]; then
        success "copy file $1 to $2 success\n"
    else
        die "copy file $1 to $2 failed\n"
    fi
}

copy_dir() {
    cp -rf "$1"/* "$2"
    if [ $? -eq 0 ]; then
        success "copy directory $1 to $2 success\n"
    else
        die "copy directory $1 to $2 failed\n"
    fi
}


get_targets() {
    #if [ "$g_stor" == "fs" ]; then
    #    bazel query 'kind("cc_binary", //dingofs/src/...)' | grep -E "$g_only" // todo
    #fi
    local build_bin_dir="build/bin"
    
    # Check if build/bin exists
    if [ ! -d "$build_bin_dir" ]; then
        die "Directory $build_bin_dir does not exist. Please run cmake build first.\n"
    fi

    # Find all executable files in build/bin directory
    find "$build_bin_dir" -type f -executable
}

gen_servicectl() {
    local src="$g_util_dir/servicectl.sh"
    local dst="$4/servicectl"
    sed -e "s|__PROJECT__|$1|g" \
        -e "s|__BIN_FILENAME__|$2|g" \
        -e "s|__START_ARGS__|$3|g" \
        $src > $dst \
    && chmod a+x $dst

    if [ $? -eq 0 ]; then
        success "generate servicectl success\n"
    else
        die "generate servicectl failed\n"
    fi
}

install_dingofs() {
    for target in `get_targets`
    do
        local regex_target="build/bin/(dingo-([^/]+))"
        if [[ ! $target =~ $regex_target ]]; then
            warn "unknown target: $target\n"
            continue
        fi
        # echo "Full match: ${BASH_REMATCH[1]}, Extracted: ${BASH_REMATCH[2]}"
        local project_bin_filename=${BASH_REMATCH[1]}  # ex: dingo_metaserver
        local project_name="${BASH_REMATCH[2]}"  # ex: metaserver
        case "$project_name" in
            "fuse")
                g_project_name="client"
                ;;
            "tool")
                g_project_name="tools"
                ;;
            *)
                g_project_name=$project_name
                ;;
        esac
        local project_prefix="$g_prefix/$g_project_name"  # ex: /path/to/project/dingofs/docker/rocky9/dingofs/metaserver
        
        local binary="$g_build_dir/$project_bin_filename"

        # The below actions:
        #   1) create project directory
        #   2) copy binary to project directory
        #   3) generate servicectl script into project directory.
        create_project_dir $project_prefix
        copy_file "$binary" "$project_prefix/sbin"
        strip_debug_info "$project_prefix/sbin/$project_bin_filename"
        # gen_servicectl \
        #     $project_name \
        #     $project_bin_filename \
        #     '--confPath=$g_conf_file' \
        #     "$project_prefix/sbin"

        # copy the binary file from build/bin directory
        mkdir -p "$g_prefix/build/bin"
        copy_file "$binary" "$g_prefix/build/bin"

        success "install $project_name success\n"
    done
}

install_playground() {
    for role in {"etcd","mds","chunkserver"}; do
        for ((i=0;i<3;i++)); do
            mkdir -p "${g_prefix}"/playground/"${role}""${i}"/{conf,data,logs}
        done
    done
}

download_etcd() {
    local now=`date +"%s%6N"`
    local download_url="https://storage.googleapis.com/etcd"
    local src="${download_url}/${g_etcd_version}/etcd-${g_etcd_version}-linux-amd64.tar.gz"
    local tmpfile="/tmp/$now-etcd-${g_etcd_version}-linux-amd64.tar.gz"
    local dst="$1"

    msg "download etcd: $src to $dst\n"

    # download etcd tarball and decompress to dst directory
    mkdir -p $dst &&
        curl -L $src -o $tmpfile &&
        tar -zxvf $tmpfile -C $dst --strip-components=1 >/dev/null 2>&1

    local ret=$?
    rm -rf $tmpfile
    if [ $ret -ne 0 ]; then
        die "download etcd-$g_etcd_version failed\n"
    else
        success "download etcd-$g_etcd_version success\n"
    fi
}

install_etcd() {
    local project_name="etcd"
    g_project_name=$project_name

    # The below actions:
    #   1) download etcd tarball from github
    #   2) create project directory
    #   3) copy binary to project directory
    #   4) generate servicectl script into project directory.
    local now=`date +"%s%6N"`
    local dst="/tmp/$now/etcd-$g_etcd_version"
    download_etcd $dst
    local project_prefix="$g_prefix/etcd"
    create_project_dir $project_prefix
    copy_file "$dst/etcd" "$project_prefix/sbin"
    copy_file "$dst/etcdctl" "$project_prefix/sbin"
    # gen_servicectl \
    #     $project_name \
    #     "etcd" \
    #     '--config-file=$g_project_prefix/conf/etcd.conf' \
    #     "$project_prefix/sbin"

    rm -rf "$dst"
    success "install $project_name success\n"
}

install_monitor() {
    local project_name="monitor"
    g_project_name=$project_name

    local project_prefix="$g_prefix/monitor"
    local dst="monitor"
    copy_file $dst $g_prefix
    success "install $project_name success\n"
}

install_tools-v2() {
    local project_name="dingofs"
    g_project_name=$project_name
    project_prefix="$g_prefix/tools-v2"
    mkdir -p $project_prefix/sbin
    mkdir -p $project_prefix/conf
    wget -O "$project_prefix/sbin/dingo" $tools_v2_dingo_file
    chmod +x "$project_prefix/sbin/dingo"
    copy_file "conf/dingo.yaml" "$g_prefix/conf"
}

install_scripts() {
    local script_prefix="$g_prefix/scripts"
    mkdir -p $script_prefix
    copy_dir "$g_deploy_script_dir" "$script_prefix"
    chmod +x "$script_prefix"/*.sh
    success "install scripts success\n"
}

main() {
    get_options "$@"
    get_build_mode

    if [ "$g_only" == "etcd" ]; then
        install_etcd
    elif [ "$g_only" == "monitor" ]; then
        install_monitor
    else
        install_dingofs
        install_tools-v2
        install_scripts
    fi
}

############################  MAIN()
main "$@"
