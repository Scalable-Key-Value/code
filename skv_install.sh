#!/bin/sh
########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
########################################
#
# Contributors:
#     lschneid - initial implementation

BASE_DIR=$(dirname $(readlink -f $0))
INST_EXEC="install -D -v"

usage() {
    cat <<-EOF

Usage: `basename $0` [-option [argument]]
    -h             print help
    -c             cleanup the skv_release dir
    -b             create release dir an build the software
    -i             run the "manual" install

EOF
}

INSTALL_DIR=""

BIN_FILE_LIST="\
 build/SKVServer \
 build/skv_base_test \
 build/test_skv_insert_command \
 build/test_skv_remove_command \
 build/skv_test_insert_retrieve_async \
 build/skv_test_insert_retrieve_sync \
 "

CONF_FILE_LIST="\
 system/bgq/etc/skv_server.conf \
 "

LIB_FILE_LIST="\
 FxLogger/libPkLinux.a \
 build/libit_api_o_verbs.a \
 build/libskv_client.a \
 build/libskv_client_mpi.a \
 build/libskv_common.a \
 build/libskvc.a \
 build/libskvc_mpi.a \
 "

HEADER_FILE_LIST="\
 FxLogger/Pk/FxLogger.hpp \
 FxLogger/Pk/Trace.hpp \
 it_api/include/it_api.h \
 it_api/include/it_api_os_specific.h \
 include/client/skv_client.hpp \
 lib/include/skv.h \
 "

skv_install_files() {
    local RC=0
    echo "Installing..."

    # binaries
    if [ ! -d ${INSTALL_DIR}/bin ]; then
        mkdir -p ${INSTALL_DIR}/bin
        RC=$[ $RC + $? ]
    fi
    for file in `echo ${BIN_FILE_LIST}`; do
        DEST=`echo $file | sed s/"^.*"//`
        ${INST_EXEC} $file ${INSTALL_DIR}/bin/$DEST
        RC=$[ $RC + $? ]
    done

    # libraries
    if [ ! -d ${INSTALL_DIR}/lib ]; then
        mkdir -p ${INSTALL_DIR}/lib
        RC=$[ $RC + $? ]
    fi
    for file in `echo ${LIB_FILE_LIST}`; do
        DEST=`echo $file | sed s/"^.*"//`
        ${INST_EXEC} -m 644 $file ${INSTALL_DIR}/lib/$DEST
        RC=$[ $RC + $? ]
    done

    # header files
    if [ ! -d ${INSTALL_DIR}/include ]; then
        mkdir -p ${INSTALL_DIR}/include
        RC=$[ $RC + $? ]
    fi
    for file in `echo ${HEADER_FILE_LIST}`; do
        DEST=`echo $file | sed s/"^.*"//`
        ${INST_EXEC} -m 644 $file ${INSTALL_DIR}/include/$DEST
        RC=$[ $RC + $? ]
    done

    # config files
    if [ ! -d ${INSTALL_DIR}/etc ]; then
        mkdir -p ${INSTALL_DIR}/etc
        RC=$[ $RC + $? ]
    fi
    for file in `echo ${CONF_FILE_LIST}`; do
        DEST=`echo $file | sed s/"^.*"//`
        ${INST_EXEC} -m 644 $file ${INSTALL_DIR}/etc/$DEST
        RC=$[ $RC + $? ]
    done

    return $RC
}

skv_check_build_env() {
    echo "CHECKING ENVIRONMENT"
    if [ -z ${MPI_DIR} ]; then
        echo "FAILED: $MPI_DIR not defined. Please point me to the base dir of your MPI installation. export MPI_DIR=??? "
        return 1
    fi
    return 0
}


skv_run_make() {
    make -C FxLogger ${MAKE_OPTION}
    if [ $? -ne 0 ]; then
        echo "FAILED: Building FxLogger"
        return 1
    fi

    make ${MAKE_OPTION}
    if [ $? -ne 0 ]; then
        echo "FAILED: Building SKV"
        return 1
    fi
    return 0
}


while getopts hbci: opt
do
    case $opt in
        c) # clean
            MAKE_OPTION="clean"
            ;;
        b) #build
            MAKE_OPTION=""
            ;;
        i) # install
            MAKE_OPTION=""
            INSTALL_DIR=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done


if [ -z ${MAKE_OPTION} ]; then
    skv_check_build_env
fi
if [ $? -ne 0 ]; then
    exit 1
fi


skv_run_make

if [ $? -ne 0 ]; then
    exit $?
fi


if [ ! -z ${INSTALL_DIR} ]; then
    skv_install_files
    if [ $? -ne 0 ]; then
        echo "FAILED: Installing files to ${INSTALL_DIR}"
    fi
fi
