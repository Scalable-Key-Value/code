project(SKV)

#--------------------------------------------------
# cmake old/new compatibility
#--------------------------------------------------
cmake_minimum_required(VERSION 2.8)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 OLD)
  cmake_policy(SET CMP0017 NEW)
  cmake_policy(SET CMP0005 NEW)
endif(COMMAND cmake_policy)

#--------------------------------------------------
# Setup Output directories
#--------------------------------------------------
SET (CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
)
SET (CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Libraries"
)
SET (CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all static libraries."
)

#------------------------------------------------------------------------------
# Machine check, see if we can tell if we are we running on BGQ or BGAS
#------------------------------------------------------------------------------
SITE_NAME(hostname)
set(BGQ   0)
set(BGAS  0)
set(BGVIZ 0)
if(hostname MATCHES bbpbg1)
  set(BGQ 1)
  message("Running on BGQ")
elseif(hostname MATCHES bbpbg2)
  set(BGAS 1)
  message("Running on BGAS")
elseif(hostname MATCHES bbpviz1)
  set(BGVIZ 1)
  message("Running on VIZ")
endif()
message("CMAKE_SYSTEM is ${CMAKE_SYSTEM} with hostname ${hostname} and processor ${CMAKE_SYSTEM_PROCESSOR}" )

#------------------------------------------------------------------------------
# MPI
#------------------------------------------------------------------------------
if (BGQ)
  include (${CMAKE_CURRENT_SOURCE_DIR}/CMake//BGAS-MPI.cmake)
endif()
if (BGAS)
  include (${CMAKE_CURRENT_SOURCE_DIR}/CMake//BGAS-MVAPICH.cmake)
  include_directories(${CMAKE_CURRENT_SOURCE_DIR}/mpi)
endif()
if(BGVIZ)
  find_package(MPI REQUIRED)
endif()

include_directories(${MPI_INCLUDE_PATH})

#------------------------------------------------------------------------------
# OFED verbs libraries
#------------------------------------------------------------------------------
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/CMake)
find_package(OFED)

#------------------------------------------------------------------------------
# Define source files
#------------------------------------------------------------------------------
set(FXLOGGER_SRCS
  ./FxLogger/FxLogger.cpp
  ./FxLogger/Trace.cpp
  ./FxLogger/dlmalloc.c
)

set(it_api_SRCS
  ./it_api/make.conf.in
  ./it_api/make.conf_dev
  ./it_api/make.conf_rpm
  ./it_api/src/it_api_o_verbs.cpp
  ./it_api/src/it_api_o_verbs_thread.cpp
)

set(C_SRCS
  ./lib/src/skv.cpp
)

#  ./make.conf.in
#  ./make.conf_dev
#  ./make.conf_rpm

set(CLIENT_SRCS
  ./src/client/skv_client.cpp
  ./src/client/skv_client_2_server_active_bcast.cpp
  ./src/client/skv_client_bulk_inserter.cpp
  ./src/client/skv_client_command_manager_if.cpp
  ./src/client/skv_client_conn_manager_if.cpp
  ./src/client/skv_client_cursor.cpp
  ./src/client/skv_client_internal.cpp
)

set(COMMON_SRCS
  ./src/common/skv_config.cpp
  ./src/common/skv_distribution_manager.cpp
  ./src/common/skv_utils.cpp
)

set(SERVER_SRCS
  ./src/server/skv_local_kv_asyncmem.cpp
  ./src/server/skv_local_kv_inmem.cpp
  ./src/server/skv_local_kv_rocksdb.cpp
  ./src/server/skv_server.cpp
  ./src/server/skv_server_IT_event_source.cpp
  ./src/server/skv_server_command_event_source.cpp
  ./src/server/skv_server_local_kv_event_source.cpp
  ./src/server/skv_server_main.cpp
  ./src/server/skv_server_network_event_manager.cpp
  ./src/server/skv_server_tree_based_container.cpp
  ./src/server/skv_server_uber_pds.cpp
  ./system/bgq/etc/skv_server.conf
)

set(TEST_SRCS
  ./test/skv_base_test.cpp
  ./test/skv_test_bulk.cpp
  ./test/skv_test_clib.c
  ./test/skv_test_insert_retrieve_async.cpp
  ./test/skv_test_insert_retrieve_sync.cpp
  ./test/test_skv_insert_command.cpp
  ./test/test_skv_remove_command.cpp
  ./unittest/test_skv_rdma_data_buffer.cpp
  ./unittest/test_skv_ringbuffer_ptr.cpp
)

#------------------------------------------------------------------------------
# Compile settings 
#------------------------------------------------------------------------------

#############################################
# Define the local KV backend to use
# inmem    - in-memory version of local kv
# asyncmem - async in-memory version for testing
# rocksdb  - rocksdb backend (tbd)
set(SKV_LOCAL_KV_BACKEND inmem)

include_directories( 
  include
  it_api/include
  lib/include
  FxLogger
  FxLogger/Pk
  profiler
)

#SKV_GLOBAL_LDFLAGS=${SKV_CONFIG} ${SKV_CCFLAGS} ${FXLOGGER_DIR}/libPkLinux.a
#${SKV_TOP}/build/libit_api_o_verbs.a -L${MPI_DIR}/lib ${LOCAL_KV_LIB_OPTS}
#${SKV_GLOBAL_LDFLAGS_EXTENSION} -lrt

add_Definitions(
# default config file
  -DDEFAULT_CONFIG_FILE="${CMAKE_CURRENT_SOURCE_DIR}/.skv_server.conf"
# use PK_LINUX to pull in several important includes and macros
  -DPK_LINUX
# use MSPACES from dlmalloc.h
  -DONLY_MSPACES
# enable logging in general
  -DPKFXLOG
# enable command header checksum
  -DSKV_HEADER_CHECKSUM
# enable workaround for missing RoQ loopback functionality
  -DSKV_ROQ_LOOPBACK_WORKAROUND
# set the local kv backend for SKV
  -DSKV_SERVER_LOCAL_KV=skv_local_kv_${SKV_LOCAL_KV_BACKEND}
# logging in tests
  -DDO_CHECK

# IT_API_CONFIG+=-DPK_LINUX

# use event-based CQ reaping, if undefined it uses polling
  -DDIT_API_USE_EVENT_CQ

# ifdef SKV_FOR_NONBGQ
  -DIT_API_COMM_DEVICE=\\"lo\\"

# enable SIW workaround for ... and set the device name to use for
# communication
  -DIT_API_USE_SIW_HACK
)

add_library(fxlogger ${FXLOGGER_SRCS})

add_library(it_api ${it_api_SRCS})

add_library(skv_common ${COMMON_SRCS})

target_link_libraries(skv_common pthread)

add_executable(skv_server
  ${SERVER_SRCS}
)

target_link_libraries(skv_server
  skv_common 
  it_api
  fxlogger
  ${OFED_LIBRARIES}
  ${MPI_LIBRARY}
)
