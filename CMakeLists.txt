project(SKV)
cmake_minimum_required(VERSION 2.8)

set(VERSION_MAJOR 0)
set(VERSION_MINOR 1)
set(VERSION_PATCH 0)
set(VERSION_ABI 1)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/CMake
  ${CMAKE_SOURCE_DIR}/CMake/common)
include(GitExternal)
include(Common)
include(logtrace)

#------------------------------------------------------------------------------
# Build platform/major env
#------------------------------------------------------------------------------
set(SKV_ENV "BGAS" CACHE STRING
    "Select the main target platform {BGAS, BGQCNK, Power, x86}")
set_property(CACHE SKV_ENV
  PROPERTY STRINGS "BGAS" "BGQCNK" "Power" "x86")


#------------------------------------------------------------------------------
# MPI
#------------------------------------------------------------------------------
# Configurations for various pre-configured MPI installations
set(SKV_MPI "AUTO" CACHE STRING 
  "Select an MPI configuration {AUTO, MVAPICH2_RHEL65, MPICH_BGQ, BGAS-MVAPICH}")
set_property(CACHE SKV_MPI
  PROPERTY STRINGS "MVAPICH2_RHEL65" "MPICH_BGQ" "BGAS-MVAPICH")


#------------------------------------------------------------------------------
# pick up any site specific configuration that might differ from default
#------------------------------------------------------------------------------
set(SKV_SITE "default" CACHE STRING
  "Select the main target platform {default, CSCS, ykt}")
set_property(CACHE SKV_SITE
  PROPERTY STRINGS "default" "CSCS" "ykt")
include( Site/${SKV_SITE} )



# include the platform and mpi config files
include( Platform/${SKV_ENV} )

message("ENV setting is: ${SKV_ENV}; MPI setting is: ${SKV_MPI}")
if( ${SKV_MPI} MATCHES "AUTO" )
  find_package(MPI REQUIRED)
else( ${SKV_MPI} MATCHES "AUTO" )
  include( MPI/${SKV_MPI} )
endif( ${SKV_MPI} MATCHES "AUTO" )

message("MPI includes in: ${MPI_INCLUDE_PATH}")

include_directories(${MPI_INCLUDE_PATH})

#------------------------------------------------------------------------------
# OFED verbs libraries
#------------------------------------------------------------------------------
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/CMake)
find_package(OFED)

#------------------------------------------------------------------------------
# Options
#------------------------------------------------------------------------------

#############################################
# Define the local KV backend to use
# inmem    - in-memory version of local kv
# asyncmem - async in-memory version for testing
# rocksdb  - rocksdb backend (requires rocksdb build)
#############################################
set(SKV_LOCAL_KV_BACKEND "inmem" CACHE STRING 
  "Select a database backend {inmem, asyncmem, rocksdb}")
set_property(CACHE SKV_LOCAL_KV_BACKEND 
  PROPERTY STRINGS "inmem" "asyncmem" "rocksdb")

set(GLOBAL_DEFS
  ${PLATFORM_GLOBAL_DEFS}
  # use PK_LINUX to pull in several important includes and macros
  -DPK_LINUX
  # use MSPACES from dlmalloc.h
  -DONLY_MSPACES
 )

set(IT_API_DEFS
  # use event-based CQ reaping, if undefined it uses polling
  -DDIT_API_USE_EVENT_CQ
  # enable SIW workaround
  -DIT_API_USE_SIW_HACK
)

#------------------------------------------------------------------------------
# include dirs
#------------------------------------------------------------------------------
include_directories(
  ./include 
  it_api/include 
  lib/include 
  FxLogger 
)

#------------------------------------------------------------------------------
# Define source files
#------------------------------------------------------------------------------

set(FXLOGGER_SOURCES
  ./FxLogger/FxLogger.cpp
  ./FxLogger/Trace.cpp
  ./FxLogger/dlmalloc.c
)

set(IT_API_SOURCES
  ./it_api/src/it_api_o_verbs.cpp
  ./it_api/src/it_api_o_verbs_thread.cpp
)

set(SKV_CLIB_SOURCES
  ./lib/src/skv.cpp
)

set(SKV_CLIENT_SOURCES
  ./src/client/skv_client.cpp
  ./src/client/skv_client_2_server_active_bcast.cpp
  ./src/client/skv_client_bulk_inserter.cpp
  ./src/client/skv_client_command_manager_if.cpp
  ./src/client/skv_client_conn_manager_if.cpp
  ./src/client/skv_client_cursor.cpp
  ./src/client/skv_client_internal.cpp
)

set(SKV_COMMON_SOURCES
  ./src/common/skv_config.cpp
  ./src/common/skv_distribution_manager.cpp
  ./src/common/skv_utils.cpp
)

set(SERVER_SOURCES
  ./src/server/skv_local_kv_${SKV_LOCAL_KV_BACKEND}.cpp
  ./src/server/skv_server.cpp
  ./src/server/skv_server_IT_event_source.cpp
  ./src/server/skv_server_command_event_source.cpp
  ./src/server/skv_server_local_kv_event_source.cpp
  ./src/server/skv_server_main.cpp
  ./src/server/skv_server_network_event_manager.cpp
  ./src/server/skv_server_tree_based_container.cpp
  ./src/server/skv_server_uber_pds.cpp
)

set(TEST_SOURCES
  ./test/skv_base_test.cpp
)

set(TEST_C_SOURCES
  ./test/skv_test_clib.c
)

set(TEST_MPI_SOURCES
  ./test/skv_test_bulk.cpp
  ./test/skv_test_insert_retrieve_async.cpp
  ./test/skv_test_insert_retrieve_sync.cpp
  ./test/test_skv_insert_command.cpp
  ./test/test_skv_remove_command.cpp
)

set(UNITTEST_SOURCES
  ./unittest/test_skv_rdma_data_buffer.cpp
  ./unittest/test_skv_ringbuffer_ptr.cpp
)

#------------------------------------------------------------------------------
# Compile settings
#------------------------------------------------------------------------------

add_definitions(
# default config file
  -DDEFAULT_CONFIG_FILE="${CMAKE_CURRENT_SOURCE_DIR}/.skv_server.conf"
# enable command header checksum
  -DSKV_HEADER_CHECKSUM
# enable workaround for missing RoQ loopback functionality
  -DSKV_ROQ_LOOPBACK_WORKAROUND
# set the local kv backend for SKV
  -DSKV_SERVER_LOCAL_KV=skv_local_kv_${SKV_LOCAL_KV_BACKEND}
# data checking in tests
  -DDO_CHECK

# seems to be necessary for the tests to link
  -DSKV_CLIENT_UNI
  ${GLOBAL_DEFS}
  ${IT_API_DEFS}
)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -std=c99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

#------------------------------------------------------------------------------
# list of libraries
# fxlogger
# it_api
# common
# client
# client-mpi
# server
# c-lib
# c-lib-mpi

add_library(fxlogger STATIC ${FXLOGGER_SOURCES})
add_library(it_api STATIC ${IT_API_SOURCES})
add_library(skv_common STATIC ${SKV_COMMON_SOURCES})

add_library(skv_client STATIC ${SKV_CLIENT_SOURCES})
target_compile_definitions(skv_client PUBLIC
  "-DSKV_CLIENT_UNI -DSKV_NON_MPI"
)
add_library(skv_client_mpi STATIC ${SKV_CLIENT_SOURCES})

add_library(skvc STATIC ${SKV_CLIB_SOURCES})
target_compile_definitions(skvc PUBLIC
  "-DSKV_CLIENT_UNI -DSKV_NON_MPI"
)
add_library(skvc_mpi STATIC ${SKV_CLIB_SOURCES})


#------------------------------------------------------------------------------
# build the server
add_executable(SKVServer ${SERVER_SOURCES})
set(SKV_COMMON_LINK_LIBRARIES pthread m dl)
target_link_libraries(SKVServer
  skv_common it_api fxlogger ${OFED_LIBRARIES} ${MPI_LIBRARY} ${SKV_COMMON_LINK_LIBRARIES})


#------------------------------------------------------------------------------
# build the tests
foreach(_test ${TEST_MPI_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  target_link_libraries(${TEST_NAME}
    skv_client_mpi
    skv_common
    it_api
    fxlogger
    ${OFED_LIBRARIES}
    ${MPI_LIBRARY}
  )
endforeach()

foreach(_test ${TEST_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  target_compile_definitions(${TEST_NAME} PUBLIC
    "-DSKV_CLIENT_UNI -DSKV_NON_MPI"
  )
  target_link_libraries(${TEST_NAME}
    skv_client
    skv_common
    it_api
    fxlogger
    ${OFED_LIBRARIES}
#    ${MPI_LIBRARY}
  )
endforeach()

foreach(_test ${TEST_C_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  target_link_libraries(${TEST_NAME}
    skvc
    skv_client
    skv_common
    it_api
    fxlogger
    ${OFED_LIBRARIES}
    ${MPI_LIBRARY}
  )
endforeach()

foreach(_test ${UNITTEST_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  target_compile_definitions(${TEST_NAME} PUBLIC
    "-DSKV_CLIENT_UNI -DSKV_NON_MPI"
  )
  target_link_libraries(${TEST_NAME}
    skv_client
    skv_common
    it_api
    fxlogger
    ${OFED_LIBRARIES}
 #   ${MPI_LIBRARY}
  )
endforeach()


if( CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT )
  set (CMAKE_INSTALL_PREFIX "/opt/SKV" CACHE PATH "SKV default location" FORCE )
endif( CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT )

set( SKV_BINARIES SKVServer )
set( SKV_TESTS skv_base_test )
set( SKV_LIBS skvc skvc_mpi skv_common skv_client skv_client_mpi it_api fxlogger )

install(TARGETS ${SKV_BINARIES} ${SKV_TESTS} ${SKV_LIBS}
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)

set( SKV_INCLUDE_FILES
 FxLogger/FxLogger.hpp FxLogger/Trace.hpp
 it_api/include/it_api.h it_api/include/it_api_os_specific.h
 lib/include/skv.h include/client/skv_client.hpp
)

install(FILES ${SKV_INCLUDE_FILES}
  DESTINATION include
)

set( SKV_CONFIG_FILES
  system/bgq/etc/skv_server.conf
)

install(FILES ${SKV_CONFIG_FILES}
  DESTINATION etc
)
