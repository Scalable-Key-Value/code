project(skv)
cmake_minimum_required(VERSION 2.8)

set(VERSION_MAJOR 0)
set(VERSION_MINOR 1)
set(VERSION_PATCH 0)
set(VERSION_ABI 1)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/CMake
  ${CMAKE_SOURCE_DIR}/CMake/common)
include(GitExternal)
include(Common)

# pick up any site specific configuration that might differ from default
set(SKV_SITE "default" CACHE STRING
  "Select the main target platform {default, CSCS, ykt}")
set_property(CACHE SKV_SITE PROPERTY STRINGS "default" "CSCS" "ykt" "pers_example")
include(Site/${SKV_SITE})

# do default magic
if(NOT SKV_ENV)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc")
    set(SKV_ENV "BGAS")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "x86")
    set(SKV_ENV "x86")
  else()
    set(SKV_ENV ${CMAKE_SYSTEM_PROCESSOR})
  endif()
endif()

if(NOT SKV_MPI)
  set(SKV_MPI "AUTO")
endif()

# store results
set(SKV_ENV ${SKV_ENV} CACHE STRING
    "Select the main target platform {BGAS, BGQCNK, Power, x86}")
set_property(CACHE SKV_ENV
  PROPERTY STRINGS "BGAS" "BGQCNK" "Power" "x86")

set(SKV_MPI ${SKV_MPI} CACHE STRING
  "MPI configuration {AUTO, MVAPICH2_RHEL65, MPICH_BGQ, BGAS-MVAPICH}")
set_property(CACHE SKV_MPI
  PROPERTY STRINGS "AUTO" "MVAPICH2_RHEL65" "MPICH_BGQ" "BGAS-MVAPICH")

include(Platform/${SKV_ENV})
message(STATUS "Environment ${SKV_ENV}; MPI ${SKV_MPI} (mpicc: ${MPI_C_COMPILER})")

if(SKV_MPI MATCHES "AUTO")
  find_package(MPI REQUIRED)
else()
  include(MPI/${SKV_MPI})
endif()

#------------------------------------------------------------------------------
# Options
#------------------------------------------------------------------------------

set(COMMON_LIBRARY_TYPE STATIC SHARED)

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

if(SKV_LOCAL_KV_BACKEND MATCHES "rocksdb")
  find_package(ZLIB REQUIRED)
  find_package(BZip2 REQUIRED)
  find_package(rocksdb REQUIRED)
endif()


set(SKV_COMM_API_TYPE "verbs" CACHE STRING
  "Select a database backend {verbs, sockets}")
set_property(CACHE SKV_COMM_API_TYPE
  PROPERTY STRINGS "verbs" "sockets")

if(SKV_COMM_API_TYPE MATCHES "verbs")
  find_package(OFED REQUIRED)
  set( IT_API_LIBS ${OFED_LIBRARIES} pthread rt )
  add_definitions(
    # enable workaround for missing RoQ loopback functionality
    -DSKV_ROQ_LOOPBACK_WORKAROUND
  )
endif()
if(SKV_COMM_API_TYPE MATCHES "sockets")
  set( IT_API_LIBS pthread rt )
endif()


include(logtrace)
set(GLOBAL_DEFS
  ${PLATFORM_GLOBAL_DEFS}
  # use PK_LINUX to pull in several important includes and macros
  -DPK_LINUX
  # use MSPACES from dlmalloc.h
  -DONLY_MSPACES
 )


#------------------------------------------------------------------------------
# include dirs
#------------------------------------------------------------------------------
include_directories(it_api FxLogger)

#------------------------------------------------------------------------------
# FxLogger library
#------------------------------------------------------------------------------
set(FXLOGGER_SOURCES
  FxLogger/FxLogger.cpp
  FxLogger/Trace.cpp
  FxLogger/dlmalloc.c
)

set(FXLOGGER_PUBLIC_HEADERS
  FxLogger/FxLogger.hpp
  FxLogger/Time.hpp
  FxLogger/Histogram.hpp
  FxLogger/Trace.hpp
)

#------------------------------------------------------------------------------
# IT_API library
#------------------------------------------------------------------------------
set(IT_API_DEFS
  # use event-based CQ reaping, if undefined it uses polling
  -DDIT_API_USE_EVENT_CQ
  # enable SIW workaround
  -DIT_API_USE_SIW_HACK
)

set(IT_API_SOURCES
  it_api/it_api_o_${SKV_COMM_API_TYPE}.cpp
  it_api/it_api_o_${SKV_COMM_API_TYPE}_thread.cpp
)
set(IT_API_PUBLIC_HEADERS
  it_api/it_api.h
  it_api/it_api_os_specific.h
)

set(IT_API_LINK_LIBRARIES ${IT_API_LIBS} )



#------------------------------------------------------------------------------
# Compile settings
#------------------------------------------------------------------------------

add_definitions(
# default config file
  -DDEFAULT_CONFIG_FILE="${CMAKE_CURRENT_SOURCE_DIR}/.skv_server.conf"
# enable command header checksum
  -DSKV_HEADER_CHECKSUM
# set the local kv backend for SKV
  -DSKV_SERVER_LOCAL_KV=skv_local_kv_${SKV_LOCAL_KV_BACKEND}
# data checking in tests
#  -DDO_CHECK

# seems to be necessary for the tests to link
  ${GLOBAL_DEFS}
  ${IT_API_DEFS}
)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -std=c99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# delay as late as possible. Include path includes mpi-gcc-specific (4.4)
# headers which might clash with the headers used by the actual gcc (4.8)
include_directories(${MPI_INCLUDE_PATH})

#------------------------------------------------------------------------------
# Create library targets
#------------------------------------------------------------------------------

common_library(fxlogger)
common_library(it_api)
set_target_properties(it_api PROPERTIES COMPILE_DEFINITIONS "${SKV_IT_API_LOGGING}" )


#------------------------------------------------------------------------------
# Define test source files
#------------------------------------------------------------------------------

set(TEST_SOURCES
  test/skv_base_test.cpp
  test/test_skv_insert_command.cpp
)

set(TEST_C_SOURCES
  test/skv_test_clib.c
)

set(TEST_MPI_SOURCES
  test/skv_test_bulk.cpp
  test/skv_bench.cpp
  test/skv_test_insert_retrieve_async.cpp
  test/skv_test_insert_retrieve_sync.cpp
  test/test_skv_remove_command.cpp
)

set(UNITTEST_SOURCES
  unittest/test_skv_rdma_data_buffer.cpp
  unittest/test_skv_ringbuffer_ptr.cpp
)

#------------------------------------------------------------------------------
# tests

add_subdirectory(skv)

foreach(_test ${TEST_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  set_target_properties(${TEST_NAME} PROPERTIES COMPILE_DEFINITIONS
    "SKV_CLIENT_UNI;SKV_NON_MPI")
  target_link_libraries(${TEST_NAME} skvc skv_client skv_common it_api fxlogger
    ${SKV_COMMON_LINK_LIBRARIES})
  install(TARGETS ${TEST_NAME} DESTINATION bin)
endforeach()

foreach(_test ${TEST_MPI_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  target_link_libraries(${TEST_NAME} skvc_mpi skv_client_mpi skv_common
    ${IT_API_LINK_LIBRARIES} fxlogger
    ${MPI_CXX_LIBRARIES} ${SKV_COMMON_LINK_LIBRARIES})
  install(TARGETS ${TEST_NAME} DESTINATION bin)
endforeach()

include(skvCPack)
