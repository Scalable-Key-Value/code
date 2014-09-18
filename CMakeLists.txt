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

find_package(OFED REQUIRED)

include_directories(${MPI_INCLUDE_PATH})

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


if(SKV_LOCAL_KV_BACKEND MATCHES "rocksdb")
  find_package(rocksdb REQUIRED)
endif()


include(logtrace)
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
include_directories(it_api/include lib/include FxLogger)

#------------------------------------------------------------------------------
# Define source files
#------------------------------------------------------------------------------

set(FXLOGGER_SOURCES
  FxLogger/FxLogger.cpp
  FxLogger/Trace.cpp
  FxLogger/dlmalloc.c
)

set(IT_API_SOURCES
  it_api/src/it_api_o_verbs.cpp
  it_api/src/it_api_o_verbs_thread.cpp
)
set(IT_API_LINK_LIBRARIES ${OFED_LIBRARIES})

set(TEST_SOURCES
  test/skv_base_test.cpp
)

set(TEST_C_SOURCES
  test/skv_test_clib.c
)

set(TEST_MPI_SOURCES
  test/skv_test_bulk.cpp
  test/skv_test_insert_retrieve_async.cpp
  test/skv_test_insert_retrieve_sync.cpp
  test/test_skv_insert_command.cpp
  test/test_skv_remove_command.cpp
)

set(UNITTEST_SOURCES
  unittest/test_skv_rdma_data_buffer.cpp
  unittest/test_skv_ringbuffer_ptr.cpp
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
  ${GLOBAL_DEFS}
  ${IT_API_DEFS}
)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w -std=c99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

#------------------------------------------------------------------------------
# libraries

# eile: Disabled static linking. For some unfathomable reason fxlogger
# then forces the link against gcc 4.4.7 libs which can't work
#set(COMMON_LIBRARY_TYPE STATIC)

common_library(fxlogger)
common_library(it_api)
add_subdirectory(skv)

foreach(_test ${TEST_SOURCES})
  get_filename_component(TEST_NAME ${_test} NAME_WE)
  add_executable(${TEST_NAME} ${_test})
  target_compile_definitions(${TEST_NAME} PUBLIC
    "SKV_CLIENT_UNI;SKV_NON_MPI")
  target_link_libraries(${TEST_NAME} skvc skv_client skv_common it_api fxlogger
    ${MPI_LIBRARIES} ${SKV_COMMON_LINK_LIBRARIES})
  install(TARGETS ${TEST_NAME} DESTINATION bin)
endforeach()

include(skvCPack)
