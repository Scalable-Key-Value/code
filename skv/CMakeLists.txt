set(SKVC_SOURCES c/skv.cpp)
set(SKVC_PUBLIC_HEADERS c/skv.h)
set(SKVC_MPI_SOURCES ${SKVC_SOURCES})

set(SKV_CLIENT_SOURCES
  client/skv_client.cpp
  client/skv_client_2_server_active_bcast.cpp
  client/skv_client_bulk_inserter.cpp
  client/skv_client_command_manager_if.cpp
  client/skv_client_conn_manager_if.cpp
  client/skv_client_cursor.cpp
  client/skv_client_internal.cpp
)
set(SKV_CLIENT_PUBLIC_HEADERS
  client/skv_client_command_manager_if.hpp
  client/skv_client_cursor_manager_if.hpp
  client/skv_c2s_active_broadcast.hpp
  client/skv_client_conn_manager_if.hpp
  client/skv_client_server_conn.hpp
  client/skv_client_types.hpp
  client/skv_client_internal.hpp
  client/skv_client.hpp
  client/commands/skv_client_bulk_insert_command_sm.hpp
  client/commands/skv_client_open_command_sm.hpp
  client/commands/skv_client_insert_command_sm.hpp
  client/commands/skv_client_retrieve_command_sm.hpp
  client/commands/skv_client_pdscntl_command_sm.hpp
  client/commands/skv_client_remove_command_sm.hpp
  client/commands/skv_client_retrieve_dist_command_sm.hpp
  client/commands/skv_client_active_bcast_command_sm.hpp
  client/commands/skv_client_retrieve_n_keys_command_sm.hpp
  client/skv_client_bulk_inserter_manager_if.hpp
)

set(SKV_CLIENT_MPI_SOURCES ${SKV_CLIENT_SOURCES})

set(SKV_COMMON_SOURCES
  common/skv_config.cpp
  common/skv_distribution_manager.cpp
  common/skv_utils.cpp
)
set(SKV_COMMON_PUBLIC_HEADERS
  common/skv_distribution_manager.hpp
  common/skv_utils.hpp
  common/skv_array_stack.hpp
  common/skv_client_server_headers.hpp
  common/skv_array_queue.hpp
  common/skv_errno.hpp
  common/skv_config.hpp
  common/skv_client_server_protocol.hpp
  common/skv_types.hpp
  common/skv_types_ext.hpp
  common/skv_mutex.hpp
)
set(SERVER_SOURCES
  server/skv_local_kv_asyncmem.cpp
  server/skv_local_kv_inmem.cpp
  server/skv_local_kv_rocksdb.cpp
  server/skv_server.cpp
  server/skv_server_IT_event_source.cpp
  server/skv_server_command_event_source.cpp
  server/skv_server_local_kv_event_source.cpp
  server/skv_server_main.cpp
  server/skv_server_network_event_manager.cpp
  server/skv_server_tree_based_container.cpp
  server/skv_server_uber_pds.cpp
  system/bgq/etc/skv_server.conf
)
#------------------------------------------------------------------------------
# libraries

# eile: Disabled static linking. For some unfathomable reason fxlogger
# then forces the link against gcc 4.4.7 libs which can't work
#set(COMMON_LIBRARY_TYPE STATIC)

common_library(skv_common)
common_library(skv_client)
common_library(skv_client_mpi)
common_library(skvc)
common_library(skvc_mpi)

target_compile_definitions(skv_client PUBLIC "-DSKV_CLIENT_UNI -DSKV_NON_MPI")
target_compile_definitions(skvc PUBLIC "-DSKV_CLIENT_UNI -DSKV_NON_MPI")

#------------------------------------------------------------------------------
# executables

set(SKV_COMMON_LINK_LIBRARIES pthread m dl)

add_executable(SKVServer ${SERVER_SOURCES})
target_link_libraries(SKVServer skv_common it_api fxlogger
  ${OFED_LIBRARIES} ${MPI_LIBRARIES} ${SKV_COMMON_LINK_LIBRARIES})
