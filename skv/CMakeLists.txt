
set(SKVC_SOURCES c/skv.cpp)
set(SKVC_PUBLIC_HEADERS c/skv.h)
set(SKVC_MPI_SOURCES ${SKVC_SOURCES})

set(SKV_COMMON_SOURCES
  common/skv_config.cpp
  common/skv_distribution_manager.cpp
  common/skv_init.cpp
  common/skv_utils.cpp
)
set(SKV_COMMON_PUBLIC_HEADERS
  common/skv_array_queue.hpp
  common/skv_array_stack.hpp
  common/skv_client_server_headers.hpp
  common/skv_client_server_protocol.hpp
  common/skv_config.hpp
  common/skv_distribution_manager.hpp
  common/skv_errno.hpp
  common/skv_init.hpp
  common/skv_mutex.hpp
  common/skv_types.hpp
  common/skv_types_ext.hpp
  common/skv_utils.hpp
)

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
set(SKV_CLIENT_LINK_LIBRARIES it_api fxlogger skv_common)
set(SKV_CLIENT_MPI_SOURCES ${SKV_CLIENT_SOURCES})
set(SKV_CLIENT_MPI_LINK_LIBRARIES ${SKV_CLIENT_LINK_LIBRARIES})

set(SKVSERVER_SOURCES
  server/skv_local_kv_${LOCAL_KV_BACKEND}.cpp
  server/skv_server.cpp
  server/skv_server_IT_event_source.cpp
  server/skv_server_command_event_source.cpp
  server/skv_server_local_kv_event_source.cpp
  server/skv_server_main.cpp
  server/skv_server_network_event_manager.cpp
  server/skv_server_tree_based_container.cpp
  server/skv_server_uber_pds.cpp
  server/skv_server_command_event_buffer.cpp
)
set(SKVSERVER_LINK_LIBRARIES pthread m dl ${OFED_LIBRARIES}
  ${MPI_CXX_LIBRARIES}  ${ZLIB_LIBRARIES} ${BZIP2_LIBRARIES}
  ${ROCKSDB_LIBRARIES} skv_common it_api fxlogger)
if(NOT BLUEGENE)
  list(APPEND SKVSERVER_LINK_LIBRARIES lz4)
endif()

#------------------------------------------------------------------------------
# libraries

common_library(skv_common)
common_library(skv_client)
common_library(skv_client_mpi)
common_library(skvc)
common_library(skvc_mpi)

set_target_properties(skv_common PROPERTIES COMPILE_DEFINITIONS
  "SKV_CLIENT_UNI;SKV_NON_MPI")
set_target_properties(skv_client PROPERTIES COMPILE_DEFINITIONS
  "SKV_CLIENT_UNI;SKV_NON_MPI")
set_target_properties(skvc PROPERTIES COMPILE_DEFINITIONS
  "SKV_CLIENT_UNI;SKV_NON_MPI")

#------------------------------------------------------------------------------
# executables
common_application(SKVServer)

if(LOCAL_KV_BACKEND MATCHES "rocksdb")
  set_target_properties(SKVServer PROPERTIES COMPILE_DEFINITIONS
    "WITH_ROCKSDB")
  # for some reason, the rocksdb includes were not found...
  include_directories(${ROCKSDB_INCLUDE_DIRS})
endif()


#------------------------------------------------------------------------------
# etc/ stuff

set( SKV_CONFIG_FILES system/bgq/etc/skv_server.conf)
install(FILES ${SKV_CONFIG_FILES} DESTINATION etc)
