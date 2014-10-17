########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
########################################

# logging options
# set logging to non-zero or uncomment to enable
add_definitions(
# enable logging in general
 -DPKFXLOG

# all logging enabled
# -DSKV_LOGGING_ALL=1

# general detailed logging
# -DSKV_DISTRIBUTION_MANAGER_LOG=1
# -DSKV_CONFIG_FILE_LOG=1

# server detailed logging
# -DSKV_C2S_ACTIVE_BROADCAST_LOG=1
# -DSKV_GET_EVENT_LOG=1
# -DSKV_GET_IT_EVENT_LOG=1
# -DSKV_PROCESS_EVENT_LOG=1
# -DSKV_PROCESS_IT_EVENT_LOG=1
# -DSKV_PROCESS_IT_EVENT_READ_LOG=1
# -DSKV_PROCESS_IT_EVENT_WRITE_LOG=1
# -DSKV_PROCESS_LOCAL_KV_EVENT_LOG=1
# -DSKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG=1
# -DSKV_SERVER_BUFFER_AND_COMMAND_LOG=1
# -DSKV_SERVER_BULK_INSERT_LOG=1
# -DSKV_SERVER_CLEANUP_LOG=1
# -DSKV_SERVER_CLIENT_CONN_EST_LOG=1
# -DSKV_SERVER_COMMAND_DISPATCH_LOG=1
# -DSKV_SERVER_COMMAND_POLLING_LOG=1
# -DSKV_SERVER_COMMAND_TRANSIT_LOG=1
# -DSKV_SERVER_CURSOR_MANAGER_IF_LOG=1
# -DSKV_SERVER_CURSOR_PREFETCH_COMMAND_SM_LOG=1
# -DSKV_SERVER_CURSOR_RESERVE_RELEASE_BUF_LOG=1
# -DSKV_SERVER_GET_COMMAND_LOG=1
# -DSKV_SERVER_HEAP_MANAGER_LOG=1
# -DSKV_SERVER_INSERT_LOG=1
# -DSKV_SERVER_LOCAL_KV_EVENT_SOURCE_LOG=1
# -DSKV_SERVER_LOCK_LOG=1
# -DSKV_SERVER_NETWORK_EVENT_MANAGER_LOG=1
# -DSKV_SERVER_OPEN_COMMAND_SM_LOG=1
# -DSKV_SERVER_PDSCNTL_COMMAND_SM_LOG=1
# -DSKV_SERVER_PENDING_EVENTS_LOG=1
# -DSKV_SERVER_RDMA_RESPONSE_PLACEMENT_LOG=1
# -DSKV_SERVER_REMOVE_COMMAND_LOG=1
# -DSKV_SERVER_RETRIEVE_COMMAND_SM_LOG=1
# -DSKV_SERVER_RETRIEVE_DIST_LOG=1
# -DSKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_LOG=1
# -DSKV_SERVER_TREE_BASED_CONTAINER_LOG=1
# -DSKV_SERVER_TREE_BASED_CONTAINER_INIT_LOG=1
# -DSKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG=1

# -DSKV_SERVER_LOOP_STATISTICS
# -DSKV_SERVER_FETCH_AND_PROCESS

# server side local kv logging
# -DSKV_LOCAL_KV_ASYNCMEM_PROCESSING_LOG=1
# -DSKV_LOCAL_KV_BACKEND_LOG=1
# -DSKV_LOCAL_KV_QUEUES_LOG=1

# client logging
# -DSKV_CLIENT_INIT_LOG=1
# -DSKV_CLIENT_BULK_INSERT_LOG=1
# -DSKV_CLIENT_C2S_ACTIVE_BCAST_LOG=1
# -DSKV_CLIENT_CONN_INFO_LOG=1
# -DSKV_CLIENT_CURSOR_LOG=1
# -DSKV_CLIENT_CCB_MANAGER_LOG=1
# -DSKV_CLIENT_FREE_QUEUE_LOG=1
# -DSKV_CLIENT_INSERT_COMMAND_SM_LOG=1
# -DSKV_CLIENT_INSERT_LOG=1
# -DSKV_CLIENT_OPEN_LOG=1
# -DSKV_CLIENT_PDSCNTL_LOG=1
# -DSKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG=1
# -DSKV_CLIENT_PROCESS_CONN_LOG=1
# -DSKV_CLIENT_PROCESS_CCB_LOG=1
# -DSKV_CLIENT_PROCESS_SEND_RECV_RACE_LOG=1
# -DSKV_CLIENT_RDMA_CMD_PLACEMENT_LOG=1
# -DSKV_CLIENT_REQUEST_COALESCING_LOG=1
# -DSKV_CLIENT_RESPONSE_POLLING_LOG=1
# -DSKV_CLIENT_RETRIEVE_COMMAND_SM_LOG=1
# -DSKV_CLIENT_RETRIEVE_LOG=1
# -DSKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG=1
# -DSKV_CLIENT_REMOVE_LOG=1


##################################
# Misc Debugging options

# puts a marker into the command/response message
# -DSKV_DEBUG_MSG_MARKER
# bulk-insert with checksum (debug option)
# -DSKV_BULK_LOAD_CHECKSUM
# -DSKV_RETRIEVE_DATA_LOG

##################################
# logging in tests
# activate inner-loop logging for test programm
# -DSKV_TEST_LOG=1




)

############################################################
#  T R A C I N G
add_definitions(
#enable tracing
# -DPKTRACE_ON
#server
# -DSKV_SERVER_TRACE=1
# -DSKV_SERVER_BULK_INSERT_TRACE=1
# -DSKV_SERVER_CURSOR_PREFETCH_TRACE=1
# -DSKV_SERVER_FILL_CURSOR_BUFFER_TRACE=1
# -DSKV_SERVER_INSERT_TRACE=1
# -DSKV_SERVER_REMOVE_TRACE=1
# -DSKV_SERVER_RETRIEVE_TRACE=1
# -DSKV_SERVER_POLL_ONLY_TRACE=1

#client
# -DSKV_CLIENT_CURSOR_STREAM_TRACE=1
# -DSKV_CLIENT_INSERT_TRACE=1
# -DSKV_CLIENT_PROCESS_CONN_TRACE=1
)

set(SKV_IT_API_LOGGING
# enable extensive logging
#FXLOG_IT_API_O_VERBS=1
#FXLOG_IT_API_O_SOCKETS=1

# enable evd thread logging
#IT_API_O_VERBS_THREAD_LOG=1

# enable connection-related logging only (incomplete coverage)
#FXLOG_IT_API_O_VERBS_CONNECT=1
#FXLOG_IT_API_O_SOCKETS_CONNECT=1

# enable logging for memory registration (incomplete coverage)
#FXLOG_IT_API_O_VERBS_MEMREG=1

# enable logging for rdma write ops (incomplete coverage)
#FXLOG_IT_API_O_VERBS_WRITE=1

# enable logging for types
#IT_API_O_VERBS_TYPES_LOG=1

#FXLOG_IT_API_O_VERBS_QUEUE_LENGTHS_LOG=1
#THREAD_SAFE_QUEUE_FXLOG=1
#THREAD_SAFE_QUEUE_INIT_FXLOG=1
)
