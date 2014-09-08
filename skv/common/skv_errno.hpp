/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     arayshu, lschneid - initial implementation
 */

#ifndef __SKV_ERRNO_HPP__
#define __SKV_ERRNO_HPP__

#include <stdio.h>

typedef enum 
  {
    SKV_SUCCESS,                                	// 0 This one is the best!
    SKV_ERRNO_NODE_NOT_OWNER,                   	// Node is not an owner
    SKV_ERRNO_NODE_FULL,                        	// Node is full, time to rebalance
    SKV_ERRNO_ELEM_NOT_FOUND,                   	// No record in the database
    SKV_ERRNO_RECORD_ALREADY_EXISTS,            	// Record already exists in the database
    SKV_ERRNO_ITER_DONE,                        	// 5 The iterator reached the end                  
    SKV_ERRNO_LOCAL_IP_NOT_FOUND,               	// No local IP addresses were found
    SKV_ERRNO_CONNECTION_REQUEST_REFUSED,       	// Client's connection to the NameService was refused
    SKV_ERRNO_NO_EVENT,                         	// No event was found on an evd
    SKV_ERRNO_CONN_FAILED,                      	// Connection failed
    SKV_ERRNO_COMMAND_LIMIT_REACHED,            	// 10 Command limit reached
    SKV_ERRNO_NOT_DONE,                         	// Command is not done
    SKV_ERRNO_MAX_UNRETIRED_CMDS,               	// Connection has max unretired recvs in flight 
    SKV_ERRNO_PDS_ALREADY_EXISTS,               	// This is return on an open() if create of a pds is requested and that pds exists
    SKV_ERRNO_PDS_DOES_NOT_EXIST,                // open() could not find a pds by a specified name
    SKV_ERRNO_PERMISSION_DENIED,                 // 15 Inconsistent permissions on pds open()
    SKV_ERRNO_IT_POST_SEND_FAILED,               // it_post_send() did not return IT_SUCCESS
    SKV_ERRNO_IT_POST_RECV_FAILED,               // it_post_recv() did not return IT_SUCCESS
    SKV_ERRNO_IT_POST_RDMA_WRITE_FAILED,         // it_post_rdma_write() did not return IT_SUCCESS
    SKV_ERRNO_OUT_OF_MEMORY,                     // Out of memory for a command
    SKV_ERRNO_KEY_NOT_FOUND,                     // 20 Key does not exist in the database  
    SKV_ERRNO_KEY_TOO_LARGE,                     // Key   too large
    SKV_ERRNO_VALUE_TOO_LARGE,                   // Value too large
    SKV_ERRNO_KEY_SIZE_OVERFLOW,                 // Specified key size is too small
    SKV_ERRNO_RECORD_IS_LOCKED,                  // Record is locked 
    SKV_ERRNO_CONTROL_MSG_LIMIT_EXCEEDED,        // 25 Data intended for control message comm is too large
    SKV_ERRNO_NOT_IMPLEMENTED,                   // Feature not yet implemented
    SKV_ERRNO_END_OF_RECORDS,                    // End of records
    SKV_ERRNO_RETRIEVE_BUFFER_MAX_SIZE_EXCEEDED, // Relational streaming cursor, GetFirst(), GetNext()
    SKV_ERRNO_FILLED_END_OF_RECORDS,             // Relational streaming cursor. 
    SKV_ERRNO_NO_BUFFER_AVAILABLE,               // 30 Streaming cursor ran out of buffers
    SKV_ERRNO_CURSOR_DONE,                       // Server data cursor reached the end
    SKV_ERRNO_BULK_INSERT_LIMIT_EXCEEDED,        // Input buffer is too large 
    SKV_ERRNO_PENDING_COMMAND_LIMIT_REACHED,     // The limit of outstanding commands is set by SKV_MAX_COMMANDS_PER_EP
    SKV_ERRNO_CHECKSUM_MISMATCH,                 // Internal use: communication buffer checksum mismatch 
    SKV_ERRNO_END_OF_BUFFER,                     // 35 Internal use: streaming cursor iterator end of buffer reached
    SKV_ERRNO_EVENTS_PENDING,                    // Server has pending events to process
    SKV_ERRNO_INSERT_OVERLAP,                    // insert failed because of overlapping access between new and old data (requires special flag to allow)
    SKV_ERRNO_NEED_DATA_TRANSFER,                // Value data not in control message, need explicit rdma transfer
    SKV_ERRNO_LOCAL_KV_EVENT,                    // Local KV could only partially operate, need multi stage processing
    SKV_ERRNO_STATE_MACHINE_ERROR,               // Error in state machine, e.g. wrong event/command types/states - almost always fatal
    SKV_ERRNO_UNSPECIFIED_ERROR
  } skv_status_t;


static
const char*
skv_status_to_string( skv_status_t aStatus )
{
  switch( aStatus )
    {
    case SKV_SUCCESS:                                          { return "SKV_SUCCESS"; }
    case SKV_ERRNO_NODE_NOT_OWNER:                             { return "SKV_ERRNO_NODE_NOT_OWNER"; } 
    case SKV_ERRNO_NODE_FULL:                                  { return "SKV_ERRNO_NODE_FULL"; }
    case SKV_ERRNO_ELEM_NOT_FOUND:                             { return "SKV_ERRNO_ELEM_NOT_FOUND"; }  
    case SKV_ERRNO_RECORD_ALREADY_EXISTS:                      { return "SKV_ERRNO_RECORD_ALREADY_EXISTS"; }  
    case SKV_ERRNO_ITER_DONE:                                  { return "SKV_ERRNO_ITER_DONE"; }  
    case SKV_ERRNO_LOCAL_IP_NOT_FOUND:                         { return "SKV_ERRNO_LOCAL_IP_NOT_FOUND"; }  
    case SKV_ERRNO_CONNECTION_REQUEST_REFUSED:                 { return "SKV_ERRNO_CONNECTION_REQUEST_REFUSED"; }  
    case SKV_ERRNO_NO_EVENT:                                   { return "SKV_ERRNO_NO_EVENT"; }  
    case SKV_ERRNO_CONN_FAILED:                                { return "SKV_ERRNO_CONN_FAILED"; }  
    case SKV_ERRNO_COMMAND_LIMIT_REACHED:                      { return "SKV_ERRNO_COMMAND_LIMIT_REACHED"; }  
    case SKV_ERRNO_NOT_DONE:                                   { return "SKV_ERRNO_NOT_DONE"; }  
    case SKV_ERRNO_MAX_UNRETIRED_CMDS:                         { return "SKV_ERRNO_MAX_UNRETIRED_CMDS"; }  
    case SKV_ERRNO_PDS_ALREADY_EXISTS:                         { return "SKV_ERRNO_PDS_ALREADY_EXISTS"; }
    case SKV_ERRNO_PDS_DOES_NOT_EXIST:                         { return "SKV_ERRNO_PDS_DOES_NOT_EXIST"; }
    case SKV_ERRNO_PERMISSION_DENIED:                          { return "SKV_ERRNO_PERMISSION_DENIED"; }  
    case SKV_ERRNO_IT_POST_SEND_FAILED:                        { return "SKV_ERRNO_IT_POST_SEND_FAILED"; }  
    case SKV_ERRNO_IT_POST_RECV_FAILED:                        { return "SKV_ERRNO_IT_POST_RECV_FAILED"; }  
    case SKV_ERRNO_IT_POST_RDMA_WRITE_FAILED:                  { return "SKV_ERRNO_IT_POST_RDMA_WRITE_FAILED"; }  
    case SKV_ERRNO_OUT_OF_MEMORY:                              { return "SKV_ERRNO_OUT_OF_MEMORY"; } 
    case SKV_ERRNO_KEY_NOT_FOUND:                              { return "SKV_ERRNO_KEY_NOT_FOUND"; }
    case SKV_ERRNO_KEY_TOO_LARGE:                              { return "SKV_ERRNO_KEY_TOO_LARGE"; }
    case SKV_ERRNO_VALUE_TOO_LARGE:                            { return "SKV_ERRNO_VALUE_TOO_LARGE"; }
    case SKV_ERRNO_END_OF_RECORDS:                             { return "SKV_ERRNO_END_OF_RECORDS"; }
    case SKV_ERRNO_KEY_SIZE_OVERFLOW:                          { return "SKV_ERRNO_KEY_SIZE_OVERFLOW"; }
    case SKV_ERRNO_RECORD_IS_LOCKED:                           { return "SKV_ERRNO_RECORD_IS_LOCKED"; }
    case SKV_ERRNO_CONTROL_MSG_LIMIT_EXCEEDED:                 { return "SKV_ERRNO_CONTROL_MSG_LIMIT_EXCEEDED"; }
    case SKV_ERRNO_NOT_IMPLEMENTED:                            { return "SKV_ERRNO_NOT_IMPLEMENTED"; }
    case SKV_ERRNO_RETRIEVE_BUFFER_MAX_SIZE_EXCEEDED:          { return "SKV_ERRNO_RETRIEVE_BUFFER_MAX_SIZE_EXCEEDED"; }
    case SKV_ERRNO_FILLED_END_OF_RECORDS:                      { return "SKV_ERRNO_FILLED_END_OF_RECORDS"; }
    case SKV_ERRNO_NO_BUFFER_AVAILABLE:                        { return "SKV_ERRNO_NO_BUFFER_AVAILABLE"; }
    case SKV_ERRNO_CURSOR_DONE:                                { return "SKV_ERRNO_CURSOR_DONE"; }
    case SKV_ERRNO_BULK_INSERT_LIMIT_EXCEEDED:                 { return "SKV_ERRNO_BULK_INSERT_LIMIT_EXCEEDED"; }
    case SKV_ERRNO_PENDING_COMMAND_LIMIT_REACHED:              { return "SKV_ERRNO_PENDING_COMMAND_LIMIT_REACHED"; }
    case SKV_ERRNO_CHECKSUM_MISMATCH:                          { return "SKV_ERRNO_CHECKSUM_MISMATCH"; }
    case SKV_ERRNO_END_OF_BUFFER:                              { return "SKV_ERRNO_END_OF_BUFFER"; }
    case SKV_ERRNO_EVENTS_PENDING:                             { return "SKV_ERRNO_EVENTS_PENDING"; }
    case SKV_ERRNO_NEED_DATA_TRANSFER:                         { return "SKV_ERRNO_NEED_DATA_TRANSFER"; }
    case SKV_ERRNO_LOCAL_KV_EVENT:                             { return "SKV_ERRNO_LOCAL_KV_EVENT"; }
    case SKV_ERRNO_STATE_MACHINE_ERROR:                        { return "SKV_ERRNO_STATE_MACHINE_ERROR"; }
    case SKV_ERRNO_UNSPECIFIED_ERROR:                          { return "SKV_ERRNO_UNSPECIFIED_ERROR"; }
    default:
      {
        printf( "skv_status_to_string: ERROR:: aStatus: %d is not recognized\n", aStatus );
        return "SKV_ERRNO_UNKNOWN_STATE";
      }
    }
}

#endif
