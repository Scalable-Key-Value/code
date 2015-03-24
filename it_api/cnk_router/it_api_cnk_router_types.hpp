/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * it_api_cnk_router_types.hpp
 *
 *  Created on: Jan 14, 2015
 *      Author: lschneid
 */

#ifndef IT_API_IT_API_CNK_ROUTER_TYPES_HPP_
#define IT_API_IT_API_CNK_ROUTER_TYPES_HPP_

#define IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION ( (unsigned char)1 )
#define IWARPEM_MULTIPLEXED_SOCKET_MAGIC ( 0x50CCFEED )
#define IWARPEM_ROUTER_INFO_SIZE ( sizeof( iWARPEM_Router_Info_t ) )

#define IT_API_MULTIPLEX_MAX_PER_SOCKET ( 128 )
#define IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE ( 16ul * 1024ul * 1024ul )

#define IWARPEM_INVALID_CLIENT_ID ( 9999 )
#define IWARPEM_INVALID_SERVER_ID ( -1 )

typedef enum
{
  MULTIPLEXED_SOCKET_MSG_TYPE_CONNECT_REQ = 0,
  MULTIPLEXED_SOCKET_MSG_TYPE_CONNECT_RESP = 1,
  MULTIPLEXED_SOCKET_MSG_TYPE_DISCONNECT_REQ = 2,
  MULTIPLEXED_SOCKET_MSG_TYPE_DISCONNECT_RESP = 3,
  MULTIPLEXED_SOCKET_MSG_TYPE_DATA = 4
} it_api_multiplexed_socket_msg_type_t;

struct it_api_multiplexed_socket_message_header_t
{
  size_t DataLen;
  unsigned char ProtocolVersion;
  unsigned char MsgType;
};

typedef uintptr_t iWARPEM_StreamId_t;

struct iWARPEM_Multiplexed_Msg_Hdr_t
{
  iWARPEM_StreamId_t ClientID;
};

#endif /* IT_API_IT_API_CNK_ROUTER_TYPES_HPP_ */
