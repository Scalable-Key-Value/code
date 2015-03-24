/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

#ifndef IT_API_O_SOCKETS_ROUTER_H
#define  IT_API_O_SOCKETS_ROUTER_H

struct rpcBuffer
  {
    uint64_t routerBuffer_addr ;
    uint32_t routermemreg_lkey ;
    size_t upstreamBufferLength ;
    unsigned long downstreamSequence ;
    unsigned int clientRank ;
  };
struct rpcAckBuffer
  {
    unsigned long upstreamSequence ;
  };

enum {
  k_IONPort = CNK_ROUTER_PORT,
  k_UpstreamBufferSize = CNK_ROUTER_BUFFER_SIZE,
  k_DownstreamBufferSize = CNK_ROUTER_BUFFER_SIZE ,
  k_CallBufferSize = sizeof(struct rpcBuffer) ,
  k_AckBufferSize = sizeof(struct rpcAckBuffer)
//  k_CallBufferCount = 64
};

struct callBuffer
  {
    char callBufferElement[k_CallBufferSize] ;
  };

struct upstreamBuffer
  {
    char upstreamBufferElement[k_UpstreamBufferSize];
  };

struct downstreamBuffer
  {
    char downstreamBufferElement[k_DownstreamBufferSize];
  };

struct ackBuffer
  {
    char ackBufferElement[k_AckBufferSize] ;
  };

struct downstreamSequence
  {
    unsigned long sequence ;
  };
struct downstreamCompleteBuffer
  {
    char downstreamCompleteBufferElement[sizeof(struct downstreamSequence)] ;
  };
struct downstreamLength
  {
    size_t length ;
    unsigned long sequence ;

  };
struct downstreamLengthBuffer
  {
    char downstreamLengthBufferElement[sizeof(struct downstreamLength)] ;
  };

struct routerBuffer
  {
    struct ackBuffer ackBuffer ;
    struct callBuffer callBuffer ;
    struct upstreamBuffer upstreamBuffer ;
    struct downstreamBuffer downstreamBuffer ;
    struct downstreamCompleteBuffer downstreamCompleteBuffer ;
    struct downstreamLengthBuffer downstreamLengthBuffer ;
  };

#endif
