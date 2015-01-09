/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * iwarpem_types.hpp
 *
 *  Created on: Jan 20, 2015
 *      Author: lschneid
 */

#ifndef IT_API_IWARPEM_TYPES_HPP_
#define IT_API_IWARPEM_TYPES_HPP_

#ifndef FXLOG_IT_API_O_SOCKETS
#define FXLOG_IT_API_O_SOCKETS ( 0 )
#endif

typedef enum
  {
    IWARPEM_CONNECTION_FLAG_DISCONNECTED                       = 0x0000,
    IWARPEM_CONNECTION_FLAG_CONNECTED                          = 0x0001,
    IWARPEM_CONNECTION_FLAG_ACTIVE_SIDE_PENDING_DISCONNECT     = 0x0002,
    IWARPEM_CONNECTION_FLAG_PASSIVE_SIDE_PENDING_DISCONNECT    = 0x0003
  } iWARPEM_Connection_Flags_t;

#ifdef WITH_CNK_ROUTER
typedef enum
{
  IWARPEM_CONNECTION_TYPE_UNKNOWN = 0x0000,
  IWARPEM_CONNECTION_TYPE_DIRECT = 0x0001,
  IWARPEM_CONNECTION_TYPE_MULTIPLEX = 0x0002,
  IWARPEM_CONNECTION_TYPE_VIRUTAL = 0x0003,
} iWARPEM_Connection_Type_t;
#endif

#define IWARPEM_SEND_WR_QUEUE_MAX_SIZE 65536
#define IWARPEM_RECV_WR_QUEUE_MAX_SIZE 8192
#define IWARPEM_LOCKLESS_QUEUE 0

typedef enum
  {
    IWARPEM_SOCKETCONTROL_TYPE_ADD    = 0x0001,
    IWARPEM_SOCKETCONTROL_TYPE_REMOVE = 0x0002
  } iWARPEM_SocketControl_Type_t;

struct iWARPEM_SocketControl_Hdr_t
{
  iWARPEM_SocketControl_Type_t mOpType;
  int                          mSockFd;
};

struct iWARPEM_Object_WorkRequest_t
  {
  iWARPEM_Message_Hdr_t  mMessageHdr;

  // Local from it_post_send
  it_ep_handle_t    ep_handle;
  it_lmr_triplet_t *segments_array;
  size_t            num_segments;
  it_dto_cookie_t   cookie;
  it_dto_flags_t    dto_flags;

//   iWARPEM_Object_WorkRequest_t *mNext;
//   iWARPEM_Object_WorkRequest_t *mPrev;
  };


struct iWARPEM_Object_WR_Queue_t
{
  ThreadSafeQueue_t< iWARPEM_Object_WorkRequest_t *, IWARPEM_LOCKLESS_QUEUE > mQueue;

  void
  Finalize()
  {
    mQueue.Finalize();
  }

  void
  Init(int aSize)
  {
    mQueue.Init( aSize );
  }

  pthread_mutex_t*
  GetMutex()
  {
    return mQueue.GetMutex();
  }
  size_t
  GetSize()
  {
    return mQueue.GetCount();
  }

  int
  Enqueue( iWARPEM_Object_WorkRequest_t *aNextIn )
  {
    return mQueue.Enqueue( aNextIn );
  }

  int
  Dequeue( iWARPEM_Object_WorkRequest_t **aNextOut )
  {
    return mQueue.Dequeue( aNextOut );
  }

  int
  DequeueAssumeLocked( iWARPEM_Object_WorkRequest_t **aNextOut )
  {
    return mQueue.DequeueAssumeLocked( aNextOut );
  }
};

// The EndPoint is here to so top down declarations work
struct iWARPEM_Object_EndPoint_t
{
  // it_ep_rc_create args
  it_pz_handle_t            pz_handle;
  it_evd_handle_t           request_sevd_handle;
  it_evd_handle_t           recv_sevd_handle;
  it_evd_handle_t           connect_sevd_handle;
  it_ep_rc_creation_flags_t flags;
  it_ep_attributes_t        ep_attr;
  it_ep_handle_t            ep_handle;

  // added infrastructure
  iWARPEM_Connection_Flags_t  ConnectedFlag;
#ifdef WITH_CNK_ROUTER
  iWARPEM_Connection_Type_t   ConnType;
  uint16_t                    ClientId;
#endif
  int                         ConnFd;
  int                         OtherEPNodeId;

  iWARPEM_Object_WR_Queue_t   RecvWrQueue;

    // Send for sends and writes
//   iWARPEM_Object_EndPoint_t* mNext;
//   iWARPEM_Object_EndPoint_t* mPrev;
};


#ifdef WITH_CNK_ROUTER
// metadata of cnk forwarding router
struct iWARPEM_Router_Info_t
{
  int RouterID;
  iWARPEM_SocketConnect_t SocketInfo;
};
#endif

// NEEDED FOR ASYNCRONOUS CONNECTIONS
struct iWARPEM_Object_ConReqInfo_t
{
  int ConnFd;
#ifdef WITH_CNK_ROUTER
  iWARPEM_Router_Info_t *RouterInfo;
  uint16_t ClientId;
#endif
};

struct iWARPEM_Object_Accept_t
{
  // record all the args passed to it_listen_create
  it_ia_handle_t      ia_handle;
  size_t              spigot_id;
  it_evd_handle_t     connect_evd;  // this is where connect requests will be posted
  it_listen_flags_t   flags;
  it_conn_qual_t      conn_qual;
  it_listen_handle_t  listen_handle;
  // added info about listen infrastructure
  int       mListenSockFd;
  pthread_t mAcceptThreadId;
};


#ifndef WITH_CNK_ROUTER
static
inline
iWARPEM_Status_t
RecvRaw( iWARPEM_Object_EndPoint_t *aEP, char * buff, int len, int* wlen )
{
  return read_from_socket( aEP->ConnFd, buff, len, wlen );
}

static
inline
iWARPEM_Status_t
SendMsg( iWARPEM_Object_EndPoint_t *aEP, char * buff, int len, int* wlen, const bool aFlush = false );
{
  return write_to_socket( aEP->ConnFd, buff, len, wlen );
}

static
inline
iWARPEM_Status_t
SendVec( iWARPEM_Object_EndPoint_t *aEP, struct iovec *iov, int iov_count, int* wlen, const bool aFlush = false )
{
  return write_to_socket_writev( aEP->ConnFd, iov, iov_count, wlen );
}

#endif


#endif /* IT_API_IWARPEM_TYPES_HPP_ */
