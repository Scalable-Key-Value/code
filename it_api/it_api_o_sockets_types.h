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

#ifndef __IT_API_O_SOCKETS_TYPES_H__
#define __IT_API_O_SOCKETS_TYPES_H__

#include <pthread.h>

#define IT_API_O_SOCKETS_LISTEN_BACKLOG    2048
#define IT_API_O_SOCKETS_MAX_SGE_LIST_SIZE 64

#ifndef IT_API_O_SOCKETS_TYPES_LOG
#define IT_API_O_SOCKETS_TYPES_LOG ( 0 )
#endif

#ifndef FXLOG_IT_API_O_SOCKETS_TYPES
#define FXLOG_IT_API_O_SOCKETS_TYPES ( 0 )
#endif

typedef enum 
  {
    POST_RECV = 0x0001,
    POST_SEND = 0x0002
  } it_api_o_sockets_post_opcode_t;

typedef enum 
  {
    CQ_RECV 	   = 0x0001,
    CQ_SEND 	   = 0x0002,
    CQ_UNINITIALIZED = 0x0003  
  } it_api_o_sockets_cq_dto_type_t;

/************************************************
 * Wrapped structures
 ************************************************/
struct it_api_o_sockets_device_mgr_t
{
  struct rdma_event_channel	*cm_channel;
  struct rdma_cm_event		*cm_event;

  struct ibv_context**           devices;
  int                            devices_count;    
};

struct it_api_o_sockets_pd_mgr_t
{
  it_api_o_sockets_device_mgr_t*  device;

  struct ibv_pd**               PDs;
};

struct it_api_o_sockets_mr_record_t
{
  struct ibv_mr*  mr;
};

struct it_api_o_sockets_mr_mgr_t
{
  it_api_o_sockets_pd_mgr_t*     pd;

  void*                        addr;
  uint32_t                     length;

  it_api_o_sockets_mr_record_t*  MRs;

//  enum  ibv_access_flags access;
};
struct iWARPEM_Object_EventQueue_t ;

struct it_api_o_sockets_aevd_mgr_t
{
  it_api_o_sockets_device_mgr_t*  mDevice;

  // Each device gets a queue
  pthread_t*          mAffQueuesTIDs;
  itov_event_queue_t* mAffQueues;
  it_api_o_sockets_cq_processing_thread_args_t* mAffThreadArgs;

  // There's one communication event queue for all devices
  pthread_t           mCMQueueTID;
  itov_event_queue_t  mCMQueue;
  it_api_o_sockets_cq_processing_thread_args_t  mCMThreadArgs;

  // Each device gets a queue
  pthread_mutex_t*    mSendCQReadyMutexes;
  pthread_t*          mSendQueuesTIDs;
  itov_event_queue_t* mSendQueues;
  it_api_o_sockets_cq_processing_thread_args_t* mSendThreadArgs;

  pthread_mutex_t*    mRecvCQReadyMutexes;
  pthread_t*          mRecvQueuesTIDs;
  itov_event_queue_t* mRecvQueues;
  it_api_o_sockets_cq_processing_thread_args_t* mRecvThreadArgs;

  pthread_cond_t      mMainCond;
  pthread_mutex_t     mEventCounterMutex;
  volatile int        mEventCounter;

  iWARPEM_Object_EventQueue_t *mCMMEVQObj ;

  void
  Init( it_api_o_sockets_device_mgr_t*  aDevice )
  {

      BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
          << "this=" << this
          << EndLogLine ;
      mCMMEVQObj = NULL ;
    pthread_cond_init( & mMainCond, NULL );
    pthread_mutex_init( & mEventCounterMutex, NULL );
    mEventCounter = 0;

    mDevice = aDevice;

//    int DeviceCount = mDevice->devices_count;
    int DeviceCount = 1 ;

    int QueueSize = sizeof( itov_event_queue_t ) * DeviceCount;

    mAffQueues = (itov_event_queue_t *) malloc( QueueSize );
    StrongAssertLogLine( mAffQueues )
      << "ERROR: QueueSize: " << QueueSize
      << EndLogLine;

    mSendQueues = (itov_event_queue_t *) malloc( QueueSize );
    StrongAssertLogLine( mSendQueues )
      << "ERROR: QueueSize: " << QueueSize
      << EndLogLine;

    mRecvQueues = (itov_event_queue_t *) malloc( QueueSize );
    StrongAssertLogLine( mRecvQueues )
      << "ERROR: QueueSize: " << QueueSize
      << EndLogLine;

    int MutexListSize = sizeof( pthread_mutex_t ) * DeviceCount;
    mSendCQReadyMutexes = (pthread_mutex_t *) malloc( MutexListSize );
    StrongAssertLogLine( mSendCQReadyMutexes )
      << "ERROR: MutexListSize: " << MutexListSize
      << EndLogLine;

    mRecvCQReadyMutexes = (pthread_mutex_t *) malloc( MutexListSize );
    StrongAssertLogLine( mRecvCQReadyMutexes )
      << "ERROR: MutexListSize: " << MutexListSize
      << EndLogLine;    

    int PthreadIdSize = sizeof( pthread_t ) * DeviceCount;
    mAffQueuesTIDs = (pthread_t *) malloc( PthreadIdSize );
    StrongAssertLogLine( mAffQueuesTIDs )
      << "ERROR: PthreadIdSize: " << PthreadIdSize
      << EndLogLine;

    mSendQueuesTIDs = (pthread_t *) malloc( PthreadIdSize );
    StrongAssertLogLine( mSendQueuesTIDs )
      << "ERROR: PthreadIdSize: " << PthreadIdSize
      << EndLogLine;

    mRecvQueuesTIDs = (pthread_t *) malloc( PthreadIdSize );
    StrongAssertLogLine( mRecvQueuesTIDs )
      << "ERROR: PthreadIdSize: " << PthreadIdSize
      << EndLogLine;

    int ThreadArgsSize = sizeof( it_api_o_sockets_cq_processing_thread_args_t ) * DeviceCount;
    mAffThreadArgs = 
      (it_api_o_sockets_cq_processing_thread_args_t *) malloc( ThreadArgsSize );
    StrongAssertLogLine( mAffThreadArgs )
      << "ERROR: ThreadArgsSize: " << ThreadArgsSize
      << EndLogLine;

    mSendThreadArgs = 
      (it_api_o_sockets_cq_processing_thread_args_t *) malloc( ThreadArgsSize );
    StrongAssertLogLine( mSendThreadArgs )
      << "ERROR: ThreadArgsSize: " << ThreadArgsSize
      << EndLogLine;

    mRecvThreadArgs = 
      (it_api_o_sockets_cq_processing_thread_args_t *) malloc( ThreadArgsSize );
    StrongAssertLogLine( mRecvThreadArgs )
      << "ERROR: ThreadArgsSize: " << ThreadArgsSize
      << EndLogLine;

    mCMQueue.Init( ITAPI_O_SOCKETS_QUEUE_MAX_COUNT );
    for( int i = 0; i < DeviceCount; i++ )
      {
        mAffQueuesTIDs[ i ] = (pthread_t)0;
        mSendQueuesTIDs[ i ] = (pthread_t)0;
        mRecvQueuesTIDs[ i ] = (pthread_t)0;

        mAffQueues[ i ].Init( ITAPI_O_SOCKETS_QUEUE_MAX_COUNT );
        mSendQueues[ i ].Init( ITAPI_O_SOCKETS_QUEUE_MAX_COUNT );
        mRecvQueues[ i ].Init( ITAPI_O_SOCKETS_QUEUE_MAX_COUNT );

        pthread_mutex_init( & mSendCQReadyMutexes[ i ], NULL );
        pthread_mutex_lock( & mSendCQReadyMutexes[ i ] );

        pthread_mutex_init( & mRecvCQReadyMutexes[ i ], NULL );
        pthread_mutex_lock( & mRecvCQReadyMutexes[ i ] );
      }
  }
};

struct it_api_o_sockets_cq_mgr_t
{
  it_api_o_sockets_aevd_mgr_t*    aevd;

  it_api_o_sockets_device_mgr_t*  device;

  it_api_o_sockets_cq_dto_type_t  dto_type;

  it_event_type_t  event_number;
  size_t           queue_size;

  union 
  {
    struct rdma_event_channel	*cm_channel;
    struct ibv_cq               **cq;
  } cq;

  void
  SetCq( int device_ordinal, struct ibv_cq* aCq )
  {
    StrongAssertLogLine( device_ordinal >= 0 && device_ordinal < device->devices_count )
      << "it_api_o_sockets_cq_mgr_t::SetCq(): ERROR: "
      << " device_ordinal: " << device_ordinal
      << " device->devices_count: " << device->devices_count
      << EndLogLine;

    cq.cq[ device_ordinal ] = aCq;
  }
};

struct it_api_o_sockets_qp_mgr_t
{
  int              device_ord;

  it_ep_attributes_t       ep_attr;

  struct rdma_cm_id *cm_conn_id;

  it_api_o_sockets_pd_mgr_t* pd;

  it_api_o_sockets_cq_mgr_t* send_cq;
  it_api_o_sockets_cq_mgr_t* recv_cq;
  it_api_o_sockets_cq_mgr_t* cm_cq;

  struct ibv_qp*           qp;

  int addr_resolved;
  int route_resolved;
};

struct it_api_o_sockets_context_queue_elem_t
{
  it_dto_cookie_t                     cookie;
  it_api_o_sockets_qp_mgr_t*            qp;

  it_api_o_sockets_context_queue_elem_t* next;

  void
  Init( it_dto_cookie_t&             aCookie,
        it_api_o_sockets_qp_mgr_t*     aQp )
  {
    CookieAssign(&cookie, &aCookie);
    /* cookie.mFirst = aCookie.mFirst; */
    /* cookie.mSecond = aCookie.mSecond; */
    qp     = aQp;
  }
};

struct it_api_o_sockets_context_queue_t
{
  it_api_o_sockets_context_queue_elem_t* head;

  int                                  elem_buffer_count;
  it_api_o_sockets_context_queue_elem_t* elem_buffer;

  pthread_mutex_t                      mQueueAccessMutex;

  void
  Init( int aCount )
  {
    elem_buffer_count = aCount;
    elem_buffer = (it_api_o_sockets_context_queue_elem_t *)
      malloc( elem_buffer_count * sizeof( it_api_o_sockets_context_queue_elem_t ) );

    StrongAssertLogLine( elem_buffer )
      << "Init(): ERROR: failed to allocate: "
      << " size: " << (elem_buffer_count * sizeof( it_api_o_sockets_context_queue_elem_t ))
      << EndLogLine;

    for( int i = 0; i < elem_buffer_count-1; i++ )
      {
        elem_buffer[ i ].next = & (elem_buffer[ i + 1 ]);
      }

    elem_buffer[ elem_buffer_count-1 ].next = NULL;

    head = elem_buffer;

    pthread_mutex_init( & mQueueAccessMutex, NULL );
  }

  void
  Push(it_api_o_sockets_context_queue_elem_t* elem)
  {
    pthread_mutex_lock( & mQueueAccessMutex );

    BegLogLine( IT_API_O_SOCKETS_TYPES_LOG )
      << "Push() Entering "
      << EndLogLine;

    elem->next = head;
    head = elem;

    pthread_mutex_unlock( & mQueueAccessMutex );
  }

  it_api_o_sockets_context_queue_elem_t*
  Pop()
  {
    pthread_mutex_lock( & mQueueAccessMutex );

    BegLogLine( IT_API_O_SOCKETS_TYPES_LOG )
      << "Pop() Entering "
      << EndLogLine;

    it_api_o_sockets_context_queue_elem_t * rc = head;

    if( head != NULL )
      head = head->next;

    pthread_mutex_unlock( & mQueueAccessMutex );

    return rc;
  }

  void
  Finalize()
  {
    free( elem_buffer );
    elem_buffer = NULL;
  }
};

struct iWARPEM_Private_Data_t
{
  int           mLen;
  unsigned char mData[ IT_MAX_PRIV_DATA ]; /* NFD-Moved - see it_event_cm_any_t */
};

typedef enum
  {
    iWARPEM_UNKNOWN_REQ_TYPE = 0,
    iWARPEM_DTO_SEND_TYPE = 1,
    iWARPEM_DTO_RECV_TYPE,
    iWARPEM_DTO_RDMA_WRITE_TYPE,
    iWARPEM_DTO_RDMA_READ_REQ_TYPE,
    iWARPEM_DTO_RDMA_READ_RESP_TYPE,
    iWARPEM_DTO_RDMA_READ_CMPL_TYPE,
    iWARPEM_DISCONNECT_REQ_TYPE,
    iWARPEM_DISCONNECT_RESP_TYPE,

    iWARPEM_SOCKET_CONNECT_REQ_TYPE,
    iWARPEM_SOCKET_CONNECT_RESP_TYPE ,
    iWARPEM_SOCKET_CLOSE_REQ_TYPE,
    iWARPEM_SOCKET_CLOSE_TYPE,

    iWARPEM_KERNEL_CONNECT_TYPE
  } iWARPEM_Msg_Type_t;

static
const char* iWARPEM_Msg_Type_to_string( int /* iWARPEM_Msg_Type_t */ aType )
{
  switch( aType )
    {
    case iWARPEM_DTO_SEND_TYPE:           { return "iWARPEM_DTO_SEND_TYPE"; }
    case iWARPEM_DTO_RECV_TYPE:           { return "iWARPEM_DTO_RECV_TYPE"; }
    case iWARPEM_DTO_RDMA_WRITE_TYPE:     { return "iWARPEM_DTO_RDMA_WRITE_TYPE"; }
    case iWARPEM_DTO_RDMA_READ_REQ_TYPE:  { return "iWARPEM_DTO_RDMA_READ_REQ_TYPE"; }
    case iWARPEM_DTO_RDMA_READ_RESP_TYPE: { return "iWARPEM_DTO_RDMA_READ_RESP_TYPE"; }
    case iWARPEM_DTO_RDMA_READ_CMPL_TYPE: { return "iWARPEM_DTO_RDMA_READ_CMPL_TYPE"; }
    case iWARPEM_DISCONNECT_REQ_TYPE:     { return "iWARPEM_DISCONNECT_REQ_TYPE"; }
    case iWARPEM_DISCONNECT_RESP_TYPE:    { return "iWARPEM_DISCONNECT_RESP_TYPE"; }
    case iWARPEM_SOCKET_CONNECT_REQ_TYPE: { return "iWARPEM_SOCKET_CONNECT_REQ_TYPE"; }
    case iWARPEM_SOCKET_CONNECT_RESP_TYPE:{ return "iWARPEM_SOCKET_CONNECT_RESP_TYPE"; }
    case iWARPEM_SOCKET_CLOSE_REQ_TYPE:   { return "iWARPEM_SOCKET_CLOSE_REQ_TYPE"; }
    case iWARPEM_SOCKET_CLOSE_TYPE:       { return "iWARPEM_SOCKET_CLOSE_TYPE"; }
    case iWARPEM_KERNEL_CONNECT_TYPE:     { return "iWARPEM_KERNEL_CONNECT_TYPE"; }
    default:
      {
        BegLogLine(1)
            << "aType=" << aType
            << " unknown"
            << EndLogLine ;
        return "iWARPEM_UNKNOWN_REQ_TYPE" ;
//        StrongAssertLogLine( 0 )
//          << "iWARPEM_Msg_Type_to_string(): "
//          << " aType: " << aType
//          << EndLogLine;
      }
    }

  return NULL;
}

struct iWARPEM_RDMA_Write_t
{
  // Addr  (absolute or relative) in the memory region
  // on the remote host
  it_rdma_addr_t   mRMRAddr;

  // Pointer to a memory region
  it_rmr_context_t mRMRContext;
};

struct iWARPEM_RDMA_Read_Req_t
{
  // Addr  (absolute or relative) in the memory region
  // on the remote host
  it_rdma_addr_t                 mRMRAddr;

  // Pointer to a memory region
  it_rmr_context_t               mRMRContext;

  int                            mDataToReadLen;

  void *                         mPrivatePtr; // iWARPEM_Object_WorkRequest_t * mRdmaReadClientWorkRequestState;
};

struct iWARPEM_RDMA_Read_Resp_t
{
  void *                         mPrivatePtr; // iWARPEM_Object_WorkRequest_t * mRdmaReadClientWorkRequestState;
};

struct iWARPEM_Send_t
{
  // place holder
};

struct iWARPEM_Recv_t
{
  // place holder
};

struct iWARPEM_SocketConnect_t
{
  // Might change these to appropriate sockaddr types later
  unsigned int ipv4_address ;
  unsigned short ipv4_port ;
};

struct iWARPEM_SocketConnect_Resp_t
  {
//    struct iWARPEM_Private_Data_t PrivateData ;
  };
struct iWARPEM_SocketClose_t
{
  // place holder
};
struct iWARPEM_KernelConnect_t
  {
  uint64_t mRouterBuffer_addr ;
  uint32_t mRoutermemreg_lkey ;
  unsigned int mClientRank ;
  };

struct iWARPEM_Message_Hdr_t
{
  iWARPEM_Msg_Type_t     mMsg_Type;
  unsigned int                    mTotalDataLen;

#if IT_API_CHECKSUM
  uint64_t               mChecksum;
#endif

  union
  {
    iWARPEM_RDMA_Write_t      mRdmaWrite;
    iWARPEM_RDMA_Read_Req_t   mRdmaReadReq;
    iWARPEM_RDMA_Read_Resp_t  mRdmaReadResp;
    iWARPEM_Send_t            mSend;
    iWARPEM_Recv_t            mRecv;
    iWARPEM_SocketConnect_t   mSocketConnect;
    iWARPEM_SocketConnect_Resp_t   mSocketConnectResp;
    iWARPEM_SocketClose_t     mSocketClose;
    iWARPEM_KernelConnect_t   mKernelConnect ;
  } mOpType;
  void EndianConvert(void)
    {
      mMsg_Type=(iWARPEM_Msg_Type_t)ntohl(mMsg_Type) ;
      BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
        << "mMsg_Type endian converted to " << mMsg_Type
        << EndLogLine ;
    }
};

static void report_it_rdma_addr_t(const it_rdma_addr_t& mRMRAddr)
  {
    BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
      << "mRMRAddr=0x" << (void *) mRMRAddr
      << EndLogLine ;
  }
static void report_it_rmr_context_t(const it_rmr_context_t& mRMRContext)
  {
    BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
        << "mRMRContext=0x" << (void *) mRMRContext
        << EndLogLine ;
  }
static void report_Send(const iWARPEM_Send_t& mSend)
  {

  }
static void report_Recv(const iWARPEM_Recv_t& mRecv)
  {

  }
static void report_RDMA_Write(const iWARPEM_RDMA_Write_t& mRdmaWrite)
  {
    report_it_rdma_addr_t(mRdmaWrite.mRMRAddr) ;
    report_it_rmr_context_t(mRdmaWrite.mRMRContext) ;
  }
static void report_RDMA_Read_Req(const iWARPEM_RDMA_Read_Req_t& mRdmaReadReq)
  {
    report_it_rdma_addr_t(mRdmaReadReq.mRMRAddr) ;
    report_it_rmr_context_t(mRdmaReadReq.mRMRContext) ;
    BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
      << "mDataToReadLen=" << mRdmaReadReq.mDataToReadLen
      << " mPrivatePtr=" << mRdmaReadReq.mPrivatePtr
      << EndLogLine ;
  }
static void report_RDMA_Read_Resp(const iWARPEM_RDMA_Read_Resp_t& mRdmaReadResp)
  {
    BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
          << " mPrivatePtr=" << mRdmaReadResp.mPrivatePtr
          << EndLogLine ;
  }
static void report_RDMA_Read_Cmpl(void)
  {

  }
static void report_Disconnect_Req(void)
  {

  }
static void report_Disconnect_Resp(void)
  {

  }
static void report_Socket_Connect(void)
  {

  }
static void report_Socket_Connect_Resp(void)
  {

  }
static void report_Socket_Close_Req(void)
  {

  }
static void report_Socket_Close(void)
  {

  }
static void report_Kernel_Connect(const iWARPEM_KernelConnect_t& aKernelConnect)
  {
    BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
          << "mRouterBuffer_addr=0x" << (void *) aKernelConnect.mRouterBuffer_addr
          << " mRoutermemreg_lkey=0x" << (void *) aKernelConnect.mRoutermemreg_lkey
          << " mClientRank=" << aKernelConnect.mClientRank
          << EndLogLine ;
  }

static void report_hdr(const iWARPEM_Message_Hdr_t& Hdr)
  {
    BegLogLine(FXLOG_IT_API_O_SOCKETS_TYPES)
        << "mMsg_Type=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
        << " mTotalDataLen=" << Hdr.mTotalDataLen
        << EndLogLine ;
    switch ( Hdr.mMsg_Type)
      {
    case iWARPEM_DTO_SEND_TYPE: report_Send(Hdr.mOpType.mSend) ;
      StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
        << "Message length was " << Hdr.mTotalDataLen
        << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
        << EndLogLine ;
      break ;
    case iWARPEM_DTO_RECV_TYPE: report_Recv(Hdr.mOpType.mRecv) ;
      StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
        << "Message length was " << Hdr.mTotalDataLen
        << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
        << EndLogLine ;
      break ;
    case iWARPEM_DTO_RDMA_WRITE_TYPE: report_RDMA_Write(Hdr.mOpType.mRdmaWrite) ; break ;
    case iWARPEM_DTO_RDMA_READ_REQ_TYPE: report_RDMA_Read_Req(Hdr.mOpType.mRdmaReadReq) ;
      StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
        << "Message length was " << Hdr.mTotalDataLen
        << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
        << EndLogLine ;
      break ;
    case iWARPEM_DTO_RDMA_READ_RESP_TYPE: report_RDMA_Read_Resp(Hdr.mOpType.mRdmaReadResp) ; break ;
    case iWARPEM_DTO_RDMA_READ_CMPL_TYPE: report_RDMA_Read_Cmpl() ;
//      StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
//        << "Message length was " << Hdr.mTotalDataLen
//        << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
//        << EndLogLine ;
      break ;
    case iWARPEM_DISCONNECT_REQ_TYPE: report_Disconnect_Req() ;
    StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
      break ;
    case iWARPEM_DISCONNECT_RESP_TYPE: report_Disconnect_Resp() ;
    StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
      break ;
    case iWARPEM_SOCKET_CONNECT_REQ_TYPE: report_Socket_Connect() ;
    StrongAssertLogLine(Hdr.mTotalDataLen == sizeof( iWARPEM_Private_Data_t ) )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be " << sizeof( iWARPEM_Private_Data_t )
      << " . mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
    case iWARPEM_SOCKET_CONNECT_RESP_TYPE: report_Socket_Connect_Resp() ;
    StrongAssertLogLine(Hdr.mTotalDataLen == sizeof( iWARPEM_Private_Data_t ) )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be " << sizeof( iWARPEM_Private_Data_t )
      << " . mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
      break ;
    case iWARPEM_SOCKET_CLOSE_REQ_TYPE: report_Socket_Close_Req() ;
    StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be 0 . mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
      break ;
    case iWARPEM_SOCKET_CLOSE_TYPE: report_Socket_Close() ;
    StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be 0 . mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
      break ;
    case iWARPEM_KERNEL_CONNECT_TYPE: report_Kernel_Connect(Hdr.mOpType.mKernelConnect) ;
    StrongAssertLogLine(Hdr.mTotalDataLen == 0 )
      << "Message length was " << Hdr.mTotalDataLen
      << " should be 0. mMsgType=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
      << EndLogLine ;
      break ;
    default:
      StrongAssertLogLine(0)
        << "Unexpected message type=" << iWARPEM_Msg_Type_to_string(Hdr.mMsg_Type)
        << " mTotalDataLen=" <<  Hdr. mTotalDataLen
        << EndLogLine ;
      }
  }

#endif
