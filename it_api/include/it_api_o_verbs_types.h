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

#ifndef __IT_API_O_VERBS_TYPES_H__
#define __IT_API_O_VERBS_TYPES_H__

#include <pthread.h>

#define IT_API_O_VERBS_LISTEN_BACKLOG    2048  
#define IT_API_O_VERBS_MAX_SGE_LIST_SIZE 64

#ifndef IT_API_O_VERBS_TYPES_LOG
#define IT_API_O_VERBS_TYPES_LOG ( 0 )
#endif

typedef enum 
  {
    POST_RECV = 0x0001,
    POST_SEND = 0x0002
  } it_api_o_verbs_post_opcode_t;

typedef enum 
  {
    CQ_RECV 	   = 0x0001,
    CQ_SEND 	   = 0x0002,
    CQ_UNINITIALIZED = 0x0003  
  } it_api_o_verbs_cq_dto_type_t;

/************************************************
 * Wrapped structures
 ************************************************/
struct it_api_o_verbs_device_mgr_t
{
  struct rdma_event_channel	*cm_channel;
  struct rdma_cm_event		*cm_event;

  struct ibv_context**           devices;
  int                            devices_count;    
};

struct it_api_o_verbs_pd_mgr_t
{
  it_api_o_verbs_device_mgr_t*  device;

  struct ibv_pd**               PDs;
};

struct it_api_o_verbs_mr_record_t
{
  struct ibv_mr*  mr;
};

struct it_api_o_verbs_mr_mgr_t
{
  it_api_o_verbs_pd_mgr_t*     pd;

  void*                        addr;
  uint32_t                     length;

  it_api_o_verbs_mr_record_t*  MRs;

  enum  ibv_access_flags access;
};

struct it_api_o_verbs_aevd_mgr_t
{
  it_api_o_verbs_device_mgr_t*  mDevice;  

  // Each device gets a queue
  pthread_t*          mAffQueuesTIDs;
  itov_event_queue_t* mAffQueues;
  it_api_o_verbs_cq_processing_thread_args_t* mAffThreadArgs;

  // There's one communication event queue for all devices
  pthread_t           mCMQueueTID;
  itov_event_queue_t  mCMQueue;
  it_api_o_verbs_cq_processing_thread_args_t  mCMThreadArgs;

  // Each device gets a queue
  pthread_mutex_t*    mSendCQReadyMutexes;
  pthread_t*          mSendQueuesTIDs;
  itov_event_queue_t* mSendQueues;
  it_api_o_verbs_cq_processing_thread_args_t* mSendThreadArgs;

  pthread_mutex_t*    mRecvCQReadyMutexes;
  pthread_t*          mRecvQueuesTIDs;
  itov_event_queue_t* mRecvQueues;
  it_api_o_verbs_cq_processing_thread_args_t* mRecvThreadArgs;

  pthread_cond_t      mMainCond;
  pthread_mutex_t     mEventCounterMutex;
  volatile int        mEventCounter;

  void
  Init( it_api_o_verbs_device_mgr_t*  aDevice )
  {

    pthread_cond_init( & mMainCond, NULL );
    pthread_mutex_init( & mEventCounterMutex, NULL );
    mEventCounter = 0;

    mDevice = aDevice;

    int DeviceCount = mDevice->devices_count;

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

    int ThreadArgsSize = sizeof( it_api_o_verbs_cq_processing_thread_args_t ) * DeviceCount;
    mAffThreadArgs = 
      (it_api_o_verbs_cq_processing_thread_args_t *) malloc( ThreadArgsSize );
    StrongAssertLogLine( mAffThreadArgs )
      << "ERROR: ThreadArgsSize: " << ThreadArgsSize
      << EndLogLine;

    mSendThreadArgs = 
      (it_api_o_verbs_cq_processing_thread_args_t *) malloc( ThreadArgsSize );
    StrongAssertLogLine( mSendThreadArgs )
      << "ERROR: ThreadArgsSize: " << ThreadArgsSize
      << EndLogLine;

    mRecvThreadArgs = 
      (it_api_o_verbs_cq_processing_thread_args_t *) malloc( ThreadArgsSize );
    StrongAssertLogLine( mRecvThreadArgs )
      << "ERROR: ThreadArgsSize: " << ThreadArgsSize
      << EndLogLine;

    mCMQueue.Init( ITAPI_O_VERBS_QUEUE_MAX_COUNT );
    for( int i = 0; i < DeviceCount; i++ )
      {
        mAffQueuesTIDs[ i ] = (pthread_t)0;
        mSendQueuesTIDs[ i ] = (pthread_t)0;
        mRecvQueuesTIDs[ i ] = (pthread_t)0;

        mAffQueues[ i ].Init( ITAPI_O_VERBS_QUEUE_MAX_COUNT );
        mSendQueues[ i ].Init( ITAPI_O_VERBS_QUEUE_MAX_COUNT );
        mRecvQueues[ i ].Init( ITAPI_O_VERBS_QUEUE_MAX_COUNT );	

        pthread_mutex_init( & mSendCQReadyMutexes[ i ], NULL );
        pthread_mutex_lock( & mSendCQReadyMutexes[ i ] );

        pthread_mutex_init( & mRecvCQReadyMutexes[ i ], NULL );
        pthread_mutex_lock( & mRecvCQReadyMutexes[ i ] );
      }
  }
};

struct it_api_o_verbs_cq_mgr_t
{
  it_api_o_verbs_aevd_mgr_t*    aevd;

  it_api_o_verbs_device_mgr_t*  device;  

  it_api_o_verbs_cq_dto_type_t  dto_type;

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
      << "it_api_o_verbs_cq_mgr_t::SetCq(): ERROR: "
      << " device_ordinal: " << device_ordinal
      << " device->devices_count: " << device->devices_count
      << EndLogLine;

    cq.cq[ device_ordinal ] = aCq;
  }
};

struct it_api_o_verbs_qp_mgr_t
{
  int              device_ord;

  it_ep_attributes_t       ep_attr;

  struct rdma_cm_id *cm_conn_id;

  it_api_o_verbs_pd_mgr_t* pd;

  it_api_o_verbs_cq_mgr_t* send_cq;
  it_api_o_verbs_cq_mgr_t* recv_cq;
  it_api_o_verbs_cq_mgr_t* cm_cq;

  struct ibv_qp*           qp;

  int addr_resolved;
  int route_resolved;
};

struct it_api_o_verbs_context_queue_elem_t
{
  it_dto_cookie_t                     cookie;
  it_api_o_verbs_qp_mgr_t*            qp;

  it_api_o_verbs_context_queue_elem_t* next;

  void
  Init( it_dto_cookie_t&             aCookie,
        it_api_o_verbs_qp_mgr_t*     aQp )
  {
    CookieAssign(&cookie, &aCookie);
    /* cookie.mFirst = aCookie.mFirst; */
    /* cookie.mSecond = aCookie.mSecond; */
    qp     = aQp;
  }
};

struct it_api_o_verbs_context_queue_t
{
  it_api_o_verbs_context_queue_elem_t* head;

  int                                  elem_buffer_count;
  it_api_o_verbs_context_queue_elem_t* elem_buffer;

  pthread_mutex_t                      mQueueAccessMutex;

  void
  Init( int aCount )
  {
    elem_buffer_count = aCount;
    elem_buffer = (it_api_o_verbs_context_queue_elem_t *) 
      malloc( elem_buffer_count * sizeof( it_api_o_verbs_context_queue_elem_t ) );

    StrongAssertLogLine( elem_buffer )
      << "Init(): ERROR: failed to allocate: "
      << " size: " << (elem_buffer_count * sizeof( it_api_o_verbs_context_queue_elem_t ))
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
  Push(it_api_o_verbs_context_queue_elem_t* elem)
  {
    pthread_mutex_lock( & mQueueAccessMutex );

    BegLogLine( IT_API_O_VERBS_TYPES_LOG )
      << "Push() Entering "
      << EndLogLine;

    elem->next = head;
    head = elem;

    pthread_mutex_unlock( & mQueueAccessMutex );
  }

  it_api_o_verbs_context_queue_elem_t*
  Pop()
  {
    pthread_mutex_lock( & mQueueAccessMutex );

    BegLogLine( IT_API_O_VERBS_TYPES_LOG )
      << "Pop() Entering "
      << EndLogLine;

    it_api_o_verbs_context_queue_elem_t * rc = head;

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
#endif
