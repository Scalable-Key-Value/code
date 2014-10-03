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

#include <Trace.hpp>
#include <it_api_o_verbs_thread.h>

#ifndef IT_API_O_VERBS_THREAD_LOG
#define IT_API_O_VERBS_THREAD_LOG ( 0 )
#endif

#ifndef IT_API_O_VERBS_THREAD_TRACE
#define IT_API_O_VERBS_THREAD_TRACE ( 0 )
#endif

static TraceClient gITAPI_RDMA_READ_AT_VERBS;
extern int gTraceRank;

void* it_api_o_verbs_aff_processing_thread( void* aArgs )
{
  it_api_o_verbs_cq_processing_thread_args_t* Args = 
    (it_api_o_verbs_cq_processing_thread_args_t *) aArgs;

  itov_event_queue_t* EventCmplQueue    = Args->mEventCmplQueue;
  struct ibv_context* Device            = Args->mDevice;

  pthread_cond_t*     MainCond          = Args->mMainCond;
  pthread_mutex_t*    EventCounterMutex = Args->mEventCounterMutex;
  volatile int*       EventCounter      = Args->mEventCounter;

  while( 1 )
    {
      struct ibv_async_event rdma_event;
      int ret = ibv_get_async_event( Device, & rdma_event );

      if( ret )
        {
          StrongAssertLogLine( 0 )
            << "ERROR: ibv_get_async_event failed " 
            << " ret: " << ret
            << " async_fd: " << Device->async_fd
            << " errno: " << errno
            << EndLogLine;	      
        }

      BegLogLine( IT_API_O_VERBS_THREAD_LOG )
        << "ERROR: event_type: " << rdma_event.event_type
        << EndLogLine;

      // NOTE: Avoid doing a copy by accessing the it_event_t 
      // in the queue directly
      it_event_t event;

      it_status_t istatus = it_api_o_verbs_process_async_event( & rdma_event,
                                                                & event );

      AssertLogLine( istatus == IT_SUCCESS )
        << "ERROR: "
        << " istatus: " << istatus
        << EndLogLine;

      ibv_ack_async_event( & rdma_event );

      EventCmplQueue->Enqueue( event );

      // Signal the main thread
      pthread_mutex_lock( EventCounterMutex );
      (*EventCounter)++;
      pthread_cond_signal( MainCond );
      pthread_mutex_unlock( EventCounterMutex );
    }
}

void* it_api_o_verbs_cm_processing_thread( void* aArgs )
{
  it_api_o_verbs_cq_processing_thread_args_t* Args = 
    (it_api_o_verbs_cq_processing_thread_args_t *) aArgs;

  itov_event_queue_t* EventCmplQueue   = Args->mEventCmplQueue;
  struct ibv_context* Device           = Args->mDevice;

  pthread_cond_t*     MainCond         = Args->mMainCond;

  pthread_mutex_t* EventCounterMutex   = Args->mEventCounterMutex;
  volatile int*    EventCounter        = Args->mEventCounter;

  struct rdma_event_channel* CmChannel = Args->mCmChannel;

  while( 1 )
    {
      struct rdma_cm_event *cm_event = NULL;
      int ret = rdma_get_cm_event( CmChannel, & cm_event );
      if( ret )
        {		  
          StrongAssertLogLine( 0 )
            << "ERROR: rdma_get_cm_event failed "
            << " ret: " << ret
            << EndLogLine;	      
        }

      it_event_t event;

      if( cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST )
        {
          it_conn_request_event_t * ConnReqEvent = (it_conn_request_event_t *) & event;

          it_status_t status = it_api_o_verbs_handle_cr_event( NULL, ConnReqEvent, cm_event );

          StrongAssertLogLine( status == IT_SUCCESS )
            << "ERROR: "
            << " status: " << status
            << EndLogLine;
        }
      else 
        {
          it_connection_event_t * ConnEvent = (it_connection_event_t *) & event;

          it_status_t status = it_api_o_verbs_handle_cm_event( NULL, ConnEvent, cm_event );

          StrongAssertLogLine( status == IT_SUCCESS )
            << "ERROR: "
            << " status: " << status
            << EndLogLine;
        }

      BegLogLine( IT_API_O_VERBS_THREAD_LOG )
        << "CM: event_type: " << cm_event->event
        << EndLogLine;

      ret = rdma_ack_cm_event( cm_event );
      if( ret )
        {		  
          StrongAssertLogLine( 0 )
            << "ERROR: rdma_ack_cm_event failed " 
            << " ret: " << ret
            << EndLogLine;	      
        }

      EventCmplQueue->Enqueue( event );      

      // Signal the main thread
      pthread_mutex_lock( EventCounterMutex );
      (*EventCounter)++;
      pthread_cond_signal( MainCond );
      pthread_mutex_unlock( EventCounterMutex );
    }
}

void* it_api_o_verbs_dto_processing_thread( void* aArgs )
{
  it_api_o_verbs_cq_processing_thread_args_t* Args = 
    (it_api_o_verbs_cq_processing_thread_args_t *) aArgs;


  itov_event_queue_t* EventCmplQueue = Args->mEventCmplQueue;
  pthread_cond_t*     MainCond       = Args->mMainCond;

  pthread_mutex_t*    CQReadyMutex   = Args->mCQReadyMutex;

  // This mutex is initiated to locked state
  // When the CQ is initialized by the main thread, 
  // The mutex is unlocked
  pthread_mutex_lock( CQReadyMutex );

  struct ibv_cq*           CQ             = Args->mCQ;
  struct ibv_comp_channel* CompChannel    = Args->mCompChannel;

  pthread_mutex_t*         EventCounterMutex = Args->mEventCounterMutex;
  volatile int*            EventCounter      = Args->mEventCounter;

  int ret = ibv_req_notify_cq( CQ, Args->mSolicitedEventsOnly );
  StrongAssertLogLine( ! ret ) 
    << "ERROR: failed to request notifications" 
    << EndLogLine;

  int CQEventsToAck = 0;
#define IBV_CQ_EVENTS_TO_ACK_MAX_COUNT 32

#define IBV_MAX_WC_EVENTS  64  
  struct ibv_wc wc_buffer[ IBV_MAX_WC_EVENTS ];

  while( 1 )
    {
      void* dst_cq_ctx;

      ret = ibv_get_cq_event( CompChannel, & CQ, &dst_cq_ctx );
      StrongAssertLogLine( !ret )
        << "it_api_o_verbs_send_processing_thread(): "
        << " ret: " << ret
        << EndLogLine;

      CQEventsToAck++;
      if( CQEventsToAck == IBV_CQ_EVENTS_TO_ACK_MAX_COUNT )
        {
          ibv_ack_cq_events( CQ, CQEventsToAck );
          CQEventsToAck = 0;
        }

      ret = ibv_req_notify_cq( CQ, Args->mSolicitedEventsOnly );
      StrongAssertLogLine( !ret )
        << "it_api_o_verbs_send_processing_thread(): "
        << " ret: " << ret
        << " errno: " << errno
        << EndLogLine;

      int try_again = 0;
      int event_count = 0;
      do
        {
          int cnt = ibv_poll_cq( CQ, IBV_MAX_WC_EVENTS, wc_buffer );

          StrongAssertLogLine( cnt >= 0 )
            << "ERROR: "
            << " cnt: " << cnt
            << " errno:" << errno
            << EndLogLine;

          try_again = ( cnt == IBV_MAX_WC_EVENTS ); 

          for( int i = 0; i < cnt; i++ )
            {
              struct ibv_wc* wc = & wc_buffer[ i ];

              if( wc->opcode == IBV_WC_RDMA_READ )
                {
                  gITAPI_RDMA_READ_AT_VERBS.HitOE( IT_API_O_VERBS_THREAD_TRACE,
                                                   "ITAPI_RDMA_READ_AT_VERBS",
                                                   gTraceRank,
                                                   gITAPI_RDMA_READ_AT_VERBS );
                }

              BegLogLine( IT_API_O_VERBS_THREAD_LOG )
                << "DTO Event: " 
                << " opcode: " << wc->opcode
                << " status: " << wc->status
                << " wr_id: " << (unsigned long long) wc->wr_id
                << EndLogLine;

              it_event_t event;
              it_dto_cmpl_event_t* DTOEvent = (it_dto_cmpl_event_t *) & event;

              it_status_t status = it_api_o_verbs_convert_wc_to_it_dto_event( NULL, wc, DTOEvent );
              StrongAssertLogLine( status == IT_SUCCESS )
                << "ERROR: "
                << " status: " << status
                << EndLogLine;	      

              EventCmplQueue->Enqueue( event );
              event_count++;
            }
        }
      while( try_again );

      // Signal the main thread
      pthread_mutex_lock( EventCounterMutex );
      (*EventCounter) += event_count;
      pthread_cond_signal( MainCond );
      pthread_mutex_unlock( EventCounterMutex );
    }
}

