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

#ifndef __IT_API_O_VERBS_THREAD_ROUTINES_HPP__
#define __IT_API_O_VERBS_THREAD_ROUTINES_HPP__

#include <FxLogger.hpp>

#include <rdma/rdma_cma.h>

extern "C"
{
#define ITAPI_ENABLE_V21_BINDINGS
#include <it_api.h>
};

#define ITAPI_O_VERBS_LOCKLESS_QUEUE  (    0 )
#include <ThreadSafeQueue.hpp>
#define ITAPI_O_VERBS_QUEUE_MAX_COUNT ( 8192 )
typedef ThreadSafeQueue_t< it_event_t, ITAPI_O_VERBS_LOCKLESS_QUEUE > itov_event_queue_t;

struct it_api_o_verbs_cq_processing_thread_args_t
{
  itov_event_queue_t*      mEventCmplQueue;

  struct ibv_context*      mDevice;

  pthread_cond_t*          mMainCond;
  pthread_mutex_t*         mEventCounterMutex;
  volatile int*            mEventCounter;

  pthread_mutex_t*         mCQReadyMutex;
  struct ibv_cq*           mCQ;

  struct ibv_comp_channel*   mCompChannel;

  struct rdma_event_channel* mCmChannel;  

  int mSolicitedEventsOnly;
};

#include <it_api_o_verbs_types.h>


void* it_api_o_verbs_aff_processing_thread( void* aArgs );
void* it_api_o_verbs_cm_processing_thread( void* aArgs );
void* it_api_o_verbs_dto_processing_thread( void* aArgs );


it_status_t
it_api_o_verbs_process_async_event( struct ibv_async_event* aRdmaEvent,
                                    it_event_t*             aITAffEvent );

it_status_t
it_api_o_verbs_handle_cr_event( it_api_o_verbs_cq_mgr_t* aCQ,
                                it_conn_request_event_t* aIT_Event, 
                                struct rdma_cm_event*    aRDMA_Event );

it_status_t
it_api_o_verbs_handle_cm_event( it_api_o_verbs_cq_mgr_t* aCQ,
                                it_connection_event_t*   aIT_Event, 
                                struct rdma_cm_event*    aRDMA_Event );

it_status_t
it_api_o_verbs_convert_wc_to_it_dto_event( it_api_o_verbs_cq_mgr_t* aCQ,
                                           struct ibv_wc*           aWc,
                                           it_dto_cmpl_event_t*     aEvent );
#endif
