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

#ifndef __SKV_SERVER_HPP__
#define __SKV_SERVER_HPP__

#include <mpi.h>
#include <pthread.h>

#include <common/skv_config.hpp>

// These are flags so that the polling 
// thread can differentiate between events
// Primarily needed for debugging
typedef enum
{
  SKV_POLL_UNAFF_EVD = 1,
  SKV_POLL_AFF_EVD,
  SKV_POLL_CMR_EVD,
  SKV_POLL_CMM_EVD,
  SKV_POLL_RQ_EVD,
  SKV_POLL_SQ_EVD,
  SKV_POLL_SEQ_EVD
} skv_poll_type_t;

static
const char* 
skv_poll_type_to_string( skv_poll_type_t aType )
{
  switch( aType )
  {
    case SKV_POLL_UNAFF_EVD: { return "SKV_POLL_UNAFF_EVD"; }
    case SKV_POLL_AFF_EVD: { return "SKV_POLL_AFF_EVD"; }
    case SKV_POLL_CMR_EVD: { return "SKV_POLL_CMR_EVD"; }
    case SKV_POLL_CMM_EVD: { return "SKV_POLL_CMM_EVD"; }
    case SKV_POLL_RQ_EVD: { return "SKV_POLL_RQ_EVD"; }
    case SKV_POLL_SQ_EVD: { return "SKV_POLL_SQ_EVD"; }
    case SKV_POLL_SEQ_EVD: { return "SKV_POLL_SEQ_EVD"; }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_poll_type_to_string():: ERROR:: "
        << " aType: " << aType
        << " Not supported"
        << EndLogLine;
      return "SKV_POLL_TYPE_UNKNOWN";
    }
  }
}

struct ThreadArgs
{
  double            mPadding[ 32 ];
  skv_poll_type_t  mEVDType;
  it_evd_handle_t   mEVDHandle;
  it_event_t*       mEventPtr;
  pthread_mutex_t*  mEventPresent;             
  pthread_mutex_t*  mReadyToPollNextEvent;
  double            mPadding1[ 32 ];
};

typedef enum
{
  SKV_SERVER_INTERNAL_EVENT_SRC_INDEX = 0,
  SKV_SERVER_NETWORK_EVENT_SRC_INDEX = 1,
  SKV_SERVER_COMMAND_EVENT_SRC_INDEX = 2,
  SKV_SERVER_LOCAL_KV_EVENT_SRC_INDEX = 3,

  // !!!! GIVING NO NUMBER HERE ASSUMES enum USES THE NEXT INTEGER VALUE BY DEFAULT !!!! 
  SKV_SERVER_EVENT_SOURCES   // always the last (== number of possible indices)
} skv_server_event_source_index_t;

class skv_server_t
{
public:
  // skv configuration container
  skv_configuration_t                         *mSKVConfiguration;
  
  skv_local_kv_t                               mLocalKV;

  // Local user event manager
  skv_server_internal_event_manager_if_t       mInternalEventManager;

  // network (IT) event manager
  skv_server_network_event_manager_if_t        mNetworkEventManager;

  // array of event sources
  skv_server_generic_event_source_t           *mEventSources[ SKV_SERVER_EVENT_SOURCES ];
  // max number of events per evt src according to priority (calculated during server Init() )
  int                                          mMaxEventCounts[ SKV_SERVER_EVENT_SOURCES ];
  // common denominator of given priorities, to calculate #event slots and to make sure priority counter wraps at fair value
  int                                          mPriorityCDN;


  // Thread Args
#define SKV_EVD_THREAD_COUNT 7  
  pthread_t   mEvdPollThreadIds[ SKV_EVD_THREAD_COUNT ];
  ThreadArgs  mEvdPollThreadArgs[ SKV_EVD_THREAD_COUNT ];

  // The mighty context table.
  EPStateMap_T*              mEPStateMap;

  skv_server_state_t         mState;

  int                        mSeqNo;

  int                        mMyRank;
  int                        mMyNodeCount;

  skv_status_t Run();

  // 3 Classes of Events
  skv_status_t GetMPIEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount );

  skv_status_t GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount );

  skv_status_t ProcessEvent( skv_server_state_t  aState, 
                                    skv_server_event_t* aEvent );

  skv_status_t ProcessPendingEvents( skv_server_event_t * aEvent );
  skv_status_t ProcessPendingEvents( skv_server_ep_state_t * aEPStatePtr );
  skv_status_t ProgressAnyEP();

  skv_status_t PollOnITEventClass( it_evd_handle_t aEvdHdl, 
                                    it_event_t*      aEvent,
                                    it_event_t*      aEventInPthread,
                                    pthread_mutex_t* aEventPresentMutex,
                                    pthread_mutex_t* aReadyToWaitOnNextEventMutex );

  static void* EvdPollThread( void* arg );

  skv_server_ep_state_t* GetEPStateForEPHdl( it_ep_handle_t aEP );

  void             SetState( skv_server_state_t aState );

  skv_server_state_t     GetState();

public:
  skv_server_t() {}

  // Initializes server's state and starts the server
  int Init( int aRank,
            int aNodeCount,
            int aFlags,
            char* aCheckpointPath );

  // Removes state and shuts down the server
  int Finalize();
};
#endif
