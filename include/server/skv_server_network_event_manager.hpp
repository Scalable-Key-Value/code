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

#ifndef __SKV_SERVER_NETWORK_EVENT_MANAGER_HPP__
#define __SKV_SERVER_NETWORK_EVENT_MANAGER_HPP__

#ifndef SKV_SERVER_NETWORK_EVENT_MANAGER_LOG
#define SKV_SERVER_NETWORK_EVENT_MANAGER_LOG ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_network_event_manager_if_t
{
  int               mMyRank;
  int               mPartitionSize;

  // Interface Adapter
  it_ia_handle_t		mIA_Hdl;

  // Protection Zone
  it_pz_handle_t		mPZ_Hdl;

  // Event Dispatchers
  it_evd_handle_t               mEvd_Unaff_Hdl;
  it_evd_handle_t               mEvd_Aff_Hdl; 
  it_evd_handle_t		mEvd_Cmr_Hdl;
  it_evd_handle_t               mEvd_Cmm_Hdl;
  it_evd_handle_t               mEvd_Rq_Hdl;
  it_evd_handle_t		mEvd_Sq_Hdl;
  it_evd_handle_t		mEvd_Seq_Hdl;
  it_evd_handle_t		mAevd_Hdl;

  // Listen hdl
  it_listen_handle_t	        mLP_Hdl;

  // Events for the it_evd_wait() call
  it_event_t                        mEvent_Unaff;
  it_event_t                        mEvent_Aff;
  it_event_t                        mEvent_Cmr;
  it_event_t                        mEvent_Cmm;
  it_event_t                        mEvent_Rq;
  it_event_t                        mEvent_Sq;
  it_event_t                        mEvent_Seq;

  it_event_t*                       mAevdEvents;

  // Signals for event being present
  pthread_mutex_t                   mIsEventPresent_Unaff;
  pthread_mutex_t                   mIsEventPresent_Aff;
  pthread_mutex_t                   mIsEventPresent_Cmm;
  pthread_mutex_t                   mIsEventPresent_Cmr;
  pthread_mutex_t                   mIsEventPresent_Rq;
  pthread_mutex_t                   mIsEventPresent_Sq;
  pthread_mutex_t                   mIsEventPresent_Seq;

  // Signals to wait for the next event
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Unaff;
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Aff;
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Cmm;
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Cmr;
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Rq;
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Sq;
  pthread_mutex_t                   mReadyToWaitOnNextEvent_Seq;


public:
  skv_status_t
  Init( int aPartitionSize,
        int aRank );

  inline
  int GetRank()
  {
    return mMyRank;
  }
  inline
  int GetPartitionSize()
  {
    return mPartitionSize;
  }

  inline it_pz_handle_t GetPZ()
  {
    return mPZ_Hdl;
  }

  inline it_evd_handle_t GetAEVD()
  {
    return mAevd_Hdl;
  }

  inline it_event_t* GetEventStorage()
  {
    return mAevdEvents;
  }

  int WaitForEvents( int aMaxEventCount )
  {
    AssertLogLine( aMaxEventCount <= SKV_SERVER_AEVD_EVENTS_MAX_COUNT )
      << "ERROR: "
      << " aMaxEventCount: " << aMaxEventCount
      << " SKV_SERVER_AEVD_EVENTS_MAX_COUNT: " << SKV_SERVER_AEVD_EVENTS_MAX_COUNT
      << EndLogLine;

    size_t itEventCount = 0;

    // \todo this is not the right place because we're in the event source here, not in the sink
    it_status_t istatus = itx_aevd_wait( mAevd_Hdl,
                                         0,
                                         aMaxEventCount,   // SKV_SERVER_AEVD_EVENTS_MAX_COUNT,
                                         mAevdEvents,
                                         &itEventCount );

    StrongAssertLogLine( istatus == IT_SUCCESS )
      << "ERROR: "
      << " istatus: " << istatus
      << EndLogLine;

    return itEventCount;
  }

  skv_status_t
  FinalizeEPState( EPStateMap_T* aEPStateMap,
                   it_ep_handle_t aEP,
                   skv_server_ep_state_t* aStateForEP );

  skv_status_t
  InitNewStateForEP( EPStateMap_T* aEPStateMap,
                     skv_server_ep_state_t** aStateForEP );

};

#endif // __SKV_SERVER_NETWORK_EVENT_MANAGER_HPP__
