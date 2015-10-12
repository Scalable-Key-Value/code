/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_server_epstate_map.hpp
 *
 *  Created on: Apr 1, 2015
 *      Author: lschneid
 */

#ifndef SKV_SERVER_SKV_SERVER_EPSTATE_MAP_HPP_
#define SKV_SERVER_SKV_SERVER_EPSTATE_MAP_HPP_

#ifndef DSKV_SERVER_CLIENT_CONN_EST_LOG
#define DSKV_SERVER_CLIENT_CONN_EST_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_CLEANUP_LOG
#define SKV_SERVER_CLEANUP_LOG ( 0 | SKV_LOGGING_ALL )
#endif


#include <map>

// the list of Endpoints and ep handles for reverse lookup
typedef std::map< it_ep_handle_t, skv_server_ep_state_t volatile* > EPStateMap_T;

class skv_server_epstate_map_t
{
  // The mighty context table.
  EPStateMap_T mEPStateMap;
  skv_mutex_t  mSequentializer;
  EPStateMap_T::iterator mIterator;
#ifdef ITERATOR_DETAIL_DEBUG
  volatile uint64_t mCountIn;
  volatile uint64_t mCountOut;
#endif

public:
  skv_server_epstate_map_t()
  {
    mIterator = mEPStateMap.begin();
#ifdef ITERATOR_DETAIL_DEBUG
    mCountIn = 0;
    mCountOut = 0;
#endif
  }
  ~skv_server_epstate_map_t() {}

  skv_server_ep_state_t*
  GetEPStateForEPHdl( const it_ep_handle_t aEP )
  {
    mSequentializer.lock();
    EPStateMap_T::iterator iter = mEPStateMap.find( aEP );

    mSequentializer.unlock();
    if( iter != mEPStateMap.end() )
    {
      return (skv_server_ep_state_t*)iter->second;
    }
    else
      return NULL;
  }

  skv_server_ep_state_t* operator[]( const it_ep_handle_t aEP )
  {
    return GetEPStateForEPHdl( aEP );
  }

  skv_status_t insert( const it_ep_handle_t aEPHdl, const skv_server_ep_state_t *aStateForEP )
  {
    mSequentializer.lock();
    BegLogLine( DSKV_SERVER_CLIENT_CONN_EST_LOG | SKV_SERVER_CLEANUP_LOG )
      << "Injecting [ 0x" << (void*)aEPHdl
      << ": 0x" << (void*)aStateForEP
      << " ] "
      << EndLogLine;

    int rc = mEPStateMap.insert( std::make_pair( (it_ep_handle_t)aEPHdl, (skv_server_ep_state_t*)aStateForEP ) ).second;
    mIterator = mEPStateMap.begin();
    mSequentializer.unlock();

    BegLogLine( DSKV_SERVER_CLIENT_CONN_EST_LOG | SKV_SERVER_CLEANUP_LOG )
      << "Injecting [ 0x" << (void*)aEPHdl
      << ": 0x" << (void*)aStateForEP
      << " ] complete"
      << EndLogLine;

    return rc == 0? SKV_ERRNO_RECORD_ALREADY_EXISTS :  SKV_SUCCESS;
  }

  skv_status_t erase( const it_ep_handle_t aEPHdl )
  {
    mSequentializer.lock();
    BegLogLine( SKV_SERVER_CLEANUP_LOG )
      << "Erasing Hdl 0x" << (void*)aEPHdl
      << EndLogLine;

    mEPStateMap.erase( mEPStateMap.find( aEPHdl ) );
    mIterator = mEPStateMap.begin();
    mSequentializer.unlock();

    BegLogLine( SKV_SERVER_CLEANUP_LOG )
      << "Erasing Hdl 0x" << (void*)aEPHdl
      << " complete."
      << EndLogLine;

    return SKV_SUCCESS;
  }

  // THIS FUNCTION BLOCKS THE CALLER IF MAP IS EMPTY!
  // gets the next EPState from internal iterator
  // prevents changes to the map until user calls Unfreeze... function
  skv_server_ep_state_t *GetNextEPStateAndFreezeForProcessing()
  {
    mSequentializer.lock();
    while( true )
    {
        skv_server_ep_state_t volatile *retval = AdvanceCurrentEPState();
        if( retval &&
            retval->mEPState_status == SKV_SERVER_ENDPOINT_STATUS_ACTIVE )
        {
            return (skv_server_ep_state_t*)retval;
        }

        mSequentializer.unlock();
        ::sched_yield();
        mSequentializer.lock();
    }
  }

  inline void UnfreezeAfterProcessing()
  {
    mSequentializer.unlock();
  }

private:
  inline skv_server_ep_state_t volatile * AdvanceCurrentEPState()
  {
    // reset iterator when we reached the end
    if( mIterator == mEPStateMap.end() )
      mIterator = mEPStateMap.begin();
    else
      {
#ifdef ITERATOR_DETAIL_DEBUG
      ++mCountIn;
#endif
      ++mIterator;
#ifdef ITERATOR_DETAIL_DEBUG
      ++mCountOut;
#endif
      }

    // avoid extra loop if ++ has reached the end
    if( mIterator == mEPStateMap.end() )
      mIterator = mEPStateMap.begin();

    // determine return value
    if( mIterator != mEPStateMap.end() )
      return mIterator->second;
    else
      return NULL;
  }

};

#endif /* SKV_SERVER_SKV_SERVER_EPSTATE_MAP_HPP_ */
