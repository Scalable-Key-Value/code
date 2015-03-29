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

#include <FxLogger.hpp>
#include <skv/common/skv_types.hpp>
#include <skv/common/skv_client_server_headers.hpp>
#include <skv/server/skv_server_types.hpp>
#include <skv/server/skv_server_network_event_manager.hpp>

#include <skv/server/skv_server_command_event_buffer.hpp>
#include <skv/server/skv_server_event_source.hpp>
#include <skv/server/skv_server_command_event_source.hpp>

#ifndef SKV_CTRLMSG_DATA_LOG
#define SKV_CTRLMSG_DATA_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_GET_COMMAND_LOG
#define SKV_SERVER_GET_COMMAND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_COMMAND_POLLING_LOG
#define SKV_SERVER_COMMAND_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif


#define SKV_SERVER_COMMAND_MEM_POLL_LOOPS 10
#if (SKV_SERVER_COMMAND_POLLING_LOG != 0)
static int state_has_changed = 1;
#endif

#ifndef SKV_SERVER_COMMAND_STATISTICS
#define SKV_SERVER_COMMAND_STATISTICS
#endif

#ifdef SKV_SERVER_COMMAND_STATISTICS
static int gServerCmdMin = 999999;
static int gServerCmdMax = 0;
static double gServerCmdAvg = 0;
#define SKV_SERVER_COMMAND_STATISTICS_WGT_NEW (0.1)
#define SKV_SERVER_COMMAND_STATISTICS_WGT_OLD (0.9)
#endif

skv_status_t
skv_server_command_event_source_t::
GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount )
{
  skv_status_t status = SKV_SUCCESS;

  skv_server_command_event_buffer_t *EventBuffer = mBufferList.GetAndFreezeReadyBuffer();
  *aEventCount = EventBuffer->GetEventCount();
  StrongAssertLogLine( *aEventCount <= aMaxEventCount )
    << "Too Many Events. Command_event_source is only capable of delivering the whole accumulated buffer. "
    << " available=" << *aEventCount
    << " limit=" << aMaxEventCount
    << EndLogLine;
  if( *aEventCount > 0 )
  {
    BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
      << "GetEvent: copying " << *aEventCount
      << " size: " << sizeof(skv_server_event_t) * (*aEventCount)
      << " Events."
      << EndLogLine;

    memcpy( aEvents,
            EventBuffer->GetBuffer(),
            sizeof(skv_server_event_t) * (*aEventCount) );

    mBufferList.mFtcCounter+= *aEventCount;

#ifdef SKV_SERVER_COMMAND_STATISTICS
    gServerCmdAvg = gServerCmdAvg*SKV_SERVER_COMMAND_STATISTICS_WGT_OLD + (double)(*aEventCount)*SKV_SERVER_COMMAND_STATISTICS_WGT_NEW;
    gServerCmdMin = std::min( gServerCmdMin, *aEventCount );
    gServerCmdMax = std::max( gServerCmdMax, *aEventCount );
    static bool printed = false;
    static int maxEvDist = 0;
    maxEvDist = std::max( maxEvDist, mBufferList.GetBufferDistance() );

    if( !printed && ( gServerCmdMin != gServerCmdMax ) && (mBufferList.mFtcCounter % 0xFFFF < 32) )
      {
      BegLogLine( 1 )
        << "Event collection avg per call: " << gServerCmdAvg
        << " min: " << gServerCmdMin
        << " max: " << gServerCmdMax
        << " evBuffDistance: " << maxEvDist
        << EndLogLine;
      gServerCmdMin = 999999;
      gServerCmdMax = 0;
      maxEvDist = 0;
      printed = true;
      }
    if( mBufferList.mFtcCounter % 0xFFFFF >= 32 )
      printed = false;
#endif

    BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
      << "GetEvent: Found: "<< mBufferList.mCmdCounter
      << " Fetched: " << mBufferList.mFtcCounter
      << " Now: " << *aEventCount
      << " From: @" << (void*)EventBuffer
      << EndLogLine
  }

  mBufferList.UnfreezeAndAdvanceReadyBuffer();
  return status;
}

void* skv_server_command_event_source_t::CommandFetchThread( void *aArgs )
{
  skv_server_command_thread_args_t *args = (skv_server_command_thread_args_t*)aArgs;
  EPStateMap_T::iterator iter;

  skv_server_command_event_buffer_list_t *buffers = args->mBufferList;
  skv_status_t status = SKV_SUCCESS;

  skv_server_command_event_source_t *CES = args->mEventSource;
  iter = CES->GetEventManager()->begin();

  BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
    << "CommandFetchThread running..."
    << EndLogLine;

  while ( args->mKeepRunning )
  {
    if( iter == CES->GetEventManager()->end() )
    {
      iter = CES->GetEventManager()->begin();
//      // if there's nothing to do, then just block on a mutex instead of pulling endlessly...
//      if ( iter == CES->GetEventManager()->end() )
//        pthread_mutex_lock( &args->mMutex );
    }

    skv_server_ep_state_t *EPState = iter->second;

    if(( iter != CES->GetEventManager()->end() ) &&
        ( EPState->mEPState_status == SKV_SERVER_ENDPOINT_STATUS_ACTIVE ) )
    {
        buffers->FillCurrentEventBuffer( EPState );
    }

    iter++;
  }
  return NULL;
}
