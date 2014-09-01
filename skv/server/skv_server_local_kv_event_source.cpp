/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     lschneid - initial implementation
 */

#ifndef SKV_SERVER_LOCAL_KV_EVENT_SOURCE_LOG
#define SKV_SERVER_LOCAL_KV_EVENT_SOURCE_LOG ( 0 | SKV_LOGGING_ALL )
#endif


#include <FxLogger.hpp>
#include <common/skv_types.hpp>
#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>

// include the implementations of the local kv backend
#include <server/skv_local_kv_interface.hpp>

#include <server/skv_server_event_source.hpp>
#include <server/skv_server_local_kv_event_source.hpp>

#define SKV_SERVER_LOCAL_KV_POLL_LOOPS ( 10 )

skv_status_t
skv_server_local_kv_event_source_t::GetEvent( skv_server_event_t* aEvents,
                                              int* aEventCount,
                                              int aMaxEventCount )
{
  skv_status_t status = SKV_SUCCESS;
  skv_local_kv_event_t* LocalEvent = NULL;
  int EvCount = 0;
  int LoopCount = SKV_SERVER_LOCAL_KV_POLL_LOOPS;

  while( ( EvCount < aMaxEventCount ) && ( LoopCount-- ) )
  {
    LocalEvent = mEventManager->GetEvent();
    if( LocalEvent )
    {
      status = PrepareEvent( LocalEvent, &aEvents[ EvCount ] );
      if( status == SKV_SUCCESS )
      {
        EvCount++;
      }
      status = mEventManager->AckEvent( LocalEvent );
    }
  }
  *aEventCount = EvCount;
  return SKV_SUCCESS;
}

skv_status_t
skv_server_local_kv_event_source_t::PrepareEvent( skv_local_kv_event_t *aLocalEvent,
                                                  skv_server_event_t *aSEvent)
{
  aSEvent->Init( aLocalEvent->mType,
                 aLocalEvent->mCookie.GetEPState(),
                 aLocalEvent->mCookie.GetOrdinal(),
                 aLocalEvent->mType );

  return SKV_SUCCESS;
}
