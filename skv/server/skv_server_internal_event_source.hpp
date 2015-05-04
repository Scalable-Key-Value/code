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

#ifndef __SKV_SERVER_INTERNAL_EVENT_SOURCE_HPP__
#define __SKV_SERVER_INTERNAL_EVENT_SOURCE_HPP__

class skv_server_internal_event_source_t :
    public skv_server_event_source_t<skv_server_internal_event_manager_if_t>
{
public:

  virtual skv_status_t
  GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount )
  {
    skv_server_event_t *newEvent;
    skv_server_event_t *nextEventSlot = aEvents;
    skv_status_t status = SKV_SUCCESS;

    *aEventCount = 0;
    while(( status == SKV_SUCCESS) && ( *aEventCount < aMaxEventCount ))
      {
      status = mEventManager->Dequeue( &newEvent );

      if( status == SKV_SUCCESS )
        {
        BegLogLine( 0 )
          << "Found new internally queued event: 0x" << (void*)newEvent
          << " cpy to: 0x" << (void*)aEvents
          << " CmdOrd: " << newEvent->mEventMetadata.mCommandFinder.mCommandOrd
          << " Event: " << skv_server_event_type_to_string( newEvent->mCmdEventType )
          << EndLogLine;

        memcpy( nextEventSlot, newEvent, sizeof( skv_server_event_t ) );
        nextEventSlot++;
        *aEventCount += 1;

        delete newEvent;
        }
      }
    BegLogLine( (0 && (*aEventCount > 0)) )
      << "Collected " << *aEventCount << " internal events"
      << EndLogLine;

    if( *aEventCount > 0 )
      return SKV_SUCCESS;
    else
      return status;
  }

};

#endif // __SKV_SERVER_INTERNAL_EVENT_SOURCE_HPP__
