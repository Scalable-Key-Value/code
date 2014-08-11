/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_local_kv_event_queue.hpp
 *
 *  Created on: May 20, 2014
 *      Author: lschneid
 */

#ifndef SKV_LOCAL_KV_EVENT_QUEUE_HPP_
#define SKV_LOCAL_KV_EVENT_QUEUE_HPP_

#ifndef SKV_LOCAL_KV_QUEUES_LOG
#define SKV_LOCAL_KV_QUEUES_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#define SKV_LOCAL_KV_MAX_EVENTS ( 1024 )

class skv_local_kv_event_queue_t {
  skv_local_kv_event_t mEventPool[ SKV_LOCAL_KV_MAX_EVENTS ];
  skv_array_stack_t< skv_local_kv_event_t*, SKV_LOCAL_KV_MAX_EVENTS > mFreeEvents;
  skv_array_queue_t< skv_local_kv_event_t*, SKV_LOCAL_KV_MAX_EVENTS > mActiveEventQueue;
  skv_mutex_t mQueueSerializer;

public:
  skv_local_kv_event_queue_t() {}
  ~skv_local_kv_event_queue_t() {}

  skv_status_t Init() {
    for( int i=0; i<SKV_LOCAL_KV_MAX_EVENTS; i++ )
    {
      mEventPool[ i ].mType = SKV_SERVER_EVENT_TYPE_NONE;
      mEventPool[ i ].mCookie = NULL;
      mFreeEvents.push( &(mEventPool[ i ]) );

    }
    return SKV_SUCCESS;
  }
  skv_local_kv_event_t* GetEvent()
  {
    skv_local_kv_event_t *Event = NULL;

    if( ! mActiveEventQueue.empty() )
    {
      mQueueSerializer.lock();
      Event = mActiveEventQueue.front();
      mActiveEventQueue.pop();
      mQueueSerializer.unlock();

      BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
        << "skv_local_kv_event_queue_t::GetEvent(): Event fetched"
        << " @" << (void*)Event
        << " type: " << skv_server_event_type_to_string( Event->mType )
        << EndLogLine;
    }

    return Event;
  }

  skv_status_t AckEvent( skv_local_kv_event_t *aEvent )
  {
    mQueueSerializer.lock();
    mFreeEvents.push(aEvent);
    aEvent->mType = SKV_SERVER_EVENT_TYPE_NONE;
    aEvent->mCookie = NULL;
    mQueueSerializer.unlock();
    return SKV_SUCCESS;
  }

  skv_status_t QueueEvent( skv_local_kv_cookie_t* aCookie )
  {
    skv_status_t status = SKV_ERRNO_LOCAL_KV_EVENT;

    skv_local_kv_event_t *Event = NULL;

    if( mFreeEvents.empty() )
    {
      return SKV_ERRNO_PENDING_COMMAND_LIMIT_REACHED;
    }

    mQueueSerializer.lock();
    Event = mFreeEvents.top();
    mFreeEvents.pop();
    mQueueSerializer.unlock();

    Event->mType = SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL;
    Event->mCookie = *aCookie;

    mQueueSerializer.lock();
    mActiveEventQueue.push(Event);
    mQueueSerializer.unlock();

    BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
      << "skv_local_kv_event_queue_t::QueueEvent(): Event created"
      << " @" << (void*)Event
      << " type: " << skv_server_event_type_to_string( Event->mType )
      << " cookie: " << *aCookie
      << EndLogLine;

    return status;
  }



};


#endif /* SKV_LOCAL_KV_EVENT_QUEUE_HPP_ */
