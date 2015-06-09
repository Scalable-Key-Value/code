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

#ifndef __SKV_SERVER_COMMAND_EVENT_SOURCE_HPP__
#define __SKV_SERVER_COMMAND_EVENT_SOURCE_HPP__

class skv_server_command_event_source_t;

struct skv_server_command_thread_args_t
{
  skv_server_command_event_source_t *mEventSource;
  skv_server_command_event_buffer_list_t *mBufferList;
  pthread_mutex_t mMutex;
  int mMaxEventCount;
  volatile bool mKeepRunning;
};

class skv_server_command_event_source_t :
    public skv_server_event_source_t<skv_server_epstate_map_t>
{
  skv_server_command_event_buffer_list_t mBufferList;
  pthread_t mPollThread;
  skv_server_command_thread_args_t mCommandFetchArgs;

public:
  skv_server_command_event_source_t( skv_server_epstate_map_t *aEVMgr = NULL,
                                     int aPrio = SKV_SERVER_EVENT_SOURCE_DEFAULT_PRIO )
  {
    SetEventManager( aEVMgr );
    SetPriority( aPrio );

    mCommandFetchArgs.mBufferList = &mBufferList;
    mCommandFetchArgs.mEventSource = this;
    mCommandFetchArgs.mKeepRunning = true;
    mCommandFetchArgs.mMaxEventCount = SKV_SERVER_EVENTS_MAX_COUNT;

    if( GetEventManager() != NULL )
      pthread_create( &mPollThread, NULL, CommandFetchThread, (void*)&mCommandFetchArgs );
    else
      StrongAssertLogLine( 0 )
        << "Impossible to create command_event_source without event manager."
        << EndLogLine;
  }
  virtual ~skv_server_command_event_source_t()
  {
    mCommandFetchArgs.mKeepRunning = false;
    void *ThreadReturn;
    pthread_join( mPollThread, &ThreadReturn);
  }
  virtual skv_status_t
  GetEvent( skv_server_event_t* aEvents,
            int* aEventCount,
            int aMaxEventCount );

  static void* CommandFetchThread( void *aArgs );
};

#endif // __SKV_SERVER_COMMAND_EVENT_SOURCE_HPP__
