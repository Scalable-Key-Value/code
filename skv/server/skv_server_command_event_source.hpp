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

#ifndef SKV_SERVER_GET_COMMAND_LOG
#define SKV_SERVER_GET_COMMAND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_COMMAND_POLLING_LOG
#define SKV_SERVER_COMMAND_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_command_event_source_t :
    public skv_server_event_source_t<EPStateMap_T>
{
public:
  virtual skv_status_t
  GetEvent( skv_server_event_t* aEvents,
            int* aEventCount,
            int aMaxEventCount );

  virtual ~skv_server_command_event_source_t() {}

private:
  skv_status_t PrepareEvent( skv_server_event_t *currentEvent,
                             skv_server_ep_state_t *aEPState );

};

#endif // __SKV_SERVER_COMMAND_EVENT_SOURCE_HPP__
