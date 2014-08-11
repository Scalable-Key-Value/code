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

#ifndef __SKV_SERVER_IT_EVENT_SOURCE_HPP__
#define __SKV_SERVER_IT_EVENT_SOURCE_HPP__

#ifndef SKV_GET_IT_EVENT_LOG
#define SKV_GET_IT_EVENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_IT_event_source_t :
    public skv_server_event_source_t<skv_server_network_event_manager_if_t>
{
public:
  virtual skv_status_t
  GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount )
  {
    *aEventCount = FetchEvents( aMaxEventCount );
    return PrepareEvents( aEvents, aEventCount );
  }

private:
  int FetchEvents( int aMaxEventCount );
  skv_status_t PrepareEvents( skv_server_event_t *aEvents, int *aEventCount );

};

#endif // __SKV_SERVER_IT_EVENT_SOURCE_HPP__
