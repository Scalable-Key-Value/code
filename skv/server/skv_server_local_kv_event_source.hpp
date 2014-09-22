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

#ifndef SKV_SERVER_LOCAL_KV_EVENT_SOURCE_HPP_
#define SKV_SERVER_LOCAL_KV_EVENT_SOURCE_HPP_

#ifndef SKV_SERVER_GET_LKV_EVENT_LOG
#define SKV_SERVER_GET_LKV_EVENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_LKV_EVENT_POLLING_LOG
#define SKV_SERVER_LKV_EVENT_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_local_kv_event_source_t :
    public skv_server_event_source_t<skv_local_kv_t>
{
public:
  virtual skv_status_t
  GetEvent( skv_server_event_t* aEvents,
            int* aEventCount,
            int aMaxEventCount );

  virtual ~skv_server_local_kv_event_source_t() {}

private:
  skv_status_t PrepareEvent( skv_local_kv_event_t *currentEvent,
                             skv_server_event_t *aSEvent );

};

#endif /* SKV_SERVER_LOCAL_KV_EVENT_SOURCE_HPP_ */
