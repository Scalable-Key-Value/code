/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_local_kv_request_queue.hpp
 *
 *  Created on: May 20, 2014
 *      Author: lschneid
 */

#ifndef SKV_LOCAL_KV_REQUEST_QUEUE_HPP_
#define SKV_LOCAL_KV_REQUEST_QUEUE_HPP_

#ifndef SKV_LOCAL_KV_QUEUES_LOG
#define SKV_LOCAL_KV_QUEUES_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#define SKV_LOCAL_KV_MAX_REQUESTS (1024 )

class skv_local_kv_request_queue_t {
  skv_local_kv_request_t mRequestPool[ SKV_LOCAL_KV_MAX_REQUESTS ];
  skv_array_stack_t< skv_local_kv_request_t*, SKV_LOCAL_KV_MAX_REQUESTS > mFreeRequests;
  skv_array_queue_t< skv_local_kv_request_t*, SKV_LOCAL_KV_MAX_REQUESTS > mActiveRequests;
  skv_mutex_t mQueueSerializer;

public:
  skv_local_kv_request_queue_t() {}
  ~skv_local_kv_request_queue_t() {}

  skv_status_t Init()
  {
    for( int i=0; i<SKV_LOCAL_KV_MAX_REQUESTS; i++ )
    {
      mRequestPool[ i ].mType = SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN;
      mFreeRequests.push( &(mRequestPool[ i ]) );
    }

    return SKV_SUCCESS;
  }

  skv_status_t Exit()
  {
    return SKV_SUCCESS;
  }

  skv_local_kv_request_t* AcquireRequestEntry()
  {
    if( mFreeRequests.empty() || (mActiveRequests.size() > SKV_LOCAL_KV_MAX_REQUESTS-2) )
      return NULL;

    mQueueSerializer.lock();
    skv_local_kv_request_t *Req = mFreeRequests.top();
    mFreeRequests.pop();
    mQueueSerializer.unlock();

    return Req;
  }
  void QueueRequest( skv_local_kv_request_t *aReq )
  {
    mQueueSerializer.lock();
    mActiveRequests.push( aReq );
    mQueueSerializer.unlock();
  }

  skv_local_kv_request_t* GetRequest()
  {
    skv_local_kv_request_t *Request = NULL;

    if( ! mActiveRequests.empty() )
    {
      mQueueSerializer.lock();
      Request = mActiveRequests.front();
      mActiveRequests.pop();
      mQueueSerializer.unlock();

      BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
        << "skv_local_kv_request_queue_t::GetRequest() Request fetched"
        << " @" << (void*)Request
        << EndLogLine;

      return Request;
    }
    return NULL;
  }

  skv_status_t AckRequest( skv_local_kv_request_t* aRequest )
  {
    mQueueSerializer.lock();
    mFreeRequests.push( aRequest );
    mQueueSerializer.unlock();

    BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
      << "skv_local_kv_request_queue_t::AckRequest() Request returned"
      << " @" << (void*)aRequest
      << EndLogLine;

    return SKV_SUCCESS;
  }

};

#endif /* SKV_LOCAL_KV_REQUEST_QUEUE_HPP_ */
