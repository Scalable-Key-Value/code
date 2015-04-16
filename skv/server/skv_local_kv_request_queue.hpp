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

#define SKV_LOCAL_KV_MAX_REQUESTS (1048576 )

class skv_local_kv_request_queue_t {
  skv_local_kv_request_t mRequestPool[ SKV_LOCAL_KV_MAX_REQUESTS ];
  skv_array_stack_t< skv_local_kv_request_t*, SKV_LOCAL_KV_MAX_REQUESTS > mFreeRequests;
  skv_array_queue_t< skv_local_kv_request_t*, SKV_LOCAL_KV_MAX_REQUESTS > mActiveRequests;
  skv_mutex_t mQueueSerializer;
  volatile int mFreeSlots;

public:
  skv_local_kv_request_queue_t() : mFreeSlots( 0 ) {}
  ~skv_local_kv_request_queue_t() {}

  skv_status_t Init()
  {
    for( int i=0; i<SKV_LOCAL_KV_MAX_REQUESTS; i++ )
    {
      mRequestPool[ i ].mType = SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN;
      mFreeRequests.push( &(mRequestPool[ i ]) );
    }
    mFreeSlots = SKV_LOCAL_KV_MAX_REQUESTS;

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
    --mFreeSlots;
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
  bool IsEmpty()
  {
    return mActiveRequests.empty();
  }
  skv_local_kv_request_t* GetRequest()
  {
    skv_local_kv_request_t *Request = NULL;

    mQueueSerializer.lock();
    if( ! mActiveRequests.empty() )
    {
      Request = mActiveRequests.front();
      mActiveRequests.pop();

      BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
        << "skv_local_kv_request_queue_t::GetRequest() Request fetched"
        << " @" << (void*)Request
        << EndLogLine;
    }
    mQueueSerializer.unlock();

    return Request;
  }

  skv_status_t AckRequest( skv_local_kv_request_t* aRequest )
  {
    mQueueSerializer.lock();
    mFreeRequests.push( aRequest );
    ++mFreeSlots;
    mQueueSerializer.unlock();

    BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
      << "skv_local_kv_request_queue_t::AckRequest() Request returned"
      << " @" << (void*)aRequest
      << EndLogLine;

    return SKV_SUCCESS;
  }

  inline int GetFreeSlots() const
  {
    return mFreeSlots;
  }
};

// for now, just start with a round-robin implementation of a list of worker-request queues
// later we might consider a "shortest-queue-first" scheme or other scheduling too
class skv_local_kv_request_queue_list_t
{
  skv_local_kv_request_queue_t **mSortedRequestQueueList;
  skv_local_kv_request_queue_t *mBestQueue;
  uint64_t mCurrentIndex;
  unsigned int mActiveQueueCount;
  unsigned int mMaxQueueCount;

public:
  skv_local_kv_request_queue_list_t( const unsigned int aMaxQueueCount )
  {
    mMaxQueueCount = aMaxQueueCount;
    mSortedRequestQueueList = new skv_local_kv_request_queue_t* [ mMaxQueueCount ];
    mCurrentIndex = 0;
    mActiveQueueCount = 0;
    mBestQueue = NULL;
  }
  ~skv_local_kv_request_queue_list_t() {}

  void sort()
  {
    // sort the list start with highest number of FreeSlots
  }

  void AddQueue( const skv_local_kv_request_queue_t * aQueue )
  {
    AssertLogLine( mActiveQueueCount < mMaxQueueCount )
      << "Queue Limit exceeded: current=" << mActiveQueueCount
      << " limit=" << mMaxQueueCount
      << EndLogLine;

    mSortedRequestQueueList[ mActiveQueueCount ] = (skv_local_kv_request_queue_t*)aQueue;
    ++mActiveQueueCount;

    if( mActiveQueueCount == 1 )
      mBestQueue = mSortedRequestQueueList[ 0 ];
  }

  skv_local_kv_request_queue_t * GetBestQueue()
  {
    skv_local_kv_request_queue_t *retval = mBestQueue;

    ++mCurrentIndex;
    mBestQueue = mSortedRequestQueueList[ mCurrentIndex % mActiveQueueCount ];

    return retval;
  }
};


#endif /* SKV_LOCAL_KV_REQUEST_QUEUE_HPP_ */
