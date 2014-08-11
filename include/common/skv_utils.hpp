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

#ifndef __SKV_UTILS_HPP__
#define __SKV_UTILS_HPP__

#include <FxLogger.hpp>

#include <stdlib.h>
// #include <stdio.h>

int GetBGPRank();

int GetBGPPartitionSize();

template<class Item>
class skv_queue_t
{
  Item *mHead;
  Item *mTail;
  int   mCount;
  int   mMax;

  int
  DequeueNonEmpty( Item **aNextOut )
  {
    *aNextOut = mHead;

    mCount--;
    if( mCount == 0 )
    {
      mHead = NULL;
      mTail = NULL;
    }
    else
    {
      mHead = mHead->mNext;
      mHead->mPrev = NULL;
    }

    (*aNextOut)->mNext = NULL;
    (*aNextOut)->mPrev = NULL;

    return 0;
  }

public:
  void
  Init()
  {
    mHead = NULL;
    mTail = NULL;
    mCount = 0;
    mMax = 0;
  }

  void
  print( const char* aContext )
  {
    Item* Buf = mHead;
    while( Buf != NULL )
    {
      BegLogLine( 1 )
        << "skv_queue_t::" << aContext << "(): "
        << " TestBuf: " << (void *) Buf
        << " TestBuf->GetSize(): " << (void *) Buf->GetSize()
        << EndLogLine;
	
      Buf = Buf->mNext;
    }
  }

  int
  enqueue( Item *aNextIn )
  {
    if( mCount == 0 )
    {
      // AssertLogLine( mHead == NULL ) << "supposed to be empty" << EndLogLine;
      // AssertLogLine( mTail == NULL ) << "supposed to be empty" << EndLogLine;
      mHead = aNextIn;
      mHead->mNext = NULL;
      mHead->mPrev = NULL;

      mTail = mHead;
      mCount = 1;
    }
    else
    {
      mCount++;

      aNextIn->mPrev = mTail;
      mTail->mNext = aNextIn;
      aNextIn->mNext = NULL;

      mTail = aNextIn;
    }
    return (0);
  }

  int
  remove( Item* aToRemove )
  {
    int rc;
    if( mCount == 0 )
      rc = -1;
    else
    {
      if( aToRemove->mNext != NULL )
        aToRemove->mNext->mPrev = aToRemove->mPrev;

      if( aToRemove->mPrev != NULL )
        aToRemove->mPrev->mNext = aToRemove->mNext;

      mCount--;

      if( mCount == 0 )
      {
        mHead = NULL;
        mTail = NULL;
      }
      else if( aToRemove == mTail )
      {
        mTail = aToRemove->mPrev;
      }
      else if( aToRemove == mHead )
      {
        mHead = mHead->mNext;
      }
      rc = 0;
    }
    return rc;
  }

  int
  dequeue( Item **aNextOut )
  {
    if( mCount == 0 )
      return -1;

    return DequeueNonEmpty( aNextOut );
  }
};

template<class T>
class skv_stack_t
{
  int mCount;

  T* mTop;

public:

  skv_stack_t()
  {
    mCount = 0;
    mTop = NULL;
  }

  ~skv_stack_t()
  {
  }

  int
  size()
  {
    return mCount;
  }

  void
  print( const char* aContext )
  {
    T* Buf = mTop;
    while( Buf != NULL )
    {
      BegLogLine( 1 )
        << "skv_stack_t::" << aContext << "(): "
        << " TestBuf: " << (void *) Buf
        << " TestBuf->GetSize(): " << (void *) Buf->GetSize()
        << EndLogLine;

      Buf = Buf->mNext;
    }
  }

  void
  push( T* aA )
  {
    aA->mNext = mTop;
    mTop = aA;
    mTop->mPrev = NULL;
    mCount++;

    // print("push");
  }

  T*
  top()
  {
    return mTop;
  }

  void
  pop()
  {
    // AssertLogLine( size() > 0 ) 
    //   << "skv_stack_t::pop(): "
    //   << " size(): " << size()
    //   << EndLogLine;

    // print("pop");

    mTop = mTop->mNext;
    mCount--;
  }
};
#endif
