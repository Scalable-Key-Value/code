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

#ifndef __THREAD_SAFE_QUEUE_HPP__
#define __THREAD_SAFE_QUEUE_HPP__

// compliments of arayshu
#include <pthread.h>

#ifndef THREAD_SAFE_QUEUE_FXLOG
#define THREAD_SAFE_QUEUE_FXLOG ( 0 )
#endif

#ifndef THREAD_SAFE_QUEUE_ENQDEQ_FXLOG
#define THREAD_SAFE_QUEUE_ENQDEQ_FXLOG ( 0 )
#endif

#ifndef THREAD_SAFE_QUEUE_INIT_FXLOG
#define THREAD_SAFE_QUEUE_INIT_FXLOG ( 0 )
#endif

#ifndef THREAD_SAFE_QUEUE_ENABLE_WITH_WAIT
//#define THREAD_SAFE_QUEUE_ENABLE_WITH_WAIT
#endif

#if defined __ppc__ || defined __ppc64__ || defined __powerpc__ || defined __powerpc64__
#  ifdef VRNIC_CNK // need a better name
#    include "/bgsys/drivers/ppcfloor/arch/include/spi/kernel_interface.h"
#  else
static void _bgp_msync(void)
  {
    asm volatile ("msync" : : : "memory");
  }
static void _bgp_mbar(void)
  {
    asm volatile ("msync" : : : "memory");
  }
//#    define _bgp_msync(void)
//    do { asm volatile ("msync" : : : "memory"); } while( 0 )
//#    define _bgp_mbar(void)
//    do { asm volatile ("mbar" : : : "memory"); } while( 0 )
#  endif
#else
static void _bgp_msync(void)
  {
    __sync_synchronize();
  }
static void _bgp_mbar(void)
  {
    __sync_synchronize();
  }
//#  define _bgp_msync(void) __sync_synchronize()
//#  define _bgp_mbar(void) __sync_synchronize()
#endif

template< class Item, int tLockless >
struct ThreadSafeQueue_t
  {
    pthread_mutex_t   mutex;
    pthread_cond_t    empty_cond;
    pthread_cond_t    full_cond;

    typedef           uint64_t tsq_counter_t;

    Item*             mItemArray;

    volatile tsq_counter_t     mPutCount;
    volatile tsq_counter_t     mGotCount;
    unsigned          mDepthMask;
    int               mMax;
    struct timespec   mCondWaitTimeout;

    size_t
    GetCount()
    {
#if !defined(PK_X86)
#if defined __ppc__ || defined __ppc64__ || defined __powerpc__ || defined __powerpc64__
      _bgp_msync();
#endif
      //_bgp_mbar();
#endif
      return (mPutCount-mGotCount);
    }

    unsigned RealDepth(unsigned aDepth)
    {
      // Round up all queue depths to the next higher power of 2
      // because then we can do some arithmetic by bit-masking
      // rather then by division/modulus.
      unsigned int rd = 1 ;
      for (int x = 0 ; x < 32 ; x += 1)
        {
          if (rd >= aDepth)
            return rd ;
          rd <<= 1 ;
        } /* endfor */

      // You could get here if someone tried to configure a queue with
      // more than 2**31 elements; but this is not sensible for other
      // reasons, so a 'firewall' trap is OK.
      StrongAssertLogLine( 0 ) << "aDepth too big at " << aDepth << EndLogLine;
      return 1024 ;
    }

    pthread_mutex_t*
    GetMutex()
    {
      StrongAssertLogLine( !tLockless )
        << "ThreadSafeQueue_t::GetMutex(): Not valid for if using the lockless queue"
        << EndLogLine;

      return &mutex;
    }

    void
    Finalize()
    {
      pthread_mutex_destroy( & mutex );
      pthread_cond_destroy( & empty_cond );
      pthread_cond_destroy( & full_cond );
      free(mItemArray) ;
    }

    void
    Init(int aMax)
    {
      pthread_mutex_init( & mutex, 0 );
      pthread_cond_init( & empty_cond, 0 );
      pthread_cond_init( & full_cond, 0 );

      mCondWaitTimeout.tv_sec  = 0;
      mCondWaitTimeout.tv_nsec = 10000000; // 0.01 seconds

      mPutCount  = 0;
      mGotCount  = 0;

      unsigned int realDepth = RealDepth( aMax );
      mDepthMask = realDepth - 1;

      mMax       = realDepth;
      int QueueSize = sizeof( Item ) * realDepth;
      mItemArray = (Item*) malloc( QueueSize );

      StrongAssertLogLine( mItemArray )
        << "ThreadSafeQueue_t::Init(): ERROR: Failed to allocate mItemArray of "
        << " QueueSize: " << QueueSize
        << " realDepth: " << realDepth
        << EndLogLine;

      BegLogLine( THREAD_SAFE_QUEUE_INIT_FXLOG )
        << "ThreadSafeQueue_t::Init(): "
        << "Q@ " << (void*)this
        << " Init with max " << aMax
        << " mMax: " << mMax
        << " mDepthMask: " << (void *)(uintptr_t)mDepthMask
        << " realDepth: " << realDepth
        << " QueueSize: " << QueueSize
        << EndLogLine;

    }



    int
    EnqueueWithWait( Item aNextIn )
    {
      //NEED: current Enqueue is actually with wait -- this prepares for a futre non-blocking Enqueue()
      return( Enqueue( aNextIn ) );
    }

    int
    Enqueue( Item aNextIn )
    {
//      int rc = -1;
      if( ! tLockless )
        pthread_mutex_lock( &mutex );

      BegLogLine( (THREAD_SAFE_QUEUE_FXLOG | THREAD_SAFE_QUEUE_ENQDEQ_FXLOG) )
        << "ThreadSafeQueue_t::Enqueue():"
        << " Q@ " << (void*) this
        << " mPutCount " << mPutCount
        << " mGotCount " << mGotCount
        << " tLockless: " << tLockless
        << " mMax: " << mMax
        << EndLogLine;

      // The queue is full
      while( GetCount() == mMax )
        {
          // See comment in DequeueAssumeLockedWithWait
          // That explains the timed out wait

          if( tLockless )
            pthread_mutex_lock( &mutex );

          struct timespec now ;
          clock_gettime(CLOCK_REALTIME,&now) ;
          struct timespec expiry ;
          long nsec=now.tv_nsec + mCondWaitTimeout.tv_nsec ;
          if (nsec >= 1000000000)
            {
              expiry.tv_nsec=nsec-1000000000 ;
              expiry.tv_sec=now.tv_sec+mCondWaitTimeout.tv_sec+1 ;
            }
          else
            {
              expiry.tv_nsec=nsec ;
              expiry.tv_sec=now.tv_sec+mCondWaitTimeout.tv_sec ;
            }

          BegLogLine( 1 )
            << "Queue full. Sleeping... "
            << " size: " << GetCount()
            << EndLogLine;
          pthread_cond_timedwait( & full_cond, & mutex, & expiry );
          BegLogLine( 1 )
            << "Queue full. Woke up... "
            << " size: " << GetCount()
            << EndLogLine;

          if( tLockless )
            pthread_mutex_unlock( &mutex );
        }

      tsq_counter_t ItemsInQueue = GetCount();

      tsq_counter_t OldCount = mPutCount;
      int ItemIndex = OldCount & mDepthMask;
      mItemArray[ ItemIndex ] = aNextIn;
      mPutCount = OldCount + 1;
#if !defined(PK_X86)
#if defined __ppc__ || defined __ppc64__ || defined __powerpc__ || defined __powerpc64__
      _bgp_msync();
#endif
      //_bgp_mbar();
#endif

      BegLogLine( (THREAD_SAFE_QUEUE_FXLOG | THREAD_SAFE_QUEUE_ENQDEQ_FXLOG) )
        << "ThreadSafeQueue_t::Enqueue():"
        << " Q@ " << (void*) this
        << " after insert to slot " << ItemIndex
        << " mPutCount " << mPutCount
        << " mGotCount " << mGotCount
        << " tLockless: " << tLockless
#ifdef ARAYSHU
        << " aNextIn: " << (void *) mItemArray[ ItemIndex ]
#endif
        << EndLogLine;

      if( ! tLockless )
        pthread_mutex_unlock( &mutex );


#ifdef THREAD_SAFE_QUEUE_ENABLE_WITH_WAIT
      if( ItemsInQueue == 0 )
        pthread_cond_broadcast( & empty_cond );
#endif

      return( 0 );
    }

    int
    DequeueAssumedLockedNonEmpty( Item *aNextOut )
    {
      AssertLogLine( mGotCount < mPutCount )
        << "ThreadSafeQueue_t::DequeueAssumedLockedNonEmpty(): "
        << "Queue is empty"
        << " Q@ " << (void*) this
        << " mGotCount: " << mGotCount
        << " mPutCount: " << mPutCount
        << EndLogLine;

      tsq_counter_t ItemsInQueue = GetCount();

      tsq_counter_t OldCount = mGotCount;

      int ItemIndex = OldCount & mDepthMask;
      *aNextOut = mItemArray[ ItemIndex ];
      mGotCount = OldCount + 1;
#if !defined(PK_X86)
#if defined __ppc__ || defined __ppc64__ || defined __powerpc__ || defined __powerpc64__
      _bgp_msync();
#endif
      //_bgp_mbar();
#endif

      BegLogLine( (THREAD_SAFE_QUEUE_FXLOG | THREAD_SAFE_QUEUE_ENQDEQ_FXLOG) )
        << "ThreadSafeQueue_t::DequeueAssumedLockedNonEmpty(): "
        << " Q@ " << (void*) this
        << " after extract from slot " << ItemIndex
        << " mPutCount " << mPutCount
        << " mGotCount " << mGotCount
#ifdef ARAYSHU
        << " mItemArray[ " << ItemIndex << " ]: " << (void *) mItemArray[ ItemIndex ]
        << " *aNextOut: " << (void *) *aNextOut
#endif
        << EndLogLine;

#ifdef ARAYSHU
      AssertLogLine(( (void *) *aNextOut != NULL ))
        << "ThreadSafeQueue_t::DequeueAssumedLockedNonEmpty(): "
        << " mItemArray[ " << ItemIndex << " ]: " << (void *) mItemArray[ ItemIndex ]
        << EndLogLine;
#endif

      if( (int) ItemsInQueue == mMax )
        {
          pthread_cond_broadcast( & full_cond );
        }
      return 0;
    }

#ifdef THREAD_SAFE_QUEUE_ENABLE_WITH_WAIT
    int
    DequeueAssumeLockedWithWait( Item *aNextOut )
    {
      while( GetCount() == 0 )
        {
          if( tLockless )
            pthread_mutex_lock( &mutex );

          /**
           * There's a race condition if we use pthread_cond_wait(), need to use
           * a timed out wait
           *
           * 1. DequeueAssumeLockedWithWait()  gets this far and gets descheduled
           * 2. Enqueueing call increments mPutCount
           * 3. Enqueueing call calls pthread_cond_broadcast
           * 4. DequeueAssumeLockedWithWait() gets scheduled, misses the pthread_cond_broadcast
           * and calls pthread_cond_wait(), blocking in definitely
           *
           **/

          struct timespec now ;
          clock_gettime(CLOCK_REALTIME,&now) ;
          struct timespec expiry ;
          long nsec=now.tv_nsec + mCondWaitTimeout.tv_nsec ;
          if (nsec >= 1000000000)
            {
              expiry.tv_nsec=nsec-1000000000 ;
              expiry.tv_sec=now.tv_sec+mCondWaitTimeout.tv_sec+1 ;
            }
          else
            {
              expiry.tv_nsec=nsec ;
              expiry.tv_sec=now.tv_sec+mCondWaitTimeout.tv_sec ;
            }

          pthread_cond_timedwait( & empty_cond, & mutex, & expiry );

          if( tLockless )
            pthread_mutex_unlock( &mutex );
        }

      return DequeueAssumedLockedNonEmpty( aNextOut );
    }
#endif

    int
    DequeueAssumeLocked( Item *aNextOut )
    {
      if( GetCount() == 0 )
        return -1;

      return DequeueAssumedLockedNonEmpty( aNextOut );
    }
#ifdef THREAD_SAFE_QUEUE_ENABLE_WITH_WAIT
    int
    DequeueWithWait( Item *aNextOut )
    {
      int rc;

      if( ! tLockless )
        pthread_mutex_lock( &mutex );

      rc = DequeueAssumeLockedWithWait( aNextOut );

      if( ! tLockless )
        pthread_mutex_unlock( &mutex );

      return(rc);
    }
#endif
    int
    Dequeue( Item *aNextOut )
    {
      int rc;

      BegLogLine(THREAD_SAFE_QUEUE_FXLOG)
        << "Q@" << this
        << EndLogLine ;

      if( ! tLockless )
        pthread_mutex_lock( &mutex );

      rc = DequeueAssumeLocked( aNextOut );

      if( ! tLockless )
        pthread_mutex_unlock( &mutex );

      return(rc);
    }
};


#endif
