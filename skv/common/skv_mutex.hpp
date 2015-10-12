/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_mutex.hpp
 *
 *  Created on: May 20, 2014
 *      Author: lschneid
 */

#ifndef SKV_MUTEX_HPP_
#define SKV_MUTEX_HPP_

#ifndef SKV_MUTEX_LOG
#define SKV_MUTEX_LOG ( 0 )
#endif

/* This is a dumb impl to encapsulate locking via pthread_mutex to work around old compilers not able to cope with C++11
 * This could be removed once we decide to require C++11 compiler support for whole SKV */
class skv_mutex_t {
  pthread_mutex_t mMutex;
#if (SKV_MUTEX_LOG != 0)
  int mLogThisMutex;
#else
#define mLogThisMutex ( 0 )
#endif

public:
#if (SKV_MUTEX_LOG == 0)
  skv_mutex_t( const pthread_mutexattr_t *aAttributes = NULL )
#else
  skv_mutex_t( const pthread_mutexattr_t *aAttributes = NULL, const int aLogThisMutex = 0 )
#endif
  {
#if (SKV_MUTEX_LOG != 0)
  mLogThisMutex = aLogThisMutex;
#endif
    pthread_mutex_init( &mMutex, 0 );
    BegLogLine( SKV_MUTEX_LOG ) << "Initialized lock: 0x" << (void*)&mMutex << EndLogLine;
  }
  ~skv_mutex_t()
  {
    BegLogLine( SKV_MUTEX_LOG && (mLogThisMutex) ) << "Destroying lock: 0x" << (void*)&mMutex << EndLogLine;
    pthread_mutex_destroy( &mMutex );
  }
  inline int lock()
  {
    BegLogLine( SKV_MUTEX_LOG && (mLogThisMutex) ) << "About to lock: 0x" << (void*)&mMutex << EndLogLine;
    return pthread_mutex_lock( &mMutex );
  }
  inline int unlock()
  {
    BegLogLine( SKV_MUTEX_LOG && (mLogThisMutex) ) << "Unlocking: 0x" << (void*)&mMutex << EndLogLine;
    return pthread_mutex_unlock( &mMutex );
  }
  inline int trylock()
  {
    return pthread_mutex_trylock( &mMutex );
  }

#if (SKV_MUTEX_LOG != 0)
  inline void init( int aLogThisMutex )
  {
    mLogThisMutex = aLogThisMutex;
  }
#else
  inline void init( int aLogThisMutex ) {}
#endif
};

#endif /* SKV_MUTEX_HPP_ */
