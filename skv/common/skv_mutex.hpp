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

/* This is a dumb impl to encapsulate locking via pthread_mutex to work around old compilers not able to cope with C++11
 * This could be removed once we decide to require C++11 compiler support for whole SKV */
class skv_mutex_t {
  pthread_mutex_t mMutex;

public:
  skv_mutex_t( const pthread_mutexattr_t *aAttributes = NULL )
  {
    pthread_mutex_init( &mMutex, aAttributes );
  }
  ~skv_mutex_t()
  {
    pthread_mutex_destroy( &mMutex );
  }
  inline int lock()
  {
    return pthread_mutex_lock( &mMutex );
  }
  inline int unlock()
  {
    return pthread_mutex_unlock( &mMutex );
  }
  inline int trylock()
  {
    return pthread_mutex_trylock( &mMutex );
  }
};

#endif /* SKV_MUTEX_HPP_ */
