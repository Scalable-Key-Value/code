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

/* WARNING: Data access is not thread-safe!! */

#ifndef __SKV_ARRAY_QUEUE_HPP__
#define __SKV_ARRAY_QUEUE_HPP__

#include <FxLogger.hpp>

#define SKV_MAX_ARRAY_QUEUE_SIZE ( 1048576 )

template<class T, size_t SKV_ARRAY_QUEUE_SIZE>
class skv_array_queue_t
{
  size_t len;
  T *Memory;
  size_t head;
  size_t tail;

public:
  skv_array_queue_t()
  {
    len = 0;
    AssertLogLine( SKV_ARRAY_QUEUE_SIZE < SKV_MAX_ARRAY_QUEUE_SIZE )
      << "skv_array_queue_t(): requested length exceeds limit of " << SKV_MAX_ARRAY_QUEUE_SIZE
      << EndLogLine;

    Memory = new T[SKV_ARRAY_QUEUE_SIZE + 1];
    head = 0;
    tail = 0;
  }

  ~skv_array_queue_t()
  {
    delete[] Memory;
  }

  void push( const T element )
  {
    Memory[head] = element;
    len++;

    AssertLogLine( len < SKV_ARRAY_QUEUE_SIZE )
      << "skv_array_queue_t::push():  Queue overflow"
      << " size: " << len
      << " max: " << SKV_ARRAY_QUEUE_SIZE
      << EndLogLine;

    head = (head + 1) % SKV_ARRAY_QUEUE_SIZE;
  }

  T front()
  {
    return Memory[tail];
  }

  void pop()
  {
    if( (len > 0) && ((len%SKV_ARRAY_QUEUE_SIZE) == len) )
    {
      tail = (tail + 1) % SKV_ARRAY_QUEUE_SIZE;
      len--;
    }
    else
      AssertLogLine( 1 )
        << "skv_array_queue_t::pop(): queue underflow"
        << EndLogLine;
  }

  int size()
  {
    return len;
  }

  bool empty()
  {
    return (len == 0);
  }

};

#endif // __SKV_ARRAY_QUEUE_HPP__
