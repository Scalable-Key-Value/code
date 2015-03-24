/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * test_skv_ringbuffer_ptr.cpp
 *
 *  Created on: May 23, 2014
 *      Author: lschneid
 */

#ifndef SKV_CLIENT_UNI
#define SKV_CLIENT_UNI
#endif

#ifndef SKV_NON_MPI
#define SKV_NON_MPI
#endif

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <FxLogger.hpp>
#include "skv/common/skv_types.hpp"
#include "skv/common/skv_mutex.hpp"
#include "skv/common/skv_array_queue.hpp"
#include "skv/server/skv_local_kv_rdma_data_buffer.hpp"

using namespace std;

int single_function_tests( int aLoops, int aGran )
{
  int rc = 0;
  size_t size = 100000 * aGran;
  char *buffer = new char[ size ];

  // initialization test
  skv_ringbuffer_ptr a(buffer, size, aGran );

  if( a.GetPtr() != buffer ) rc++;
  if( a.GetOffset() != 0 ) rc++;
  if( a.GetSpace() != size ) rc++;

  if( rc ) return rc;

  for( int i=0; i<aLoops; i++ )
  {
    size_t aligned = ( random()*(size_t)aGran ) % ( size * 2 );   // create an aligned size
    size_t offset = aligned - ( random() % aGran );               // reduce the size to be potentially unaligned
    a = a + offset;
    if( a.GetOffset() != (aligned%size) ) rc++;

    size_t alignedM = ( random()*(size_t)aGran ) % ( size * 2 );   // create an aligned size
    size_t offsetM = alignedM - ( random() % aGran );               // reduce the size to be potentially unaligned
    a = a - (offsetM);
    if( a.GetOffset() != ((aligned + 2*size -alignedM)%size) ) rc += aLoops*10;

//    cout << "rc=" << rc << "; a.off=" << a.GetOffset() << "/" << size << "; sub=" << offset << "-" << offsetM
//        << "; aligned=" << aligned << "-" << alignedM << endl;

    a.Reset();
  }

  delete buffer;
  return rc;
}

int chasing_test()
{
  skv_ringbuffer_ptr rbH, rbT;

  int rc = 0;
  const int buffersize = 50000;
  const int maxInSize = 20000;
  const int maxOutSize = 19500;

  bool overrun = false;
  bool underrun = false;

  char *bufferbase = new char[ buffersize ];

  rbH.Init( bufferbase, buffersize );
  rbT.Init( bufferbase, buffersize );

  while ( rbH >= rbT )
  {
    size_t inSize = random()%maxInSize;
    size_t outSize = random()%maxOutSize;

    rbH = rbH + inSize;
    rbT = rbT + outSize;

    overrun = ( rbH.diff( rbT ) > buffersize );
    if(  (rbH.GetOffset()>rbT.GetOffset()) && (rbH.GetWrapState()!=rbT.GetWrapState()) )
    {
      if( !overrun )
      {
        rc++;
        cout << "Head passed Tail: undetected buffer overrun!" << endl;
      }
    }

    underrun = ( rbH.diff( rbT) < 0 );
    if( (rbH.GetOffset() < rbT.GetOffset()) && (rbH.GetWrapState() == rbT.GetWrapState()) )
    {
      if( !underrun )
      {
        rc++;
        cout << "Tail passed Head: undetected buffer underrun!" << endl;
      }
    }
  }
  return 0;
}

int main( int argc, char **argv )
{
  int rc=0;
  for( int g=1; g<32; g = g << 1 )
  {
    rc += single_function_tests(10000, g);
    cout << "Single_Function_Test with granularity " << g << " completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;
  }

  int testloops = 10000;
  for( int i=0; i<testloops; i++ )
  {
    rc += chasing_test();
  }
  cout << "Head/Tail_Chasing_Test completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;
  return rc;
}
