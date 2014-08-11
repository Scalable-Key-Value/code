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

#define SKV_CLIENT_UNI
#define SKV_NON_MPI

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <FxLogger.hpp>
#include "common/skv_types.hpp"
#include "common/skv_mutex.hpp"
#include "common/skv_array_queue.hpp"
#include "server/skv_local_kv_rdma_data_buffer.hpp"

using namespace std;

int single_function_tests( int aLoops )
{
  int rc = 0;
  size_t size = 10000;
  char *buffer = new char[ size ];

  // initialization test
  skv_ringbuffer_ptr a(buffer, size);

  if( a.GetPtr() != buffer ) rc++;
  if( a.GetOffset() != 0 ) rc++;
  if( a.GetSpace() != size ) rc++;

  if( rc ) return rc;

  for( int i=0; i<aLoops; i++ )
  {
    size_t offset = random()%(size * 2);
    a = a + offset;
    if( a.GetOffset() != (offset%size) ) rc++;
    a.Reset();
  }

  if( rc ) return rc;

  for( int i=0; i<aLoops; i++ )
  {
    size_t offset = random()%(size);
    a = a - (offset);
    if(( a.GetSpace() != (offset%size) ) || ( !a.GetWrapState() )) rc += aLoops*10;
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
  rc += single_function_tests(10000);
  cout << "Single_Function_Test completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;

  int testloops = 10000;
  for( int i=0; i<testloops; i++ )
  {
    rc += chasing_test();
  }
  cout << "Head/Tail_Chasing_Test completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;
  return rc;
}
