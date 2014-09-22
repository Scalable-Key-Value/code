/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * test_skv_rdma_buffer_allocator.cpp
 *
 *  Created on: July 1, 2014
 *      Author: lschneid
 */

#define SKV_CLIENT_UNI
#define SKV_NON_MPI

#define SKV_UNIT_TEST

#define ITAPI_ENABLE_V21_BINDINGS
#define VP_NAME "vp_softrdma"

#include <cstdint>
#include <cstdio>
#include <string>
#include <iostream>
#include <unordered_map>
#include <thread>
#include <FxLogger.hpp>
#include "it_api.h"
#include "common/skv_types.hpp"
#include "common/skv_mutex.hpp"
#include "common/skv_array_queue.hpp"
#include "server/skv_server_heap_manager.hpp"
#include "server/skv_local_kv_rdma_data_buffer.hpp"
#include "server/skv_rdma_buffer_allocator.hpp"

using namespace std;

skv_thread_id_map_t *skv_thread_id_map_t::mThreadMap = NULL;

// Interface Adapter
it_ia_handle_t  mIA_Hdl;

// Protection Zone
it_pz_handle_t  mPZ_Hdl;

int init_itape()
{
  /***********************************************************
   * Initialize the interface adapter
   ***********************************************************/
  it_status_t itstatus = it_ia_create( VP_NAME, 2, 0, & mIA_Hdl );
  if( itstatus != IT_SUCCESS )
    return 1;

  /************************************************************
   * Initialize the protection zone
   ***********************************************************/
  itstatus = it_pz_create( mIA_Hdl, & mPZ_Hdl);
  if( itstatus != IT_SUCCESS )
    return 2;

  return 0;
}

int exit_itape()
{
  it_status_t itstatus = it_pz_free( mPZ_Hdl );
  if( itstatus != IT_SUCCESS )
    return 2;

  itstatus = it_ia_free( mIA_Hdl );
  if( itstatus != IT_SUCCESS )
    return 1;

  return 0;
}

int single_function_tests( int aLoops )
{
  int rc = 0;
  skv_local_kv_rdma_data_buffer_t rdb;
  skv_status_t status = SKV_SUCCESS;

  status = rdb.Init( mPZ_Hdl, 65536, 8192);
  if( status != SKV_SUCCESS )
  {
    cout << "ERROR Initializing rdma buffer." << endl;
    return 1;
  }

  // insert the data buffer into the thread-id-map to make sure the allocator finds the right buffer
  skv_thread_id_map_t *tm = skv_thread_id_map_t::GetThreadIdMap();
  tm->InsertRDB( &rdb );

  typedef skv_rdma_buffer_allocator_t<char> rdma_allocator;
  typedef basic_string<char, char_traits<char>, rdma_allocator> mystring;

  mystring test_str;

  test_str = "HELLO WORLD.";
  cout << "CONTENT:_" << test_str.c_str() << "_" << endl;

  test_str.append( " This is the test.");
  cout << "CONTENT:_" << test_str.c_str() << "_" << endl;

  return rc;
}

int chasing_test( int aTestLoops )
{
  const int LMR_COUNT=16;
  skv_local_kv_rdma_data_buffer_t rdb;
  skv_lmr_triplet_t lmr_list[LMR_COUNT];
  int lmr_idx = 0;
  skv_status_t status;

  int AcquiredSpace = 0;

  int rc = 0;
  const int buffersize = 500000;
  const int maxInSize = 10000;

  bool OverrunDetected = false;
  bool UnderrunDetected = false;

  int CorrectUnderRuns = 0;
  int CorrectOverRuns = 0;

  memset( lmr_list, 0, sizeof( skv_lmr_triplet_t )*LMR_COUNT );
  rdb.Init( mPZ_Hdl, buffersize, maxInSize );

  while ( --aTestLoops > 0 )
  {
    OverrunDetected = false;
    UnderrunDetected = false;

    lmr_idx = random() % LMR_COUNT;
    skv_lmr_triplet_t *lmr = &(lmr_list[ lmr_idx ]);

    // if lmr at idx is uninitialized, then acquire a new area
    if( lmr->GetAddr() == 0 )
    {
      size_t inSize = random()%maxInSize;
      status = rdb.AcquireDataArea( inSize, lmr );
      switch( status )
      {
        case SKV_SUCCESS:
          if( inSize != lmr->GetLen() )
          {
            cout << "LMR not properly set up during AcquireDataArea() call. " << inSize << " != " << lmr->GetLen() << endl;
            rc++;
          }
          AcquiredSpace += lmr->GetLen();
          break;
        case SKV_ERRNO_NO_BUFFER_AVAILABLE:
          CorrectOverRuns++;
          OverrunDetected = true;
          break;
        case SKV_ERRNO_VALUE_TOO_LARGE:
          cout << "Requested value too large: " << skv_status_to_string( status ) << " attempted size: " << inSize << endl;
          OverrunDetected = true;
          break;
        case SKV_ERRNO_NOT_DONE:
          // zero length lmr found - that's fine for the test here
          lmr->InitAbs( 0, 0, 0);
          break;
        default:
          cout << "AcquireDataArea returned unexpected error code: " << skv_status_to_string( status ) << endl;
          rc += 1000000;
          break;
      }
    }
    else
    {
      status = rdb.ReleaseDataArea( lmr );
      switch( status )
      {
        case SKV_SUCCESS:
          AcquiredSpace -= lmr->GetLen();
          break;
        case SKV_ERRNO_ELEM_NOT_FOUND:
          // error detection worked unless the lmr-size was != 0
          if(( lmr->GetLen() != 0 ) || ( lmr->GetAddr() != 0 ))
            rc++;
          if( rdb.IsEmpty() )
            UnderrunDetected = true;
          break;
        case SKV_ERRNO_VALUE_TOO_LARGE:
          OverrunDetected = true;
          break;
        default:
          cout << "ReleaseDataArea returned unexpected error code: " << skv_status_to_string( status ) << endl;
          rc += 1000000;
          break;
      }
      lmr->InitAbs(0, 0, 0);
    }

    if( UnderrunDetected )
      CorrectUnderRuns++;

    if(( !OverrunDetected ) && ( AcquiredSpace > buffersize ))
    {
      rc++;
      cout << "Allocated space (" << AcquiredSpace << ") exceeds buffer size (" << buffersize << "). undetected buffer overrun!" << endl;
    }
    if(( !UnderrunDetected ) && ( AcquiredSpace < 0 ))
    {
      rc++;
      cout << "Allocated space (" << AcquiredSpace << ") is less than zero (" << buffersize << "). undetected buffer underrun!" << endl;
    }
  }

  cout << "Correctly found buffer overruns:" << CorrectOverRuns << "; underruns:" << CorrectUnderRuns << endl;
  return rc;
}


int main( int argc, char **argv )
{
  FxLogger_Init( argv[ 0 ] );

  int rc=0;
  rc += init_itape();
  cout << "IT_APE Initialization completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;
  rc += single_function_tests( 1000000 );
  cout << "Single_Function_Test completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;
  rc += chasing_test( 1000000 );
  rc += exit_itape();
  cout << "IT_APE Exit completed with rc=" << rc << " [" << (rc==0?"PASS":"FAIL") << "]" << endl;
  return rc;
}
