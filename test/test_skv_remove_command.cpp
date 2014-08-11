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

#include <time.h>
#include <mpi.h>
#include <FxLogger.hpp>
#include <Trace.hpp>
#include <client/skv_client.hpp>
#include <math.h>

#ifndef FXLOG_SKV_TEST_SINGLE_STEPS
#define FXLOG_SKV_TEST_SINGLE_STEPS ( 0 )
#endif

#define SKV_TEST_USING_MPI


#define DATA_SIZE 300


skv_client_t Client;

#include "test_skv_utils.hpp"


int 
main(int argc, char **argv) 
{  
  printf( "skv_client::entering main \n" ); fflush( stdout );

  FxLogger_Init( argv[ 0 ] );

#ifdef SKV_TEST_USING_MPI
  int rank, numProcs;

  MPI_Init( &argc, &argv);
  MPI_Comm_size( MPI_COMM_WORLD, &numProcs );
  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
#endif


  data_direction = 1;

  int Rank = 0;
  int NodeCount = 1;

  dual_status_t ins_ret = {SKV_SUCCESS, SKV_SUCCESS};

  /*****************************************************************************
   * Init the SKV Client
   ****************************************************************************/ 
  skv_status_t status = Client.Init( 0,
#ifdef SKV_TEST_USING_MPI
                                      MPI_COMM_WORLD,
#endif
                                      0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "test_skv_remove_command::main():: SKV Client Init succeded "
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "test_skv_remove_command::main():: SKV Client Init FAILED "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }  
  /****************************************************************************/ 



  /*****************************************************************************
   * Connect to the SKV Server
   ****************************************************************************/ 
  BegLogLine( 1 )
    << "test_skv_remove_command::main():: About to connect "
    << EndLogLine;

  status = Client.Connect( NULL, 0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "test_skv_remove_command::main():: SKV Client connected"
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "test_skv_remove_command::main():: SKV Client FAILED to connect "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/ 


  /*****************************************************************************
   * Open a test PDS
   ****************************************************************************/
  char MyTestPdsName[ SKV_MAX_PDS_NAME_SIZE ];
  bzero( MyTestPdsName, SKV_MAX_PDS_NAME_SIZE );

  if( Rank == 0 )
    {
      struct timespec ts;

      //clock_gettime( CLOCK_REALTIME, &ts );

      //sprintf( MyTestPdsName, "TestPds_%08X_%08X", ts.tv_sec, ts.tv_nsec );
      sprintf( MyTestPdsName, "UnitTestInsertPds" );
    }

  BegLogLine( 1 )
    << "test_skv_remove_command::main():: About to open pds name: "
    << " MyTestPdsName: " << MyTestPdsName
    << EndLogLine;

  skv_pds_id_t  MyPDSId;
  status = Client.Open( MyTestPdsName,
                        (skv_pds_priv_t) (SKV_PDS_READ | SKV_PDS_WRITE),
                        (skv_cmd_open_flags_t) SKV_COMMAND_OPEN_FLAGS_CREATE,
                        & MyPDSId );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "test_skv_remove_command::main():: SKV Client successfully opened: "
        << MyTestPdsName
        << " MyPDSId: " << MyPDSId
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "test_skv_remove_command::main():: SKV Client FAILED to open: "
        << MyTestPdsName
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  
  int numServers = SKVTestGetServerCount();

  /****************************************************************************/
  // test default insert flag (FLAGS_NONE)
  skv_cmd_remove_flags_t testFlag = SKV_COMMAND_REMOVE_FLAGS_NONE;

  int Key = 0;
  for( int initialKey = 0; initialKey < numServers; initialKey++)  // loop to make sure that we tried to talk to all servers
    {
      BegLogLine( 1 )
        << "Running initialKey: " << initialKey
        << " out of: " << numServers
        << EndLogLine;

      Key = initialKey;


#ifdef SKV_TEST_USING_MPI
      Key += rank;
#endif
      ins_ret = DoInsertRetrieveTest( &MyPDSId,
                                      Key,
                                      SKV_COMMAND_RIU_FLAGS_NONE,
                                      DATA_SIZE );

      BegLogLine( 1 )
        << "test_skv_remove_command::main():: Inserting test with " << skv_RIU_flags_to_string ( SKV_COMMAND_RIU_FLAGS_NONE ) 
        << ". Insert status: " << skv_status_to_string( ins_ret.insert )
        << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
        << EndLogLine;
  

      /****************************************************************************/
      // test default remove flag (FLAGS_NONE)
 
      testFlag = SKV_COMMAND_REMOVE_FLAGS_NONE;
      status = Client.Remove( &MyPDSId,
                              (char*) &Key,
                              sizeof(Key),
                              testFlag );

      BegLogLine( 1 )
        << "test_skv_remove_command::main():: Removing "
        << ". Status: " << skv_status_to_string( status )
        << EndLogLine;

    }
  
#ifdef SKV_TEST_USING_MPI
  MPI_Barrier( MPI_COMM_WORLD );
#endif

  /****************************************************************************/
  // error-condition checks if it's really removed
  char *RetrieveBuf  = (char*)malloc(DATA_SIZE * 10);
  int   RetrieveSize = DATA_SIZE;
  int   Retrieved    = 0;

  status = Client.Retrieve( &MyPDSId,
                            (char *) &Key,
                            sizeof( Key ),
                            RetrieveBuf,
                            RetrieveSize,
                            & Retrieved,
                            0,
                            SKV_COMMAND_RIU_FLAGS_NONE );

  BegLogLine( 1 )
    << "test_skv_remove_command::main():: Retrieving "
    << ". Status: " << skv_status_to_string( status )
    << EndLogLine;
  

  /****************************************************************************/
  // test remove of non-existent key
 
  Key += numServers;
#ifdef SKV_TEST_USING_MPI
  Key += rank;
  Key += numProcs;
#endif
  testFlag = SKV_COMMAND_REMOVE_FLAGS_NONE;
  status = Client.Remove( &MyPDSId,
                          (char*) &Key,
                          sizeof(Key),
                          testFlag );

  BegLogLine( 1 )
    << "test_skv_remove_command::main():: Removing "
    << ". Status: " << skv_status_to_string( status )
    << EndLogLine;
  


  /*****************************************************************/
  /* close and disconnect */
  //  status = Client.Close( &MyPDSId );


#ifdef SKV_TEST_USING_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif

  BegLogLine( 1 )
    << "test_skv_remove_command::main():: Closed with status: " << status
    << EndLogLine;

  status = Client.Disconnect();

  BegLogLine( 1 )
    << "test_skv_remove_command::main():: Disconnected with status: " << status
    << EndLogLine;

  status = Client.Finalize();


#ifdef SKV_TEST_USING_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif

#ifdef SKV_TEST_USING_MPI
  MPI_Finalize();
#endif
  
  return 0;
}

