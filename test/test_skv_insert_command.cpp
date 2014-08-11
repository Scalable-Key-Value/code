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

 // make sure this is a single-node program
#ifndef SKV_CLIENT_UNI
#define SKV_CLIENT_UNI
#endif

#include <time.h>
//#include <mpi.h>
#include <FxLogger.hpp>
#include <Trace.hpp>
#include <client/skv_client.hpp>
#include <math.h>

#ifndef FXLOG_SKV_TEST_SINGLE_STEPS
#define FXLOG_SKV_TEST_SINGLE_STEPS ( 0 )
#endif

#define DEFAULT_DATA_SIZE 256

skv_client_t Client;

#include "test_skv_utils.hpp"


int 
main(int argc, char **argv) 
{  
  printf( "skv_client::entering main \n" ); fflush( stdout );

  FxLogger_Init( argv[ 0 ] );
  data_direction = 1;

  int Rank = 0;
  int NodeCount = 1;

  dual_status_t ins_ret = {SKV_SUCCESS, SKV_SUCCESS};

  /*****************************************************************************
   * Init the SKV Client
   ****************************************************************************/ 
  skv_status_t status = Client.Init( 0,
#ifndef SKV_CLIENT_UNI
                                      MPI_COMM_WORLD,
#endif
                                      0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "test_skv_insert_command::main():: SKV Client Init succeded "
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "test_skv_insert_command::main():: SKV Client Init FAILED "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }  
  /****************************************************************************/ 



  /*****************************************************************************
   * Connect to the SKV Server
   ****************************************************************************/ 
  BegLogLine( 1 )
    << "test_skv_insert_command::main():: About to connect "
    << EndLogLine;

  status = Client.Connect( NULL, 0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "test_skv_insert_command::main():: SKV Client connected"
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "test_skv_insert_command::main():: SKV Client FAILED to connect "
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
    << "test_skv_insert_command::main():: About to open pds name: "
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
        << "test_skv_insert_command::main():: SKV Client successfully opened: "
        << MyTestPdsName
        << " MyPDSId: " << MyPDSId
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "test_skv_insert_command::main():: SKV Client FAILED to open: "
        << MyTestPdsName
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/
  // test default insert flag (FLAGS_NONE)
  int Key = 1;
  skv_cmd_RIU_flags_t testFlag = SKV_COMMAND_RIU_FLAGS_NONE;
  BegLogLine( 1 )
    << "test_skv_insert_command::main()::"
    << " New Key: " << Key
    << EndLogLine;

  ins_ret = DoInsertRetrieveTest( &MyPDSId,
                                  Key,
                                  testFlag,
                                  DEFAULT_DATA_SIZE );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  /****************************************************************************/
  // test default insert flag (FLAGS_UPDATE)

  testFlag = SKV_COMMAND_RIU_UPDATE;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  /****************************************************************************/
  // test default insert flag (FLAGS_UPDATE with smaller size)

  testFlag = SKV_COMMAND_RIU_UPDATE;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   -1 );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  
  /****************************************************************************/
  // test default insert flag (FLAGS_UPDATE with larger size)

  testFlag = SKV_COMMAND_RIU_UPDATE;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   5);

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  
  /****************************************************************************/
  // test default insert flag (FLAGS_APPEND with larger size)

  testFlag = SKV_COMMAND_RIU_APPEND;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   0);

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  /****************************************************************************/
  // test default insert flag (FLAGS_APPEND with larger size)

  testFlag = SKV_COMMAND_RIU_APPEND;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   0);

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  /****************************************************************************/
  // test default insert flag (FLAGS_APPEND with larger size)

  testFlag = SKV_COMMAND_RIU_APPEND;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   0);

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  

  /****************************************************************************/
  // test default insert flag (FLAGS_EXPAND with larger size)

  // insert a new record first
  Key = 3;
  BegLogLine( 1 )
    << "test_skv_insert_command::main()::"
    << " New Key: " << Key
    << EndLogLine;

  testFlag = SKV_COMMAND_RIU_FLAGS_NONE;
  ins_ret = DoInsertRetrieveTest( &MyPDSId,
                                  Key,
                                  testFlag,
                                  DEFAULT_DATA_SIZE );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  
  // expand this record
  testFlag = (skv_cmd_RIU_flags_t)(SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE | SKV_COMMAND_RIU_INSERT_OVERLAPPING);
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   10);

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  

  /****************************************************************************/
  // test default insert flag (FLAGS_OVERWRITE_ON_DUP with larger size)

  testFlag = SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP;
  ins_ret =  DoInsertRetrieveTest( &MyPDSId,
                                   Key,
                                   testFlag,
                                   DEFAULT_DATA_SIZE,
                                   0);

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Check with " << skv_RIU_flags_to_string ( testFlag ) 
    << ". Insert status: " << skv_status_to_string( ins_ret.insert )
    << " Retrieve status: " << skv_status_to_string( ins_ret.retrieve )
    << EndLogLine;
  


  /*****************************************************************/
  /* close and disconnect */
  skv_pds_attr_t pds_attr;
  pds_attr.mPDSId = MyPDSId;
  pds_attr.mSize  = 500;

  status = Client.PDScntl( SKV_PDSCNTL_CMD_STAT_SET,
                           & pds_attr );
  BegLogLine( 1 )
    << "test_skv_insert_command::main():: "
    << " PDScntl return: " << pds_attr
    << EndLogLine;



  status = Client.PDScntl( SKV_PDSCNTL_CMD_STAT_GET,
                           & pds_attr );
  BegLogLine( 1 )
    << "test_skv_insert_command::main():: "
    << " PDScntl return: " << pds_attr
    << EndLogLine;


  status = Client.Close( &MyPDSId );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Closed with status: " << status
    << EndLogLine;

  status = Client.Disconnect();

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: Disconnected with status: " << status
    << EndLogLine;

  
  return 0;
}
