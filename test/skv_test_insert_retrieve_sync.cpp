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

#ifndef SKV_TEST_TRACE
#define SKV_TEST_TRACE ( 1 )
#endif

#ifndef SKV_TEST_LOG
#define SKV_TEST_LOG ( 0 )
#endif

skv_client_t Client;
#define DATA_SIZE        ( 4096 )

#include "test_skv_utils.hpp"
#include <server/skv_server_heap_manager.hpp>   // to get the server space per snode!


static TraceClient SKVInsertStart;
static TraceClient SKVInsertFinis;

static TraceClient SKVRetrieveStart;
static TraceClient SKVRetrieveFinis;

// #define NUMBER_OF_TRIES  (  16 * 512  )
#define PURESTORAGE_FACTOR ( (double)0.7 )                                                      // factor to reflect space overhead in server space, represents the available fraction of space per server
#define STORAGE_SPACE_PER_SNODE     ( (int)(PERSISTENT_IMAGE_MAX_LEN * PURESTORAGE_FACTOR) )    // space available per server (reduced by an arbitrary space-overhead factor
#define MAX_NUMBER_OF_TRIES_PER_SRV (  250  )    // max number of attempts per server
#define MAX_TEST_SIZE_EXP ( 20 )       // 2^x max value size
#define SKV_TEST_START_INDEX ( 2 )    // 2^x min value size

//#define DATA_SIZE        ( 1024 * 1024 )
//#define DATA_SIZE        ( 10 * 1024 * 1024 )

//#define DO_CHECK

void
getDataSizes( int** aDataSizeArray, int* aCount, int aBegPower, int aEndPower )
{
  int beginPower = aBegPower;
  int endPower   = aEndPower;

  int SizeCount = endPower - beginPower + 1;

  int* SizeArray = (int *) malloc( sizeof( int ) * SizeCount );
  StrongAssertLogLine( SizeArray != NULL )
    << "ERROR: "
    << EndLogLine;

  int BeginSize = 1;
  for( int i=0; i < beginPower; i++ )
    {
      BeginSize *= 2;
    }

  SizeArray[ 0 ] = BeginSize;

  for( int i = 1; i < SizeCount; i++ )
    {
      SizeArray[ i ] = 2 * SizeArray[ i - 1 ];
    }

  *aDataSizeArray = SizeArray;
  *aCount = SizeCount;
}

static inline
int
calculateValue( int r, int a, int b )
{
  return (int)((a + b) & 0xFF);
}

static inline
int
calculateKey( int rank,
              int si,
              int t,
              int num_tries )
{
  return ( rank * num_tries ) + t;
}


int 
main(int argc, char **argv) 
{  
  printf( "skv_client::entering main \n" ); fflush( stdout );

  FxLogger_Init( argv[ 0 ] );
  pkTraceServer::Init();

  int Rank = 0;
  int NodeCount = 1;

  /*****************************************************************************
   * Init MPI
   ****************************************************************************/ 
  MPI_Init( &argc, &argv );
  MPI_Comm_rank( MPI_COMM_WORLD, &Rank );
  MPI_Comm_size( MPI_COMM_WORLD, &NodeCount );
  /****************************************************************************/ 

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
        << "skv_test_n_inserts_retrieves::main():: SKV Client Init succeded "
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "skv_test_n_inserts_retrieves::main():: SKV Client Init FAILED "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }  
  /****************************************************************************/ 



  /*****************************************************************************
   * Connect to the SKV Server
   ****************************************************************************/ 
  BegLogLine( 1 )
    << "skv_test_n_inserts_retrieves::main():: About to connect "
    << EndLogLine;

  status = Client.Connect( NULL, 0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "skv_test_n_inserts_retrieves::main():: SKV Client connected "
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to connect "
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
      sprintf( MyTestPdsName, "TestPds" );
    }
#if 0
  BegLogLine( 1 )
    << "skv_test_n_inserts_retrieves::main():: Before MPI_Bcast() "
    << EndLogLine;

  MPI_Bcast( MyTestPdsName,
             SKV_MAX_PDS_NAME_SIZE,
             MPI_CHAR, 
             0,
             MPI_COMM_WORLD );

  BegLogLine( 1 )
    << "skv_test_n_inserts_retrieves::main():: About to open pds name: "
    << " MyTestPdsName: " << MyTestPdsName
    << EndLogLine;

  MPI_Barrier( MPI_COMM_WORLD );
#endif

  BegLogLine( 1 )
    << "skv_test_n_inserts_retrieves::main():: About to open pds name: "
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
        << "skv_test_n_inserts_retrieves::main():: SKV Client successfully opened: "
        << MyTestPdsName
        << " MyPDSId: " << MyPDSId
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to open: "
        << MyTestPdsName
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/


  int* DataSizes = NULL;
  int  DataSizeCount = 0;
  getDataSizes( & DataSizes, & DataSizeCount, 2, MAX_TEST_SIZE_EXP );

  MPI_Barrier( MPI_COMM_WORLD );

  uint64_t NUMBER_OF_TRIES = 0;
  uint64_t NUMBER_OF_SNODES = SKVTestGetServerCount();

  for( int sizeIndex=SKV_TEST_START_INDEX; sizeIndex < DataSizeCount; sizeIndex++ )
    {
      int testDataSize = DataSizes[ sizeIndex ];

      uint64_t total_records = (uint64_t)STORAGE_SPACE_PER_SNODE / (uint64_t) testDataSize * (uint64_t)NUMBER_OF_SNODES;
      NUMBER_OF_TRIES = total_records / NodeCount;

      BegLogLine( 1 | SKV_TEST_LOG )
        << "SKV_TEST: servers: " << NUMBER_OF_SNODES
        << " clients: " << NodeCount
        << " vsize: " << testDataSize
        << " totalRecords: " << total_records
        << " iterations: " << NUMBER_OF_TRIES
        << " max_iter: " << MAX_NUMBER_OF_TRIES_PER_SRV * NUMBER_OF_SNODES
        << EndLogLine;

      if( NUMBER_OF_TRIES > MAX_NUMBER_OF_TRIES_PER_SRV * NUMBER_OF_SNODES )
        NUMBER_OF_TRIES = MAX_NUMBER_OF_TRIES_PER_SRV * NUMBER_OF_SNODES;

      printf( "running test with data size: %d\n", testDataSize );

      /*****************************************************************************
       * Allocate Insert/Retrieve data arrays
       ****************************************************************************/
      // Create Insert Key / Value
      int   MyDataInsertSize = testDataSize;
      char* MyDataInsert    = (char *) malloc( MyDataInsertSize );
      StrongAssertLogLine( MyDataInsert != NULL )    
        << "skv_test_n_inserts_retrieves::main():: ERROR:: MyDataInsert != NULL"
        << EndLogLine;

      // Allocate Retrieve Data
      char* MyDataRetrieve    = (char *) malloc( MyDataInsertSize );
      StrongAssertLogLine( MyDataRetrieve != NULL )    
        << "skv_test_n_inserts_retrieves::main():: ERROR:: MyDataRetrieve != NULL"
        << EndLogLine;

      double StartTime = MPI_Wtime();
      double InsertTime = 0.0;
      double RetrieveTime = 0.0;

      int TestFailed = 0;
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {

          /*****************************************************************************
           * Insert Key / Value
           ****************************************************************************/
#ifdef DO_CHECK
          for( int i=0; i < MyDataInsertSize; i++ )
            {
              char ch = i;
              MyDataInsert[ i ] = calculateValue( Rank, ch, t );
            }
#endif      
          // int Key = Rank * NUMBER_OF_TRIES + t;
          int Key = calculateKey( Rank, sizeIndex, t, NUMBER_OF_TRIES );
          // int Key = ( Rank * DataSizeCount * NUMBER_OF_TRIES) 
          //   + (sizeIndex * NUMBER_OF_TRIES ) 
          //   + t;

          BegLogLine( SKV_TEST_LOG )
            << "skv_test_n_inserts_retrieves::main():: About to Insert "
            << " into MyPDSId: " << MyPDSId
            << EndLogLine;      

          double InsertStartTime = MPI_Wtime();

          SKVInsertStart.HitOE( SKV_TEST_TRACE, 
                                 "SKVInsert",
                                 Rank, 
                                 SKVInsertStart );

          status = Client.Insert( &MyPDSId,
                                  (char *) &Key,
                                  sizeof( int ),
                                  MyDataInsert, 
                                  MyDataInsertSize,
                                  0,
                                  SKV_COMMAND_RIU_FLAGS_NONE );

          SKVInsertFinis.HitOE( SKV_TEST_TRACE, 
                                 "SKVInsert",
                                 Rank, 
                                 SKVInsertFinis );

          double InsertFinishTime = MPI_Wtime();

          InsertTime += ( InsertFinishTime - InsertStartTime );

          if( status == SKV_SUCCESS )
            {
              BegLogLine( SKV_TEST_LOG )
                << "skv_test_n_inserts_retrieves::main():: SKV Client successfully Inserted "
                << " into MyPDSId: " << MyPDSId
                << EndLogLine;
            }
          else
            {
              BegLogLine( 1 )
                << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to Insert: "
                << " into MyPDSId: " << MyPDSId
                << " status: " << skv_status_to_string( status )
                << EndLogLine;
            }  
          /****************************************************************************/





          /*****************************************************************************
           * Retrieve Key / Value
           ****************************************************************************/
#ifdef DO_CHECK
          bzero( MyDataRetrieve, MyDataInsertSize );
#endif

          int RetrieveSize = -1;

          double RetrieveStartTime = MPI_Wtime();

          SKVRetrieveStart.HitOE( SKV_TEST_TRACE, 
                                   "SKVRetrieve",
                                   Rank, 
                                   SKVRetrieveStart );

          status = Client.Retrieve( &MyPDSId,
                                    (char *) &Key,
                                    (int) sizeof( int ),
                                    MyDataRetrieve, 
                                    MyDataInsertSize,
                                    & RetrieveSize,
                                    0,
                                    SKV_COMMAND_RIU_FLAGS_NONE );

          SKVRetrieveFinis.HitOE( SKV_TEST_TRACE, 
                                   "SKVRetrieve",
                                   Rank, 
                                   SKVRetrieveFinis );

          double RetrieveFinishTime = MPI_Wtime();

          RetrieveTime += ( RetrieveFinishTime - RetrieveStartTime );

          if( status == SKV_SUCCESS )
            {
              BegLogLine( SKV_TEST_LOG )
                << "skv_test_n_inserts_retrieves::main():: SKV Client successfully Retrieved: "
                << " MyPDSId: " << MyPDSId
                << EndLogLine;
            }
          else
            {
              BegLogLine( 1 )
                << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to Retieve: "
                << " MyPDSId: " << MyPDSId
                << " status: " << skv_status_to_string( status )
                << EndLogLine;
            }  
          /****************************************************************************/



#ifdef DO_CHECK
          /*****************************************************************************
           * Check results
           ****************************************************************************/
          int Match = 1;
          for( int i=0; i<MyDataInsertSize; i++ )
            {
              char ch = i;
              if( MyDataRetrieve[ i ] != calculateValue( Rank, ch, t ) )
                {
                  BegLogLine( 1 )
                    << "skv_test_n_inserts_retrieves::main():: Retrieve Result does NOT match: { "
                    << MyDataRetrieve[ i ] << " , "
                    << calculateValue( Rank, ch, t ) << " }" 
                    << EndLogLine;

                  Match = 0;
                }
            }

          if( !Match )	
            {
              TestFailed = 1;
              break;
            }
          /****************************************************************************/
#endif
        }

      double FinisTime = MPI_Wtime();

      /****************************************************************************/
      /* REMOVE content for next try **/
      /****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          int RetrieveSize = -1;
          // int Key = Rank * NUMBER_OF_TRIES + t;
          int Key = calculateKey( Rank, sizeIndex, t, NUMBER_OF_TRIES );
          // int Key = ( Rank * DataSizeCount * NUMBER_OF_TRIES) 
          //   + (sizeIndex * NUMBER_OF_TRIES ) 
          //   + t;

          int RetrivedSize = 0;
          status = Client.Remove( &MyPDSId,
                                  (char *) &Key,
                                  (int) sizeof( int ),
                                  SKV_COMMAND_REMOVE_FLAGS_NONE
                                  );

          StrongAssertLogLine( status == SKV_SUCCESS )
            << "ERROR: removing data"
            << EndLogLine;	  
        }


      /****************************************************************************/






      double AvgTime         = ((FinisTime-StartTime) / NUMBER_OF_TRIES);
      double InsertAvgTime   = ( InsertTime / NUMBER_OF_TRIES );
      double RetrieveAvgTime = ( RetrieveTime / NUMBER_OF_TRIES );

      double GlobalInsertAvgTime = 0.0;
      double GlobalRetrieveAvgTime = 0.0;
      MPI_Reduce( & InsertAvgTime, & GlobalInsertAvgTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );
      MPI_Reduce( & RetrieveAvgTime, & GlobalRetrieveAvgTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );

      GlobalInsertAvgTime = GlobalInsertAvgTime / (1.0 * NodeCount);
      GlobalRetrieveAvgTime = GlobalRetrieveAvgTime / (1.0 * NodeCount);

      double InsertBandwidth   = ( testDataSize / ( GlobalInsertAvgTime * 1024.0 * 1024.0 ) );
      double RetrieveBandwidth = ( testDataSize / ( GlobalRetrieveAvgTime * 1024.0 * 1024.0 ) );

      BegLogLine( 1 )
        << "skv_test_n_inserts_retrieves::main():: TIMING: " 
        << " log2(ValueSize): " << log2( (double) testDataSize )
        << " ValueSize: " << testDataSize
        << " TryCount: " << NUMBER_OF_TRIES
        // << " AvgTime: " << AvgTime
        << " InsertAvgTime: " << GlobalInsertAvgTime
        << " InsertBandwidth: " << InsertBandwidth
        << " RetrieveAvgTime: " << GlobalRetrieveAvgTime
        << " RetrieveBandwidth: " << RetrieveBandwidth	
        << EndLogLine;


      if( TestFailed )
        {
          BegLogLine( 1 )
            << "skv_test_n_inserts_retrieves::main():: SKV Client Result Match FAILED :-("
            << EndLogLine;
        }
      else
        {
          BegLogLine( 1 )
            << "skv_test_n_inserts_retrieves::main():: SKV Client Result Match SUCCESSFUL :-)"
            << EndLogLine;
        }  

      free( MyDataInsert );
      MyDataInsert = NULL;

      free( MyDataRetrieve );
      MyDataRetrieve = NULL;

      MPI_Barrier( MPI_COMM_WORLD );
    }

  pkTraceServer::FlushBuffer();

  MPI_Finalize();

  return 0;
}
