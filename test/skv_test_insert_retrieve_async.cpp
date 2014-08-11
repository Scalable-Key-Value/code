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

#include <map>

skv_client_t Client;
#define DATA_SIZE        ( 4096 )

#include "test_skv_utils.hpp"
#include <server/skv_server_heap_manager.hpp>   // to get the server space per snode!

// #define NUMBER_OF_TRIES  (  16 * 512  )
#define PURESTORAGE_FACTOR ( (double)0.8 )                                                      // factor to reflect space overhead in server space, represents the available fraction of space per server
#define STORAGE_SPACE_PER_SNODE     ( (int)(PERSISTENT_IMAGE_MAX_LEN * PURESTORAGE_FACTOR) )    // space available per server (reduced by an arbitrary space-overhead factor
#define MAX_NUMBER_OF_TRIES_PER_SRV (  250  )    // max number of attempts per server
#define MAX_TEST_SIZE_EXP ( 20 )       // 2^x max value size
#define SKV_TEST_START_INDEX ( 2 )    // 2^x min value size

#define MAX_ALLOC_FOR_TRIES ( 8 * 1024 * 1024)


//#define DATA_SIZE        ( 1024 * 1024 )
//#define DATA_SIZE        ( 10 * 1024 * 1024 )

#ifndef SKV_TEST_LOG
#define SKV_TEST_LOG ( 0 )
#endif

//#define SKIP_LOCAL_KEYS
//#define SLOWDOWN 10000
//#define SKV_TEST_MAPPED_HANDELS
//#define DONT_RETRIEVE
//#define DO_CHECK

#define SERVER_COUNT (8)
const int DCSMAP[SERVER_COUNT] = {3, 0, 5, 2, 7, 4, 1, 6};
//const int DCSMAP[SERVER_COUNT] = {0, 1};


struct skv_async_command_helper_t
{
  int                       mBufferSize;
  char*                     mBuffer;
  int                       mRetrieveBufferSize;

  skv_client_cmd_ext_hdl_t mCommandHdl;
};

#define STL_MAP( Key, Data ) std::map< Key, Data >

typedef STL_MAP( skv_client_cmd_ext_hdl_t, skv_async_command_helper_t * ) command_handle_to_timer_map_t;

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
  printf(" %d: MPI_Init complete\n", Rank);
  /****************************************************************************/ 



  /*****************************************************************************
   * Init the SKV Client
   ****************************************************************************/ 
  skv_status_t status = Client.Init( 0,
#ifndef SKV_CLIENT_UNI
                                      MPI_COMM_WORLD,
#endif
                                      Rank );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client Init succeded "
        << EndLogLine;
    }
  else
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client Init FAILED "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }  
  /****************************************************************************/ 



  /*****************************************************************************
   * Connect to the SKV Server
   ****************************************************************************/ 
  BegLogLine( SKV_TEST_LOG )
    << "skv_test_n_inserts_retrieves::main():: About to connect "
    << EndLogLine;

  status = Client.Connect( NULL, 0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client connected"
        << EndLogLine;
    }
  else
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to connect. "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/ 




  /*****************************************************************************
   * Open a test PDS
   ****************************************************************************/
  char MyTestPdsName[ SKV_MAX_PDS_NAME_SIZE ];
  bzero( MyTestPdsName, SKV_MAX_PDS_NAME_SIZE );

  if( Rank >= 0 )
    {
      struct timespec ts;

      //clock_gettime( CLOCK_REALTIME, &ts );

      //sprintf( MyTestPdsName, "TestPds_%08X_%08X", ts.tv_sec, ts.tv_nsec );
      sprintf( MyTestPdsName, "TestPds" );
    }
#if 0
  BegLogLine( SKV_TEST_LOG )
    << "skv_test_n_inserts_retrieves::main():: Before MPI_Bcast() "
    << EndLogLine;

  MPI_Bcast( MyTestPdsName,
             SKV_MAX_PDS_NAME_SIZE,
             MPI_CHAR, 
             0,
             MPI_COMM_WORLD );

  BegLogLine( SKV_TEST_LOG )
    << "skv_test_n_inserts_retrieves::main():: About to open pds name: "
    << " MyTestPdsName: " << MyTestPdsName
    << EndLogLine;

  MPI_Barrier( MPI_COMM_WORLD );
#endif

  BegLogLine( SKV_TEST_LOG )
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
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client successfully opened: "
        << MyTestPdsName
        << " MyPDSId: " << MyPDSId
        << EndLogLine;
    }
  else
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to open: "
        << MyTestPdsName
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/


#ifdef SKV_TEST_MAPPED_HANDELS
  command_handle_to_timer_map_t* CommandHandleToTimerMap = new command_handle_to_timer_map_t;
  StrongAssertLogLine( CommandHandleToTimerMap != NULL )
    << "ERROR: "
    << EndLogLine;
#endif

  int* DataSizes = NULL;
  int  DataSizeCount = 0;
  getDataSizes( & DataSizes, & DataSizeCount, 0, MAX_TEST_SIZE_EXP );

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

#ifdef DO_CHECK
      // limit the number to tries to fit buffers in memory
      if( NUMBER_OF_TRIES * testDataSize > MAX_ALLOC_FOR_TRIES )
        NUMBER_OF_TRIES = MAX_ALLOC_FOR_TRIES / testDataSize;
#endif

      printf( "running test with data size: %d\n", testDataSize );

      /*****************************************************************************
       * Allocate Insert/Retrieve data arrays
       ****************************************************************************/
      skv_async_command_helper_t commandHelpers[ NUMBER_OF_TRIES ];

#ifdef DO_CHECK
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {

          commandHelpers[ t ].mBufferSize = testDataSize;
          commandHelpers[ t ].mBuffer = (char *) malloc( testDataSize );
          StrongAssertLogLine( commandHelpers[ t ].mBuffer != NULL )    
            << "ERROR:: "
            << " testDataSize: " << testDataSize
            << EndLogLine;

          for( int i=0; i < testDataSize; i++ )
            {
              char ch = i;
              commandHelpers[ t ].mBuffer[ i ] = calculateValue( Rank, ch, t );
            }
        }
#else
      char* Buffer = (char *) malloc( testDataSize );
      StrongAssertLogLine( Buffer != NULL )    
        << "ERROR:: "
        << " testDataSize: " << testDataSize
        << EndLogLine;      
#endif

      /************************************************************************
       * create key-list
       ************************************************************************/
      int skips = 0;
      int *key_list = (int*)malloc(sizeof(int) * NUMBER_OF_TRIES);
      StrongAssertLogLine( key_list != NULL )
        << "ERROR: "
        << " key_list allocation. maybe OOM"
        << EndLogLine;

      for( int t=0; t < NUMBER_OF_TRIES+skips; t++ )
        {      
          int Key = calculateKey( Rank, sizeIndex, t, (NUMBER_OF_TRIES*NodeCount) );
#ifdef SKIP_LOCAL_KEYS
          if (Key % NUMBER_OF_SNODES == DCSMAP[Rank]) {
            skips++;
            continue;
          }
#endif
          key_list[t-skips] = Key;
        }


      /*****************************************************************************
       * Insert Key / Value
       ****************************************************************************/
      double InsertTimeStart = MPI_Wtime();
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {      
          int *Key = &key_list[t];

          BegLogLine( SKV_TEST_LOG )
            << "skv_test_n_inserts_retrieves::main():: About to Insert "
            << " into MyPDSId: " << MyPDSId
            << " key: " << *Key
            << EndLogLine;      

          status = Client.iInsert( &MyPDSId,
                                   (char *) Key,
                                   sizeof( int ),
#ifdef DO_CHECK
                                   commandHelpers[ t ].mBuffer,
                                   commandHelpers[ t ].mBufferSize,
#else
                                   Buffer, 
                                   testDataSize,
#endif
                                   0,
                                   SKV_COMMAND_RIU_FLAGS_NONE,
                                   & ( commandHelpers[ t ].mCommandHdl ) );

          if( status == SKV_SUCCESS )
            {

#ifdef DO_CHECK
              uintptr_t *buffer = (uintptr_t*)commandHelpers[ t ].mBuffer;
#else
              uintptr_t *buffer = (uintptr_t*)Buffer;
#endif
              BegLogLine( SKV_TEST_LOG )
                << "insert key: " << *Key
                << " " << HEXLOG(buffer[ 0 ])
                << " " << HEXLOG(buffer[ 1 ])
                << " " << HEXLOG(buffer[ 2 ])
                << " " << HEXLOG(buffer[ 3 ])
                << " " << HEXLOG(buffer[ 4 ])
                << EndLogLine;

              BegLogLine( SKV_TEST_LOG )
                << "skv_test_n_inserts_retrieves::main():: SKV Client successfully posted Insert command "
                << " into MyPDSId: " << MyPDSId
                << " key: " << *Key
                << " idx: " << t
                << EndLogLine;
            }
          else
            {
              BegLogLine( SKV_TEST_LOG )
                << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to Insert: "
                << " into MyPDSId: " << MyPDSId
                << " status: " << skv_status_to_string( status )
                << EndLogLine;

            }  

#ifdef SKV_TEST_MAPPED_HANDELS
          int rc = CommandHandleToTimerMap->insert( std::make_pair( commandHelpers[ t ].mCommandHdl , & commandHelpers[ t ] ) ).second;
          StrongAssertLogLine( rc == 1 )
            << "ERROR: "
            << " rc: " << rc
            << " commandHelpers[ t ].mCommandHdl: " << (void *) commandHelpers[ t ].mCommandHdl
            << EndLogLine;
#endif
#ifdef SLOWDOWN
          usleep(SLOWDOWN);
#endif
        }
      /****************************************************************************/



      BegLogLine( SKV_TEST_LOG )
        << "Waiting for Insert Completion"
        << EndLogLine;

      /****************************************************************************
       * Wait for insert commands to finish
       ****************************************************************************/
      skv_client_cmd_ext_hdl_t CommandHdl;
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
#ifdef SKV_TEST_MAPPED_HANDELS          
          status = Client.WaitAny( & CommandHdl );
#else
          CommandHdl = commandHelpers[ t ].mCommandHdl;
          status = Client.Wait( CommandHdl );
#endif

          BegLogLine( SKV_TEST_LOG )
            << "Insert command completed: "
            << " t: " << t
            << " CommandHdl: " << (void *) CommandHdl
            << EndLogLine;

#ifdef SKV_TEST_MAPPED_HANDELS          
          command_handle_to_timer_map_t::iterator iter = CommandHandleToTimerMap->find( CommandHdl );

          StrongAssertLogLine( iter != CommandHandleToTimerMap->end() )
            << "ERROR: command handle not found in command map " 
            << " command handle: " << (void *) CommandHdl
            << EndLogLine;

          skv_async_command_helper_t* commandHelper = iter->second;

          CommandHandleToTimerMap->erase( iter );
#endif
        }

      double InsertTime = MPI_Wtime() - InsertTimeStart;
      /****************************************************************************/


#ifndef DONT_RETRIEVE
      // for( int nRet = 0; nRet < 10000; nRet++ ) {
      /*****************************************************************************
       * Retrieve Key / Value
       ****************************************************************************/
#ifdef DO_CHECK
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          bzero( commandHelpers[ t ].mBuffer, commandHelpers[ t ].mBufferSize );
        }
#endif

      double RetrieveTimeStart = MPI_Wtime();      
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          int RetrieveSize = -1;
          int *Key = &key_list[t];

          int RetrivedSize = 0;
          status = Client.iRetrieve( &MyPDSId,
                                     (char *) Key,
                                     (int) sizeof( int ),
#ifdef DO_CHECK
                                     commandHelpers[ t ].mBuffer,
                                     commandHelpers[ t ].mBufferSize,
                                     & commandHelpers[ t ].mRetrieveBufferSize,
#else
                                     Buffer, 
                                     testDataSize,
                                     & RetrivedSize,
#endif
                                     0,
                                     SKV_COMMAND_RIU_FLAGS_NONE,
                                     & (commandHelpers[ t ].mCommandHdl) );

          if( status == SKV_SUCCESS )
            {
              BegLogLine( SKV_TEST_LOG )
                << "skv_test_n_inserts_retrieves::main():: SKV Client successfully posted retrieve command: "
                << " MyPDSId: " << MyPDSId
                << " key: " << *Key
                << " idx: " << t
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

#ifdef SKV_TEST_MAPPED_HANDELS
          int rc = CommandHandleToTimerMap->insert( std::make_pair( commandHelpers[ t ].mCommandHdl , & commandHelpers[ t ] ) ).second;
          StrongAssertLogLine( rc == 1 )
            << "ERROR: "
            << " rc: " << rc
            << " commandHelpers[ t ].mCommandHdl: " << (void *) commandHelpers[ t ].mCommandHdl
            << EndLogLine;	  
#endif
#ifdef SLOWDOWN
          usleep(SLOWDOWN);
#endif
        }
      /****************************************************************************/




      /****************************************************************************
       * Wait for retrieve commands to finish
       ****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
#ifdef SKV_TEST_MAPPED_HANDELS          
          status = Client.WaitAny( & CommandHdl );
#else
          CommandHdl = commandHelpers[ t ].mCommandHdl;
          status = Client.Wait( CommandHdl );
#endif

          BegLogLine( SKV_TEST_LOG )
            << "Retrieve command completed: "
            << " t: " << t
            << " CommandHdl: " << (void *) CommandHdl
            << EndLogLine;

          StrongAssertLogLine( status == SKV_SUCCESS )
            << "Retrieve ERROR: " << skv_status_to_string( status )
            << " test#: " << t
            << " key: " << key_list[t]
            << EndLogLine;

#ifdef DO_CHECK
          uintptr_t *buffer = (uintptr_t*) commandHelpers[ t ].mBuffer;
          BegLogLine( SKV_TEST_LOG )
            << "retrieve cmpl: " << key_list[t]
            << " " << HEXLOG(buffer[ 0 ])
            << " " << HEXLOG(buffer[ 1 ])
            << " " << HEXLOG(buffer[ 2 ])
            << " " << HEXLOG(buffer[ 3 ])
            << " " << HEXLOG(buffer[ 4 ])
            << " @: " << (void*)buffer
            << EndLogLine;
#endif

#ifdef SKV_TEST_MAPPED_HANDELS
          command_handle_to_timer_map_t::iterator iter = CommandHandleToTimerMap->find( CommandHdl );

          StrongAssertLogLine( iter != CommandHandleToTimerMap->end() )
            << "ERROR: command handle not found in command map " 
            << " command handle: " << (void *) CommandHdl
            << EndLogLine;

          skv_async_command_helper_t* commandHelper = iter->second;

          CommandHandleToTimerMap->erase( iter );
#endif
        }

      double RetrieveTime = MPI_Wtime() - RetrieveTimeStart;
      /****************************************************************************/
      // }
      // double RetrieveTimeStart = 0.0;
      // double RetrieveTime = -10;

#ifdef DO_CHECK
      /*****************************************************************************
       * Check results
       ****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          skv_async_command_helper_t* commandHelper = & commandHelpers[ t ];

          int TestFailed = 0;
          for( int i=0; i < commandHelper->mBufferSize; i++ )
            {
              char ch = i;
              if( commandHelper->mBuffer[ i ] !=  calculateValue( Rank, ch, t ) )
                {
                  BegLogLine( SKV_TEST_LOG )
                    << "Retrieve Result does NOT match: { "
                    << (int)commandHelper->mBuffer[ i ] << " != "
                    << calculateValue( Rank, ch, t ) << " }" 
                    << EndLogLine;

                  TestFailed = 1;
                  break;
                }
            }

          if( TestFailed )
            {
              BegLogLine( 1 )
                << "SKV Client Result Match FAILED :-("
                << " buf@ " << (void*)(commandHelper->mBuffer)
                << " key: " << key_list[ t ]
                << EndLogLine;

              uintptr_t *buffer = (uintptr_t*)commandHelper->mBuffer;
              BegLogLine( SKV_TEST_LOG )
                << "retrieved key: " << key_list[ t ]
                << " " << HEXLOG(buffer[ 0 ])
                << " " << HEXLOG(buffer[ 1 ])
                << " " << HEXLOG(buffer[ 2 ])
                << " " << HEXLOG(buffer[ 3 ])
                << " " << HEXLOG(buffer[ 4 ])
                << " @: " << (void*)buffer
                << EndLogLine;

              StrongAssertLogLine( 0 )
                << "ERROR: Test failed "
                << " testDataSize: " << testDataSize
                << " buf@ " << (void*)(commandHelper->mBuffer)
                << EndLogLine;
            }
          else
            {
              BegLogLine( SKV_TEST_LOG )
                << "SKV Client Result Match SUCCESSFUL :-)"
                << " buf@ " << (void*)(commandHelper->mBuffer)
                << EndLogLine;
            }
        }
#endif
      /****************************************************************************/

#else // DONT_RETRIEVE
      double RetrieveTimeStart = 1;      
      double RetrieveTime = 1000000000;
#endif // DONT_RETRIEVE

      BegLogLine( SKV_TEST_LOG )
        << "Removing Data"
        << EndLogLine;

      double RemoveTimeStart = MPI_Wtime();      
      /****************************************************************************/
      /* REMOVE content for next try **/
      /****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          int *Key = &key_list[t];

          status = Client.iRemove( &MyPDSId,
                                  (char *) Key,
                                  (int) sizeof( int ),
                                  SKV_COMMAND_REMOVE_FLAGS_NONE,
                                   & ( commandHelpers[ t ].mCommandHdl )
                                  );

          StrongAssertLogLine( (status == SKV_SUCCESS) || (status == SKV_ERRNO_ELEM_NOT_FOUND) )
            << "ERROR: posting remove: " << *Key
            << " idx: " << t
            << " status: " << skv_status_to_string( status )
            << EndLogLine;	  

          BegLogLine( SKV_TEST_LOG )
            << "Remove command posted: "
            << " t: " << t
            << " CommandHdl: " << (void *) (commandHelpers[ t ].mCommandHdl )
            << EndLogLine;

#ifdef SKV_TEST_MAPPED_HANDELS
          int rc = CommandHandleToTimerMap->insert( std::make_pair( commandHelpers[ t ].mCommandHdl , & commandHelpers[ t ] ) ).second;
          StrongAssertLogLine( rc == 1 )
            << "ERROR: "
            << " rc: " << rc
            << " commandHelpers[ t ].mCommandHdl: " << (void *) commandHelpers[ t ].mCommandHdl
            << EndLogLine;	  
#endif
        }


      /****************************************************************************
       * Wait for remove commands to finish
       ****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
#ifdef SKV_TEST_MAPPED_HANDELS          
          status = Client.WaitAny( & CommandHdl );
#else
          CommandHdl = commandHelpers[ t ].mCommandHdl;
          status = Client.Wait( CommandHdl );
#endif

          BegLogLine( SKV_TEST_LOG )
            << "Remove command completed: "
            << " t: " << t
            << " CommandHdl: " << (void *) CommandHdl
            << EndLogLine;

#ifdef SKV_TEST_MAPPED_HANDELS
          command_handle_to_timer_map_t::iterator iter = CommandHandleToTimerMap->find( CommandHdl );

          StrongAssertLogLine( iter != CommandHandleToTimerMap->end() )
            << "ERROR: command handle not found in command map " 
            << " command handle: " << (void *) CommandHdl
            << EndLogLine;

          skv_async_command_helper_t* commandHelper = iter->second;

          CommandHandleToTimerMap->erase( iter );
#endif
        }

      double RemoveTime = MPI_Wtime() - RemoveTimeStart;
      /****************************************************************************/




      double InsertAvgTime   = ( InsertTime / NUMBER_OF_TRIES );
      double RetrieveAvgTime = ( RetrieveTime / NUMBER_OF_TRIES );
      double RemoveAvgTime   = ( RemoveTime / NUMBER_OF_TRIES );

      double GlobalInsertAvgTime = 0.0;
      double GlobalRetrieveAvgTime = 0.0;
      double GlobalRemoveAvgTime = 0.0;
      MPI_Reduce( & InsertAvgTime, & GlobalInsertAvgTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );
      MPI_Reduce( & RetrieveAvgTime, & GlobalRetrieveAvgTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );
      MPI_Reduce( & RemoveAvgTime, & GlobalRemoveAvgTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );

      GlobalInsertAvgTime = GlobalInsertAvgTime / (1.0 * NodeCount);
      GlobalRetrieveAvgTime = GlobalRetrieveAvgTime / (1.0 * NodeCount);
      GlobalRemoveAvgTime = GlobalRemoveAvgTime / (1.0 * NodeCount);

      double InsertBandwidth   = ( testDataSize / ( GlobalInsertAvgTime * 1024.0 * 1024.0 ) );
      double RetrieveBandwidth = ( testDataSize / ( GlobalRetrieveAvgTime * 1024.0 * 1024.0 ) );

      BegLogLine( 1 )
        << "skv_test_n_inserts_retrieves::main():: TIMING: " 
        << " log2(ValueSize): " << log2( (double) testDataSize )
        << " ValueSize: " << testDataSize
        << " TryCount: " << NUMBER_OF_TRIES
        << " InsertAvgTime: " << GlobalInsertAvgTime
        << " InsertBandwidth: " << InsertBandwidth
        << " RetrieveAvgTime: " << GlobalRetrieveAvgTime
        << " RetrieveBandwidth: " << RetrieveBandwidth	
        << " RemoveAvgTime: " << GlobalRemoveAvgTime
        << EndLogLine;

      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {

#ifdef DO_CHECK
          free( commandHelpers[ t ].mBuffer );
          commandHelpers[ t ].mBuffer = NULL;
#else
          free( Buffer );
          Buffer = NULL;
#endif
        }

      free(key_list);
      MPI_Barrier( MPI_COMM_WORLD );
    }

  pkTraceServer::FlushBuffer();

  Client.Disconnect();
  Client.Finalize();

  MPI_Finalize();

  return 0;
}
