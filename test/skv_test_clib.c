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

#include <time.h>
#include <mpi.h>
#include <skv.h>
#include <math.h>
#include <malloc.h>
#include <stdlib.h>
#include <strings.h>
#include <inttypes.h>

skv_hdl_t Client;
#define DATA_SIZE  ( 4096 )

/* #define NUMBER_OF_TRIES  (  16 * 512  ) */
#define MAX_NUMBER_OF_TRIES_PER_SRV (  250  )    // max number of attempts per server
#define STORAGE_SPACE_PER_SNODE     ( (int)(1 * 1024 * 1024) )   // space available per server (reduced by an arbitrary space-overhead fact
#define MAX_TEST_SIZE_EXP ( 3 )       // 2^x max value size
#define SKV_TEST_START_INDEX ( 2 )    // 2^x min value size

#ifndef SKV_TEST_LOG
#define SKV_TEST_LOG ( 0 )
#endif

#define BegLogLine(cond, x) if( cond ) { printf("%s\n",x); }

//#define SLOWDOWN 10000
//#define DONT_RETRIEVE
//#define DO_CHECK

typedef struct skv_async_command_helper_t
  {
  int                       mBufferSize;
  char*                     mBuffer;
  int                       mRetrieveBufferSize;

  skv_client_cmd_ext_hdl_t mCommandHdl;
  } skv_async_command_helper_t;


void
getDataSizes( int** aDataSizeArray, int* aCount, int aBegPower, int aEndPower )
{
  int beginPower = aBegPower;
  int endPower   = aEndPower;

  int SizeCount = endPower - beginPower + 1;

  int* SizeArray = (int *) malloc( sizeof( int ) * SizeCount );

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

static
int
SKVTestGetServerCount()
{
  int srvCount = 1;
  return srvCount;
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

  if( argc != 2 )
    {
    printf("skv_test_n_inserts_retrieves::main():: ERROR:: argc: %d", argc);
    exit(1);
    }
    
  char* ConfigFile = argv[ 1 ];

  int Rank = 0;
  int NodeCount = 1;


  /*****************************************************************************
   * Init MPI
   ****************************************************************************/ 
#ifndef SKV_CLIENT_UNI
  MPI_Init( &argc, &argv );
  MPI_Comm_rank( MPI_COMM_WORLD, &Rank );
  MPI_Comm_size( MPI_COMM_WORLD, &NodeCount );
  printf(" %d: MPI_Init complete\n", Rank);
#endif
  /****************************************************************************/ 



  /*****************************************************************************
   * Init the SKV Client
   ****************************************************************************/ 
  skv_status_t status = SKV_Init( Rank,
#ifndef SKV_CLIENT_UNI
                                    MPI_COMM_WORLD,
#endif
                                    0,
                                    ConfigFile,
                                    &Client );

  if( status == SKV_SUCCESS )
    {
    BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client Init succeded ");
    }
  else
    {
    BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client Init FAILED ");
    }  
  /****************************************************************************/ 



  /*****************************************************************************
   * Connect to the SKV Server
   ****************************************************************************/ 
  BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: About to connect ");

  status = SKV_Connect( Client, ConfigFile, 0 );

  if( status == SKV_SUCCESS )
    {
    BegLogLine( SKV_TEST_LOG,  "skv_test_n_inserts_retrieves::main():: SKV Client connected to server");
    }
  else
    {
    BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to connect ");
    }
  /****************************************************************************/ 




  /*****************************************************************************
   * Open a test PDS
   ****************************************************************************/
  char MyTestPdsName[ SKV_MAX_PDS_NAME_SIZE ];
  bzero( MyTestPdsName, SKV_MAX_PDS_NAME_SIZE );

  if( Rank >= 0 )
    {
      /* struct timespec ts; */

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

  BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: About to open pds");

  skv_pds_hdl_t  MyPDSId;
  status = SKV_Open( Client,
                      MyTestPdsName,
                      (skv_pds_priv_t) (SKV_PDS_READ | SKV_PDS_WRITE),
                      (skv_cmd_open_flags_t) SKV_COMMAND_OPEN_FLAGS_CREATE,
                      & MyPDSId );

  if( status == SKV_SUCCESS )
    {
    BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client successfully opened");
    }
  else
    {
    BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to open");
    }
  /****************************************************************************/


  int* DataSizes = NULL;
  int  DataSizeCount = 0;
  getDataSizes( & DataSizes, & DataSizeCount, 0, MAX_TEST_SIZE_EXP );

#ifndef SKV_CLIENT_UNI
  MPI_Barrier( MPI_COMM_WORLD );
#endif

  uint64_t NUMBER_OF_TRIES = 0;
  uint64_t NUMBER_OF_SNODES = SKVTestGetServerCount();

  for( int sizeIndex=SKV_TEST_START_INDEX; sizeIndex < DataSizeCount; sizeIndex++ )
    {
      int testDataSize = DataSizes[ sizeIndex ];

      uint64_t total_records = (uint64_t)STORAGE_SPACE_PER_SNODE / (uint64_t) testDataSize * (uint64_t)NUMBER_OF_SNODES;
      NUMBER_OF_TRIES = total_records / NodeCount;

      if( NUMBER_OF_TRIES > MAX_NUMBER_OF_TRIES_PER_SRV * NUMBER_OF_SNODES )
        NUMBER_OF_TRIES = MAX_NUMBER_OF_TRIES_PER_SRV * NUMBER_OF_SNODES;

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
          if( commandHelpers[ t ].mBuffer == NULL )    
            {
            BegLogLine( 1, "error allocating command handles");
            exit(1);
            }

          /*****************************************************************************
           * Insert Key / Value
           ****************************************************************************/
          for( int i=0; i < testDataSize; i++ )
            {
              char ch = i;
              commandHelpers[ t ].mBuffer[ i ] = calculateValue( Rank, ch, t );
            }
        }
#else
      char* Buffer = (char *) malloc( testDataSize );
      if( commandHelpers[ t ].mBuffer == NULL )    
        {
        BegLogLine( 1, "error allocating command handles");
        exit(1);
        }
#endif

      double InsertTimeStart = MPI_Wtime();
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {      
          // int Key = Rank * NUMBER_OF_TRIES + t;
          int Key = calculateKey( Rank, sizeIndex, t, NUMBER_OF_TRIES );
          // int Key = ( Rank * DataSizeCount * NUMBER_OF_TRIES) 
          //   + (sizeIndex * NUMBER_OF_TRIES ) 
          //   + t;

          BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: About to Insert");

          status = SKV_Iinsert( Client,
                                 &MyPDSId,
                                 (char *) &Key,
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
              BegLogLine( SKV_TEST_LOG, "insert key: ");
              BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client successfully posted Insert command ");
            }
          else
            {
            BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to Insert: ");

            }  

#ifdef SLOWDOWN
          usleep(SLOWDOWN);
#endif
        }
      /****************************************************************************/



      BegLogLine( SKV_TEST_LOG, "Waiting for Insert Completion");

      /****************************************************************************
       * Wait for insert commands to finish
       ****************************************************************************/
      skv_client_cmd_ext_hdl_t CommandHdl;
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          CommandHdl = commandHelpers[ t ].mCommandHdl;
          status = SKV_Wait( Client, CommandHdl );
          BegLogLine( SKV_TEST_LOG, "Insert command completed: ");
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
          // int Key = Rank * NUMBER_OF_TRIES + t;
          int Key = calculateKey( Rank, sizeIndex, t, NUMBER_OF_TRIES );
          // int Key = ( Rank * DataSizeCount * NUMBER_OF_TRIES) 
          //   + (sizeIndex * NUMBER_OF_TRIES ) 
          //   + t;
#ifndef DO_CHECK
          int RetrivedSize = 0;
#endif
          status = SKV_Iretrieve( Client, &MyPDSId,
                                   (char *) &Key,
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
            BegLogLine( SKV_TEST_LOG, "skv_test_n_inserts_retrieves::main():: SKV Client successfully posted retrieve command:");
            }
          else
            {
            BegLogLine( 1, "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to Retieve: ");
            }  

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
          CommandHdl = commandHelpers[ t ].mCommandHdl;
          status = SKV_Wait( Client, CommandHdl );

          BegLogLine( SKV_TEST_LOG, "Retrieve command completed: ");

          if( status != SKV_SUCCESS )
            {
            BegLogLine( 1, "Retrieve ERROR: ");
            exit(1);
            }
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
                BegLogLine( SKV_TEST_LOG, "Retrieve Result does NOT match: ");
                TestFailed = 1;
                break;
                }
            }

          if( TestFailed )
            {
            BegLogLine( 1,  "SKV Client Result Match FAILED :-(" );

            exit(1);
            }
          else
            {
            BegLogLine( SKV_TEST_LOG, "SKV Client Result Match SUCCESSFUL :-)");
            }
        }
#endif
      /****************************************************************************/

#else // DONT_RETRIEVE
      double RetrieveTimeStart = 1;      
      double RetrieveTime = 1000000000;
#endif // DONT_RETRIEVE

      BegLogLine( SKV_TEST_LOG, "Removing Data");

      double RemoveTimeStart = MPI_Wtime();      
      /****************************************************************************/
      /* REMOVE content for next try **/
      /****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          // int Key = Rank * NUMBER_OF_TRIES + t;
          int Key = calculateKey( Rank, sizeIndex, t, NUMBER_OF_TRIES );

          status = SKV_Iremove( Client,
                                 &MyPDSId,
                                 (char *) &Key,
                                 (int) sizeof( int ),
                                 SKV_COMMAND_REMOVE_FLAGS_NONE,
                                 & ( commandHelpers[ t ].mCommandHdl ) );

          if( (status != SKV_SUCCESS) && (status != SKV_ERRNO_ELEM_NOT_FOUND) )
            {
            BegLogLine( 1, "ERROR: posting remove: " );
            exit(1);
            }

          BegLogLine( SKV_TEST_LOG, "Remove command posted: ");

        }


      /****************************************************************************
       * Wait for retrieve commands to finish
       ****************************************************************************/
      for( int t=0; t < NUMBER_OF_TRIES; t++ )
        {
          CommandHdl = commandHelpers[ t ].mCommandHdl;
          status = SKV_Wait( Client, CommandHdl );

          BegLogLine( SKV_TEST_LOG, "Remove command completed: ");
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

      printf("%s %2.0lf %s %d %s %ld %s %lf %s %lf %s %lf %s %lf %s %lf\n",
             "skv_test_n_inserts_retrieves::main():: TIMING: log2(ValueSize): ", log2( (double) testDataSize ),
             " ValueSize: ",               testDataSize,
             " TryCount: ",                NUMBER_OF_TRIES,
             " InsertAvgTime: ",           GlobalInsertAvgTime,
             " InsertBandwidth: ",         InsertBandwidth,
             " RetrieveAvgTime: ",         GlobalRetrieveAvgTime,
             " RetrieveBandwidth: ",       RetrieveBandwidth,
             " RemoveAvgTime: ",           GlobalRemoveAvgTime);

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

#ifndef SKV_CLIENT_UNI
      MPI_Barrier( MPI_COMM_WORLD );
#endif
    }

  SKV_Disconnect( Client );
  SKV_Finalize( Client );

#ifndef SKV_CLIENT_UNI
  MPI_Finalize();
#endif

  return 0;
}
