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

#ifndef __TEST_SKV_UTILS_HPP__
#define __TEST_SKV_UTILS_HPP__


#include <fstream>
#include <string>
#include <common/skv_config.hpp>

#ifndef SKV_TEST_LOG
#define SKV_TEST_LOG ( 0 )
#endif


typedef struct {
  skv_status_t insert;
  skv_status_t retrieve;
} dual_status_t;
  

int data_direction;

#define HEXLOG( x )  (  (void*) (*((uint64_t*) &(x)) ) )

// \todo currently only supports single flag setting, future versions should be able to handle ORed flags too
static
const char*
skv_RIU_flags_to_string( skv_cmd_RIU_flags_t aFlags )
{
  switch( aFlags )
    {
    case SKV_COMMAND_RIU_FLAGS_NONE:  { return "SKV_COMMAND_RIU_FLAGS_NONE"; }
    case SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE: {return "SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE"; }
    case SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS:  {return "SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS"; }
    case SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG: {return "SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG"; }
    case SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG:  {return "SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG"; }
    case SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP:  {return "SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP"; }
    case SKV_COMMAND_RIU_RETRIEVE_SPECIFIC_VALUE_LEN:    {return "SKV_COMMAND_RIU_RETRIEVE_SPECIFIC_VALUE_LEN"; }
    case SKV_COMMAND_RIU_UPDATE: { return "SKV_COMMAND_RIU_UPDATE"; }
    case SKV_COMMAND_RIU_APPEND: { return "SKV_COMMAND_RIU_APPEND"; }
    case SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE|SKV_COMMAND_RIU_INSERT_OVERLAPPING: {return "SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE|SKV_COMMAND_RIU_INSERT_OVERLAPPING"; }
    default:
      {
        printf( "skv_RIU_flags_to_string: ERROR:: aFlags: %d is not recognized\n", aFlags );
        return "UNKNOWN";
      }
    }
}


static inline
int CheckData( char *MyDataInsert,
               char *MyDataRetrieve,
               int   MyDataInsertSize,
               int   seed )
{
  int rem_seed = *(int*)MyDataRetrieve;
  srand(rem_seed);

  for( int i=sizeof(int); i<MyDataInsertSize; i++ )
    {
      if(( MyDataInsert[i] != MyDataRetrieve[i] ) || (MyDataRetrieve[i] != (char)( random() & 0xFF )))
        return SKV_ERRNO_CHECKSUM_MISMATCH;
    }

  return 0;
}

static inline
int InitializeInsertData( char  *aData,
                          int    aSize,
                          int    seed )
{
  srand(seed);
  *(int*)aData = seed;

  for( int n=sizeof(int); n<aSize; n+=data_direction )
    {
    aData[n] = (char)( random() & 0xFF );
    }
  return 0;
}

static inline
int ResetRetrieveData( char  *aData,
                       int    aSize )
{
  bzero(aData, aSize);
  return 0;
}

static inline
int PrintBuffer( char       *aData,
                 int         aSize,
                 const char *msg )
{
  printf( "%s\n", msg );
  for ( int n=0; n<aSize; n++ )
    {
      printf( "%4d", (int)aData[n] );
      if( (n+1) % 10 == 0 )
        printf( "\n" );
    }
  printf("\n");


  // log into file
  uintptr_t *buffer = (uintptr_t*)aData;
  BegLogLine( SKV_TEST_LOG )
    << msg 
    << " " << HEXLOG(buffer[ 0 ])
    << " " << HEXLOG(buffer[ 1 ])
    << " " << HEXLOG(buffer[ 2 ])
    << " " << HEXLOG(buffer[ 3 ])
    << " " << HEXLOG(buffer[ 4 ])
    << EndLogLine;


  return 0;
}


static
dual_status_t DoInsertRetrieveTest(skv_pds_id_t        *aPDSId,
                                   int                   aKey,
                                   skv_cmd_RIU_flags_t  aFlags,
                                   int                  aDataSize,
                                   int                   aModSize=0)
{
  int MyDataInsertSize = aDataSize + aModSize;
  char* MyDataInsert   = (char *) malloc( aDataSize * 5 ); //  allocate larger space for later tests

  int MyDataRetrieveSize = aDataSize;
  char* MyDataRetrieve   = (char *) malloc( aDataSize * 5 );

  int RetrieveSize = 0;
  int Offset = 0;
  dual_status_t status = {SKV_SUCCESS, SKV_SUCCESS};

  int seed = 0;
  
  if( aFlags & SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE )
    {
      Offset = aModSize;
    }


  InitializeInsertData(MyDataInsert, MyDataInsertSize, seed);
  ResetRetrieveData(MyDataRetrieve, MyDataInsertSize);
  
  printf("******************************************************************************\n" );
  printf("Testing flag: %s\n", skv_RIU_flags_to_string( aFlags ) );

  PrintBuffer( MyDataInsert, MyDataInsertSize, "inserting");


  status.insert = Client.Insert( aPDSId,
                                 (char *) &aKey,
                                 sizeof( int ),
                                 MyDataInsert, 
                                 MyDataInsertSize,
                                 Offset,
                                 aFlags );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: insert with " << skv_RIU_flags_to_string(aFlags)
    << ". Status: " << skv_status_to_string( status.insert )
    << EndLogLine;

  status.retrieve = Client.Retrieve( aPDSId,
                                     (char *) &aKey,
                                     (int) sizeof( int ),
                                     MyDataRetrieve, 
                                     MyDataInsertSize,
                                     & RetrieveSize,
                                     0,
                                     SKV_COMMAND_RIU_FLAGS_NONE );

  BegLogLine( 1 )
    << "test_skv_insert_command::main():: retrieve with " << skv_RIU_flags_to_string(aFlags)
    << ". Retrieved: " << RetrieveSize
    << " Status: " << skv_status_to_string( status.retrieve )
    << EndLogLine;

  PrintBuffer( MyDataRetrieve, RetrieveSize, "buffer after retrieve");

  if( (status.retrieve == SKV_ERRNO_VALUE_TOO_LARGE)  && (RetrieveSize != MyDataInsertSize) )
    {
      printf( "stored data is larger: %d\n", RetrieveSize );

      status.retrieve = Client.Retrieve( aPDSId,
                                         (char *) &aKey,
                                         (int) sizeof( int ),
                                         MyDataRetrieve, 
                                         RetrieveSize,
                                         & RetrieveSize,
                                         0,
                                         SKV_COMMAND_RIU_FLAGS_NONE );

      PrintBuffer( MyDataRetrieve, RetrieveSize, "buffer after full retrieve");
    }
  else 
    if( CheckData(MyDataInsert, MyDataRetrieve, RetrieveSize, seed) != SKV_SUCCESS )
      {
        status.retrieve = SKV_ERRNO_CHECKSUM_MISMATCH;
      }

  free( MyDataRetrieve );
  free( MyDataInsert );
  return status;
}

static
int
SKVTestGetServerCount()
{
  int srvCount = 0;

  skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

#define COMPUTE_NODE_FILE_PATH_SIZE 256
  char ComputeFileNamePath[ COMPUTE_NODE_FILE_PATH_SIZE ];
  bzero( ComputeFileNamePath, COMPUTE_NODE_FILE_PATH_SIZE );  


#ifdef SKV_RUNNING_LOCAL
  char ServerAddrInfoFilename[ 256 ];
  bzero( ServerAddrInfoFilename, 256 );

  sprintf( ServerAddrInfoFilename,
           "%s",
           config->GetServerLocalInfoFile()
           );

  strcpy( ComputeFileNamePath, ServerAddrInfoFilename );      
#else

  strcpy( ComputeFileNamePath, config->GetMachineFile() );
#endif


  ifstream fin( ComputeFileNamePath );

  int MAX_LENGTH = 100;
  char line[ MAX_LENGTH ];
  while( fin.getline(line, MAX_LENGTH) ) 
    {
      srvCount++;
    }
 

  return srvCount;
}


#endif // __TEST_SKV_UTILS_HPP__
