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
 * Created on: Feb 1, 2014
 */

#ifndef SKV_CLIENT_UNI
#define SKV_CLIENT_UNI
#endif

// make sure this is a single-node program
#include <time.h>
#include <errno.h>
#include <Pk/FxLogger.hpp>
#include <Pk/Trace.hpp>
#include <client/skv_client.hpp>
#include <math.h>
#include <iostream>
#include <string>

enum skv_test_mode_t
{
  SKV_BASE_TEST_PDS = ( 1 ),
  SKV_BASE_TEST_INSERT = ( 1 << 1 ),
  SKV_BASE_TEST_RETRIEVE = ( 1 << 2 ),
  SKV_BASE_TEST_REMOVE = ( 1 << 3 ),
  SKV_BASE_TEST_BULKINSERT = ( 1 << 4 ),
  SKV_BASE_TEST_CURSOR = ( 1 << 5 ),
  SKV_BASE_TEST_ALL = ( 0xFF ),
};

typedef struct
{
  std::string CONF_FILE;
  short TEST_MODE;
  uint16_t KEYSIZE;
} skv_test_config_t;

typedef struct
{
  skv_client_t Client;
} skv_global_state_t;

static skv_test_config_t config = {
    "skv_server.conf",
    SKV_BASE_TEST_ALL,
    4
};

static skv_global_state_t gdata;

static int test_count = 0;
#define TC test_count++

static const char *PassFail[2] = { "[PASS]", "[FAIL]" };

//#define TEST_RESULT(x, e) ( (x)==(e)? 0 : 1 )
static inline
int TEST_RESULT( skv_status_t in,
                 skv_status_t expected,
                 const char* aMain,
                 const char* aExt )
{
  int rc = 0;
  rc = (in == expected ) ? 0 : 1;

  if( ! rc )
  {
    BegLogLine( 1 )
      << FormatString("%16s::") << aMain
      << FormatString("%12s") << aExt
      << " " << PassFail[ rc ]
      << "  expected: " << skv_status_to_string( expected )
      << EndLogLine;
  }
  else
  {
    BegLogLine( 1 )
      << FormatString("%16s::") << aMain
      << FormatString("%12s") << aExt
      << " \t" << PassFail[ rc ]
      << "::\t expected: " << skv_status_to_string( expected )
      << " got: " << skv_status_to_string( in )
      << EndLogLine;
  }
  TC;
  return rc;
}


#include "skv_base_test.hpp"


static inline
int Init_test( int argc, char **argv )
{
  int status = 0;
  int op;
  while( (op = getopt( argc, argv, "ha:k:m:" )) != -1 )
  {
    char *endp;
    switch( op )
    {
      default:
        status = -EINVAL;
      case 'h':
      {
        std::cout << "USAGE: random_read \n";
        std::cout << "  Arguments:\n";
        std::cout << "  -f <config_file>   : skv config file (default ./skv_server.conf)\n";
        std::cout << "  -h                 : print this help\n";
        std::cout << "  -k <keysize>       : keysize for testing (default: 4)\n";
        std::cout << "  -m <test_mode>     : Testmode: [a]|[bcdipr]\n";
        std::cout << "                     :     a-all; b-bulkinsert, c-cursor, d-remove\n";
        std::cout << "                     :     i-insert, p-PDS, r-retrieve\n";
        std::cout << "  " << std::endl;
        return 1;
      }
      case 'a':
        config.CONF_FILE.assign( optarg );
        break;
      case 'k':
        config.KEYSIZE = atoi( optarg );
        break;
      case 'm':
        config.TEST_MODE = 0;
        for( int n=0; n < strnlen(optarg, 10); n++ )
        {
          switch (optarg[n])
          {
            case 'a':
              config.TEST_MODE = SKV_BASE_TEST_ALL;
              break;
            case 'b':
              config.TEST_MODE |= SKV_BASE_TEST_BULKINSERT;
              break;
            case 'c':
              config.TEST_MODE |= SKV_BASE_TEST_CURSOR;
              break;
            case 'd':
              config.TEST_MODE |= SKV_BASE_TEST_REMOVE;
              break;
            case 'i':
              config.TEST_MODE |= SKV_BASE_TEST_INSERT;
              break;
            case 'p':
              config.TEST_MODE |= SKV_BASE_TEST_PDS;
              break;
            case 'r':
              config.TEST_MODE |= SKV_BASE_TEST_RETRIEVE;
              break;
            default:
              std::cout << "Unknown Test Mode." << std::endl;
          }
        }
        break;
    }
  }
  return status;
}

static inline
skv_status_t Init_skv()
{
  skv_status_t status;

  /*****************************************************************************
   * Init the SKV Client
   ****************************************************************************/
  status = gdata.Client.Init( 0,
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
    << "skv_base_test::Init_skv():: About to connect "
    << " ConfigFile: " << config.CONF_FILE.c_str()
    << EndLogLine;

  status = gdata.Client.Connect( config.CONF_FILE.data(), 0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( 1 )
        << "skv_base_test::Init_skv():: SKV Client connected "
        << EndLogLine;
    }
  else
    {
      BegLogLine( 1 )
        << "skv_base_test::Init_skv():: SKV Client FAILED to connect "
        << " config file: " << config.CONF_FILE.c_str()
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/

  return status;
}

static inline
skv_status_t Cleanup_skv()
{
  skv_status_t status = gdata.Client.Disconnect();

  BegLogLine( 1 )
    << "skv_base_test::Cleanup_skv():: Disconnected with status: " << status
    << EndLogLine;

  return status;
}

static inline
int Test_PDS()
{
  int status = 0;
  int tmp_s = 0;
  skv_pds_priv_t priv = (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE);

  status += TEST_RESULT( skv_base_test_open_pds( "SKV_BASE_TEST_PDS", priv, SKV_COMMAND_OPEN_FLAGS_NONE ),
                         SKV_ERRNO_PDS_DOES_NOT_EXIST,
                         "OPEN", "NOEXIST" );
  status += TEST_RESULT( skv_base_test_open_pds( "SKV_BASE_TEST_PDS", priv, SKV_COMMAND_OPEN_FLAGS_CREATE ),
                         SKV_SUCCESS,
                         "OPEN", "CREATE" );
  status += TEST_RESULT( skv_base_test_open_pds( "SKV_BASE_TEST_PDS", priv, SKV_COMMAND_OPEN_FLAGS_CREATE ),
                         SKV_SUCCESS,
                         "OPEN", "CREATEAGAIN" );
  status += TEST_RESULT( skv_base_test_open_pds( "SKV_BASE_TEST_PDS", priv, SKV_COMMAND_OPEN_FLAGS_EXCLUSIVE ),
                         SKV_ERRNO_PDS_ALREADY_EXISTS,
                         "OPEN", "EXCLUSIVE" );
  status += TEST_RESULT( skv_base_test_open_pds( "SKV_BASE_TEST_PDS_2",
                                                 priv,
                                                 (skv_cmd_open_flags_t) (SKV_COMMAND_OPEN_FLAGS_CREATE| SKV_COMMAND_OPEN_FLAGS_EXCLUSIVE) ),
                         SKV_SUCCESS,
                         "OPEN",
                         "CREATE+EXCL" );
  status += TEST_RESULT( skv_base_test_open_pds( "SKV_BASE_TEST_PDS_3",
                                                 priv,
                                                 (skv_cmd_open_flags_t) (SKV_COMMAND_OPEN_FLAGS_CREATE| SKV_COMMAND_OPEN_FLAGS_DUP) ),
                         SKV_SUCCESS,
                         "OPEN",
                         "DUP" );

  status += TEST_RESULT( skv_base_test_stat_pds_get( "SKV_BASE_TEST_PDS_ERR" ),
                         SKV_ERRNO_PDS_DOES_NOT_EXIST,
                         "STAT", "GET_ERR" );
  status += TEST_RESULT( skv_base_test_stat_pds_get( "SKV_BASE_TEST_PDS" ),
                         SKV_SUCCESS,
                         "STAT", "GET" );
  return status;
}

static inline
int Test_Insert( int aDataSize, const char *aText )
{
  int status = 0;
  int Key = aDataSize;
  skv_cmd_RIU_flags_t testFlag = SKV_COMMAND_RIU_FLAGS_NONE;
  std::string mtext = "INSERT:";
  mtext.append( aText );

  status += TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                               Key,
                                               aDataSize,
                                               0,
                                               testFlag ),
                         SKV_SUCCESS,
                         mtext.c_str(), "NONE" );

  status += TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                               Key,
                                               aDataSize,
                                               0,
                                               testFlag ),
                         SKV_ERRNO_RECORD_ALREADY_EXISTS,
                         mtext.c_str(), "EXIST" );

  testFlag = SKV_COMMAND_RIU_APPEND;
  status += TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                               Key,
                                               aDataSize,
                                               0,
                                               testFlag ),
                         SKV_SUCCESS,
                         mtext.c_str(), "APPEND" );

  testFlag = SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE;
  status += TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                               Key,
                                               aDataSize,
                                               0,
                                               testFlag ),
                         SKV_SUCCESS,
                         mtext.c_str(), "EXPAND" );

  testFlag = SKV_COMMAND_RIU_UPDATE;
  status += TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                               Key,
                                               aDataSize,
                                               0,
                                               testFlag ),
                         SKV_SUCCESS,
                         mtext.c_str(), "UPDATE" );
  return status;
}

static inline
int Test_Retrieve( int aDataSize, const char *aText )
{
  int status = 0;
  std::string mtext="RETRIEVE:";
  mtext.append( aText );

  int Key = aDataSize;
  skv_cmd_RIU_flags_t testFlag = SKV_COMMAND_RIU_FLAGS_NONE;

  // first thing for retrieve test: make sure there's data to read ;-)
  status +=TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                              Key,
                                              65536,
                                              0,
                                              (skv_cmd_RIU_flags_t)(SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE|SKV_COMMAND_RIU_INSERT_OVERLAPPING) ),
                        SKV_SUCCESS,
                        mtext.c_str(), "INIT_INSERT" );

  status += TEST_RESULT( skv_base_test_retrieve( "SKV_BASE_TEST_PDS",
                                                 Key,
                                                 aDataSize,
                                                 0,
                                                 testFlag ),
                         SKV_SUCCESS,
                         mtext.c_str(), "NONE" );

  status += TEST_RESULT( skv_base_test_retrieve( "SKV_BASE_TEST_PDS",
                                                 Key,
                                                 aDataSize,
                                                 (aDataSize>>1),
                                                 testFlag ),
                         SKV_SUCCESS,
                         mtext.c_str(), "OFFSET" );

  Key++;
  status += TEST_RESULT( skv_base_test_retrieve( "SKV_BASE_TEST_PDS",
                                                 Key,
                                                 aDataSize,
                                                 0,
                                                 testFlag ),
                         SKV_ERRNO_ELEM_NOT_FOUND,
                         mtext.c_str(), "WRONG_KEY" );

  return status;
}

static inline
int Test_Remove( )
{
  int status = 0;
  int Key = 2045;

  // first thing for retrieve test: make sure there's data to read ;-)
  status += TEST_RESULT( skv_base_test_insert( "SKV_BASE_TEST_PDS",
                                               Key,
                                               Key, // use key as data size too
                                               0,
                                               (skv_cmd_RIU_flags_t)(SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE|SKV_COMMAND_RIU_INSERT_OVERLAPPING) ),
                         SKV_SUCCESS,
                         "REMOVE", "INIT_INSERT" );
  status += TEST_RESULT( skv_base_test_remove( "SKV_BASE_TEST_PDS",
                                               Key ),
                         SKV_SUCCESS,
                         "REMOVE", "NONE" );
  status += TEST_RESULT( skv_base_test_remove( "SKV_BASE_TEST_PDS",
                                               Key ),
                         SKV_ERRNO_ELEM_NOT_FOUND,
                         "REMOVE", "TWICE" );

  return status;
}

static inline
int Test_BulkInsert( int aRND_SEED, int aKeySize, int aMaxSize, int aCount )
{
  int status = 0;

  status += TEST_RESULT( skv_base_test_bulkinsert( "SKV_BASE_TEST_PDS",
                                                   aCount,
                                                   aKeySize,
                                                   aMaxSize,
                                                   aRND_SEED ),
                         SKV_SUCCESS,
                         "BULKINS", "NONE" );

  return status;
}

static inline
int Test_Cursor( int aRND_SEED, int aKeySize, int aMaxSize, int aCount )
{
  int status = 0;

  status += TEST_RESULT( skv_base_test_bulkinsert( "SKV_BULK_TEST_PDS",
                                                   aCount,
                                                   aKeySize,
                                                   aMaxSize,
                                                   aRND_SEED ),
                         SKV_SUCCESS,
                         "CURSOR", "INIT_INSERT" );

  status += TEST_RESULT( skv_base_test_cursor( "SKV_BULK_TEST_PDS",
                                               aCount,
                                               aKeySize,
                                               aMaxSize,
                                               aRND_SEED,
                                               true ),
                         SKV_SUCCESS,
                         "CURSOR", "LOC" );

  status += TEST_RESULT( skv_base_test_cursor( "SKV_BULK_TEST_PDS",
                                               aCount,
                                               aKeySize,
                                               aMaxSize,
                                               aRND_SEED,
                                               false ),
                         SKV_SUCCESS,
                         "CURSOR", "DIST" );

  return status;
}

int
main(int argc, char **argv)
{
  FxLogger_Init( argv[ 0 ] );
  int rc = 0;  // failed tests counter

  // Parse args and initialize configuration
  switch( Init_test( argc, argv ) )
  {
    case -1: return -1;
    case 1: return 0;
    default: break;
  }

  // Initialize skv connection to server
  if( Init_skv() != SKV_SUCCESS )
  {
    Cleanup_skv();
    return -1;
  }

  if( config.TEST_MODE & (SKV_BASE_TEST_PDS) )
  {
    rc += Test_PDS();
  }

  if( config.TEST_MODE & (SKV_BASE_TEST_INSERT) )
  {
    rc += Test_Insert( 64, "SMALL" );
    rc += Test_Insert( 8192, "MEDIUM" );
  }

  if( config.TEST_MODE & (SKV_BASE_TEST_RETRIEVE) )
  {
    rc += Test_Retrieve( 64, "SMALL" );
    rc += Test_Retrieve( 8192, "MEDIUM" );
  }

  if( config.TEST_MODE & (SKV_BASE_TEST_REMOVE) )
  {
    rc += Test_Remove();
  }

  if( config.TEST_MODE & (SKV_BASE_TEST_BULKINSERT) )
  {
    rc += Test_BulkInsert( 246, config.KEYSIZE, 8192, 1000 );
  }

  if( config.TEST_MODE & (SKV_BASE_TEST_CURSOR) )
  {
    rc += Test_Cursor( 246, config.KEYSIZE, 8192, 1000 );
  }

  // cleanup, disconnect, shutdown
  if( Cleanup_skv() != SKV_SUCCESS )
  {
    return -1;
  }

  std::cout << "Test result: = " << test_count-rc << "/" << test_count << " passed" << std::endl;

  return (rc == 0 ? 0 : 1);
}
