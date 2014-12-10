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

#ifndef SKV_BASE_TEST_HPP_
#define SKV_BASE_TEST_HPP_

#ifndef SKV_TEST_LOG
#define SKV_TEST_LOG ( 0 )
#endif

static inline
skv_status_t skv_base_test_open_pds( const char * aPDSName,
                                     const skv_pds_priv_t aPrivs,
                                     const skv_cmd_open_flags_t aFlags )
{
  skv_status_t status = SKV_SUCCESS;
  skv_pds_id_t PDSId;

  BegLogLine( SKV_TEST_LOG )
    << "skv_base_test_open_pds() calling open with: "
    << "  PDSName: " << aPDSName
    << "  privs: " << aPrivs
    << "  flags: " << aFlags
    << EndLogLine;

  status = gdata.Client.Open( (char*)aPDSName,
                              aPrivs,
                              aFlags,
                              &PDSId );

  switch( status )
  {
    case SKV_SUCCESS:
      BegLogLine( SKV_TEST_LOG )
        << "skv_base_test_open_pds() calling close with: "
        << " PdsId: " << PDSId
        << EndLogLine;

      if( gdata.Client.Close( &PDSId ) != SKV_SUCCESS )
      {
        BegLogLine( 1 )
          << "Successful open, but closing the PDS FAILED."
          << EndLogLine;
        status = SKV_ERRNO_UNSPECIFIED_ERROR;
      }
      break;
    default:
      break;
  }

  return status;
}

skv_status_t skv_base_test_stat_pds_get( const char *aPDSName )
{
  skv_status_t status = SKV_SUCCESS;
  skv_pds_attr_t attr;
  skv_pds_id_t PDSId;

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_NONE,
                              & PDSId );

  if( status != SKV_SUCCESS )
  {
    return status;
  }

  memset( &attr, 0, sizeof(skv_pds_attr_t) );
  attr.mPDSId = PDSId;

  status = gdata.Client.PDScntl( SKV_PDSCNTL_CMD_STAT_GET, &attr );
  if( status == SKV_SUCCESS )
  {
    if( strncmp( attr.mPDSName, aPDSName, SKV_MAX_PDS_NAME_SIZE ))
    {
      BegLogLine( 1 )
        << "pdscntl get: PDSname doesn't match. pdscntl_get went wrong."
        << EndLogLine;
      status = SKV_ERRNO_UNSPECIFIED_ERROR;
    }
    else
      status = SKV_SUCCESS;
  }

  if( gdata.Client.Close( &PDSId ) != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "pdscntl get closing failed."
      << EndLogLine;
    status = SKV_ERRNO_UNSPECIFIED_ERROR;
  }

  return status;
}

skv_status_t skv_base_test_stat_pds_set( const char *aPDSName,
                                         const skv_pds_priv_t aPrivs,
                                         const uint64_t aSize = 0 )
{
  skv_status_t status = SKV_SUCCESS;
  skv_pds_attr_t attr, readback;
  skv_pds_id_t PDSId;

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_NONE,
                              & PDSId );

  if( status != SKV_SUCCESS )
  {
    return status;
  }

  attr.mPDSId = PDSId;
  memcpy(attr.mPDSName,
         aPDSName,
         strnlen( aPDSName, SKV_MAX_PDS_NAME_SIZE ) );
  attr.mPrivs = aPrivs;

  if( aSize != 0 )
    attr.mSize = aSize;

  status = gdata.Client.PDScntl( SKV_PDSCNTL_CMD_STAT_SET, &attr );

  if( status == SKV_SUCCESS )
  {
    gdata.Client.PDScntl( SKV_PDSCNTL_CMD_STAT_GET, &readback );

    if(  ( readback.mPrivs != aPrivs ) || ( strncmp(readback.mPDSName, aPDSName, SKV_MAX_PDS_NAME_SIZE ) )  )
    {
      BegLogLine( 1 )
        << "pdscntl set data mismatch, changes not applied"
        << EndLogLine;
      status = SKV_ERRNO_UNSPECIFIED_ERROR;
    }
  }

  if( gdata.Client.Close( &PDSId ) != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "pdscntl set closing failed."
      << EndLogLine;
    status = SKV_ERRNO_UNSPECIFIED_ERROR;
  }

  return status;
}

void generate_data( char *aBuf,
                    int aSize,
                    int aKeySeed,
                    int aOffset )
{
  srandom( aKeySeed );
  // skip the first aOffset random numbers to generate the same sequence depending on the key
  for( int i=0; i<aOffset; i++ )
    random();

  for( int i=0; i<aSize; i++ )
  {
    aBuf[i] = (char)random();
  }
}

bool verify_data( char *aBuf,
                  int aSize,
                  int aKeySeed,
                  int aOffset )
{
  srandom( aKeySeed );
  // skip the first aOffset random numbers to generate the same sequence depending on the key
  for( int i=0; i<aOffset; i++ )
    random();

  for( int i=0; i<aSize; i++ )
  {
    if( aBuf[i] != (char)random() )
      return false;
  }
  return true;
}



skv_status_t skv_base_test_insert( const char *aPDSName,
                                   int aKey,
                                   int aDataSize,
                                   int aOffset,
                                   skv_cmd_RIU_flags_t aFlags )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_pds_id_t PDSId;

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_CREATE,
                              & PDSId );

  if( status != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "insert: OPEN failed with: " << skv_status_to_string( status )
      << EndLogLine;
    return status;
  }

  char value[65536];
  if( aDataSize > 65536 )
    return SKV_ERRNO_VALUE_TOO_LARGE;
  generate_data( value, aDataSize, aKey, aOffset );

  status = gdata.Client.Insert( &PDSId, (char*)&aKey, sizeof(int),
                                value, aDataSize, aOffset,
                                aFlags );

  if( gdata.Client.Close( &PDSId ) != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "insert: closing failed."
      << EndLogLine;
  }

  return status;
}

skv_status_t skv_base_test_retrieve( const char *aPDSName,
                                     int aKey,
                                     int aDataSize,
                                     int aOffset,
                                     skv_cmd_RIU_flags_t aFlags )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_pds_id_t PDSId;

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_CREATE,
                              & PDSId );

  if( status != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "retrieve: OPEN failed with: " << skv_status_to_string( status )
      << EndLogLine;
    return status;
  }

  int retrieved = 0;
  char value[65536];

  if( aDataSize > 65536 )
    return SKV_ERRNO_VALUE_TOO_LARGE;
  memset( value, 0, 65536 );

  status = gdata.Client.Retrieve( &PDSId, (char *) &aKey, (int) sizeof(int),
                                  value, aDataSize, &retrieved, aOffset,
                                  aFlags );
  if( (status == SKV_SUCCESS) && (!verify_data( value, aDataSize, aKey, aOffset)) )
    status = SKV_ERRNO_CHECKSUM_MISMATCH;

  if(( retrieved != aDataSize )&&(status == SKV_SUCCESS))
    BegLogLine( 1 )
      << "retrieve: requested data size differs from returned size: " << retrieved << " != " << aDataSize
      << EndLogLine;
  if( !(aFlags & SKV_COMMAND_RIU_RETRIEVE_SPECIFIC_VALUE_LEN) && ( retrieved != aDataSize ))
    status == SKV_ERRNO_VALUE_TOO_LARGE;

  if( gdata.Client.Close( &PDSId ) != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "retrieve: closing failed."
      << EndLogLine;
  }
  return status;
}

skv_status_t skv_base_test_remove( const char *aPDSName,
                                   int aKey )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_pds_id_t PDSId;
  int DataSize = 64;

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_CREATE,
                              & PDSId );

  if( status != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "remove: OPEN failed with: " << skv_status_to_string( status )
      << EndLogLine;
    return status;
  }

  char value[65536];
  if( DataSize > 65536 )
    return SKV_ERRNO_VALUE_TOO_LARGE;
  generate_data( value, DataSize, aKey, 0 );

  status = gdata.Client.Remove( &PDSId, (char*)&aKey, sizeof(int), SKV_COMMAND_REMOVE_FLAGS_NONE );

  if( gdata.Client.Close( &PDSId ) != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "remove: closing failed."
      << EndLogLine;
  }

  return status;
}

skv_status_t skv_base_test_bulkinsert( const char *aPDSName,
                                       int aKeyCount,
                                       int aKeySize,
                                       int aMaxDataSize,
                                       int aRndSeed )
{
  int KeyBufferSize = std::max( aKeySize, (int)sizeof(uint64_t) );
  char KeyBuffer[ KeyBufferSize ];
  uint64_t *Key = (uint64_t*)&(KeyBuffer[ KeyBufferSize - sizeof(uint64_t) ]);
  char* pureKey = &(KeyBuffer[ KeyBufferSize - aKeySize ]);

  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_pds_id_t PDSId;
  int test_level = 0;
  srandom( aRndSeed );

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_CREATE,
                              & PDSId );
  if( status != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "bulkinsert: OPEN failed with: " << skv_status_to_string( status )
      << EndLogLine;
  }
  else
    test_level = 1;

  skv_client_bulk_inserter_ext_hdl_t BulkLoaderHandle;
  if( test_level >= 1 )
    status = gdata.Client.CreateBulkInserter( & PDSId,
                                              (skv_bulk_inserter_flags_t) 0,
                                              & BulkLoaderHandle );

  if( status == SKV_SUCCESS )
    test_level = 2;

  // fill larger keys with constant random data and update only lower portion
  if( aKeySize > sizeof(uint64_t) )
  {
    for (int n=0; n<aKeySize; n++)
      KeyBuffer[n] = random() & 0xFF;
  }

  for( int i=0; ( test_level >= 2 ) && ( ( status == SKV_SUCCESS ) && ( i < aKeyCount ) ); i++ )
  {
    *Key = htobe64( aRndSeed+i );

    int DataSize = random() % aMaxDataSize;
    char value[ 65536 ];
    generate_data( value, DataSize, *Key, 0 );

    status = gdata.Client.Insert( BulkLoaderHandle,
                                  pureKey,
                                  aKeySize,
                                  value,
                                  DataSize,
                                  SKV_BULK_INSERTER_FLAGS_NONE );
    BegLogLine( status != SKV_SUCCESS )
      << "BulkInsert:Insert() return status: " << skv_status_to_string( status )
      << EndLogLine;
  }
  if( test_level >= 2 )
  {
    status = gdata.Client.Flush( BulkLoaderHandle );
    BegLogLine( status != SKV_SUCCESS )
      << "BulkInsert:FLush() return status: " << skv_status_to_string( status )
      << EndLogLine;

    status = gdata.Client.CloseBulkInserter( BulkLoaderHandle );
    BegLogLine( status != SKV_SUCCESS )
      << "BulkInsert:Close() return status: " << skv_status_to_string( status )
      << EndLogLine;

    test_level = 1;
  }

  for( int i=0; ( test_level >= 1 ) && ( ( status == SKV_SUCCESS ) && ( i < aKeyCount ) ); i++ )
  {
    *Key = htobe64( aRndSeed+i );
    int DataSize = random() % aMaxDataSize;
    int Retrieved;
    char value[ 65536 ];
    status = gdata.Client.Retrieve( &PDSId, pureKey, aKeySize,
                                    value, aMaxDataSize, &Retrieved, 0,
                                    SKV_COMMAND_RIU_FLAGS_NONE );

    if( (status == SKV_SUCCESS) && (!verify_data( value, Retrieved, *Key, 0)) )
    {
      status = SKV_ERRNO_CHECKSUM_MISMATCH;
      break;
    }
  }


  if( (test_level >= 1) && (gdata.Client.Close( &PDSId ) != SKV_SUCCESS) )
  {
    BegLogLine( 1 )
      << "bulkinsert: closing failed."
      << EndLogLine;
  }

  return status;
}

skv_status_t skv_base_test_cursor( const char *aPDSName,
                                   int aKeyCount,
                                   int aKeySize,
                                   int aMaxDataSize,
                                   int aRndSeed,
                                   bool local=true )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_status_t ctrl_status = SKV_SUCCESS;
  skv_pds_id_t PDSId;
  int test_level = 0;
  int KeyCount = 0;
  srandom( aRndSeed );

  status = gdata.Client.Open( (char*)aPDSName,
                              (skv_pds_priv_t)(SKV_PDS_READ | SKV_PDS_WRITE),
                              SKV_COMMAND_OPEN_FLAGS_CREATE,
                              & PDSId );
  if( status != SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "cursor: PDSOPEN failed with: " << skv_status_to_string( status )
      << EndLogLine;
  }
  else
    test_level = 1;

  skv_client_cursor_ext_hdl_t CursorHdl;

  if(test_level >= 1)
  {
    if( local )
      status = gdata.Client.OpenLocalCursor( 0,
                                             &PDSId,
                                             &CursorHdl );
    else
      status = gdata.Client.OpenCursor( &PDSId,
                                        &CursorHdl );

    if( status == SKV_SUCCESS )
      test_level = 2;

    if( test_level >= 2)
    {
      int KeyBufferSize = std::max( aKeySize, (int)sizeof(uint64_t) );
      char KeyBuffer[ KeyBufferSize ];
      uint64_t *Key = (uint64_t*)&(KeyBuffer[ KeyBufferSize - sizeof(uint64_t) ]);
      char* pureKey = &(KeyBuffer[ KeyBufferSize - aKeySize ]);

      int KeySize = 0;
      char value[65536];
      int valueSize;

      // fill larger keys with constant random data and update only lower portion
      if( aKeySize > sizeof(uint64_t) )
      {
        for (int n=0; n<aKeySize; n++)
          KeyBuffer[n] = random() & 0xFF;
      }


      if( local )
        status = gdata.Client.GetFirstLocalElement( CursorHdl,
                                                    pureKey,
                                                    &KeySize,
                                                    aKeySize,
                                                    value,
                                                    &valueSize,
                                                    aMaxDataSize,
                                                    SKV_CURSOR_NONE_FLAG );
      else
        status = gdata.Client.GetFirstElement( CursorHdl,
                                               pureKey,
                                               &KeySize,
                                               aKeySize,
                                               value,
                                               &valueSize,
                                               aMaxDataSize,
                                               SKV_CURSOR_NONE_FLAG );

      switch( status )
      {
        case SKV_SUCCESS:
          KeyCount++;
          if (!verify_data( value, valueSize, *Key, 0))
            status = SKV_ERRNO_CHECKSUM_MISMATCH;
          break;
        case SKV_ERRNO_END_OF_RECORDS:
          break;
        default:
          BegLogLine( 1 )
            << "skv_base_test: Error after GetFirstElement: " << skv_status_to_string( status )
            << EndLogLine;
      }

      while( status == SKV_SUCCESS )
      {
        *Key = 0;
        if( local )
          status = gdata.Client.GetNextLocalElement( CursorHdl,
                                                     pureKey,
                                                     &KeySize,
                                                     aKeySize,
                                                     value,
                                                     &valueSize,
                                                     aMaxDataSize,
                                                     SKV_CURSOR_NONE_FLAG );
        else
          status = gdata.Client.GetNextElement( CursorHdl,
                                                pureKey,
                                                &KeySize,
                                                aKeySize,
                                                value,
                                                &valueSize,
                                                aMaxDataSize,
                                                SKV_CURSOR_NONE_FLAG );

        if( status == SKV_SUCCESS )
          KeyCount++;
        if( (status != SKV_SUCCESS) && (KeyCount< aKeyCount) )
          BegLogLine( 1 )
            << "skv_base_test: Cursor stopped after: " << KeyCount << "/" << aKeyCount << " Keys."
            << " rc: " << skv_status_to_string( status )
            << EndLogLine;

        if( (status == SKV_SUCCESS) && (!verify_data( value, valueSize, *Key, 0)) )
          status = SKV_ERRNO_CHECKSUM_MISMATCH;
      }

      if( local )
        ctrl_status = gdata.Client.CloseLocalCursor( CursorHdl );
      else
        ctrl_status = gdata.Client.CloseCursor( CursorHdl );
    }
  }

  if( test_level >= 2 )
  {
    BegLogLine( 1 )
      << "Cursor fetched: " << KeyCount
      << " Keys, local="<< local
      << EndLogLine;

    if( !local && (KeyCount != aKeyCount) )
      status = SKV_ERRNO_CURSOR_DONE;
    if( local && (KeyCount > aKeyCount) )
      status = SKV_ERRNO_RETRIEVE_BUFFER_MAX_SIZE_EXCEEDED;
    test_level = 1;
  }

  if( (test_level >= 1) && (gdata.Client.Close( &PDSId ) != SKV_SUCCESS ) )
  {
    BegLogLine( 1 )
      << "cursor closing failed."
      << EndLogLine;
  }
  // propagate ctrl-error status if the other operations where clean
  if( (ctrl_status != SKV_SUCCESS) && (status == SKV_SUCCESS))
    status = ctrl_status;
  return status;
}
#endif /* SKV_BASE_TEST_HPP_ */
