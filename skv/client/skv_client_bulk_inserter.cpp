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

#include <client/skv_client_internal.hpp>

TraceClient gSKVClientiBulkInsertStart;
TraceClient gSKVClientiBulkInsertFinis;

#ifndef SKV_CLIENT_iBULKINSERT_TRACE
#define SKV_CLIENT_iBULKINSERT_TRACE ( 1 )
#endif

#ifndef SKV_CLIENT_BULK_INSERT_LOG
#define SKV_CLIENT_BULK_INSERT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_BULK_INSERT_FLUSH_LOG
#define SKV_CLIENT_BULK_INSERT_FLUSH_LOG ( 0 | SKV_LOGGING_ALL )
#endif

skv_status_t 
skv_client_internal_t::
iBulkInsert( int                 aNodeId,
             skv_pds_id_t*      aPDSId,      
             char*               aBuffer,
             int                 aBufferSize,
             it_lmr_handle_t     aBufferLMR,
             it_rmr_context_t    aBufferRMR,
             skv_client_ccb_t** aCCB )
{
  BegLogLine( SKV_CLIENT_BULK_INSERT_LOG )
    << "skv_client_internal_t::iBulkInsert(): Entering "
    << " aNodeId: " << aNodeId
    << " aPDSId: " << *aPDSId
    << " aBuffer: " << (void *) aBuffer
    << " aBufferSize: " << aBufferSize
    << " aBufferRMR: " << (void *) aBufferRMR
    << EndLogLine;

#if 0
  if( (mMyRank == 357) && (aBuffer == (char *) 0xABBC3CC8) )
  {
    StrongAssertLogLine( 0 )
      << "skv_client_internal_t::iBulkInsert(): BOGUS pointer "
      << " aBuffer: " << (void *) aBuffer
      << " aBufferSize: " << aBufferSize
      << " aBufferRMR: " << (void *) aBufferRMR
      << " aNodeId: " << aNodeId
      << EndLogLine;
  }
#endif

#if 0 // SKV_CLIENT_BULK_INSERT_LOG
  int TotalBytesProcessed = 0;
  char* CurrentRow = aBuffer;
  int RowCount = 0;

  while( TotalBytesProcessed < aBufferSize )
  {
    int BytesInRow = skv_bulk_insert_get_total_len( CurrentRow );

    HexDump FxString( CurrentRow, BytesInRow );

    BegLogLine( 1 )
      << "skv_client_internal_t::iBulkInsert(): "
      << " RowCount: " << RowCount
      << " BytesInRow: " << BytesInRow
      << " FxString: " << FxString
      << EndLogLine;

    CurrentRow += BytesInRow;
    TotalBytesProcessed += BytesInRow;
    RowCount++;
  }

  BegLogLine( 1 )
    << "skv_client_internal_t::iBulkInsert(): "
    << " RowCount: " << RowCount
    << EndLogLine;
#endif

  gSKVClientiBulkInsertStart.HitOE( SKV_CLIENT_iBULKINSERT_TRACE,
                                    "SKVClientiBulkInsert",
                                    mMyRank,
                                    gSKVClientiBulkInsertStart );

  AssertLogLine( aBufferSize > 0 )
    << "skv_client_internal_t::iBulkInsert(): ERROR: "
    << " aBufferSize: " << aBufferSize
    << EndLogLine;

  AssertLogLine( ((void *)aBufferRMR) != NULL )
    << "skv_client_internal_t::iBulkInsert(): ERROR: "
    << EndLogLine;

  AssertLogLine( ((void *) aBuffer) != NULL )
    << "skv_client_internal_t::iBulkInsert(): ERROR: "
    << EndLogLine;

  if( aBufferSize > SKV_BULK_INSERT_LIMIT )
    return SKV_ERRNO_BULK_INSERT_LIMIT_EXCEEDED;

  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();  
  skv_cmd_bulk_insert_req_t* Req = (skv_cmd_bulk_insert_req_t *) SendCtrlMsgBuff;

  uint64_t BufferChecksum = 0;
#ifdef SKV_BULK_LOAD_CHECKSUM 
  for( int i = 0; i < aBufferSize; i++)
  {
    BufferChecksum += aBuffer[ i ];
  }

  BegLogLine( 1 )
    << "On client: BufferChecksum: " << BufferChecksum
    // << " aBufferSize: " << aBufferSize
    << EndLogLine;
#endif

  Req->Init( aNodeId, 
             & mConnMgrIF,
             aPDSId,
             SKV_COMMAND_BULK_INSERT,
             SKV_SERVER_EVENT_TYPE_IT_DTO_BULK_INSERT_CMD,
             CmdCtrlBlk,
             aBuffer,
             aBufferSize,
             aBufferLMR,
             // aBufferRMR,
             BufferChecksum );
  /*****************************************************/



  /******************************************************
   * Set the local client state used on response
   *****************************************************/  
  CmdCtrlBlk->mCommand.mType                                             = SKV_COMMAND_BULK_INSERT;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandBulkInsert.mBuffer         = aBuffer;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandBulkInsert.mBufferSize     = aBufferSize;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandBulkInsert.mBufferChecksum = BufferChecksum;
  /*****************************************************/  



  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/  
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL );
  /*****************************************************/  

  skv_status_t status = mConnMgrIF.Dispatch( aNodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iBulkInsert():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCCB = CmdCtrlBlk;

  BegLogLine( SKV_CLIENT_BULK_INSERT_LOG )
    << "skv_client_internal_t::iBulkInsert():: Leaving... "
    << EndLogLine;

  gSKVClientiBulkInsertFinis.HitOE( SKV_CLIENT_iBULKINSERT_TRACE,
                                     "SKVClientiBulkInsert", 
                                     mMyRank,
                                     gSKVClientiBulkInsertFinis );
  return status;  

}

skv_status_t 
skv_client_internal_t::
CreateBulkInserter( skv_pds_id_t*                       aPDSId,
                    skv_bulk_inserter_flags_t           aFlags,
                    skv_client_bulk_inserter_hdl_t*     aBulkInserterHandle )
{
  skv_client_bulk_insert_control_block_t* BulkInsertControlBlock = 
    (skv_client_bulk_insert_control_block_t *) malloc( sizeof( skv_client_bulk_insert_control_block_t ) );

  AssertLogLine( BulkInsertControlBlock != NULL )
    << "skv_client_internal_t::CreateBulkInserter(): ERROR: "
    << EndLogLine;

  BulkInsertControlBlock->Init( mPZ_Hdl, 
                                aPDSId,
                                aFlags,
                                mConnMgrIF.GetServerConnCount() );

  *aBulkInserterHandle = BulkInsertControlBlock;

  return SKV_SUCCESS;
}

skv_status_t
skv_client_internal_t::
Insert( skv_client_bulk_inserter_hdl_t                  aBulkInserterHandle,
        char*                                            aKeyBuffer,
        int                                              aKeyBufferSize,
        char*                                            aValueBuffer,
        int                                              aValueBufferSize,			
        skv_bulk_inserter_flags_t                       aFlags )
{  
  BegLogLine( SKV_CLIENT_BULK_INSERT_LOG )
    << "skv_client_internal_t::Insert(): Entering "
    << " aBulkInserterHandle: " << (void *) aBulkInserterHandle
    << " aKeyBuffer: " << (void *) aKeyBuffer
    << " aKeyBufferSize: " << aKeyBufferSize
    << " aValueBuffer: " << (void *) aValueBuffer
    << " aValueBufferSize: " << aValueBufferSize
    << " aFlags: " << aFlags
    << EndLogLine;

  AssertLogLine( aKeyBufferSize > 0 && aKeyBufferSize <= SKV_KEY_LIMIT )
    << "skv_client_internal_t::Insert(): ERROR: "
    << " aKeyBufferSize: " << aKeyBufferSize
    << " SKV_KEY_LIMIT: " << SKV_KEY_LIMIT
    << EndLogLine;

  AssertLogLine( aValueBufferSize > 0 && aValueBufferSize <= SKV_VALUE_LIMIT )
    << "skv_client_internal_t::Insert(): ERROR: "
    << " aValueBufferSize: " << aValueBufferSize
    << " SKV_KEY_LIMIT: " << SKV_KEY_LIMIT
    << EndLogLine;

  // Kick pipes
  mConnMgrIF.ProcessConnectionsRqSq();

  int TotalSize = aKeyBufferSize + aValueBufferSize + 2 * sizeof( int );

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  skv_key_t UserKey;
  UserKey.Init( aKeyBuffer, aKeyBufferSize );

  int NodeId = mDistribution.GetNode( &UserKey );  
  /*****************************************/

  AssertLogLine( NodeId >= 0 && NodeId < aBulkInserterHandle->mServerNodeCount )
    << "skv_client_internal_t::Insert(): ERROR: "
    << " NodeId: " << NodeId
    << " aBulkInserterHandle->mServerNodeCount: " << aBulkInserterHandle->mServerNodeCount
    << EndLogLine;

  skv_client_bulk_insert_buffer_list_t* BufferList = & aBulkInserterHandle->mBufferListPerServer[ NodeId ];

  skv_client_bulk_insert_buffer_t*      Buffer     = BufferList->mCurrentBuffer;

  int   CurrentIndex   = Buffer->mCurrentIndex;
  char* BufferDataPtr  = Buffer->mBufferData;

  BegLogLine( SKV_CLIENT_BULK_INSERT_LOG )
    << "skv_client_internal_t::Insert(): "
    << " NodeId: " << NodeId
    << " TotalSize: " << TotalSize
    << " CurrentIndex: " << CurrentIndex
    << " Buffer->mBufferSize: " << Buffer->mBufferSize
    << " BufferDataPtr: " << BufferDataPtr
    << " BufferList->mCurrentBuffer: " << BufferList->mCurrentBuffer
    << " Buffer: " << (void *) Buffer
    << " BufferList: " << (void *) BufferList
    << EndLogLine;

  if( CurrentIndex + TotalSize > Buffer->mBufferSize )
  {
    AssertLogLine( CurrentIndex > 0 )
      << "skv_client_internal_t::Insert(): ERROR: Buffer size is smaller the first element."
      << " Please increase the bulk insert buffer size."
      << EndLogLine;

    skv_status_t bulk_status = iBulkInsert( NodeId,
                                            &aBulkInserterHandle->mPDSId,
                                            BufferDataPtr,
                                            CurrentIndex,
                                            Buffer->mBufferLMR,
                                            Buffer->mBufferRMR,
                                            &Buffer->mCommandHandle );

    Buffer->mState = SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_PENDING;

    AssertLogLine( bulk_status == SKV_SUCCESS )
      << "skv_client_internal_t::Insert(): ERROR: "
      << " bulk_status: " << skv_status_to_string( bulk_status )
      << EndLogLine;

    if( BufferList->mCurrentBuffer->mNext == NULL )
    {
      BufferList->mCurrentBuffer = BufferList->mHead;
    }
    else
    {
      BufferList->mCurrentBuffer = BufferList->mCurrentBuffer->mNext;
    }

    Buffer = BufferList->mCurrentBuffer;

    if( Buffer->mState == SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_PENDING )
    {
      skv_status_t wstatus = Wait( Buffer->mCommandHandle );

      if( wstatus == SKV_ERRNO_RECORD_ALREADY_EXISTS )
      {
        BegLogLine( 1 )
          << "skv_client_internal_t::Flush(): WARNING: "
          << " Buffer: " << (void *) Buffer
          << " wstatus: " << skv_status_to_string( wstatus )
          << EndLogLine;
      }
      else
      AssertLogLine( wstatus == SKV_SUCCESS )
        << "skv_client_internal_t::Insert(): ERROR: "
        << " wstatus: " << skv_status_to_string( wstatus )
        << EndLogLine;

      // 	  if( wstatus != SKV_SUCCESS )
      // 	    return wstatus;

      Buffer->mState = SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_READY;
    }

    Buffer->mCurrentIndex = 0;
    BufferDataPtr = Buffer->mBufferData;
  }

  int TotalLength = skv_bulk_insert_pack( &BufferDataPtr[Buffer->mCurrentIndex],
                                          aKeyBuffer,
                                          aKeyBufferSize,
                                          aValueBuffer,
                                          aValueBufferSize );

  AssertLogLine( TotalLength == TotalSize )
    << "skv_client_internal_t::Insert(): ERROR: "
    << " TotalLength: " << TotalLength
    << " TotalSize: " << TotalSize
    << EndLogLine;

  Buffer->mCurrentIndex += TotalLength;

  BegLogLine( SKV_CLIENT_BULK_INSERT_LOG )
    << "skv_client_internal_t::Insert(): Leaving "
    << " Buffer->mCurrentIndex: " << Buffer->mCurrentIndex
    << " TotalLength: " << TotalLength
    << " Buffer: " << (void *) Buffer
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t 
skv_client_internal_t::
Flush( skv_client_bulk_inserter_hdl_t aBulkInserterHandle )  
{
  BegLogLine( SKV_CLIENT_BULK_INSERT_FLUSH_LOG )
    << "skv_client_internal_t::Flush(): Entering... "
    << " aBulkInserterHandle: " << (void *) aBulkInserterHandle
    << EndLogLine;

  /****
   * Trigger inserts of all the bulk load buffers
   ***/

  int ServerNodeCount = aBulkInserterHandle->mServerNodeCount;
  for( int i = 0; i < ServerNodeCount; i++ )
  {
    skv_client_bulk_insert_buffer_list_t* BufferList = &aBulkInserterHandle->mBufferListPerServer[i];

    skv_client_bulk_insert_buffer_t* Buffer = BufferList->mHead;

    while( Buffer != NULL )
    {
      if( (Buffer->mState != SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_PENDING) &&
          (Buffer->mCurrentIndex > 0) )
      {
        skv_status_t bulk_status = iBulkInsert( i,
                                                &aBulkInserterHandle->mPDSId,
                                                Buffer->mBufferData,
                                                Buffer->mCurrentIndex,
                                                Buffer->mBufferLMR,
                                                Buffer->mBufferRMR,
                                                &Buffer->mCommandHandle );

        AssertLogLine( bulk_status == SKV_SUCCESS )
          << "skv_client_internal_t::Flush(): ERROR: "
          << " bulk_status: " << skv_status_to_string( bulk_status )
          << EndLogLine;

        Buffer->mState = SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_PENDING;
      }

      Buffer = Buffer->mNext;
    }
  }

  /****
   * Make sure that all the buffers are completed
   ***/
  for( int i = 0; i < ServerNodeCount; i++ )
  {
    skv_client_bulk_insert_buffer_list_t* BufferList = &aBulkInserterHandle->mBufferListPerServer[i];

    skv_client_bulk_insert_buffer_t* Buffer = BufferList->mHead;

    while( Buffer != NULL )
    {
      if( Buffer->mState == SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_PENDING )
      {
        BegLogLine( SKV_CLIENT_BULK_INSERT_FLUSH_LOG )
          << "skv_client_internal_t::Flush(): About to Wait "
          << " Buffer->mCommandHandle: " << (void *) Buffer->mCommandHandle
          << EndLogLine;

        skv_status_t wait_status = Wait( Buffer->mCommandHandle );

        BegLogLine( SKV_CLIENT_BULK_INSERT_FLUSH_LOG )
          << "skv_client_internal_t::Flush(): Done waiting on "
          << " Buffer->mCommandHandle: " << (void *) Buffer->mCommandHandle
          << " wait_status: " << skv_status_to_string( wait_status )
          << EndLogLine;
#if 1
        // 	      if( wait_status != SKV_SUCCESS )
        // 		return wait_status;
        if( wait_status == SKV_ERRNO_RECORD_ALREADY_EXISTS )
        {
          BegLogLine( 1 )
            << "skv_client_internal_t::Flush(): WARNING: "
            << " NodeId: " << i
            << " Buffer: " << (void *) Buffer
            << " wait_status: " << skv_status_to_string( wait_status )
            << EndLogLine;
        }
        else
          AssertLogLine( wait_status == SKV_SUCCESS )
            << "skv_client_internal_t::Flush(): ERROR: "
            << " wait_status: " << skv_status_to_string( wait_status )
            << EndLogLine;
#endif
        Buffer->mState = SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_READY;
        Buffer->mCurrentIndex = 0;
      }

      Buffer = Buffer->mNext;
    }
  }

  BegLogLine( SKV_CLIENT_BULK_INSERT_FLUSH_LOG )
    << "skv_client_internal_t::Flush(): Leaving "
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t 
skv_client_internal_t::
CloseBulkInserter(  skv_client_bulk_inserter_hdl_t      aBulkInserterHandle )
{
  skv_client_bulk_insert_control_block_t* BulkInsertControlBlock = aBulkInserterHandle;
  AssertLogLine( BulkInsertControlBlock != NULL )
    << "skv_client_internal_t::CloseBulkInserter(): ERROR: "
    << EndLogLine;

  skv_status_t status = Flush( aBulkInserterHandle );

#if 0
  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::CloseBulkInserter(): ERROR: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;
#endif
  BulkInsertControlBlock->Finalize();

  free( BulkInsertControlBlock );

  return status;
}
/*****************************************************************************/
