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
#include <common/skv_utils.hpp>

#ifndef SKV_CLIENT_CURSOR_LOG
#define SKV_CLIENT_CURSOR_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG
#define SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RETRIEVE_N_KEYS_DATA_LOG
#define SKV_CLIENT_RETRIEVE_N_KEYS_DATA_LOG ( 0 )
#endif

/**********************************************************
 * Local Cursor Interface
 **********************************************************/

/***
 * skv_client_internal_t::GetLocalServerRanks::
 * Desc: get a list of serverIDs that are on the client's local node (if any)
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
GetLocalServerRanks( int **aLocalServers, int *aCount )
{
  int                        count       = mConnMgrIF.GetServerConnCount();
  skv_client_server_conn_t *connections = mConnMgrIF.GetServerConnections();

  // count the number of local servers
  int localCount = 0;
  for( int conn=0; conn<count; conn++ )
    if( connections[ conn ].mServerIsLocal )
      localCount ++;

  if( localCount == 0 )
  {
    *aCount = 0;
    *aLocalServers = NULL;
    return SKV_SUCCESS;
  }

  // allocate list
  *aLocalServers = (int*)malloc( sizeof(int) * localCount );
  *aCount = localCount;

  // fill list
  int localIdx = 0;
  for( int conn=0; conn<count; conn++ )
    if( connections[ conn ].mServerIsLocal )
    {
      *aLocalServers[ localIdx ] = conn;
      localIdx++;
    }

  return SKV_SUCCESS;
};


/***
 * skv_client_internal_t::OpenLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
OpenLocalCursor( int                           aNodeId, 
                 skv_pds_id_t*                aPDSId, 
                 skv_client_cursor_handle_t*  aCursorHdl )
{
  mCursorManagerIF.InitCursorHdl( mPZ_Hdl,
                                  aNodeId,
                                  aPDSId,
                                  aCursorHdl);
  return SKV_SUCCESS;
}

/***
 * skv_client_internal_t::CloseLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
CloseLocalCursor( skv_client_cursor_handle_t  aCursorHdl )
{
  mCursorManagerIF.FinalizeCursorHdl( aCursorHdl );
  return SKV_SUCCESS;
}

/***
 * skv_client_internal_t::RetrieveNextCachedKey::
 * NOTE: Assumes that the key exists in cache
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
RetrieveNextCachedKey( skv_client_cursor_handle_t   aCursorHdl,
                       char*                         aRetrievedKeyBuffer,
                       int*                          aRetrievedKeySize,
                       int                           aRetrievedKeyMaxSize,
                       char*                         aRetrievedValueBuffer,
                       int*                          aRetrievedValueSize,
                       int                           aRetrievedValueMaxSize,
                       skv_cursor_flags_t           aFlags )
{
  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t::RetrieveNextCachedKey():: Entering: "
    << " aCursorHdl: " << (void *) aCursorHdl
    << " aRetrievedKeyBuffer: " << (void *) aRetrievedKeyBuffer
    << " *aRetrievedKeySize: " << *aRetrievedKeySize
    << " aRetrievedKeyMaxSize: " << aRetrievedKeyMaxSize
    << " aRetrievedValueBuffer: " << (void *) aRetrievedValueBuffer
    << " aRetrievedValueSize: " << aRetrievedValueSize
    << " aRetrievedValueMaxSize: " << aRetrievedValueMaxSize
    << " mCurrentCachedKey: " << (void*)aCursorHdl->mCurrentCachedKey
    << " mCachedKeys: " << (void*)aCursorHdl->mCachedKeys
    << " aFlags: " << aFlags
    << " Key: " << *(int*)(aCursorHdl->mCurrentCachedKey + sizeof(int))
    << EndLogLine;

  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DATA_LOG )
    << " CachedKeys@"<< (void*)aCursorHdl->mCachedKeys << ": " << HexDump( aCursorHdl->mCachedKeys, SKV_CACHED_KEYS_BUFFER_SIZE )
    << EndLogLine;

  AssertLogLine( aCursorHdl->mCurrentCachedKey >= aCursorHdl->mCachedKeys &&
                 aCursorHdl->mCurrentCachedKey < (aCursorHdl->mCachedKeys + SKV_CACHED_KEYS_BUFFER_SIZE ) )
    << "skv_client_internal_t::RetrieveNextCachedKey():: ERROR:: "
    << " aCursorHdl->mCurrentCachedKey: " << (void *) aCursorHdl->mCurrentCachedKey
    << " aCursorHdl->mCachedKeysCount: " << aCursorHdl->mCachedKeysCount
    << EndLogLine;

  int CachedKeySize = *((int *) aCursorHdl->mCurrentCachedKey );

  if( CachedKeySize > aRetrievedKeyMaxSize )
  {
    BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
      << "skv_client_internal_t::RetrieveNextCachedKey():: Leaving with ERROR:: "
      << " CachedKeySize: " << CachedKeySize
      << " aRetrievedKeyMaxSize: " << aRetrievedKeyMaxSize
      << " SKV_ERRNO_KEY_SIZE_OVERFLOW"
      << EndLogLine;

    return SKV_ERRNO_KEY_SIZE_OVERFLOW;
  }

  *aRetrievedKeySize = CachedKeySize;

  char* SrcKeyBuffer = aCursorHdl->mCurrentCachedKey + sizeof(int);

  memcpy( aRetrievedKeyBuffer, 
          SrcKeyBuffer,
          CachedKeySize );  

  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t::RetrieveNextCachedKey():: "
    << " CachedKeySize: " << CachedKeySize
    << " aCursorHdl->mCurrentCachedKeyIdx: " << aCursorHdl->mCurrentCachedKeyIdx
    << " aCursorHdl->mCachedKeysCount: " << aCursorHdl->mCachedKeysCount
    << EndLogLine;

  skv_status_t status = Retrieve( & aCursorHdl->mPdsId,
                                  aRetrievedKeyBuffer,
                                  *aRetrievedKeySize,
                                  aRetrievedValueBuffer,
                                  aRetrievedValueMaxSize,
                                  aRetrievedValueSize,
                                  0,
                                  SKV_COMMAND_RIU_FLAGS_NONE );

  // Need to set this, to be able to have access to the last key.
  // This is the starting key for the next batch of keys
  if( (aCursorHdl->mCurrentCachedKeyIdx + 1) == aCursorHdl->mCachedKeysCount )
    aCursorHdl->mPrevCachedKey = aCursorHdl->mCurrentCachedKey;

  aCursorHdl->mCurrentCachedKey += (sizeof( int ) + CachedKeySize );

  aCursorHdl->mCurrentCachedKeyIdx++;

  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t::RetrieveNextCachedKey():: Leaving "
    << EndLogLine;

  return status;
}


/***
 * skv_client_internal_t::RetrieveNKeys::
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
RetrieveNKeys( skv_client_cursor_handle_t  aCursorHdl,
               char*                        aStartingKeyBuffer,  
               int                          aStartingKeyBufferSize,
               skv_cursor_flags_t          aFlags )
{
  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t::RetrieveNKeys():: Entering..."
    << EndLogLine;

  StrongAssertLogLine( mState = SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::RetrieveNKeys()::"
    << " aFlags: " << aFlags
    << " mState: " << mState
    << EndLogLine;

  /**************************************************
   * Check limits
   *************************************************/
  if( aStartingKeyBufferSize > SKV_KEY_LIMIT )
    return SKV_ERRNO_KEY_TOO_LARGE;

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;
  /*************************************************/



  /**************************************************
   * Init cursor state
   *************************************************/
  aCursorHdl->ResetCurrentCachedState();
  /*************************************************/



  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();  

  int RoomForData = SKV_CONTROL_MESSAGE_SIZE - sizeof( skv_cmd_retrieve_n_keys_req_t );

  AssertLogLine( RoomForData >= 0 )
    << "skv_client_internal_t::RetrieveNKeys():: ERROR:: "
    << " RoomForData: " << RoomForData
    << " sizeof( skv_cmd_retrieve_n_keys_KeyFitsInMsg_req_t ): " << sizeof( skv_cmd_retrieve_n_keys_req_t )
    << " SKV_CONTROL_MESSAGE_SIZE: " << SKV_CONTROL_MESSAGE_SIZE
    << EndLogLine;

  int KeyFitsInCtrlMsg = (aStartingKeyBufferSize <= RoomForData);

  skv_cmd_retrieve_n_keys_req_t* Req = 
    (skv_cmd_retrieve_n_keys_req_t *) SendCtrlMsgBuff;

  // If the key fits into the control message it's safe to
  // send the list of buffers to cached keys
  Req->Init( aCursorHdl->mCurrentNodeId,
             & mConnMgrIF,
             aCursorHdl->mPdsId,	     
             SKV_COMMAND_RETRIEVE_N_KEYS, 
             SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_N_KEYS_CMD, 
             CmdCtrlBlk,
             aFlags,
             aStartingKeyBuffer,
             aStartingKeyBufferSize,
             KeyFitsInCtrlMsg,
             aCursorHdl->mKeysDataLMRHdl,
             aCursorHdl->mKeysDataRMRHdl,
             aCursorHdl->mCachedKeys,
             SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE );    
  /*****************************************************/

  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t: Created RetrieveN request:"
    << " KeyDataAddr: " << (uint64_t)aCursorHdl->mCachedKeys
    << EndLogLine;


  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/  
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_WAITING_FOR_VALUE_TX_ACK );
  /*****************************************************/  

  /******************************************************
   * Set the local client state used on response
   *****************************************************/  
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_RETRIEVE_N_KEYS;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieveNKeys.mCachedKeysCountPtr  = & aCursorHdl->mCachedKeysCount;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieveNKeys.mCachedKeysCountMax  = SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE;
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( aCursorHdl->mCurrentNodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::RetrieveNKeys():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  status = Wait( CmdCtrlBlk );  

  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t::RetrieveNKeys():: Leaving..."
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  return status;
}


/***
 * skv_client_internal_t::GetFirstLocalElement::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
GetFirstLocalElement( skv_client_cursor_handle_t aCursorHdl,
                      char*                       aRetrievedKeyBuffer,
                      int*                        aRetrievedKeySize,
                      int                         aRetrievedKeyMaxSize,
                      char*                       aRetrievedValueBuffer,
                      int*                        aRetrievedValueSize,
                      int                         aRetrievedValueMaxSize,
                      skv_cursor_flags_t         aFlags )
{

  BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_DIST_LOG )
    << "skv_client_internal_t::GetFirstLocalElement():: Entering..."
    << EndLogLine;

  StrongAssertLogLine( mState = SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::GetFirstLocalElement():: ERROR:: "
    << " mState: " << mState
    << EndLogLine;

  int   StartSizeToRetrieve = 0;
  char* StartToRetrive = NULL;

  if( aFlags & SKV_CURSOR_WITH_STARTING_KEY_FLAG )
  {
    StartToRetrive = aRetrievedKeyBuffer;
    StartSizeToRetrieve = *aRetrievedKeySize;
  }

  skv_status_t status = RetrieveNKeys( aCursorHdl, 
                                       StartToRetrive,
                                       StartSizeToRetrieve,
                                       (skv_cursor_flags_t) ( (int)aFlags | SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG ));

  if( status != SKV_SUCCESS )
    return status;

  AssertLogLine( aCursorHdl->mCachedKeysCount > 0 )
    << "skv_client_internal_t::GetFirstLocalElement():: ERROR:: "
    << " aCursorHdl->mCachedKeysCount: " << aCursorHdl->mCachedKeysCount
    << EndLogLine;

  status = RetrieveNextCachedKey( aCursorHdl, 
                                  aRetrievedKeyBuffer,
                                  aRetrievedKeySize,
                                  aRetrievedKeyMaxSize,
                                  aRetrievedValueBuffer,
                                  aRetrievedValueSize,
                                  aRetrievedValueMaxSize,
                                  aFlags );

  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetFirstLocalElement():: Leaving..."
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::GetNextLocalElement::
 * Desc: Get the next element in the cursor pointed to a node id
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
GetNextLocalElement(  skv_client_cursor_handle_t   aCursorHdl,
                      char*                         aRetrievedKeyBuffer,
                      int*                          aRetrievedKeySize,
                      int                           aRetrievedKeyMaxSize,
                      char*                         aRetrievedValueBuffer,
                      int*                          aRetrievedValueSize,
                      int                           aRetrievedValueMaxSize,
                      skv_cursor_flags_t           aFlags )
{
  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetNextLocalElement():: Entering..."
    << EndLogLine;

  StrongAssertLogLine( mState = SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::GetNextLocalElement():: ERROR:: "
    << " mState: " << mState
    << EndLogLine;

  AssertLogLine( aCursorHdl->mCachedKeysCount > 0 )
    << "skv_client_internal_t::GetNextLocalElement():: ERROR:: aCursorHdl->mCachedKeysCount > 0 "
    << " aCursorHdl->mCachedKeysCount: " << aCursorHdl->mCachedKeysCount
    << EndLogLine;

  if( aCursorHdl->mCurrentCachedKeyIdx == aCursorHdl->mCachedKeysCount )
  {
    // Reached the end of the cached records

    char* StartingKeyBuffer = aCursorHdl->mPrevCachedKey + sizeof(int);
    int StartingKeyBufferSize = *((int *) aCursorHdl->mPrevCachedKey);

    skv_status_t status = RetrieveNKeys( aCursorHdl,
                                         StartingKeyBuffer,
                                         StartingKeyBufferSize,
                                         aFlags );

    if( status != SKV_SUCCESS )
      return status;
  }

  skv_status_t status = RetrieveNextCachedKey( aCursorHdl,
                                               aRetrievedKeyBuffer,
                                               aRetrievedKeySize,
                                               aRetrievedKeyMaxSize,
                                               aRetrievedValueBuffer,
                                               aRetrievedValueSize,
                                               aRetrievedValueMaxSize,
                                               aFlags );

  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetNextLocalElement():: Leaving..."
    << EndLogLine;

  return status;
}

/**********************************************************
 * Global Cursor Interface
 **********************************************************/
/***
 * skv_client_internal_t::OpenLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
OpenCursor( skv_pds_id_t*                aPDSId, 
            skv_client_cursor_handle_t*  aCursorHdl )
{
  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::OpenCursor(): Entering "
    << " aPDSId: " << *aPDSId
    << EndLogLine;

  mCursorManagerIF.InitCursorHdl( mPZ_Hdl, 
                                  0,
                                  aPDSId, 
                                  aCursorHdl );

  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::OpenCursor(): Leaving "
    << " aPDSId: " << *aPDSId
    << " aCursorHdl: " << (void *) *aCursorHdl
    << EndLogLine;

  return SKV_SUCCESS;
}

/***
 * skv_client_internal_t::CloseLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
CloseCursor( skv_client_cursor_handle_t aCursorHdl )
{
  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::CloseCursor(): Entering "
    << " aCursorHdl: " << (void *) aCursorHdl
    << EndLogLine;

  mCursorManagerIF.FinalizeCursorHdl( aCursorHdl );

  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::CloseCursor(): Leaving "
    << EndLogLine;

  return SKV_SUCCESS;
}


/***
 * skv_client_internal_t::GetFirstElement::
 * Desc: Get the first element in the cursor
 * NOTE: This call is only valid after a call to OpenCursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
GetFirstElement( skv_client_cursor_handle_t  aCursorHdl,
                 char*                        aRetrievedKeyBuffer,
                 int*                         aRetrievedKeySize,
                 int                          aRetrievedKeyMaxSize,
                 char*                        aRetrievedValueBuffer,
                 int*                         aRetrievedValueSize,
                 int                          aRetrievedValueMaxSize,
                 skv_cursor_flags_t          aFlags )
{
  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetFirstElement(): Entering "
    << " aCursorHdl: " << (void *) aCursorHdl
    << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
    << " aFlags: " << (void *) aFlags
    << EndLogLine;

  skv_status_t status = SKV_SUCCESS;

  int CurrentNodeId = 0;
  while( CurrentNodeId < mConnMgrIF.GetServerConnCount() )
  {
    // Done with the previous node, continue on the next one.
    aCursorHdl->SetNodeId( CurrentNodeId );

    status = GetFirstLocalElement( aCursorHdl,
                                   aRetrievedKeyBuffer,
                                   aRetrievedKeySize,
                                   aRetrievedKeyMaxSize,
                                   aRetrievedValueBuffer,
                                   aRetrievedValueSize,
                                   aRetrievedValueMaxSize,
                                   aFlags );

    if( status != SKV_ERRNO_END_OF_RECORDS )
    {
      skv_store_t DebugKey;
      DebugKey.Init( aRetrievedKeyBuffer, *aRetrievedKeySize );

      skv_store_t DebugValue;
      DebugValue.Init( aRetrievedValueBuffer, *aRetrievedValueSize );

      BegLogLine( SKV_CLIENT_CURSOR_LOG )
        << "skv_client_internal_t::GetFirstElement(): Leaving: "
        << " status: " << skv_status_to_string ( status )
        << " aCursorHdl: " << (void *) aCursorHdl
        << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
        << " aCursorHdl->mCurrentNodeId: " << aCursorHdl->mCurrentNodeId
        << " Key: " << DebugKey
        << " Value: " << DebugValue
        << EndLogLine;

      return status;
    }

    CurrentNodeId++;
  }

  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetFirstElement(): Leaving "
    << " status: SKV_ERRNO_END_OF_RECORDS"
    << " aCursorHdl: " << (void *) aCursorHdl
    << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
    << EndLogLine;

  return SKV_ERRNO_END_OF_RECORDS;
}

/***
 * skv_client_internal_t::GetNextElement::
 * Desc: Get the next global element in the cursor 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
GetNextElement( skv_client_cursor_handle_t  aCursorHdl,
                char*                        aRetrievedKeyBuffer,
                int*                         aRetrievedKeySize,
                int                          aRetrievedKeyMaxSize,
                char*                        aRetrievedValueBuffer,
                int*                         aRetrievedValueSize,
                int                          aRetrievedValueMaxSize,
                skv_cursor_flags_t          aFlags )
{
  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetNextElement(): Entering "
    << " aCursorHdl: " << (void *) aCursorHdl
    << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
    << " aFlags: " << (void *) aFlags
    << EndLogLine;

  skv_status_t status = GetNextLocalElement( aCursorHdl,
                                             aRetrievedKeyBuffer,
                                             aRetrievedKeySize,
                                             aRetrievedKeyMaxSize,
                                             aRetrievedValueBuffer,
                                             aRetrievedValueSize,
                                             aRetrievedValueMaxSize,
                                             aFlags );
  if( status == SKV_ERRNO_END_OF_RECORDS )
  {
    if( aCursorHdl->mCurrentNodeId == mConnMgrIF.GetServerConnCount() - 1 )
      return SKV_ERRNO_END_OF_RECORDS;
    else
    {
      int NewNodeId = -1;
      do
      {
        // Done with the previous node, continue on the next one.
        int LastNodeId = aCursorHdl->GetNodeId();
        NewNodeId = LastNodeId + 1;

        aCursorHdl->SetNodeId( NewNodeId );

        BegLogLine( SKV_CLIENT_CURSOR_LOG )
          << "skv_client_internal_t::GetNextElement(): Fetching first element from "
          << " new node: " << aCursorHdl->GetNodeId()
          << " aCursorHdl->mCurrentNodeId: " << aCursorHdl->mCurrentNodeId
          << EndLogLine;

        status = GetFirstLocalElement( aCursorHdl,
                                       aRetrievedKeyBuffer,
                                       aRetrievedKeySize,
                                       aRetrievedKeyMaxSize,
                                       aRetrievedValueBuffer,
                                       aRetrievedValueSize,
                                       aRetrievedValueMaxSize,
                                       aFlags );

        if( status != SKV_ERRNO_END_OF_RECORDS )
        {
          skv_store_t DebugKey;
          DebugKey.Init( aRetrievedKeyBuffer, *aRetrievedKeySize );

          skv_store_t DebugValue;
          DebugValue.Init( aRetrievedValueBuffer, *aRetrievedValueSize );

          BegLogLine( SKV_CLIENT_CURSOR_LOG )
            << "skv_client_internal_t::GetNextElement(): Leaving: "
            << " status: " << skv_status_to_string ( status )
            << " aCursorHdl: " << (void *) aCursorHdl
            << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
            << " aCursorHdl->mCurrentNodeId: " << aCursorHdl->mCurrentNodeId
            << " Key: " << DebugKey
            << " Value: " << DebugValue
            << EndLogLine;
          return status;
        }
      }
      while( NewNodeId != (mConnMgrIF.GetServerConnCount() - 1) );

      // Reached the last node
      BegLogLine( SKV_CLIENT_CURSOR_LOG )
        << "skv_client_internal_t::GetNextElement(): Leaving "
        << " status: SKV_ERRNO_END_OF_RECORDS"
        << " aCursorHdl: " << (void *) aCursorHdl
        << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
        << " aCursorHdl->mCurrentNodeId: " << aCursorHdl->mCurrentNodeId
        << EndLogLine;

      return SKV_ERRNO_END_OF_RECORDS;
    }
  }

  BegLogLine( SKV_CLIENT_CURSOR_LOG )
    << "skv_client_internal_t::GetNextElement(): Leaving "
    << " status: " << skv_status_to_string ( status )
    << " aCursorHdl: " << (void *) aCursorHdl
    << " aCursorHdl->mPdsId: " << aCursorHdl->mPdsId
    << " aCursorHdl->mCurrentNodeId: " << aCursorHdl->mCurrentNodeId
    << EndLogLine;

  return status;  
}
