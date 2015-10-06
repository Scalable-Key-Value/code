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

#include <skv/client/skv_client_internal.hpp>
#include <skv/common/skv_utils.hpp>

#include <netdb.h>	/* struct hostent */

#ifndef SKV_CLIENT_CONNECTION_LOG
#define SKV_CLIENT_CONNECTION_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RETRIEVE_DIST_LOG
#define SKV_CLIENT_RETRIEVE_DIST_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_OPEN_LOG
#define SKV_CLIENT_OPEN_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_INIT_LOG
#define SKV_CLIENT_INIT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_INSERT_LOG
#define SKV_CLIENT_INSERT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RETRIEVE_LOG
#define SKV_CLIENT_RETRIEVE_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_REMOVE_LOG
#define SKV_CLIENT_REMOVE_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_ENDIAN_LOG
#define SKV_CLIENT_ENDIAN_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_iINSERT_TRACE
#define SKV_CLIENT_iINSERT_TRACE ( 0 )
#endif

TraceClient gSKVClientiInsertStart;
TraceClient gSKVClientiInsertFinis;

/***
 * skv_client_internal_t::Init::
 * Desc: Initializes the state of the skv_client
 * Gets the client ready to establish a  connection
 * with the serverp
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
#ifdef SKV_CLIENT_UNI
skv_status_t
skv_client_internal_t::
Init( skv_client_group_id_t aCommGroupId,
      int aFlags,
      const char* aConfigFile )
{
  mMyRank = 0;
  mClientGroupCount = 1;
#else
skv_status_t
skv_client_internal_t::
Init( skv_client_group_id_t aCommGroupId,
      MPI_Comm aComm,
      int aFlags,
      const char* aConfigFile )
{
  mComm = aComm;
  MPI_Comm_rank( aComm, & mMyRank );
  MPI_Comm_size( aComm, & mClientGroupCount );
#endif

  BegLogLine( SKV_CLIENT_INIT_LOG )
    << "skv_client_internal_t::Init(): Entering.. "
    << " aCommGroupId: " << aCommGroupId
    << " aFlags: " << aFlags
    << EndLogLine;

  mFlags = aFlags;

  mState = SKV_CLIENT_STATE_DISCONNECTED;

  mCommGroupId = aCommGroupId;

  BegLogLine( SKV_CLIENT_INIT_LOG )
    << "skv_client_internal_t::Init(): "
    << " mMyRank: " << mMyRank
    << " mClientGroupCount: " << mClientGroupCount
    << EndLogLine;

  mSKVConfiguration = skv_configuration_t::GetSKVConfiguration( aConfigFile );

  /************************************************************
   * Initialize the interface adapter
   ***********************************************************/
  it_status_t status = it_ia_create( VP_NAME,
                                     2,
                                     0,
                                     & mIA_Hdl);

  if( status != IT_SUCCESS )
  {
      BegLogLine( SKV_CLIENT_INIT_LOG )
          << "skv_client_internal_t::Init::ERROR:: after it_ia_create()"
          << " status: " <<  status
          << EndLogLine;
      return SKV_ERRNO_CONN_FAILED;
  }

  itx_init_tracing( "skv_client", mMyRank );
  /***********************************************************/

  /************************************************************
   * Initialize the protection zone
   ***********************************************************/
  status = it_pz_create(  mIA_Hdl,
                          &mPZ_Hdl );

  if( status != IT_SUCCESS )
  {
      BegLogLine( SKV_CLIENT_INIT_LOG )
          << "skv_client_internal_t::Init::ERROR:: after it_pz_create()"
          << " status: " << status
          << EndLogLine;
      return SKV_ERRNO_CONN_FAILED;
  }

  /***********************************************************/

  /************************************************************
   * Initialize the manager of command control blocks
   ***********************************************************/
  mCCBMgrIF.Init( mPZ_Hdl );
  /***********************************************************/

  /************************************************************
   * Initialize the connector to servers
   ***********************************************************/
  mConnMgrIF.Init( mCommGroupId,
                   mMyRank,
                   & mIA_Hdl,
                   & mPZ_Hdl,
                   0,
                   & mCCBMgrIF );
  /***********************************************************/

  /************************************************************
   * Initialize the cursor manager
   ***********************************************************/
  mCommandMgrIF.Init( & mConnMgrIF, & mCCBMgrIF );
  /***********************************************************/

  BegLogLine( SKV_CLIENT_INIT_LOG )
    << "skv_client_internal_t::Init(): Leaving... "
    << EndLogLine;

  return SKV_SUCCESS;
}

/***
 * skv_client_internal_t::Disconnect::
 * Desc: Connect to the SKV Server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Disconnect()
{
  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Disconnect():: Entering "
    << EndLogLine;

  skv_status_t status = mConnMgrIF.Disconnect();

  StrongAssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Disconnect():: ERROR:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  // Return all pending commands to the free list

  mState = SKV_CLIENT_STATE_DISCONNECTED;

  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Disconnect():: Leaving "
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::Connect::
 * Desc: Connect to the SKV Server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Connect( const char* aConfigFile,
         int   aFlags )
{
  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Connect():: Entering "
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_DISCONNECTED )
    << "skv_client_internal_t::Connect(): ERROR:: "
    << " mState: " << mState
    << EndLogLine;

  /*****************************************
   * Create connections to the skv servers
   * aServerGroupName is the identifier
   * of a file which contains the skv server
   * addresses
   *****************************************/
  skv_status_t status = mConnMgrIF.Connect( aConfigFile, aFlags );
  if( status != IT_SUCCESS )
  {
      BegLogLine( SKV_CLIENT_INIT_LOG )
          << "skv_client_internal_t::Connect():: ERROR:: "
          << " status: " << skv_status_to_string( status )
          << EndLogLine;
      return SKV_ERRNO_CONN_FAILED;
  }

  mState = SKV_CLIENT_STATE_CONNECTED;

  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Connect():: Connected to "
    << " aServerGroupName: " << aConfigFile
    << EndLogLine;
  /*****************************************/

  /*****************************************
   * Get Distribution from the Server
   *****************************************/
  status = RetrieveDistribution( & mDistribution );
  if( status != IT_SUCCESS )
  {
      BegLogLine( SKV_CLIENT_INIT_LOG )
          << "skv_client_internal_t::Connect():: ERROR:: "
          << " status: " << skv_status_to_string( status )
          << EndLogLine;
      return SKV_ERRNO_CONN_FAILED;
  }

  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Connect():: Retrieved distribution "
    << EndLogLine;
  /*****************************************/

#if 0
  /*****************************************
   * Open the SKVSystem table
   *****************************************/
  status = Open( "SKVSystem",
                 SKV_PDS_READ,
                 (skv_cmd_open_flags_t) 0,
                 & mSKVSystemPds );

  StrongAssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Connect():: ERROR:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Connect():: Opened the skv system table "
    << EndLogLine;
  /*****************************************/
#endif

  BegLogLine( SKV_CLIENT_CONNECTION_LOG )
    << "skv_client_internal_t::Connect():: Exiting "
    << EndLogLine;

  return status;
}

/***
 * \brief skv_client_internal_t::iOpen::
 *
 * Async interface to opening a PDS
 * \param[in]  aPDSName   name of PDS as char
 * \param[in]  aPrivs     access rights/privileges
 * \param[in]  aFlags     steering flags
 * \param[out] aPDSId     assigned ID in case the PDS is opened
 * \param[out] aCmdHdl    command handle to use with wait/test
 *
 * \return SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
iOpen( char*                  aPDSName,
       skv_pds_priv_t        aPrivs,
       skv_cmd_open_flags_t  aFlags,
       skv_pds_id_t*         aPDSId,
       skv_client_cmd_hdl_t* aCmdHdl )
{
  BegLogLine( SKV_CLIENT_OPEN_LOG )
    << "skv_client_internal_t::iOpen():: Entering"
    << " aPDSName: " << aPDSName
    << " aPrivs: " << (int) aPrivs
    << " aFlags: " << (int) aFlags
    << " aPDSId: " << (void *) aPDSId
    << " aCmdHdl: " << (void *) aCmdHdl
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iOpen():: "
    << " aPDSName: " << aPDSName
    << " aFlags: " << aFlags
    << " mState: " << mState
    << EndLogLine;

  /*****************************************
   * In MPI mode, decide on who opens the
   * PDS here
   *****************************************/
  /*****************************************/

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  int PDSNameSize = strlen( aPDSName ) + 1;

  skv_key_t PDSNameAsKey;
  PDSNameAsKey.Init( aPDSName, PDSNameSize );

  int NodeId = mDistribution.GetNode( & PDSNameAsKey );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  bzero( SendCtrlMsgBuff, sizeof( skv_cmd_open_req_t ));
  skv_cmd_open_req_t* Req = (skv_cmd_open_req_t *) SendCtrlMsgBuff;
  Req->Init( SKV_COMMAND_OPEN,
             SKV_SERVER_EVENT_TYPE_IT_DTO_OPEN_CMD,
             CmdCtrlBlk,
             aPrivs,
             aFlags,
             aPDSName,
             PDSNameSize );
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian converting the Req"
    << EndLogLine ;
  Req->EndianConvert() ;
  /*****************************************************/

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_OPEN;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandOpen.mPDSId = aPDSId;
  /*****************************************************/

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_PENDING );
  /*****************************************************/

  BegLogLine( SKV_CLIENT_OPEN_LOG )
    << "skv_client_internal_t::iOpen():: About to call Dispatch( "
    << NodeId << " ,  "
    << (void *) CmdCtrlBlk << " )"
    << EndLogLine;

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iOpen():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  BegLogLine( SKV_CLIENT_OPEN_LOG )
    << "skv_client_internal_t::iOpen():: Leaving"
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::iRetrieve::
 * Desc: Async interface to retrieving a record
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
iRetrieve( skv_pds_id_t*          aPDSId,
           char*                   aKeyBuffer,
           int                     aKeyBufferSize,
           char*                   aValueBuffer,
           int                     aValueBufferSize,
           int*                    aValueRetrievedSize,
           int                     aOffset,
           skv_cmd_RIU_flags_t    aFlags,
           skv_client_cmd_hdl_t*  aCmdHdl )
{
  skv_store_t DebugKey;
  DebugKey.Init( aKeyBuffer, aKeyBufferSize );

  BegLogLine( SKV_CLIENT_RETRIEVE_LOG )
    << "skv_client_internal_t::iRetrieve():: Entering "
    << " aPDSId@: " << aPDSId
    << " aPDSId: " << *aPDSId
    << " KeyBuffer: " << (void *) aKeyBuffer
    << " ValueBuffer: " << (void *) aValueBuffer
    << " aKeyBufferSize: " << aKeyBufferSize
    << " aValueBufferSize: " << aValueBufferSize
    << " Key: " << DebugKey
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iRetrieve():: "
    << " aFlags: " << aFlags
    << " mState: " << mState
    << EndLogLine;

  /**************************************************
   * Check limits
   *************************************************/
  if( aKeyBufferSize > SKV_KEY_LIMIT )
    return SKV_ERRNO_KEY_TOO_LARGE;

  if( aValueBufferSize > SKV_VALUE_LIMIT )
    return SKV_ERRNO_VALUE_TOO_LARGE;

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  skv_key_t UserKey;
  UserKey.Init( aKeyBuffer, aKeyBufferSize );

  int NodeId = mDistribution.GetNode( &UserKey );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_RIU_req_t* Req = (skv_cmd_RIU_req_t *) SendCtrlMsgBuff;

  const int RoomForData = skv_cmd_RIU_req_t::GetMaxPayloadSize();
  const bool ValueFitsInBuff = (aValueBufferSize <= RoomForData);
  const bool KeyFitsInBuff = (aKeyBufferSize <= RoomForData);

  if( ValueFitsInBuff )
    aFlags = (skv_cmd_RIU_flags_t) (SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG | aFlags);
  else if( KeyFitsInBuff )
    aFlags = (skv_cmd_RIU_flags_t) (SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG | aFlags);

  it_lmr_handle_t KeyLMR;
  it_lmr_handle_t ValueLMR;

  Req->Init( NodeId,
             & mConnMgrIF,
             aPDSId,
             SKV_COMMAND_RETRIEVE,
             SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_CMD,
             CmdCtrlBlk,
             aOffset,
             aFlags,
             aKeyBufferSize,
             aKeyBuffer,
             aValueBufferSize,
             aValueBuffer,
             mPZ_Hdl,
             & KeyLMR,
             & ValueLMR );
  /*****************************************************/
  Req->EndianConvert() ;

  Req->mRMRTriplet.EndianConvert() ; // todo tjcw check if this is right
  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_RETRIEVE;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieve.mFlags              = aFlags;

  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieve.mValueAddr = aValueBuffer;

  // if user request indicates a large transfer, we need to store the LMR
  if( aFlags & SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG)
    CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieve.mValueLMR  = (it_lmr_handle_t)IT_NULL_HANDLE;
  else
    CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieve.mValueLMR  = ValueLMR;

  // this is no longer ugly - it was just mad!!!
  // this is a little ugly but we need a temporary storage for the original buffer size in case the actual value in storage has a different size
  // if( *aValueRetrievedSize != aValueBufferSize )
  //   *aValueRetrievedSize = aValueBufferSize;

  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieve.mValueRequestedSize = aValueBufferSize;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieve.mValueRetrievedSize = aValueRetrievedSize;
  /*****************************************************/

  BegLogLine( 0 )
    << "iRetrieve "
    << " CCB: " << (void*)CmdCtrlBlk
    << " vAddr: " << Req->mRMRTriplet
    << " uBuf: " << (void*)aValueBuffer
    << " Now dispatching..."
    << EndLogLine;

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_WAITING_FOR_VALUE_TX_ACK );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iRetrieve():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  BegLogLine( SKV_CLIENT_RETRIEVE_LOG )
    << "skv_client_internal_t::iRetrieve():: Leaving SUCCESS"
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::iUpdate::
 * Desc: Async interface to updating a record
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
iUpdate( skv_pds_id_t*          aPDSId,
         char*                   aKeyBuffer,
         int                     aKeyBufferSize,
         char*                   aValueBuffer,
         int                     aValueUpdateSize,
         int                     aOffset,
         skv_cmd_RIU_flags_t    aFlags,
         skv_client_cmd_hdl_t*  aCmdHdl )
{
  StrongAssertLogLine( 0 )
    << "skv_client_internal_t::iUpdate(): Not yet implemented."
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iUpdate():: "
    << " aFlags: " << aFlags
    << " mState: " << mState
    << EndLogLine;

  /**************************************************
   * Check limits
   *************************************************/
  if( aKeyBufferSize > SKV_KEY_LIMIT )
    return SKV_ERRNO_KEY_TOO_LARGE;

  if( aValueUpdateSize > SKV_VALUE_LIMIT )
    return SKV_ERRNO_VALUE_TOO_LARGE;

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  skv_key_t UserKey;
  UserKey.Init( aKeyBuffer, aKeyBufferSize );

  int NodeId = mDistribution.GetNode( &UserKey );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_RIU_req_t * Req = (skv_cmd_RIU_req_t *) SendCtrlMsgBuff;

  const int RoomForData = skv_cmd_RIU_req_t::GetMaxPayloadSize();
  const bool KeyValueFitsInBuff = ((aKeyBufferSize + aValueUpdateSize) <= RoomForData);
  const bool KeyFitsInBuff = (aKeyBufferSize <= RoomForData);

  if( KeyValueFitsInBuff )
    aFlags = (skv_cmd_RIU_flags_t) (SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG | aFlags);
  else if( KeyFitsInBuff )
    aFlags = (skv_cmd_RIU_flags_t) (SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG | aFlags);

  it_lmr_handle_t KeyLMR;
  it_lmr_handle_t ValueLMR;

  Req->Init( NodeId,
             & mConnMgrIF,
             aPDSId,
             SKV_COMMAND_UPDATE,
             SKV_SERVER_EVENT_TYPE_IT_DTO_UPDATE_CMD,
             CmdCtrlBlk,
             aOffset,
             aFlags,
             aKeyBufferSize,
             aKeyBuffer,
             aValueUpdateSize,
             aValueBuffer,
             mPZ_Hdl,
             & KeyLMR,
             & ValueLMR );
  /*****************************************************/

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_UPDATE;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandUpdate.mValueLMR     = ValueLMR;
  /*****************************************************/

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_PENDING );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iUpdate():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  return status;
}


/***
 * skv_client_internal_t::iInsert::
 * Desc: Async interface to inserting a record
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
iInsert( skv_pds_id_t*          aPDSId,
         char*                   aKeyBuffer,
         int                     aKeyBufferSize,
         char*                   aValueBuffer,
         int                     aValueBufferSize,
         int                     aValueBufferOffset,
         skv_cmd_RIU_flags_t    aFlags,
         skv_client_cmd_hdl_t*  aCmdHdl )
{
  gSKVClientiInsertStart.HitOE( SKV_CLIENT_iINSERT_TRACE,
                                 "SKVClientiInsert",
                                 mMyRank,
                                 gSKVClientiInsertStart );

  skv_store_t DebugKey;
  DebugKey.Init( aKeyBuffer, aKeyBufferSize );

  skv_store_t DebugValue;
  DebugValue.Init( aValueBuffer, aValueBufferSize );

  BegLogLine( SKV_CLIENT_INSERT_LOG )
    << "skv_client_internal_t::iInsert():: Entering "
    << " aPDSId@: " << aPDSId
    << " aPDSId: " << *aPDSId
    << " aKeyBufferSize: " << aKeyBufferSize
    << " aValueBufferSize: " << aValueBufferSize
    << " Key: " << DebugKey
    << " Value: " << DebugValue
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iInsert():: "
    << " aFlags: " << aFlags
    << " mState: " << mState
    << EndLogLine;

  /**************************************************
   * Check limits
   *************************************************/
  if( aKeyBufferSize > SKV_KEY_LIMIT )
    return SKV_ERRNO_KEY_TOO_LARGE;

  if( aValueBufferSize > SKV_VALUE_LIMIT )
    return SKV_ERRNO_VALUE_TOO_LARGE;

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  skv_key_t UserKey;
  UserKey.Init( aKeyBuffer, aKeyBufferSize );

  int NodeId = mDistribution.GetNode( &UserKey );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_RIU_req_t* Req = (skv_cmd_RIU_req_t *) SendCtrlMsgBuff;

  const int RoomForData = skv_cmd_RIU_req_t::GetMaxPayloadSize();
  const bool KeyValueFitsInBuff = (aKeyBufferSize+aValueBufferSize <= RoomForData);
  const bool KeyFitsInBuff = (aKeyBufferSize <= RoomForData);

  if( KeyValueFitsInBuff )
    aFlags = (skv_cmd_RIU_flags_t) (SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG | aFlags);
  else if( KeyFitsInBuff )
    aFlags = (skv_cmd_RIU_flags_t) (SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG | aFlags);

  BegLogLine( SKV_CLIENT_INSERT_LOG )
    << "skv_client_internal_t::iInsert():: "
    << " NodeId: " << NodeId
    << " KeyFitsInBuff: " << KeyFitsInBuff
    << " RoomForData: " << RoomForData
    << EndLogLine;

  it_lmr_handle_t KeyLMR;
  it_lmr_handle_t ValueLMR;

  Req->Init( NodeId,
             & mConnMgrIF,
             aPDSId,
             SKV_COMMAND_INSERT,
             SKV_SERVER_EVENT_TYPE_IT_DTO_INSERT_CMD,
             CmdCtrlBlk,
             aValueBufferOffset,
             aFlags,
             aKeyBufferSize,
             aKeyBuffer,
             aValueBufferSize,
             aValueBuffer,
             mPZ_Hdl,
             & KeyLMR,
             & ValueLMR );
  Req->EndianConvert() ;
  /*****************************************************/

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType                                     = SKV_COMMAND_INSERT;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandInsert.mFlags      = aFlags;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandInsert.mValueLMR   = ValueLMR;
  /*****************************************************/

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iInsert():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  BegLogLine( SKV_CLIENT_INSERT_LOG )
    << "skv_client_internal_t::iInsert():: Leaving "
    << EndLogLine;

  gSKVClientiInsertFinis.HitOE( SKV_CLIENT_iINSERT_TRACE,
                                 "SKVClientiInsert",
                                 mMyRank,
                                 gSKVClientiInsertFinis );
  return status;
}

/***
 * skv_client_internal_t::iClose::
 * Desc: Async interface to closing a PDS
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
iClose( skv_pds_id_t*         aPDSId,
        skv_client_cmd_hdl_t* aCmdHdl )
{
  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iClose():: ERROR:: "
    << " mState: " << mState
    << EndLogLine;

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  AssertLogLine( aPDSId != NULL )
    << "skv_client_internal_t::iClose():: ERROR:: "
    << " aPDSId != NULL "
    << EndLogLine;

  // pdsid is stored in network byte order since it's used only at the server
  // however, we need to get the node-id out of it here....
  int NodeId = ntohl( aPDSId->mOwnerNodeId );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  // Set attributes to close the pds (close will update some parameters there)
  skv_pds_attr_t  PDSAttr;
  PDSAttr.mPDSId = *aPDSId;
  PDSAttr.mSize  = 10000;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_pdscntl_req_t* Req = (skv_cmd_pdscntl_req_t *) SendCtrlMsgBuff;
  Req->Init( SKV_COMMAND_CLOSE,
             SKV_SERVER_EVENT_TYPE_IT_DTO_PDSCNTL_CMD,
             CmdCtrlBlk,
             SKV_PDSCNTL_CMD_CLOSE,
             &PDSAttr );
  Req->EndianConvert() ;
  /*****************************************************/

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_CLOSE;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandPDScntl.mCmd = SKV_PDSCNTL_CMD_CLOSE;
  // we don't set further commandbundle options here because close doesn't expect any return beyond status
  /*****************************************************/

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_PENDING );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iClose():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  return status;
}


/***
 * skv_client_internal_t::iClose::
 * Desc: Async interface to closing a PDS
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
iPDScntl( skv_pdscntl_cmd_t     aCmd,
          skv_pds_attr_t       *aPDSAttr,
          skv_client_cmd_hdl_t *aCmdHdl )
  {
  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iPDScntl():: ERROR:: "
    << " mState: " << mState
    << EndLogLine;

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  AssertLogLine( aPDSAttr != NULL )
    << "skv_client_internal_t::iPDScntl():: ERROR:: "
    << " aPDSAttr != NULL "
    << EndLogLine;

  // pdsid is stored in network byte order since it's used only at the server
  // however, we need to get the node-id out of it here....
  int NodeId = ntohl( aPDSAttr->mPDSId.mOwnerNodeId );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  // Set attributes to close the pds (close will update some parameters there)
  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_pdscntl_req_t* Req = (skv_cmd_pdscntl_req_t *) SendCtrlMsgBuff;
  Req->Init( SKV_COMMAND_PDSCNTL,
             SKV_SERVER_EVENT_TYPE_IT_DTO_PDSCNTL_CMD,
             CmdCtrlBlk,
             aCmd,
             aPDSAttr );
  Req->EndianConvert() ;
  /*****************************************************/

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_PDSCNTL;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandPDScntl.mCmd     = aCmd;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandPDScntl.mPDSAttr = aPDSAttr;
  /*****************************************************/

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_PENDING );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iPDScntl():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  return status;
}


/***
 * skv_client_internal_t::TestAny::
 * Desc: Check if any command is done
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
TestAny( skv_client_cmd_hdl_t* aCmdHdl )
{
  return mCommandMgrIF.TestAny( aCmdHdl );
}

/***
 * skv_client_internal_t::Test::
 * Desc: Check if a command is done
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Test( skv_client_cmd_hdl_t aCmdHdl )
{
  return mCommandMgrIF.Test( aCmdHdl );
}

/***
 * skv_client_internal_t::WaitAny::
 * Desc: Wait on any command handle
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
WaitAny( skv_client_cmd_hdl_t* aCmdHdl )
{
  return mCommandMgrIF.WaitAny( aCmdHdl );
}

/***
 * skv_client_internal_t::Wait::
 * Desc: Wait on a command handle
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Wait( skv_client_cmd_hdl_t aCmdHdl )
{
  return mCommandMgrIF.Wait( aCmdHdl );
}

/***
 * skv_client_internal_t::Open::
 * Desc: Create or open a new PDS (partition data set)
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Open( char*                   aPDSName,
      skv_pds_priv_t         aPrivs,
      skv_cmd_open_flags_t   aFlags,
      skv_pds_id_t*          aPDSId )
{
  BegLogLine( SKV_CLIENT_OPEN_LOG )
    << "skv_client_internal_t::Open():: Entering"
    << " aPDSName: " << aPDSName
    << " aPrivs: " << (int) aPrivs
    << " aFlags: " << (int) aFlags
    << EndLogLine;

  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iOpen( aPDSName,
                                aPrivs,
                                aFlags,
                                aPDSId,
                                & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Open:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

#if 0
  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Open:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;
#endif

  BegLogLine( SKV_CLIENT_OPEN_LOG )
    << "skv_client_internal_t::Open():: Leaving"
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::Close::
 * Desc: Close the pds
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Close( skv_pds_id_t* aPDSId )
{
  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iClose( aPDSId,
                                 & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Close:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

  return status;
}

skv_status_t
skv_client_internal_t::
PDScntl( skv_pdscntl_cmd_t  aCmd,
         skv_pds_attr_t    *aPDSAttr )
  {
  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iPDScntl( aCmd,
                                   aPDSAttr,
                                   & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::PDScntl(): "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

  return status;
}

/***
 * skv_client_internal_t::Retrieve::
 * Desc: Retrieve a record from the skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Retrieve( skv_pds_id_t*       aPDSId,
          char*                aKeyBuffer,
          int                  aKeyBufferSize,
          char*                aValueBuffer,
          int                  aValueBufferSize,
          int*                 aValueRetrievedSize,
          int                  aOffset,
          skv_cmd_RIU_flags_t aFlags  )
{
  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iRetrieve( aPDSId,
                                    aKeyBuffer,
                                    aKeyBufferSize,
                                    aValueBuffer,
                                    aValueBufferSize,
                                    aValueRetrievedSize,
                                    aOffset,
                                    aFlags,
                                    & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Retrieve:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

#if 0
  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Retrieve:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;
#endif

  return status;
}

/***
 * skv_client_internal_t::Update::
 * Desc: Update a record on the skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Update( skv_pds_id_t*         aPDSId,
        char*                  aKeyBuffer,
        int                    aKeyBufferSize,
        char*                  aValueBuffer,
        int                    aValueUpdateSize,
        int                    aOffset,
        skv_cmd_RIU_flags_t   aFlags )
{
  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iUpdate( aPDSId,
                                  aKeyBuffer,
                                  aKeyBufferSize,
                                  aValueBuffer,
                                  aValueUpdateSize,
                                  aOffset,
                                  aFlags,
                                  & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Update:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

  return status;
}

/***
 * skv_client_internal_t::Insert::
 * Desc: Insert a record into the skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
Insert( skv_pds_id_t*       aPDSId,
        char*                aKeyBuffer,
        int                  aKeyBufferSize,
        char*                aValueBuffer,
        int                  aValueBufferSize,
        int                  aValueBufferOffset,
        skv_cmd_RIU_flags_t aFlags )
{
  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iInsert( aPDSId,
                                  aKeyBuffer,
                                  aKeyBufferSize,
                                  aValueBuffer,
                                  aValueBufferSize,
                                  aValueBufferOffset,
                                  aFlags,
                                  & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Insert:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

#if 0
  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Insert:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;
#endif

  return status;
}

/***
 * skv_client_internal_t::RetrieveDistribution::
 * Desc: Retrieve the distribution from a random skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
RetrieveDistribution( skv_distribution_t* aDist )
{
  BegLogLine( SKV_CLIENT_RETRIEVE_DIST_LOG )
    << "skv_client_internal_t::RetrieveDistribution():: Entering"
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::RetrieveDistribution():: "
    << " mState: " << mState
    << EndLogLine;

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_retrieve_dist_req_t* Req = (skv_cmd_retrieve_dist_req_t *) SendCtrlMsgBuff;
  Req->Init( SKV_COMMAND_RETRIEVE_DIST,
             SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_DIST_CMD,
             CmdCtrlBlk );
  /*****************************************************/

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType = SKV_COMMAND_RETRIEVE_DIST;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRetrieveDist.mDist = & mDistribution;
  /*****************************************************/

  /******************************************************
   * Send request to a random server node
   *****************************************************/
  int ServerConnCount = mConnMgrIF.GetServerConnCount();

  srand( mMyRank );
  int RandomNode = (int) (ServerConnCount * (rand() / (RAND_MAX + 1.0)));

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_PENDING );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( RandomNode, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Open():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;
  /*****************************************************/

  status = Wait( CmdCtrlBlk );

  BegLogLine( SKV_CLIENT_RETRIEVE_DIST_LOG )
    << "skv_client_internal_t::RetrieveDistribution():: Leaving"
    << " mDistribution : " << mDistribution
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::DumpPDS::
 * Desc:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_internal_t::
DumpPDS( skv_pds_id_t aPDSId,
         int           aMaxKeySize,
         int           aMaxValueSize )
{
  // Test the iterator.
  skv_client_cursor_handle_t MyCursor;
  skv_pds_id_t PDSId = aPDSId;
  skv_status_t status = OpenCursor( & PDSId,
                                     & MyCursor );

  if( status == SKV_SUCCESS )
  {
    BegLogLine( 1 )
      << "skv_client_internal_t::DumpPDS():: SKV Client successfully opened: "
      << " Cursor for aPDSId: " << aPDSId
      << " cursor: " << (void *) MyCursor
      << EndLogLine;
  }
  else
  {
    BegLogLine( 1 )
      << "skv_client_internal_t::DumpPDS():: SKV Client FAILED to open cursor for: "
      << " aPDSId: " << aPDSId
      << " status: " << skv_status_to_string( status )
      << EndLogLine;
  }

  int RetrieveKeyBufferSize = aMaxKeySize;
  int RetrievedKeySize = -1;
  char* RetrieveKeyBuffer = (char *) malloc( RetrieveKeyBufferSize );

  StrongAssertLogLine( RetrieveKeyBuffer != NULL )
    << "skv_client_internal_t::DumpPDS():: RetrieveKeyBuffer != NULL "
    << " RetrieveKeyBufferSize: " << RetrieveKeyBufferSize
    << EndLogLine;

  int RetrieveValueBufferSize = aMaxValueSize;
  int RetrievedValueSize = -1;
  char* RetrieveValueBuffer = (char *) malloc( RetrieveValueBufferSize );

  StrongAssertLogLine( RetrieveValueBuffer != NULL )
    << "skv_client_internal_t::DumpPDS():: RetrieveValueBuffer != NULL "
    << " RetrieveValueBufferSize: " << RetrieveValueBufferSize
    << EndLogLine;

  status = GetFirstElement( MyCursor,
                            RetrieveKeyBuffer,
                            & RetrievedKeySize,
                            RetrieveKeyBufferSize,
                            RetrieveValueBuffer,
                            & RetrievedValueSize,
                            RetrieveValueBufferSize,
                            (skv_cursor_flags_t) 0 );

  int RecordCount = 0;
  if( status != SKV_ERRNO_END_OF_RECORDS )
  {
    skv_store_t PrintKey;
    PrintKey.Init( RetrieveKeyBuffer, RetrievedKeySize );

    skv_store_t PrintValue;
    PrintValue.Init( RetrieveValueBuffer, RetrievedValueSize );

    BegLogLine( 1 )
      << "skv_client_internal_t::DumpPDS():: Iterator Results: "
      << " Key: " << PrintKey
      << " Value: " << PrintValue
      << EndLogLine;

    RecordCount = 1;
  }

  while( status != SKV_ERRNO_END_OF_RECORDS )
  {
    RetrievedKeySize = -1;
    RetrievedValueSize = -1;

    status = GetNextElement( MyCursor,
                             RetrieveKeyBuffer,
                             &RetrievedKeySize,
                             RetrieveKeyBufferSize,
                             RetrieveValueBuffer,
                             &RetrievedValueSize,
                             RetrieveValueBufferSize,
                             (skv_cursor_flags_t) 0 );

    if( status != SKV_ERRNO_END_OF_RECORDS )
    {
      skv_store_t PrintKey;
      PrintKey.Init( RetrieveKeyBuffer, RetrievedKeySize );

      skv_store_t PrintValue;
      PrintValue.Init( RetrieveValueBuffer, RetrievedValueSize );

      BegLogLine( 1 )
        << "skv_client_internal_t::DumpPDS():: Iterator Results: "
        << " Key: " << PrintKey
        << " Value: " << PrintValue
        << EndLogLine;

      RecordCount++;
    }
  }

  BegLogLine( 1 )
    << "skv_client_internal_t::DumpPDS():: "
    << "Iterator Results Count: " << RecordCount
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t
skv_client_internal_t::
DumpPersistentImage( char* aPath )
{
  int pathLen = strlen( aPath ) + 1;

  skv_status_t status = C2S_ActiveBroadcast( SKV_ACTIVE_BCAST_DUMP_PERSISTENCE_IMAGE_FUNC_TYPE,
                                              aPath,
                                              pathLen,
                                              (void *) NULL ); // pointer to the cursor control block

  return status;
}

skv_status_t
skv_client_internal_t::
Finalize()
{
  it_status_t istatus = IT_SUCCESS;
  skv_status_t status = SKV_SUCCESS;

  mCCBMgrIF.Finalize();

  status = mConnMgrIF.Finalize();

  mCommandMgrIF.Finalize();

  istatus = it_pz_free( mPZ_Hdl );

  if( istatus != IT_SUCCESS )
    status = SKV_ERRNO_UNSPECIFIED_ERROR;

  istatus = it_ia_free( mIA_Hdl );
  if( istatus != IT_SUCCESS )
    status = SKV_ERRNO_UNSPECIFIED_ERROR;

  return status;
}

/** \brief non-blocking remove of record from kv-store
 * param[in]  aPDSId           PDS identifier
 * param[in]  aKeyBuffer       key
 * param[in]  aKeyBufferSize   size of key
 * param[in]  aFlags           flags/modifiers
 * param[out] aCmdHdl          command handle to keep track of the command status
 *
 * \return returns SKV_SUCCESS or error code
 */
skv_status_t
skv_client_internal_t::
iRemove( skv_pds_id_t*          aPDSId,
         char*                   aKeyBuffer,
         int                     aKeyBufferSize,
         skv_cmd_remove_flags_t aFlags,
         skv_client_cmd_hdl_t*  aCmdHdl )
{
  skv_store_t DebugKey;
  DebugKey.Init( aKeyBuffer, aKeyBufferSize );

  BegLogLine( SKV_CLIENT_REMOVE_LOG )
    << "skv_client_internal_t::iRemove():: Entering "
    << " aPDSId@: " << aPDSId
    << " aPDSId: " << *aPDSId
    << " aKeyBufferSize: " << aKeyBufferSize
    << " Key: " << DebugKey
    << EndLogLine;

  StrongAssertLogLine( mState == SKV_CLIENT_STATE_CONNECTED )
    << "skv_client_internal_t::iRemove():: "
    << " aFlags: " << aFlags
    << " mState: " << mState
    << EndLogLine;

  /**************************************************
   * Check limits
   *************************************************/
  if( aKeyBufferSize > SKV_KEY_LIMIT )
    return SKV_ERRNO_KEY_TOO_LARGE;

  /*****************************************
   * Figure out the owner of this key
   *****************************************/
  skv_key_t UserKey;
  UserKey.Init( aKeyBuffer, aKeyBufferSize );

  int NodeId = mDistribution.GetNode( &UserKey );
  /*****************************************/

  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();
  skv_cmd_remove_req_t* Req = (skv_cmd_remove_req_t *) SendCtrlMsgBuff;

  const int RoomForData = skv_cmd_RIU_req_t::GetMaxPayloadSize();
  const bool KeyFitsInBuff = (aKeyBufferSize <= RoomForData);

  if( KeyFitsInBuff )
    aFlags = (skv_cmd_remove_flags_t) (SKV_COMMAND_REMOVE_KEY_FITS_IN_CTL_MSG | aFlags);

  BegLogLine( SKV_CLIENT_REMOVE_LOG )
    << "skv_client_internal_t::iRemove():: "
    << " NodeId: " << NodeId
    << " KeyFitsInBuff: " << KeyFitsInBuff
    << " RoomForData: " << RoomForData
    << EndLogLine;

  char keybuff[ 128 ];
  bzero( keybuff, 128 );
  memcpy( keybuff, aKeyBuffer, aKeyBufferSize );

  Req->Init( NodeId,
             & mConnMgrIF,
             aPDSId,
             SKV_COMMAND_REMOVE,
             SKV_SERVER_EVENT_TYPE_IT_DTO_REMOVE_CMD,
             CmdCtrlBlk,
             aFlags,
             aKeyBufferSize,
             aKeyBuffer);
  /*****************************************************/
  Req->EndianConvert() ;

  /******************************************************
   * Set the local client state used on response
   *****************************************************/
  CmdCtrlBlk->mCommand.mType                                     = SKV_COMMAND_REMOVE;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandRemove.mFlags      = aFlags;
  /*****************************************************/

  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL );
  /*****************************************************/

  skv_status_t status = mConnMgrIF.Dispatch( NodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iRemove():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  BegLogLine( SKV_CLIENT_REMOVE_LOG )
    << "skv_client_internal_t::iRemove():: Leaving "
    << EndLogLine;

  return status;
}

skv_status_t
skv_client_internal_t::
Remove( skv_pds_id_t*          aPDSId,
        char*                   aKeyBuffer,
        int                     aKeyBufferSize,
        skv_cmd_remove_flags_t aFlags )
{
  skv_client_cmd_hdl_t CmdHdl;

  skv_status_t status = iRemove( aPDSId,
                                  aKeyBuffer,
                                  aKeyBufferSize,
                                  aFlags,
                                  & CmdHdl );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::Remove:: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  if( status != SKV_SUCCESS )
    return status;

  status = Wait( CmdHdl );

  return status;
}
