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
#include <client/skv_c2s_active_broadcast.hpp>
#include <common/skv_utils.hpp>

#ifndef SKV_CLIENT_C2S_ACTIVE_BCAST_LOG
#define SKV_CLIENT_C2S_ACTIVE_BCAST_LOG ( 0 | SKV_LOGGING_ALL )
#endif

/***
 * skv_client_internal_t::iSendActiveBcastReq::
 * Desc: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
iSendActiveBcastReq( int                                   aNodeId,
                     skv_c2s_active_broadcast_func_type_t aFuncType,
                     char*                                 aBuff,
                     int                                   aBuffSize,
                     void*                                 aIncommingDataMgrIF,
                     it_pz_handle_t                        aPZHdl,
                     skv_client_cmd_hdl_t*                aCmdHdl )
{  
  // Starting a new command, get a command control block
  skv_client_ccb_t* CmdCtrlBlk;
  skv_status_t rsrv_status = mCommandMgrIF.Reserve( & CmdCtrlBlk );
  if( rsrv_status != SKV_SUCCESS )
    return rsrv_status;

  /******************************************************
   * Set the client-server protocol send ctrl msg buffer
   *****************************************************/
  char* SendCtrlMsgBuff = CmdCtrlBlk->GetSendBuff();  

  skv_cmd_active_bcast_req_t* Req = (skv_cmd_active_bcast_req_t *) SendCtrlMsgBuff;

  it_lmr_handle_t lmrHandle;

  Req->Init( aNodeId,
             & mConnMgrIF,
             SKV_COMMAND_ACTIVE_BCAST,
             SKV_SERVER_EVENT_TYPE_ACTIVE_BCAST_CMD,
             CmdCtrlBlk,
             aFuncType,
             aBuffSize,
             aBuff,
             aPZHdl,
             & lmrHandle );

  /******************************************************
   * Set the local client state used on response
   *****************************************************/  
  CmdCtrlBlk->mCommand.mType                                                  = SKV_COMMAND_ACTIVE_BCAST;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandActiveBcast.mFuncType           = aFuncType;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandActiveBcast.mNodeId             = aNodeId;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandActiveBcast.mIncommingDataMgrIF = aIncommingDataMgrIF;
  CmdCtrlBlk->mCommand.mCommandBundle.mCommandActiveBcast.mBufferLMRHdl       = lmrHandle;
  /*****************************************************/



  /******************************************************
   * Transit the CCB to an appropriate state
   *****************************************************/  
  CmdCtrlBlk->Transit( SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL );
  /*****************************************************/  

  skv_status_t status = mConnMgrIF.Dispatch( aNodeId, CmdCtrlBlk );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_client_internal_t::iSendActiveBcastReq():: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  *aCmdHdl = CmdCtrlBlk;

  BegLogLine( SKV_CLIENT_C2S_ACTIVE_BCAST_LOG )
    << "skv_client_internal_t::iSendActiveBcastReq():: Leaving "
    << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::C2S_ActiveBroadcast::
 * Desc: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_internal_t::
C2S_ActiveBroadcast( skv_c2s_active_broadcast_func_type_t aFuncType,
                     char*                                 aBuff,
                     int                                   aBuffSize,
                     void*                                 aIncommingDataMgrIF )
{
  BegLogLine( SKV_CLIENT_C2S_ACTIVE_BCAST_LOG )
    << "skv_client_internal_t::C2S_ActiveBroadcast():: Entering "
    << " aFuncType: " << aFuncType
    << " aBuff: " << (void *) aBuff
    << " aBuffSize: " << aBuffSize
    << EndLogLine;

  int ServerConnCount = mConnMgrIF.GetServerConnCount();

  int                    BcastHdlsSize = sizeof(skv_client_cmd_hdl_t) * ServerConnCount;
  skv_client_cmd_hdl_t* BcastHdls     = (skv_client_cmd_hdl_t *) malloc( BcastHdlsSize );

  StrongAssertLogLine( BcastHdls )
    << "skv_client_internal_t::C2S_ActiveBroadcast():: Error:: Not enough memory for "
    << " BcastHdlsSize: " << BcastHdlsSize
    << EndLogLine;

  for( int i = 0; i < ServerConnCount; i++ )
  {
    skv_status_t istatus = iSendActiveBcastReq( i,
                                                aFuncType,
                                                aBuff,
                                                aBuffSize,
                                                aIncommingDataMgrIF,
                                                mPZ_Hdl,
                                                &BcastHdls[i] );
    AssertLogLine( istatus == SKV_SUCCESS )
      << "skv_client_internal_t:: ERROR: "
      << " istatus: " << skv_status_to_string( istatus )
      << EndLogLine;
  }

  int CompletedNodes = 0;
  while( CompletedNodes < ServerConnCount )
  {
    skv_status_t wstatus = mCommandMgrIF.Wait( BcastHdls[CompletedNodes] );

    AssertLogLine( wstatus == SKV_SUCCESS )
      << "skv_client_internal_t::C2S_ActiveBroadcast():: ERROR: "
      << " wstatus: " << skv_status_to_string( wstatus )
      << EndLogLine;

      CompletedNodes++;
  }

  if( BcastHdls != NULL )
  {
    free( BcastHdls );
    BcastHdls = NULL;
  }

  BegLogLine( SKV_CLIENT_C2S_ACTIVE_BCAST_LOG )
    << "skv_client_internal_t::C2S_ActiveBroadcast():: Leaving..."
    << EndLogLine;

  return SKV_SUCCESS;
}
