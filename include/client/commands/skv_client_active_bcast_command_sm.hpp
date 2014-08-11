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

#ifndef __SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_HPP__
#define __SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_HPP__

#ifndef SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG 
#define SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

class skv_client_active_bcast_command_sm
{
  static
  skv_status_t
  Release( skv_client_server_conn_t* aConn, 
           skv_client_ccb_t*         aCCB )
  {
    BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
      << "skv_client_active_bcast_command_sm::Release(): Entering"
      << EndLogLine;

    /**********************************************************************
     * Release resources
     **********************************************************************/
    // Command is completed, release resources 
    int CommandOrd = aCCB->GetCmdOrd();
    aConn->ReleaseCmdOrdinal( CommandOrd );

    aCCB->mCCBMgrIF->AddToDoneCCBQueue( aCCB );

    // Release LMRs
    it_lmr_handle_t lmrHdl = aCCB->mCommand.mCommandBundle.mCommandActiveBcast.mBufferLMRHdl;

    BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
      << "skv_client_active_bcast_command_sm::Release(): About to call it_lmr_free( ): "
      << " lmr: " << (void *) lmrHdl
      << EndLogLine;

    it_status_t status = it_lmr_free( lmrHdl );

    AssertLogLine( status == IT_SUCCESS )
      << "skv_client_active_bcast_command_sm::Release(): ERROR:: it_lmr_free() failed"
      << " lmr: " << (void *) lmrHdl
      << " status: " << status
      << EndLogLine;	

    BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
      << "skv_client_active_bcast_command_sm::Release(): Leaving"
      << EndLogLine;

    return SKV_SUCCESS;
    /**********************************************************************/    
  }

public:

  static
  skv_status_t
  Execute( skv_client_conn_manager_if_t * aConnMgrIF,
           skv_client_server_conn_t*      aConn, 
           skv_client_ccb_t*              aCCB )
  {
    char* RecvBuff = aCCB->GetRecvBuff();
    skv_server_to_client_cmd_hdr_t* Hdr = (skv_server_to_client_cmd_hdr_t *) RecvBuff;

    skv_client_command_state_t State = aCCB->mState;    
    skv_client_event_t  Event = Hdr->mEvent;

    BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
      << " skv_client_active_bcast_command_sm::Execute(): Entering... "
      << " State: " << skv_client_command_state_to_string( State )
      << " Event: " << skv_client_event_to_string( Event )
      << EndLogLine;

    skv_status_t status = SKV_SUCCESS;

    switch( State )
      {
      case SKV_CLIENT_COMMAND_STATE_IDLE:
      case SKV_CLIENT_COMMAND_STATE_DONE:
        {
          StrongAssertLogLine( 0 )
            << "skv_client_active_bcast_command_sm::Execute(): ERROR: Invalid State: "
            << " State: " << skv_client_command_state_to_string( State )
            << EndLogLine;

          break;
        }
      case SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL:
        {
          switch( Event )
            {
            case SKV_CLIENT_EVENT_CMD_COMPLETE:
              {
                skv_cmd_active_bcast_resp_t* Resp = 
                  (skv_cmd_active_bcast_resp_t *) RecvBuff;

                skv_c2s_active_broadcast_func_type_t FuncType = 
                  aCCB->mCommand.mCommandBundle.mCommandActiveBcast.mFuncType;

                int NodeId = 
                  aCCB->mCommand.mCommandBundle.mCommandActiveBcast.mNodeId;

                // Extract the handle to the appropriate index/cursor
                // manager

                switch( FuncType )
                  {
                  case SKV_ACTIVE_BCAST_DUMP_PERSISTENCE_IMAGE_FUNC_TYPE:
                    {
                      break;
                    }
                  default:
                    {
                      StrongAssertLogLine( 0 )
                        << "skv_client_active_bcast_command_sm::Execute(): ERROR: Invalid FuncType: "
                        << " FuncType: " << FuncType
                        << EndLogLine;

                      break;
                    }
                  }

                BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
                  << " skv_client_active_bcast_command_sm::Execute(): "
                  << " NodeId: " << NodeId
                  << " Resp->mServerCursorHandle: " << (void *) Resp->mServerHandle
                  << EndLogLine;                

                aCCB->mStatus = Resp->mStatus;

                BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
                  << "skv_client_active_bcast_command_sm::Execute(): In final action block"
                  << " status: " << skv_status_to_string( aCCB->mStatus )
                  << EndLogLine;

                Release( aConn, aCCB );

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );

                break;
              }
            case SKV_CLIENT_EVENT_ERROR:
              {
                // Server returned an error.
                skv_cmd_err_resp_t* ErrResp = (skv_cmd_err_resp_t *) RecvBuff;

                BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
                  << "skv_client_active_bcast_command_sm::Execute(): ERROR: response from server: "
                  << " status: " << skv_status_to_string( ErrResp->mStatus )
                  << EndLogLine;

                aCCB->mStatus = ErrResp->mStatus;

                // Command is completed, release resources 
                int CommandOrd = aCCB->GetCmdOrd();
                aConn->ReleaseCmdOrdinal( CommandOrd );

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );

                break;
              }
            default:
              {
                StrongAssertLogLine( 0 )
                  << "skv_client_active_bcast_command_sm::Execute(): ERROR: Invalid Event: "
                  << " Event: " << skv_client_event_to_string( Event )
                  << EndLogLine;

                break;
              }
            }

          break;
        }
      default:
        {
          StrongAssertLogLine( 0 )
            << "skv_client_active_bcast_command_sm::Execute(): ERROR: Invalid State: "
            << " State: " << skv_client_command_state_to_string( State )
            << EndLogLine;

          break;
        }
      }

    BegLogLine( SKV_CLIENT_ACTIVE_BCAST_COMMAND_SM_LOG )
      << "skv_client_active_bcast_command_sm::Execute(): Leaving... "
      << EndLogLine;

    return status;	
  }
};


#endif

