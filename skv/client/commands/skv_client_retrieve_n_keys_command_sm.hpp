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

#ifndef __SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_HPP__
#define __SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_HPP__

#ifndef SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_LOG 
#define SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

class skv_client_retrieve_n_keys_command_sm
{
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

    BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
      << "skv_client_retrieve_n_keys_command_sm::Execute:: Entering... "
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
            << "skv_client_retrieve_n_keys_command_sm::Execute:: ERROR:: Invalid State: "
            << " State: " << skv_client_command_state_to_string( State )
            << EndLogLine;

          break;
        }
      case SKV_CLIENT_COMMAND_STATE_WAITING_FOR_VALUE_TX_ACK:
        {
          switch( Event )
            {
            case SKV_CLIENT_EVENT_RDMA_WRITE_VALUE_ACK: 	      
              {
                // Operation has completed.
                skv_cmd_retrieve_n_keys_rdma_write_ack_t* Ack = 
                  (skv_cmd_retrieve_n_keys_rdma_write_ack_t *) RecvBuff;

                // Set the sizes of the retrieved keys
                int RetrievedCachedKeysCount  = Ack->mCachedKeysCount;

                AssertLogLine( RetrievedCachedKeysCount >= 0 && 
                               RetrievedCachedKeysCount <= SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE )
                  << "skv_client_retrieve_n_keys_command_sm::Execute:: ERROR:: Response from server: "
                  << " RetrievedCachedKeysCount: " << RetrievedCachedKeysCount
                  << EndLogLine;

                *(aCCB->mCommand.mCommandBundle.mCommandRetrieveNKeys.mCachedKeysCountPtr) = RetrievedCachedKeysCount;

                aCCB->mStatus = Ack->mStatus;

                // Command is completed, release resources 
                int CommandOrd = aCCB->GetCmdOrd();
                aConn->ReleaseCmdOrdinal( CommandOrd );

                aCCB->mCCBMgrIF->AddToDoneCCBQueue( aCCB );

                BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
                  << "skv_client_retrieve_n_keys_command_sm::Execute:: In final action block"
                  << " status: " << skv_status_to_string( aCCB->mStatus )
                  << " RetrievedCachedKeysCount: " <<  RetrievedCachedKeysCount
                  << EndLogLine;

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );

                break;
              }
            case SKV_CLIENT_EVENT_ERROR:
              {
                // Server returned an error.
                skv_cmd_err_resp_t* ErrResp = (skv_cmd_err_resp_t *) RecvBuff;

                BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
                  << "skv_client_retrieve_n_keys_command_sm::Execute:: ERROR response from server: "
                  << " status: " << skv_status_to_string( ErrResp->mStatus )
                  << EndLogLine;

                aCCB->mStatus = ErrResp->mStatus;

                // Command is completed, release resources 
                int CommandOrd = aCCB->GetCmdOrd();
                aConn->ReleaseCmdOrdinal( CommandOrd );

                aCCB->mCCBMgrIF->AddToDoneCCBQueue( aCCB );

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );

                break;
              }
            default:
              {
                StrongAssertLogLine( 0 )
                  << "skv_client_retrieve_n_keys_command_sm::Execute:: ERROR:: Invalid State: "
                  << " State: " << skv_client_command_state_to_string( State )
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
            << "skv_client_retrieve_n_keys_command_sm::Execute:: ERROR:: Invalid State: "
            << " State: " << skv_client_command_state_to_string( State )
            << EndLogLine;

          break;
        }
      }

    BegLogLine( SKV_CLIENT_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
      << "skv_client_retrieve_n_keys_command_sm::Execute:: Leaving... "
      << EndLogLine;

    return status;
  }
};

#endif
