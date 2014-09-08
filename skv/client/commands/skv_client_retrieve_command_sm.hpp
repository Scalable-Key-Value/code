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

#ifndef __SKV_CLIENT_RETRIEVE_COMMAND_SM_HPP__
#define __SKV_CLIENT_RETRIEVE_COMMAND_SM_HPP__

#ifndef SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG 
#define SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

class skv_client_retrieve_command_sm
{
public:

  static
  skv_status_t
  Release( skv_client_server_conn_t* aConn, 
           skv_client_ccb_t*         aCCB )
  {
    /**********************************************************************
     * Release resources
     **********************************************************************/
    // Command is completed, release resources 
    int CommandOrd = aCCB->GetCmdOrd();
    aConn->ReleaseCmdOrdinal( CommandOrd );

    aCCB->mCCBMgrIF->AddToDoneCCBQueue( aCCB );

    skv_cmd_RIU_flags_t Flags = aCCB->mCommand.mCommandBundle.mCommandRetrieve.mFlags;

    if( !( Flags & SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG ) )
      {
        // Release LMRs
        it_lmr_handle_t lmrHdl = aCCB->mCommand.mCommandBundle.mCommandRetrieve.mValueBufferRef.mValueLMR;

        it_status_t status = it_lmr_free( lmrHdl );

        AssertLogLine( status == IT_SUCCESS )
          << "skv_client_release_command_sm::Release():: ERROR:: it_lmr_free() failed"
          << " lmr: " << (void *) lmrHdl
          << " status: " << status
          << EndLogLine;	
      }

    return SKV_SUCCESS;
    /**********************************************************************/    
  }

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

    BegLogLine( SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG )
      << "skv_client_retrieve_command_sm::Execute:: Entering... "
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
            << "skv_client_retrieve_command_sm::Execute:: ERROR:: Invalid State: "
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
                skv_cmd_retrieve_value_rdma_write_ack_t* Ack = 
                  (skv_cmd_retrieve_value_rdma_write_ack_t *) RecvBuff;

                int retrievedSize = Ack->mValue.mValueSize;

                // if the server indicates that there's more data, then only get the amount that fits into user buffer!
                if( Ack->mStatus == SKV_ERRNO_VALUE_TOO_LARGE )
                  {
                  retrievedSize = aCCB->mCommand.mCommandBundle.mCommandRetrieve.mValueRequestedSize;
                  // Ack->mStatus = SKV_SUCCESS;
                  }

                // get value data out of response if flags indicate that it fit
                if( aCCB->mCommand.mCommandBundle.mCommandRetrieve.mFlags & SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG )
                  {
                    void* valueBuffer = aCCB->mCommand.mCommandBundle.mCommandRetrieve.mValueBufferRef.mValueAddr;

                    BegLogLine( SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG )
                      << "skv_client_retrieve_command_sm::Execute(): about to copy retrieved data"
                      << " uBuf: " << (void*)valueBuffer
                      << " rBuf: " << (void*)Ack->mValue.mData
                      << " size: " << retrievedSize
                      << " CCB: " << (void*)aCCB
                      << " value: " << (void*)(*(uint64_t*)(Ack->mValue.mData))
#ifdef SKV_DEBUG_MSG_MARKER
                      << " msg: " << Ack->mHdr.mMarker
#endif
                      << EndLogLine;

                    memcpy( valueBuffer,
                            Ack->mValue.mData, 
                            retrievedSize );
                  }

                aCCB->mStatus = Ack->mStatus;

                // user wanted to know the actual retrieved size
                if( aCCB->mCommand.mCommandBundle.mCommandRetrieve.mValueRetrievedSize != NULL )
                  *aCCB->mCommand.mCommandBundle.mCommandRetrieve.mValueRetrievedSize = Ack->mValue.mValueSize;

                status = Release( aConn, aCCB );

                AssertLogLine( status == SKV_SUCCESS )
                  << "skv_client_retrieve_command_sm::Execute():: ERROR:: Release failed: "
                  << " status: " << status
                  << EndLogLine;

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );

                break;
              }
            case SKV_CLIENT_EVENT_ERROR:
              {
                // Server returned an error.
                skv_cmd_err_resp_t* ErrResp = (skv_cmd_err_resp_t *) RecvBuff;

                BegLogLine( SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG )
                  << "skv_client_retrieve_command_sm::Execute:: ERROR response from server: "
                  << " status: " << ErrResp->mStatus
                  << EndLogLine;

                aCCB->mStatus = ErrResp->mStatus;

                status = Release( aConn, aCCB );

                AssertLogLine( status == SKV_SUCCESS )
                  << "skv_client_retrieve_command_sm::Execute():: ERROR:: Release failed: "
                  << " status: " << status
                  << EndLogLine;

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );

                break;
              }
            default:
              {
                StrongAssertLogLine( 0 )
                  << "skv_client_retrieve_command_sm::Execute:: ERROR:: Invalid State: "
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
            << "skv_client_retrieve_command_sm::ProcessCCB:: ERROR:: Invalid State: "
            << " State: " << skv_client_command_state_to_string( State )
            << EndLogLine;

          break;
        }
      }

    BegLogLine( SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG )
      << "skv_client_retrieve_command_sm::Execute:: Leaving... "
      << EndLogLine;

    return status;
  }
};
#endif
