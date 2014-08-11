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

#ifndef __SKV_SERVER_REMOVE_COMMAND_SM_HPP__
#define __SKV_SERVER_REMOVE_COMMAND_SM_HPP__

#ifndef SKV_SERVER_REMOVE_COMMAND_LOG 
#define SKV_SERVER_REMOVE_COMMAND_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_REMOVE_TRACE
#define SKV_SERVER_REMOVE_TRACE ( 1 )
#endif

class skv_server_remove_command_sm
{
private:
  static inline
  skv_status_t create_multi_stage( skv_server_ep_state_t *aEPState,
                                   skv_local_kv_t *aLocalKV,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
      << "skv_server_remove_command_sm:: "
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    // \todo check if we need locking when going multi-stage! (locking during remove isn't considered yet)
    // To be ready, we have the aLocalKV as a function arguments

    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }

  static inline
  skv_status_t post_response( skv_status_t aRC,
                              skv_server_ep_state_t *aEPState,
                              skv_cmd_remove_cmpl_t *aCmpl,
                              skv_server_ccb_t *aCommand,
                              int aCommandOrdinal,
                              int *aSeqNo )
  {
    skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;

    if( aRC == SKV_SUCCESS )
      aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_CMD_COMPLETE;
    else
      aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_ERROR;

    aCmpl->mStatus     = aRC;

    BegLogLine( SKV_SERVER_REMOVE_COMMAND_LOG )
      << "skv_server_remove_command_sm::Execute()::"
      << " completing remove with status: " << skv_status_to_string( aRC )
      << EndLogLine;

    status = aEPState->Dispatch( aCommand,
                                 aSeqNo,
                                 aCommandOrdinal );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_remove_command_sm::Execute():: ERROR: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    return status;
  }

public:
  static
  skv_status_t
  Execute( skv_server_internal_event_manager_if_t* aEventQueueManager,
           skv_local_kv_t *aLocalKV,
           skv_server_ep_state_t* aEPState,
           int aCommandOrdinal,
           skv_server_event_t* aEvent,
           int* aSeqNo,
           int aMyRank )
  {
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->mState;

    skv_server_event_type_t EventType = aEvent->mCmdEventType;

    BegLogLine( SKV_SERVER_REMOVE_COMMAND_LOG )
      << "skv_server_remove_command_sm::Execute():: Entering "
      << " Command: " << (void *) Command
      << " State: " << skv_server_command_state_to_string( State )
      << " Event: " << skv_server_event_type_to_string( EventType )
      << " Ord: " << aCommandOrdinal
      << EndLogLine;


    skv_status_t status = SKV_SUCCESS;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        gSKVServerRemoveEnter.HitOE( SKV_SERVER_REMOVE_TRACE,
                                     "SKVServerRemoveEnter",
                                     aMyRank,
                                     gSKVServerRemoveEnter );
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_REMOVE_CMD:
          {
            skv_cmd_remove_req_t *Req = (skv_cmd_remove_req_t *) Command->GetSendBuff();

            int KeySize   = Req->mKeyValue.mKeySize;

            BegLogLine( SKV_SERVER_REMOVE_COMMAND_LOG )
              << "skv_server_remove_command_sm::Execute():: request fetched "
              << " KeySize: " << KeySize
              << " Flags: " << Req->mFlags
              << EndLogLine;

            // Check if the key is in the buffer
            if( Req->mFlags & SKV_COMMAND_REMOVE_KEY_FITS_IN_CTL_MSG )
            {
              /*****************************************
               * Remove and deallocate the old from the local store
               *****************************************/
              skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
              cookie->Set( aCommandOrdinal, aEPState );
              status = aLocalKV->Remove( Req->mPDSId,
                                         Req->mKeyValue.mData,
                                         KeySize,
                                         cookie );
            }
            else
              status = SKV_ERRNO_KEY_TOO_LARGE;

            switch( status )
            {
              case SKV_ERRNO_RECORD_IS_LOCKED:
                BegLogLine( SKV_SERVER_LOCK_LOG )
                  << "skv_server_remove_command_sm::Execute()::"
                  << " record is locked"
                  << EndLogLine;

                aEventQueueManager->Enqueue( aEvent );
                break;

              case SKV_ERRNO_LOCAL_KV_EVENT:
                status = create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
                Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP );
                break;

              case SKV_SUCCESS:
              default:
                status = post_response( status, aEPState, (skv_cmd_remove_cmpl_t*)Req, Command, aCommandOrdinal, aSeqNo );
                Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
                break;
            }

            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_remove_command_sm:: Execute():: ERROR: State not recognized"
              << " State: " << State
              << " EventType: " << EventType
              << EndLogLine;

            break;
          }
        }

        gSKVServerRemoveLeave.HitOE( SKV_SERVER_REMOVE_TRACE,
                                     "SKVServerRemoveLeave",
                                     aMyRank,
                                     gSKVServerRemove );

        break;
      }
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
          {
            status = Command->mLocalKVrc;
            status = post_response( status, aEPState,
                                    (skv_cmd_remove_cmpl_t*)Command->GetSendBuff(),
                                    Command, aCommandOrdinal, aSeqNo );
            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
            StrongAssertLogLine( 0 )
              << "skv_server_insert_command_sm::Execute(): ERROR:: EventType not recognized. "
              << " EventType: " << EventType
              << EndLogLine;
        }
        break;
      }
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_remove_command_sm:: Execute():: ERROR: State not recognized"
          << " State: " << State
          << EndLogLine;

        break;
      }
    }

    BegLogLine( SKV_SERVER_REMOVE_COMMAND_LOG )
      << "skv_server_remove_command_sm::Execute():: Leaving"
      << EndLogLine;

    return status;
  }
};
#endif // __SKV_SERVER_REMOVE_COMMAND_SM_HPP__
