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

#ifndef __SKV_SERVER_RETRIEVE_DIST_COMMAND_SM_HPP__
#define __SKV_SERVER_RETRIEVE_DIST_COMMAND_SM_HPP__

#ifndef SKV_SERVER_RETRIEVE_DIST_LOG
#define SKV_SERVER_RETRIEVE_DIST_LOG ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_retrieve_dist_command_sm
{
private:
  static inline
  skv_status_t create_multi_stage( skv_server_ep_state_t *aEPState,
                                   skv_local_kv_t *aLocalKV,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_RETRIEVE_DIST_LOG )
      << "skv_server_retrieve_dist_command_sm::create_multi_stage()::create_multi_stage"
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }

  static inline
  skv_status_t dist_post_response( skv_server_ep_state_t *aEPState,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal,
                                   int *aSeqNo,
                                   skv_status_t rc,
                                   skv_distribution_t *aDist )
  {
    // Get the distribution
    skv_cmd_retrieve_dist_resp_t* RetrieveDistResp = (skv_cmd_retrieve_dist_resp_t *) aCommand->GetSendBuff();

    // RetrieveDistResp->mHdr          = RetrieveDistReq->mHdr;
    RetrieveDistResp->mHdr.mEvent = SKV_CLIENT_EVENT_CMD_COMPLETE;
    RetrieveDistResp->mStatus     = SKV_SUCCESS;

    memcpy( (char *) & RetrieveDistResp->mDist,
            (char *) aDist,
            sizeof( skv_distribution_t ) );

    skv_status_t status = aEPState->Dispatch( aCommand,
                                              aSeqNo,
                                              aCommandOrdinal );

    BegLogLine( SKV_SERVER_RETRIEVE_DIST_LOG )
      << "skv_server_retrieve_dist_command_sm::dist_post_response():: "
      << " Sending to client: " << *RetrieveDistResp
      << EndLogLine;

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_retrieve_dist_command_sm::Execute():: ERROR: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    return status;
  }

public:
  static
  skv_status_t
  Execute( skv_local_kv_t *aLocalKV,
           skv_server_ep_state_t *aEPState,
           int aCommandOrdinal,
           skv_server_event_t *aEvent,
           int *aSeqNo )
  {
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->mState;

    // skv_server_event_type_t EventType = aEvent->mEventType;

    skv_server_event_type_t EventType = aEvent->mCmdEventType;

    BegLogLine( SKV_SERVER_RETRIEVE_DIST_LOG )
      << "skv_server_retrieve_dist_command_sm::Execute:: "
      << " State: " << State
      << " EventType: " << EventType
      << " Command: " << (void *) Command
      << EndLogLine;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_ERROR:
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
          {
            BegLogLine( SKV_SERVER_RETRIEVE_DIST_LOG )
              << "skv_server_retrieve_dist_command_sm::Execute():: returned from async"
              << " PDSId: " << Command->mLocalKVData.mPDSOpen.mPDSId
              << EndLogLine;

            skv_status_t status = dist_post_response( aEPState,
                                                      Command,
                                                      aCommandOrdinal,
                                                      aSeqNo,
                                                      Command->mLocalKVrc,
                                                      Command->mLocalKVData.mDistribution.mDist );
            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_retrieve_dist_command_sm:: Execute():: ERROR: Event not recognized"
              << " CommandState: " << Command->mState
              << " EventType: " << EventType
              << EndLogLine;

            break;
          }
        }
        break;
      }
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_DIST_CMD:
          {
            AssertLogLine( sizeof(skv_cmd_retrieve_dist_resp_t) <= SKV_CONTROL_MESSAGE_SIZE )
              << "skv_server_retrieve_dist_command_sm::Execute():: ERROR: "
              << " sizeof( skv_cmd_retrieve_dist_resp_t ): " << sizeof( skv_cmd_retrieve_dist_resp_t )
              << " SKV_CONTROL_MESSAGE_SIZE: " << SKV_CONTROL_MESSAGE_SIZE
              << EndLogLine;

            skv_distribution_t *dist;
            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );
            skv_status_t status = aLocalKV->GetDistribution( &dist, cookie );

            if( status == SKV_ERRNO_LOCAL_KV_EVENT )
            {
              create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
              Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP );
            }
            else
            {
              dist_post_response( aEPState,
                                  Command,
                                  aCommandOrdinal,
                                  aSeqNo,
                                  status,
                                  dist );
              Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            }
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_retrieve_dist_command_sm:: Execute():: ERROR: Event not recognized"
              << " CommandState: " << Command->mState
              << " EventType: " << EventType
              << EndLogLine;

            break;
          }
        }

        break;
      }
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_retrieve_dist_command_sm:: Execute():: ERROR: State not recognized"
          << " CommandState: " << Command->mState
          << EndLogLine;

        break;
      }
    }

    return SKV_SUCCESS;
  }
};

#endif
