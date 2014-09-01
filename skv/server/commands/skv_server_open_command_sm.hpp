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

#ifndef __SKV_SERVER_OPEN_COMMAND_SM_HPP__
#define __SKV_SERVER_OPEN_COMMAND_SM_HPP__

#include <common/skv_types.hpp>

#ifndef SKV_SERVER_OPEN_COMMAND_SM_LOG 
#define SKV_SERVER_OPEN_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_open_command_sm
{
private:
  static inline
  skv_status_t open_create_multi_stage( skv_server_ep_state_t *aEPState,
                                        skv_local_kv_t *aLocalKV,
                                        skv_server_ccb_t *aCommand,
                                        int aCommandOrdinal )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_OPEN_COMMAND_SM_LOG )
      << "skv_server_open_command_sm::create_multi_stage()::open_create_multi_stage"
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }

  static inline
  skv_status_t open_post_response( skv_server_ep_state_t *aEPState,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal,
                                   int *aSeqNo,
                                   skv_status_t rc,
                                   skv_pds_id_t PDSId )
  {
    skv_cmd_open_resp_t* OpenResp = (skv_cmd_open_resp_t *) aCommand->GetSendBuff();

    // OpenResp->mHdr            = OpenReq->mHdr;
    OpenResp->mHdr.mEvent     = SKV_CLIENT_EVENT_CMD_COMPLETE;
    OpenResp->mStatus = rc;
    OpenResp->mPDSId = PDSId;

    skv_status_t status = aEPState->Dispatch( aCommand,
                                              aSeqNo,
                                              aCommandOrdinal );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_open_command_sm::open_post_response():: ERROR: "
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
    skv_status_t status = SKV_ERRNO_STATE_MACHINE_ERROR;
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->GetState();

    // skv_server_event_type_t EventType = aEvent->mEventType;
    skv_server_event_type_t EventType = aEvent->mCmdEventType;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP:
      {
        BegLogLine( SKV_SERVER_OPEN_COMMAND_SM_LOG )
          << "skv_server_open_command_sm::Execute():: returned from async"
          << " PDSId: " << Command->mLocalKVData.mPDSOpen.mPDSId
          << EndLogLine;

        status = open_post_response( aEPState,
                                     Command,
                                     aCommandOrdinal,
                                     aSeqNo,
                                     Command->mLocalKVrc,
                                     Command->mLocalKVData.mPDSOpen.mPDSId );
        Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
        break;
      }
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_OPEN_CMD:
          {
            // we have copied all Req data into response buffer already at cmd init
            skv_cmd_open_req_t* OpenReq = (skv_cmd_open_req_t *) Command->GetSendBuff();

            char*                  PDSName = OpenReq->mPDSName;
            skv_pds_priv_t         Privs   = OpenReq->mPrivs;
            skv_cmd_open_flags_t   Flags   = OpenReq->mFlags;

            skv_pds_id_t PDSId;

            BegLogLine( SKV_SERVER_OPEN_COMMAND_SM_LOG )
              << "skv_server_open_command_sm::Execute():: "
              << " PDSName: " << PDSName
              << " Privs: " << (int) Privs
              << " Flags: " << (int) Flags
              << EndLogLine;

            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );
            status = aLocalKV->PDS_Open( PDSName,
                                         Privs,
                                         Flags,
                                         &PDSId,
                                         cookie );

            BegLogLine( SKV_SERVER_OPEN_COMMAND_SM_LOG )
              << "skv_server_open_command_sm::Execute():: "
              << " PDSId: " << PDSId
              << " status: " << skv_status_to_string( status )
              << EndLogLine;

            if( status == SKV_ERRNO_LOCAL_KV_EVENT )
            {
              status = open_create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
              Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP );
            }
            else
            {
              status = open_post_response( aEPState,
                                           Command,
                                           aCommandOrdinal,
                                           aSeqNo,
                                           status,
                                           PDSId );
            }
            break;
          }
          default:
            {
            StrongAssertLogLine( 0 )
              << "skv_open_command_sm:: Execute():: ERROR: Event not recognized"
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
          << "skv_open_command_sm:: Execute():: ERROR: State not recognized"
          << " CommandState: " << Command->mState
          << EndLogLine;

        break;
      }
    }

    return status;
  }
};

#endif
