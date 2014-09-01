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

#ifndef __SKV_SERVER_PDSCNTL_COMMAND_SM_HPP__
#define __SKV_SERVER_PDSCNTL_COMMAND_SM_HPP__

#include <common/skv_types.hpp>

#ifndef SKV_SERVER_PDSCNTL_COMMAND_SM_LOG 
#define SKV_SERVER_PDSCNTL_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

class skv_server_pdscntl_command_sm
{
private:
  static inline
  skv_status_t create_multi_stage( skv_server_ep_state_t *aEPState,
                                   skv_local_kv_t *aLocalKV,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_PDSCNTL_COMMAND_SM_LOG)
      << "skv_server_pdscntl_command_sm::create_multi_stage()::create_multi_stage"
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }

  static inline
  void init_response( skv_cmd_pdscntl_resp_t *aResp,
                      skv_pdscntl_cmd_t aCntlCmd,
                      skv_pds_attr_t *aPDSAttr,
                      skv_status_t aRC )
  {
    aResp->mHdr.mEvent     = SKV_CLIENT_EVENT_CMD_COMPLETE;

    if( aCntlCmd != SKV_PDSCNTL_CMD_CLOSE )
    {
      aResp->mPDSAttr.mPDSId = aPDSAttr->mPDSId;
      aResp->mPDSAttr.mSize  = aPDSAttr->mSize;
      aResp->mPDSAttr.mPrivs = aPDSAttr->mPrivs;
      strncpy(aResp->mPDSAttr.mPDSName, aPDSAttr->mPDSName, SKV_MAX_PDS_NAME_SIZE);
    }
    aResp->mStatus = aRC;

    BegLogLine( SKV_SERVER_PDSCNTL_COMMAND_SM_LOG )
      << "skv_server_pdscntl_command_sm::init_response():: "
      << " after cmd: " << aCntlCmd
      << " PDSAttr: "   << aPDSAttr
      << " Resp.Attr: " << aResp->mPDSAttr
      << " status: "    << skv_status_to_string( aRC )
      << EndLogLine;
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
    skv_status_t status;
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->GetState();

    // skv_server_event_type_t EventType = aEvent->mEventType;
    skv_server_event_type_t EventType = aEvent->mCmdEventType;    

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP:
      {
        BegLogLine( SKV_SERVER_PDSCNTL_COMMAND_SM_LOG )
          << "skv_server_pdscntl_command_sm::Execute():: returned from async"
          << " PDSId: " << Command->mLocalKVData.mPDSStat.mPDSAttr.mPDSId
          << EndLogLine;

        init_response( (skv_cmd_pdscntl_resp_t *)( Command->GetSendBuff() ),
                       Command->mLocalKVData.mPDSStat.mCntlCmd,
                       &(Command->mLocalKVData.mPDSStat.mPDSAttr),
                       Command->mLocalKVrc );

        status = aEPState->Dispatch( Command,
                                     aSeqNo,
                                     aCommandOrdinal );

        Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );

        AssertLogLine( status == SKV_SUCCESS )
          << "skv_server_pdscntl_command_sm:: Execute():: ERROR: "
          << " status: "    << skv_status_to_string( status )
          << EndLogLine;

        break;
      }
    case SKV_SERVER_COMMAND_STATE_INIT:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_PDSCNTL_CMD:
          {
            // we have copied all Req data into response buffer already at cmd init
            skv_cmd_pdscntl_req_t* StatReq = (skv_cmd_pdscntl_req_t *) Command->GetSendBuff();

            skv_pdscntl_cmd_t cntl_cmd = StatReq->mCntlCmd;
            skv_pds_attr_t *PDSAttr    = &(StatReq->mPDSAttr);

            skv_pds_id_t PDSId = PDSAttr->mPDSId;

            BegLogLine( SKV_SERVER_PDSCNTL_COMMAND_SM_LOG )
              << "skv_server_pdscntl_command_sm::Execute():: "
              << " cmd: "  << cntl_cmd
              << " attr: " << *PDSAttr
              << " ord: " << aCommandOrdinal
              << EndLogLine;

            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );
            switch( cntl_cmd )
            {
              case SKV_PDSCNTL_CMD_STAT_GET:
              case SKV_PDSCNTL_CMD_STAT_SET:
              {
                status = aLocalKV->PDS_Stat( cntl_cmd, PDSAttr, cookie );
                break;
              }
                    
              case SKV_PDSCNTL_CMD_CLOSE:
              {
                status = aLocalKV->PDS_Close( PDSAttr, cookie );
                break;
              }

              default:
                AssertLogLine( 0 )
                  << "skv_server_pdscntl_command_sm::Execute():: "
                  << " unrecognized PDS-CNTL command: " << cntl_cmd
                  << EndLogLine;
            }
                    
            if( status == SKV_ERRNO_LOCAL_KV_EVENT )
            {
              create_multi_stage( aEPState,
                                  aLocalKV,
                                  Command,
                                  aCommandOrdinal );
              Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP );
            }
            else
            {
              init_response( (skv_cmd_pdscntl_resp_t*)StatReq,
                             cntl_cmd,
                             PDSAttr,
                             status );

              status = aEPState->Dispatch( Command,
                                           aSeqNo,
                                           aCommandOrdinal );

              AssertLogLine( status == SKV_SUCCESS )
                << "skv_server_pdscntl_command_sm:: Execute():: ERROR: "
                << " status: "    << skv_status_to_string( status )
                << EndLogLine;
            }
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_pdscntl_command_sm:: Execute():: ERROR: Event not recognized"
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
          << "skv_server_pdscntl_command_sm:: Execute():: ERROR: State not recognized"
          << " CommandState: " << Command->mState
          << EndLogLine;

        break;
      }
    }

    return SKV_SUCCESS;
  }
};

#endif
