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

#ifndef __SKV_SERVER_RETRIEVE_COMMAND_SM_HPP__
#define __SKV_SERVER_RETRIEVE_COMMAND_SM_HPP__

#ifndef SKV_SERVER_RETRIEVE_COMMAND_SM_LOG 
#define SKV_SERVER_RETRIEVE_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_RETRIEVE_TRACE
#define SKV_SERVER_RETRIEVE_TRACE ( 0 )
#endif

class skv_server_retrieve_command_sm
{
  static inline
  skv_status_t create_multi_stage( skv_server_ep_state_t *aEPState,
                                   skv_local_kv_t *aLocalKV,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
      << "skv_server_retrieve_command_sm:: "
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    // \todo check if we need locking when going multi-stage! (retreive-side locking isn't considered yet)
    // To be ready, we have the aLocalKV as a function arguments

    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }

  static inline
  skv_status_t retrieve_post_rdma( skv_server_ep_state_t *aEPState,
                                   skv_cmd_RIU_req_t *aReq,
                                   skv_lmr_triplet_t *aValueMemRep,
                                   int aCommandOrdinal,
                                   int aMyRank )
  {

    skv_server_rdma_write_cmpl_cookie_t Cookie;
    skv_rmr_triplet_t *RemoteMemRepValue = &(aReq->mRMRTriplet);

    Cookie.Init( aEPState,
                 EPSTATE_RetrieveWriteComplete,
                 aCommandOrdinal,
                 1 );

    it_dto_flags_t dto_flags = (it_dto_flags_t) (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG);
    // it_dto_flags_t dto_flags = (it_dto_flags_t) ( 0 );

    gSKVServerRetrieveRDMAWriteStart.HitOE( SKV_SERVER_RETRIEVE_TRACE,
                                            "SKVServerRetrieveRDMAWrite",
                                            aMyRank,
                                            gSKVServerRetrieveRDMAWriteStart );

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
      << "skv_server_retrieve_command_sm:: Before rdma_write "
      << " LMR triplet { "
      << " len: " << aValueMemRep->GetLen()
      << " addr: " << (void *) aValueMemRep->GetAddr()
      << " lmr: " << (void *) aValueMemRep->GetLMRHandle()
      << " } "
      << " RMR triplet {"
      << " len: " << RemoteMemRepValue->GetLen()
      << " addr: " << (void *) RemoteMemRepValue->GetAddr()
      << " context: " << (void *) RemoteMemRepValue->GetRMRContext()
      << " } "
      << EndLogLine;

    // rdma_write the value
    it_status_t itstatus = it_post_rdma_write( aEPState->mEPHdl,
                                               &(aValueMemRep->GetTriplet()),
                                               1,
                                               Cookie.GetCookie(),
                                               dto_flags,
                                               (it_rdma_addr_t) RemoteMemRepValue->GetAddr(),
                                               RemoteMemRepValue->GetRMRContext() );

    gSKVServerRetrieveRDMAWriteFinis.HitOE( SKV_SERVER_RETRIEVE_TRACE,
                                            "SKVServerRetrieveRDMAWrite",
                                            aMyRank,
                                            gSKVServerRetrieveRDMAWriteFinis );

    AssertLogLine( itstatus == IT_SUCCESS )
      << "skv_server_retrieve_command_sm::"
      << " cmd: " << aCommandOrdinal
      << " post_rdma_write failed with status: " << itstatus
      << EndLogLine;

    if( itstatus == IT_SUCCESS )
      return SKV_SUCCESS;
    else
      return SKV_ERRNO_IT_POST_RDMA_WRITE_FAILED;
  }

  static inline
  skv_status_t command_completion( skv_status_t aRC,
                                   skv_server_ep_state_t *aEPState,
                                   skv_cmd_retrieve_value_rdma_write_ack_t *aCmpl,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal,
                                   int *aSeqNo )
  {
    AssertLogLine( aCmpl != NULL )
      << "skv_server_retrieve_command_sm:: ERROR: "
      << EndLogLine;

    switch( aRC )
    {
      case SKV_ERRNO_NEED_DATA_TRANSFER:
        aRC = SKV_SUCCESS;
        // no break, because the successful response is: write_value_ack
      case SKV_SUCCESS:
        aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_RDMA_WRITE_VALUE_ACK;
        break;
      default:
        aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_ERROR;
    }

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG)
      << "skv_server_retrieve_command_sm::Execute()::"
      << " completing retrieve with status: " << skv_status_to_string( aRC )
      << EndLogLine;

    aCmpl->mStatus = aRC;

    skv_status_t status = aEPState->Dispatch( aCommand,
                                              aSeqNo,
                                              aCommandOrdinal );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_retrieve_command_sm:: ERROR: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    return status;
  }

  static inline
  skv_status_t retrieve_sequence( skv_server_ep_state_t *aEPState,
                                  skv_local_kv_t *aLocalKV,
                                  skv_server_ccb_t *aCommand,
                                  int aCommandOrdinal,
                                  skv_cmd_RIU_req_t *aReq,
                                  int aMyRank,
                                  skv_lmr_triplet_t *aValueMemRep,
                                  int *aTotalSize )
  {
    skv_status_t status;
    skv_rmr_triplet_t *RemoteMemRepValue = &(aReq->mRMRTriplet);

    skv_key_value_in_ctrl_msg_t *KeyValue = &(aReq->mKeyValue);
    char*  aKeyData = KeyValue->mData;
    int    aKeySize = KeyValue->mKeySize;
    int    aValueSize = KeyValue->mValueSize;

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
      << "skv_server_retrieve_command_sm:: "
      << " aRemoteMemRepValue.GetLen(): " << RemoteMemRepValue->GetLen()
      << " aValueSize: " << aValueSize
      << EndLogLine;

    gSKVServerInsertRetrieveFromTreeStart.HitOE( SKV_SERVER_RETRIEVE_TRACE,
                                                 "SKVServerInsertRetrieveFromTree",
                                                 aMyRank,
                                                 gSKVServerInsertRetrieveFromTreeStart );

    // Check if the key exists
    skv_local_kv_cookie_t *cookie = &aCommand->mLocalKVCookie;
    cookie->Set( aCommandOrdinal, aEPState );
    status = aLocalKV->Retrieve( aReq->mPDSId,
                                 aKeyData,
                                 aKeySize,
                                 aReq->mOffset,
                                 aValueSize,
                                 aReq->mFlags,
                                 aValueMemRep,
                                 aTotalSize,
                                 cookie );

    gSKVServerInsertRetrieveFromTreeFinis.HitOE( SKV_SERVER_RETRIEVE_TRACE,
                                                 "SKVServerInsertRetrieveFromTree",
                                                 aMyRank,
                                                 gSKVServerInsertRetrieveFromTreeFinis );

    if( status == SKV_SUCCESS )
      BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
        << "skv_server_retrieve_command_sm:: "
        << " local retrieve status: " << skv_status_to_string( status )
        << " aKeySize: " << aKeySize
        << " aRemoteMemRepValue.GetLen(): " << RemoteMemRepValue->GetLen()
        << " aLocalMemRepValue.GetLen(): " << aValueMemRep->GetLen()
        << " aValueSize: " << aValueSize
        << EndLogLine;

    return status;
  }

public:
  static
  skv_status_t
  Execute( skv_local_kv_t *aLocalKV,
           skv_server_ep_state_t* aEPState,
           int aCommandOrdinal,
           skv_server_event_t* aEvent,
           int* aSeqNo,
           it_pz_handle_t aPZHdl,
           int aMyRank )
  {
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    StrongAssertLogLine( Command != NULL )
      << "skv_server_retrieve_command_sm:: ERROR: "
      << " aCommandOrdinal: " << aCommandOrdinal
      << " aEPState: " << (void *) aEPState
      << EndLogLine;

    skv_server_command_state_t State = Command->mState;
    skv_server_event_type_t EventType = aEvent->mCmdEventType;
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
      << "skv_server_retrieve_command_sm:: Entering"
      << " State: " << skv_server_command_state_to_string( State )
      << " Event: " << skv_server_event_type_to_string( EventType )
      << EndLogLine;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        gSKVServerRetrieveEnter.HitOE( SKV_SERVER_RETRIEVE_TRACE,
                                       "SKVServerRetrieveEnter",
                                       aMyRank,
                                       gSKVServerRetrieveEnter );
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_CMD:
          {
            skv_cmd_RIU_req_t* Req = (skv_cmd_RIU_req_t *) Command->GetSendBuff();
            skv_cmd_retrieve_value_rdma_write_ack_t* Resp = (skv_cmd_retrieve_value_rdma_write_ack_t*) Req;
            skv_lmr_triplet_t ValueMemRep;
            int TotalSize = 0;

            if( Req->mFlags & (SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG|SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG) )
            {
              status = retrieve_sequence( aEPState,
                                          aLocalKV,
                                          Command,
                                          aCommandOrdinal,
                                          Req,
                                          aMyRank,
                                          &ValueMemRep,
                                          &TotalSize );
            }
            else
            {
              status = SKV_ERRNO_KEY_TOO_LARGE;
            }

            switch( status )
            {
              case SKV_SUCCESS:
                if( Req->mFlags & (SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG|SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG) )
                {
                  memcpy( Resp->mValue.mData,
                        (const void*) ValueMemRep.GetAddr(),
                        ValueMemRep.GetLen() );
                  Resp->mValue.mValueSize = TotalSize;
                }
                else
                  AssertLogLine( 1 )
                    << "skv_server_retrieve_command_sm:: BUG: incorrect return code: data doesn't fit into Ctrl-msg."
                    << EndLogLine;

                status = command_completion( status,
                                             aEPState,
                                             Resp,
                                             Command,
                                             aCommandOrdinal,
                                             aSeqNo );
                Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
                break;
              case SKV_ERRNO_NEED_DATA_TRANSFER:
                status = create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
                status = retrieve_post_rdma( aEPState,
                                             Req,
                                             &ValueMemRep,
                                             aCommandOrdinal,
                                             aMyRank );

                Resp->mValue.mValueSize = TotalSize;

                Command->Transit( SKV_SERVER_COMMAND_STATE_WAITING_RDMA_WRITE_CMPL );
                break;
              case SKV_ERRNO_LOCAL_KV_EVENT:
                // storage needs background operation to complete; nothing to do, come back via localKV event
                status = create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
                Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP );
                return SKV_SUCCESS;
              default:
                // go and report any other errors
                status = command_completion( status,
                                             aEPState,
                                             Resp,
                                             Command,
                                             aCommandOrdinal,
                                             aSeqNo );
                Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
                break;
            }
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_retrieve_command_sm:: ERROR: State not recognized"
              << " State: " << State
              << " EventType: " << EventType
              << EndLogLine;

            break;
          }
        }

        break;
      }
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
          {
            // check the outcome of the initial retrieve op
            skv_cmd_RIU_req_t *Req = (skv_cmd_RIU_req_t*)Command->GetSendBuff();
            skv_cmd_retrieve_value_rdma_write_ack_t* Resp = (skv_cmd_retrieve_value_rdma_write_ack_t*) Command->GetSendBuff();

            int TotalSize = Command->mLocalKVData.mRDMA.mSize;
            status = Command->mLocalKVrc;

            BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
              << "skv_server_retrieve_command_sm:: async status: " << skv_status_to_string( status )
              << EndLogLine;

            switch( status )
            {
              case SKV_ERRNO_NEED_DATA_TRANSFER:
              {
                status = create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
                skv_lmr_triplet_t *ValueMemRep = &(Command->mLocalKVData.mRDMA.mValueRDMADest);

                status = retrieve_post_rdma( aEPState,
                                             Req,
                                             ValueMemRep,
                                             aCommandOrdinal,
                                             aMyRank );
                Resp->mValue.mValueSize = TotalSize;

                Command->Transit( SKV_SERVER_COMMAND_STATE_WAITING_RDMA_WRITE_CMPL );
                break;
              }
              case SKV_SUCCESS:
              {
                skv_lmr_triplet_t *ValueMemRep = &(Command->mLocalKVData.mRDMA.mValueRDMADest);

                if( Req->mFlags & (SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG|SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG) )
                {
                  memcpy( Resp->mValue.mData,
                          (const void*) ValueMemRep->GetAddr(),
                          ValueMemRep->GetLen() );
                  Resp->mValue.mValueSize = TotalSize;
                }
                else
                  AssertLogLine( 1 )
                    << "skv_server_retrieve_command_sm:: BUG: data doesn't fit into Ctrl-msg with incorrect return code."
                    << EndLogLine;

                status = aLocalKV->RetrievePostProcess( &(Command->mLocalKVData.mRDMA.mReqCtx) );
                if( status == SKV_ERRNO_LOCAL_KV_EVENT )
                  status = SKV_SUCCESS;

                status = command_completion( status,
                                             aEPState,
                                             Resp,
                                             Command,
                                             aCommandOrdinal,
                                             aSeqNo );
                Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
                break;
              }
              default:
                status = command_completion( status,
                                             aEPState,
                                             Resp,
                                             Command,
                                             aCommandOrdinal,
                                             aSeqNo );
                Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            }
            break;
          }
          default:
            StrongAssertLogLine( 0 )
              << "skv_server_retrieve_command_sm:: ERROR: State not recognized"
              << " State: " << State
              << " EventType: " << EventType
              << EndLogLine;
        }
        break;
      }
      case SKV_SERVER_COMMAND_STATE_WAITING_RDMA_WRITE_CMPL:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_WRITE_CMPL:
          {
            status = aLocalKV->RetrievePostProcess( &(Command->mLocalKVData.mRDMA.mReqCtx) );
            status = command_completion( status,
                                         aEPState,
                                         (skv_cmd_retrieve_value_rdma_write_ack_t*)Command->GetSendBuff(),
                                         Command,
                                         aCommandOrdinal,
                                         aSeqNo );

            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_retrieve_command_sm:: ERROR:: EventType not recognized. "
              << " EventType: " << EventType
              << EndLogLine;
          }
        }
        break;
      }
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_retrieve_command_sm:: ERROR: State not recognized"
          << " State: " << State
          << EndLogLine;

        break;
      }
    }

    BegLogLine( SKV_SERVER_RETRIEVE_COMMAND_SM_LOG )
      << "skv_server_retrieve_command_sm:: Leaving"
      << EndLogLine;

    return status;
  }
};
#endif
