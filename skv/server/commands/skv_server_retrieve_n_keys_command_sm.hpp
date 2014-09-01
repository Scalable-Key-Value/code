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

#ifndef __SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_HPP__
#define __SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_HPP__

#ifndef SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_LOG 
#define SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_RETRIEVE_N_KEYS_DATA_LOG
#define SKV_SERVER_RETRIEVE_N_KEYS_DATA_LOG ( 0 )
#endif

class skv_server_retrieve_n_keys_command_sm
{
  static inline
  skv_status_t create_multi_stage( skv_server_ep_state_t *aEPState,
                                   skv_local_kv_t *aLocalKV,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
      << "skv_server_retrieve_n_keys_command_sm:: "
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
  skv_status_t retrieve_n_start( skv_server_ep_state_t *aEPState,
                                 skv_local_kv_t *aLocalKV,
                                 skv_server_ccb_t *aCommand,
                                 int aCommandOrdinal,
                                 skv_cmd_retrieve_n_keys_req_t* aReq,
                                 int *aRetrievedKeysCount,
                                 skv_lmr_triplet_t *aRetrievedKeysSizesSegs,
                                 int *aRetrievedKeysSizesSegsCount )
  {
    // Check if the key is in the buffer
    if( ! aReq->mIsKeyInCtrlMsg )
      return SKV_ERRNO_KEY_TOO_LARGE;

    AssertLogLine( SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE == aReq->mKeysDataListMaxCount )
      << "skv_server_retrieve_n_keys_command_sm:: ERROR: "
      << " aRemoteMemKeysMaxCount: " << aReq->mKeysDataListMaxCount
      << " SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE: " << SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE
      << EndLogLine;

    // Check if the key exists
    skv_local_kv_cookie_t *cookie = &aCommand->mLocalKVCookie;
    cookie->Set( aCommandOrdinal, aEPState );
    skv_status_t status = aLocalKV->RetrieveNKeys( aReq->mPDSId,
                                                   aReq->mStartingKeyData,
                                                   aReq->mStartingKeySize,
                                                   aRetrievedKeysSizesSegs,
                                                   aRetrievedKeysCount,
                                                   aRetrievedKeysSizesSegsCount,
                                                   aReq->mKeysDataListMaxCount,
                                                   aReq->mFlags,
                                                   cookie );

    BegLogLine( SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
      << "skv_server_retrieve_n_keys_command_sm:: Results of local call to RetrieveNKeys: "
      << " RetrievedKeysCount: " << *aRetrievedKeysCount
      << " RetrievedKeysSizesSegsCount: " << aRetrievedKeysSizesSegsCount
      << " aRemoteMemKeysMaxCount: " << aReq->mKeysDataListMaxCount
      << " first Key: " << aRetrievedKeysSizesSegs[0].GetAddr()
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    return status;
  }

  static inline
  skv_status_t post_rdma_write( skv_server_ep_state_t *aEPState,
                                skv_lmr_triplet_t *aRetrievedKeysSizesSegs,
                                int aRetrievedKeysSizesSegsCount,
                                skv_cmd_retrieve_n_keys_req_t* aReq )
  {
    it_status_t itstatus = IT_SUCCESS;

    skv_server_rdma_write_cmpl_cookie_t Cookie;
    Cookie.Init( NULL, NULL );

    //        it_dto_flags_t dto_flags = (it_dto_flags_t) ( IT_COMPLETION_FLAG | IT_NOTIFY_FLAG );
    it_dto_flags_t dto_flags = (it_dto_flags_t) ( 0 );

    BegLogLine( SKV_SERVER_RETRIEVE_N_KEYS_DATA_LOG )
      << "skv_server_retrieve_n_keys_command_sm::post_rdma_write(): LMRs: "
      << " " << aRetrievedKeysSizesSegs[0]
      << " " << aRetrievedKeysSizesSegs[1]
      << " " << aRetrievedKeysSizesSegs[2]
      << " " << aRetrievedKeysSizesSegs[3]
      << EndLogLine;

    BegLogLine( SKV_SERVER_RETRIEVE_N_KEYS_DATA_LOG )
      << "skv_server_retrieve_n_keys_command_sm::post_rdma_write(): first keysize: " << *(int*)(aRetrievedKeysSizesSegs[0].GetAddr())
      << " writeTo: " << aReq->mKeysDataList
      << " KeyData: " << HexDump( (void*)aRetrievedKeysSizesSegs[0].GetAddr(), aRetrievedKeysSizesSegs[0].GetLen() )
      << " " << HexDump( (void*)aRetrievedKeysSizesSegs[1].GetAddr(), aRetrievedKeysSizesSegs[1].GetLen() )
      << " " << HexDump( (void*)aRetrievedKeysSizesSegs[2].GetAddr(), aRetrievedKeysSizesSegs[2].GetLen() )
      << " " << HexDump( (void*)aRetrievedKeysSizesSegs[3].GetAddr(), aRetrievedKeysSizesSegs[3].GetLen() )
      << EndLogLine;

    // rdma_write the value
    itstatus = it_post_rdma_write( aEPState->mEPHdl,
                                   (it_lmr_triplet_t *) aRetrievedKeysSizesSegs,
                                   aRetrievedKeysSizesSegsCount,
                                   Cookie.GetCookie(),
                                   dto_flags,
                                   (it_rdma_addr_t) aReq->mKeysDataList,
                                   aReq->mKeyDataCacheMemReg.mKeysDataCacheRMR );

    AssertLogLine( itstatus == IT_SUCCESS )
      << "skv_server_retrieve_n_keys_command_sm:: ERROR: "
      << " itstatus: " << itstatus
      << EndLogLine;

    return SKV_SUCCESS;
  }

  static inline
  skv_status_t command_completion( skv_status_t aRC,
                                   skv_server_ep_state_t *aEPState,
                                   skv_server_ccb_t *aCommand,
                                   int aRetrievedKeysCount,
                                   skv_lmr_triplet_t *aRetrievedKeysSizesSegs,
                                   skv_cmd_retrieve_n_keys_rdma_write_ack_t *aCmpl,
                                   int aCommandOrdinal,
                                   int *aSeqNo )
  {
    skv_status_t status;

    if( (aRC == SKV_SUCCESS) || (aRC == SKV_ERRNO_END_OF_RECORDS) )
    {
      aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_RDMA_WRITE_VALUE_ACK;
      aCmpl->mCachedKeysCount = aRetrievedKeysCount;
    }
    else
    {
      aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_ERROR;
      aCmpl->mCachedKeysCount = 0;
    }
    aCmpl->mStatus          = aRC;

    BegLogLine( SKV_SERVER_RETRIEVE_N_KEYS_COMMAND_SM_LOG )
      << "skv_server_retrieve_n_keys_command_sm:: "
      << " About to Dispatch(): "
      << " status: " << skv_status_to_string( aCmpl->mStatus )
      << " mCachedKeysCount: " << aCmpl->mCachedKeysCount
      << EndLogLine;

    status = aEPState->Dispatch( aCommand,
                                 aSeqNo,
                                 aCommandOrdinal );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_retrieve_n_keys_command_sm:: ERROR: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    delete aRetrievedKeysSizesSegs;

    return status;
  }


public:
  static
  skv_status_t
  Execute( skv_local_kv_t *aLocalKV,
           skv_server_ep_state_t *aEPState,
           int aCommandOrdinal,
           skv_server_event_t *aEvent,
           int *aSeqNo,
           it_pz_handle_t aPZHdl )
  {
    skv_status_t status = SKV_SUCCESS;
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );
    skv_server_command_state_t State = Command->mState;
    skv_server_event_type_t EventType = aEvent->mCmdEventType;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_N_KEYS_CMD:
          {
            int RetrievedKeysCount = 0;
            int RetrievedKeysSizesSegsCount = 0;
            // NEED NEED NEED:: This will live on the stack for now. Think about how to allocate this memory based
            // on aRemoteMemKeysMaxCount. Need a better place to put this.
            //
#define SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE_SEND_VEC ( 2 * SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE )
//            skv_lmr_triplet_t RetrievedKeysSizesSegs[ SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE_SEND_VEC ];
            skv_lmr_triplet_t *RetrievedKeysSizesSegs = (skv_lmr_triplet_t*)new char( SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE_SEND_VEC * sizeof(skv_lmr_triplet_t) );

            status = retrieve_n_start( aEPState,
                                       aLocalKV,
                                       Command,
                                       aCommandOrdinal,
                                       (skv_cmd_retrieve_n_keys_req_t*) Command->GetSendBuff(),
                                       & RetrievedKeysCount,
                                       RetrievedKeysSizesSegs,
                                       & RetrievedKeysSizesSegsCount );

            switch( status )
            {
              case SKV_ERRNO_END_OF_RECORDS:
                // skip the rdma_write only if there was no key retrieved, otherwise rdma the remaining keys
                if( RetrievedKeysSizesSegsCount == 0 )
                  break;
              case SKV_SUCCESS:
              {
                post_rdma_write( aEPState,
                                 RetrievedKeysSizesSegs,
                                 RetrievedKeysSizesSegsCount,
                                 (skv_cmd_retrieve_n_keys_req_t*) Command->GetSendBuff()
                                 );
                break;
              }
              case SKV_ERRNO_LOCAL_KV_EVENT:
                create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal );
                Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP );
                return SKV_SUCCESS;
              default:

                break;
            }

            status = command_completion( status,
                                         aEPState,
                                         Command,
                                         RetrievedKeysCount,
                                         RetrievedKeysSizesSegs,
                                         (skv_cmd_retrieve_n_keys_rdma_write_ack_t*)Command->GetSendBuff(),
                                         aCommandOrdinal,
                                         aSeqNo );
            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_retrieve_n_keys_command_sm:: Execute():: ERROR: State not recognized"
              << " State: " << State
              << " EventType: " << EventType
              << EndLogLine;

            break;
          }
        }
        break;
      }
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
          {
            status = Command->mLocalKVrc;
            switch( status )
            {
              case SKV_ERRNO_END_OF_RECORDS:
                // skip the rdma_write only if there was no key retrieved
                if( Command->mLocalKVData.mRetrieveNKeys.mKeysCount == 0 )
                  break;
                // no break on purpose: EOR might be signaled even if there were a few keys available?
              case SKV_SUCCESS:
                post_rdma_write( aEPState,
                                 Command->mLocalKVData.mRetrieveNKeys.mKeysSizesSegs,
                                 Command->mLocalKVData.mRetrieveNKeys.mKeysSizesSegsCount,
                                 (skv_cmd_retrieve_n_keys_req_t*) Command->GetSendBuff() );
                break;
            }
            status = command_completion( status,
                                         aEPState,
                                         Command,
                                         Command->mLocalKVData.mRetrieveNKeys.mKeysCount,
                                         Command->mLocalKVData.mRetrieveNKeys.mKeysSizesSegs,
                                         (skv_cmd_retrieve_n_keys_rdma_write_ack_t*)Command->GetSendBuff(),
                                         aCommandOrdinal,
                                         aSeqNo );
            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
            status = SKV_ERRNO_STATE_MACHINE_ERROR;
        }
        break;
      }
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_retrieve_n_keys_command_sm:: Execute():: ERROR: State not recognized"
          << " State: " << State
          << EndLogLine;

        break;
      }
    }

    return status;
  }
};
#endif
