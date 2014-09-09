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

#ifndef __SKV_SERVER_INSERT_COMMAND_SM_HPP__
#define __SKV_SERVER_INSERT_COMMAND_SM_HPP__

#ifndef SKV_SERVER_INSERT_LOG 
#define SKV_SERVER_INSERT_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_INSERT_TRACE
#define SKV_SERVER_INSERT_TRACE ( 0 )
#endif

#ifndef SKV_SERVER_LOCK_LOG
#define SKV_SERVER_LOCK_LOG ( 0 )
#endif

class skv_server_insert_command_sm
{
private:
  static inline
  skv_status_t insert_create_multi_stage( skv_server_ep_state_t *aEPState,
                                          skv_local_kv_t *aLocalKV,
                                          skv_server_ccb_t *aCommand,
                                          int aCommandOrdinal,
                                          skv_cmd_RIU_req_t *aReq )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm::Execute()::insert_create_multi_stage"
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    /*******************************************************************
     * Command requires at least one data transfer (rdma read)
     ******************************************************************/
    skv_rec_lock_handle_t RecLock;
    if( aReq->mFlags & SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS )
    {
      status = aLocalKV->Lock( &(aReq->mPDSId),
                               &(aReq->mKeyValue),
                               &RecLock );
    }
    /******************************************************************/

    /*******************************************************************
     * Save local command state
     ******************************************************************/
    aCommand->mCommandState.mCommandInsert.mHdr        = aReq->mHdr;
    aCommand->mCommandState.mCommandInsert.mFlags      = aReq->mFlags;
    aCommand->mCommandState.mCommandInsert.mRecLockHdl = RecLock;
    /******************************************************************/
    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }


  static inline
  skv_status_t insert_lookup_sequence( skv_local_kv_t *aLocalKV,
                                       skv_server_ccb_t *aCommand,
                                       skv_cmd_RIU_req_t **aReq,
                                       skv_local_kv_cookie_t *aCookie,
                                       skv_lmr_triplet_t *aValueRep )
  {
    // we have copied all Req data into response buffer already at cmd init
    skv_cmd_RIU_req_t *Req = (skv_cmd_RIU_req_t *) aCommand->GetSendBuff();
    *aReq = Req;

    // Check if the key is in the buffer
    int KeySize = Req->mKeyValue.mKeySize;

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm:: Lookup with flags: " << (void*)( Req->mFlags & (SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE
          | SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP
          | SKV_COMMAND_RIU_UPDATE
          | SKV_COMMAND_RIU_APPEND) )
      << EndLogLine;

    AssertLogLine( Req->mKeyValue.mKeySize >= 0 &&
                   Req->mKeyValue.mKeySize < SKV_KEY_LIMIT )
      << "skv_server_insert_command_sm:: Execute():: ERROR: "
      << "Req->mKeyValue.mKeySize: " << Req->mKeyValue.mKeySize
      << EndLogLine;

    AssertLogLine( (Req->mFlags & SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG) ||
                   (Req->mFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG) )
      << "skv_server_insert_command_sm:: Execute():: ERROR: "
      << " Assume that key fits into the control message"
      << EndLogLine;

    // Check if the key exists
    return aLocalKV->Lookup( Req->mPDSId,
                             Req->mKeyValue.mData,
                             KeySize,
                             Req->mFlags,
                             aValueRep,
                             aCookie );
  }


  static inline
  void insert_post_rdma( skv_server_ep_state_t *aEPState,
                         skv_local_kv_t *aLocalKV,
                         int aCommandOrdinal,
                         skv_cmd_RIU_req_t *aReq,
                         skv_lmr_triplet_t *aValueRepForRdmaRead,
                         int* aSeqNo,
                         int aMyRank )
  {
    skv_server_cookie_t Cookie;
    Cookie.Init( aEPState,
                 *aSeqNo,
                 aCommandOrdinal );

    it_dto_cookie_t* DtoCookie = (it_dto_cookie_t*) &Cookie;

    it_dto_flags_t dto_flags = (it_dto_flags_t) (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG);

    gSKVServerInsertRDMAReadStart.HitOE( SKV_SERVER_INSERT_TRACE,
                                         "SKVServerInsertRdmaRead",
                                         aMyRank,
                                         gSKVServerInsertRDMAReadStart );

    aLocalKV->RDMABoundsCheck( "Insert1",
                               (char *) aValueRepForRdmaRead->GetAddr(),
                               aValueRepForRdmaRead->GetLen() );

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm:: Execute():: Before rdma_read "
      << " LMR triplet { "
      << " len: " << aValueRepForRdmaRead->GetLen()
      << " addr: " << (void *) aValueRepForRdmaRead->GetAddr()
      << " lmr: " << (void *) aValueRepForRdmaRead->GetLMRHandle()
      << " } "
      << " RMR info "
      << " rmr addr: " << (void *) aReq->mRMRTriplet.GetAddr()
      << " rmr context: " << (void *) aReq->mRMRTriplet.GetRMRContext()
      << EndLogLine;

    // rdma_read the value
    it_status_t itstatus = it_post_rdma_read( aEPState->mEPHdl,
                                              aValueRepForRdmaRead->GetTripletPtr(),
                                              1,
                                              *DtoCookie,
                                              dto_flags,
                                              (it_rdma_addr_t) aReq->mRMRTriplet.GetAddr(),
                                              aReq->mRMRTriplet.GetRMRContext() );

    gSKVServerInsertRDMAReadFinis.HitOE( SKV_SERVER_INSERT_TRACE,
                                         "SKVServerInsertRdmaRead",
                                         aMyRank,
                                         gSKVServerInsertRDMAReadFinis );

    AssertLogLine( itstatus == IT_SUCCESS )
      << "skv_server_insert_command_sm::Execute():: ERROR: "
      << " istatus: " << itstatus
      << EndLogLine;
  }

  static inline
  skv_status_t insert_command_completion( skv_status_t aRC,
                                          skv_server_ep_state_t *aEPState,
                                          skv_cmd_RIU_req_t *aReq,
                                          skv_server_ccb_t *aCommand,
                                          int aCommandOrdinal,
                                          int *aSeqNo )
  {
    skv_cmd_insert_cmpl_t *Cmpl;
    Cmpl = (skv_cmd_insert_cmpl_t *) aReq;

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm::Execute()::"
      << " completing insert with status: " << skv_status_to_string( aRC )
      << EndLogLine;

    AssertLogLine( Cmpl != NULL )
      << "skv_server_insert_command_sm::Execute():: ERROR: "
      << EndLogLine;

    if( aRC != SKV_SUCCESS )
      Cmpl->mHdr.mEvent = SKV_CLIENT_EVENT_ERROR;
    else
      Cmpl->mHdr.mEvent = SKV_CLIENT_EVENT_CMD_COMPLETE;

    Cmpl->mStatus = aRC;

    skv_status_t status = aEPState->Dispatch( aCommand,
                                              aSeqNo,
                                              aCommandOrdinal );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_insert_command_sm::Execute():: ERROR: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    return status;
  }

  static inline
  skv_status_t insert_sequence( skv_local_kv_t *aLocalKV,
                                skv_server_ep_state_t *aEPState,
                                skv_server_ccb_t *aCommand,
                                int aCommandOrdinal,
                                skv_cmd_RIU_req_t *aReq,
                                skv_status_t lookup_status,
                                int *aSeqNo,
                                int aMyRank,
                                skv_lmr_triplet_t *aValueRepInStore )
  {
    skv_status_t status;
    skv_lmr_triplet_t ValueRepForRdmaRead;

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm::Execute():: Lookup returned: " << skv_status_to_string( lookup_status )
      << " will go for case: " << (void*)( aReq->mFlags & (SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE
          | SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP
          | SKV_COMMAND_RIU_UPDATE
          | SKV_COMMAND_RIU_APPEND) )
      << EndLogLine;

    skv_local_kv_cookie_t *cookie = &aCommand->mLocalKVCookie;
    cookie->Set( aCommandOrdinal, aEPState );
    status = aLocalKV->Insert( aReq,
                               lookup_status,
                               aValueRepInStore,
                               &ValueRepForRdmaRead,
                               cookie );
    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm::insert_sequence():: Insert returned: " << skv_status_to_string( status )
      << EndLogLine;

    switch ( status )
    {
      case SKV_SUCCESS:
        /*******************************************************************
         * Command complete, ready to dispatch response to client
         ******************************************************************/
        status = insert_command_completion( status, aEPState, aReq, aCommand, aCommandOrdinal, aSeqNo );
        aCommand->Transit( SKV_SERVER_COMMAND_STATE_INIT );
        return status;

      case SKV_ERRNO_NEED_DATA_TRANSFER:
        /*******************************************************************
         * Issue an rdma read from the client
         ******************************************************************/
        status = insert_create_multi_stage( aEPState, aLocalKV, aCommand, aCommandOrdinal, aReq );
        insert_post_rdma( aEPState,
                          aLocalKV,
                          aCommandOrdinal,
                          aReq,
                          &ValueRepForRdmaRead,
                          aSeqNo,
                          aMyRank );
        aCommand->Transit( SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL );
        break;

      case SKV_ERRNO_LOCAL_KV_EVENT:
        // insert requires multiple stages including going through async storage steps
        status = insert_create_multi_stage( aEPState, aLocalKV, aCommand, aCommandOrdinal, aReq );
        aCommand->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP );
        break;

      case SKV_ERRNO_RECORD_ALREADY_EXISTS:
        // if record exists, we don't need to crash-exit, just return error to client
        status = insert_command_completion( status, aEPState, aReq, aCommand, aCommandOrdinal, aSeqNo );
        aCommand->Transit( SKV_SERVER_COMMAND_STATE_INIT );
        break;

      case SKV_ERRNO_COMMAND_LIMIT_REACHED:
        AssertLogLine( 1 )
          << "skv_server_insert_command_sm::insert_sequence():: Back-end ran out of command slots."
          << EndLogLine;
        break;

      default:
        BegLogLine( SKV_SERVER_INSERT_LOG )
          << "skv_server_insert_command_sm::Execute()::ERROR in local insert"
          << " status: " << skv_status_to_string( status )
          << EndLogLine;

        status = insert_command_completion( status, aEPState, aReq, aCommand, aCommandOrdinal, aSeqNo );
        aCommand->Transit( SKV_SERVER_COMMAND_STATE_INIT );
    }
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

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm::Execute():: Entering "
      << " Command: " << (void *) Command
      << " State: " << skv_server_command_state_to_string( State )
      << " Event: " << skv_server_event_type_to_string( EventType )
      << " Hdr: " << (void*) Command->GetSendBuff() 
      << " Ord: " << aCommandOrdinal
      << EndLogLine;

    skv_status_t rc_status = SKV_SUCCESS;
    skv_status_t status;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        gSKVServerInsertEnter.HitOE( SKV_SERVER_INSERT_TRACE,
                                     "SKVServerInsertEnter",
                                     aMyRank,
                                     gSKVServerInsertEnter );

        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_INSERT_CMD:
          {
            BegLogLine( SKV_SERVER_INSERT_LOG )
              << "skv_server_insert_command_sm::Execute():: Entering action block for "
              << " State: " << skv_server_command_state_to_string( State )
              << " Event: " << skv_server_event_type_to_string( EventType )
              << " Ord: " << aCommandOrdinal
              << EndLogLine;

            skv_lmr_triplet_t ValueRepInStore;
            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );
            skv_cmd_RIU_req_t *Req;
            status = insert_lookup_sequence( aLocalKV,
                                             Command,
                                             &Req,
                                             cookie,
                                             &ValueRepInStore);
            switch( status )
            {
              case SKV_ERRNO_RECORD_IS_LOCKED:
                // Put the event back in the User Event Queue
                // ... Spin Waiting ...

                BegLogLine( SKV_SERVER_LOCK_LOG )
                  << "skv_server_insert_command_sm::Execute()::"
                  << " record is locked"
                  << EndLogLine;

                aEventQueueManager->Enqueue( aEvent );
                return SKV_SUCCESS;

              case SKV_ERRNO_COMMAND_LIMIT_REACHED:
                aEventQueueManager->Enqueue( aEvent );
                return SKV_SUCCESS;

              case SKV_ERRNO_LOCAL_KV_EVENT:
                status = insert_create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal, Req );
                Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP );
                return SKV_SUCCESS;

              default:
                break;
            }

            status = insert_sequence( aLocalKV,
                                      aEPState,
                                      Command,
                                      aCommandOrdinal,
                                      Req,
                                      status,
                                      aSeqNo,
                                      aMyRank,
                                      &ValueRepInStore );
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_insert_command_sm:: Execute():: ERROR: State not recognized"
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
            BegLogLine( SKV_SERVER_INSERT_LOG )
              << "skv_server_insert_command_sm::Execute():: Entering action block for "
              << " State: " << skv_server_command_state_to_string( State )
              << " Event: " << skv_server_event_type_to_string( EventType )
              << " Ord: " << aCommandOrdinal
              << EndLogLine;

            skv_cmd_RIU_req_t *Req = (skv_cmd_RIU_req_t *) Command->GetSendBuff();

            status = insert_sequence( aLocalKV,
                                      aEPState,
                                      Command,
                                      aCommandOrdinal,
                                      Req,
                                      Command->mLocalKVrc,
                                      aSeqNo,
                                      aMyRank,
                                      &Command->mLocalKVData.mLookup.mValueRepInStore );
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
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
            BegLogLine( SKV_SERVER_INSERT_LOG )
              << "skv_server_insert_command_sm::Execute():: Entering action block for "
              << " State: " << skv_server_command_state_to_string( State )
              << " Event: " << skv_server_event_type_to_string( EventType )
              << " Ord: " << aCommandOrdinal
              << EndLogLine;

            if( Command->mLocalKVrc == SKV_ERRNO_NEED_DATA_TRANSFER )
            {
              insert_post_rdma( aEPState,
                                aLocalKV,
                                aCommandOrdinal,
                                (skv_cmd_RIU_req_t*)Command->GetSendBuff(),
                                &Command->mLocalKVData.mRDMA.mValueRDMADest,
                                aSeqNo,
                                aMyRank );
              Command->Transit( SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL );
              return SKV_SUCCESS;
            }

            gSKVServerInsertSendingRDMAReadAck.HitOE( SKV_SERVER_INSERT_TRACE,
                                                      "SKVServerInsertRdmaRead",
                                                      aMyRank,
                                                      gSKVServerInsertSendingRDMAReadAck );

            insert_command_completion( Command->mLocalKVrc,
                                       aEPState,
                                       (skv_cmd_RIU_req_t *) Command->GetSendBuff(),
                                       Command,
                                       aCommandOrdinal,
                                       aSeqNo );
            /*******************************************************************
             * Handle Locking
             ******************************************************************/
            if( Command->mCommandState.mCommandInsert.mFlags & SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS )
            {
              aLocalKV->Unlock( Command->mCommandState.mCommandInsert.mRecLockHdl );
            }
            /******************************************************************/

            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          default:
            StrongAssertLogLine( 0 )
              << "skv_server_insert_command_sm::Execute(): ERROR:: EventType not recognized. "
              << " EventType: " << EventType
              << EndLogLine;
        }
        break;
      }
      case SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_READ_CMPL:
          {
            BegLogLine( SKV_SERVER_INSERT_LOG )
              << "skv_server_insert_command_sm::Execute():: Entering action block for "
              << " State: " << skv_server_command_state_to_string( State )
              << " Event: " << skv_server_event_type_to_string( EventType )
              << " Ord: " << aCommandOrdinal
              << EndLogLine;

            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );
            status = aLocalKV->InsertPostProcess( &(Command->mLocalKVData.mRDMA.mReqCtx ),
                                                  &(Command->mLocalKVData.mRDMA.mValueRDMADest),
                                                  cookie );

            if( status == SKV_ERRNO_LOCAL_KV_EVENT )
            {
              Command->Transit(SKV_SERVER_COMMAND_STATE_LOCAL_KV_READY);
              return SKV_SUCCESS;
            }

            gSKVServerInsertSendingRDMAReadAck.HitOE( SKV_SERVER_INSERT_TRACE,
                                                      "SKVServerInsertRdmaRead",
                                                      aMyRank,
                                                      gSKVServerInsertSendingRDMAReadAck );

            insert_command_completion( status,
                                       aEPState,
                                       (skv_cmd_RIU_req_t *) Command->GetSendBuff(),
                                       Command,
                                       aCommandOrdinal,
                                       aSeqNo );
            /*******************************************************************
             * Handle Locking
             ******************************************************************/
            if( Command->mCommandState.mCommandInsert.mFlags & SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS )
            {
              aLocalKV->Unlock( Command->mCommandState.mCommandInsert.mRecLockHdl );
            }
            /******************************************************************/

            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );

            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_insert_command_sm::Execute(): ERROR:: EventType not recognized. "
              << " EventType: " << EventType
              << EndLogLine;
          }
        }
        break;
      }
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_READY:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
            BegLogLine( SKV_SERVER_INSERT_LOG )
              << "skv_server_insert_command_sm::Execute():: Entering action block for "
              << " State: " << skv_server_command_state_to_string( State )
              << " Event: " << skv_server_event_type_to_string( EventType )
              << " Ord: " << aCommandOrdinal
              << EndLogLine;

            gSKVServerInsertSendingRDMAReadAck.HitOE( SKV_SERVER_INSERT_TRACE,
                                                      "SKVServerInsertRdmaRead",
                                                      aMyRank,
                                                      gSKVServerInsertSendingRDMAReadAck );

            insert_command_completion( Command->mLocalKVrc,
                                       aEPState,
                                       (skv_cmd_RIU_req_t *) Command->GetSendBuff(),
                                       Command,
                                       aCommandOrdinal,
                                       aSeqNo );
            /*******************************************************************
             * Handle Locking
             ******************************************************************/
            if( Command->mCommandState.mCommandInsert.mFlags & SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS )
            {
              aLocalKV->Unlock( Command->mCommandState.mCommandInsert.mRecLockHdl );
            }
            /******************************************************************/

            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
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
          << "skv_server_insert_command_sm:: Execute():: ERROR: State not recognized"
          << " State: " << State
          << EndLogLine;

        break;
      }
    }

    BegLogLine( SKV_SERVER_INSERT_LOG )
      << "skv_server_insert_command_sm::Execute(): Exiting. Status: " << skv_status_to_string(rc_status)
      << EndLogLine;

    return rc_status;
  }
};
#endif

