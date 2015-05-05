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

#ifndef __SKV_SERVER_BULK_INSERT_COMMAND_SM_HPP__
#define __SKV_SERVER_BULK_INSERT_COMMAND_SM_HPP__

#ifndef SKV_SERVER_BULK_INSERT_LOG 
#define SKV_SERVER_BULK_INSERT_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_BULK_INSERT_TRACE
#define SKV_SERVER_BULK_INSERT_TRACE ( 0 )
#endif

class skv_server_bulk_insert_command_sm
{
public:
  static inline
  skv_status_t create_multi_stage( skv_server_ep_state_t *aEPState,
                                   skv_local_kv_t *aLocalKV,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal,
                                   skv_cmd_bulk_insert_req_t *aReq,
                                   skv_lmr_triplet_t *aNewRecordAllocRep )
  {
    skv_status_t status = SKV_SUCCESS;

    BegLogLine( SKV_SERVER_BULK_INSERT_LOG )
      << "skv_server_bulk_insert_command_sm::"
      << " Command requires async operation (async storage or data transfer)..."
      << EndLogLine;

    // check if we're already multi-stage class command
    if ( aCommand->GetCommandClass() == SKV_COMMAND_CLASS_MULTI_STAGE )
      return status;

    /*******************************************************************
     * Save local command state
     ******************************************************************/
    aCommand->mCommandState.mCommandBulkInsert.mHdr             = aReq->mHdr;
    aCommand->mCommandState.mCommandBulkInsert.mLocalBuffer.Init( *aNewRecordAllocRep );
    aCommand->mCommandState.mCommandBulkInsert.mPDSId           = aReq->mPDSId;

    // This is for debugging
    aCommand->mCommandState.mCommandBulkInsert.mRemoteBufferRMR      = aReq->mBufferRMR;
    aCommand->mCommandState.mCommandBulkInsert.mRemoteBufferAddr     = aReq->mBuffer;

#ifdef SKV_BULK_LOAD_CHECKSUM
            aCommand->mCommandState.mCommandBulkInsert.mRemoteBufferChecksum = aReq->mBufferChecksum;
            /******************************************************************/

            BegLogLine( 0 )
              << "skv_server_bulk_insert_command_sm::Execute(): "
              << " RemoteBufferChecksum: "
              << aCommand->mCommandState.mCommandBulkInsert.mRemoteBufferChecksum
              << " Req->mBufferChecksum: " << aReq->mBufferChecksum
              << EndLogLine;
#endif

    /******************************************************************/
    aEPState->ReplaceAndInitCommandBuffer( aCommand, aCommandOrdinal );

    return status;
  }

  static inline
  skv_status_t command_completion( skv_status_t aRC,
                                   skv_server_ep_state_t *aEPState,
                                   skv_cmd_insert_cmpl_t *aCmpl,
                                   skv_server_ccb_t *aCommand,
                                   int aCommandOrdinal,
                                   int *aSeqNo )
  {
    AssertLogLine( aCmpl != NULL )
      << "skv_server_bulk_insert_command_sm:: ERROR: "
      << EndLogLine;

    switch( aRC )
    {
      case SKV_ERRNO_NEED_DATA_TRANSFER:
        aRC = SKV_SUCCESS;
        // no break, because the successful response is: write_value_ack
      case SKV_SUCCESS:
        aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_CMD_COMPLETE;
        break;
      default:
        aCmpl->mHdr.mEvent = SKV_CLIENT_EVENT_ERROR;
    }

    BegLogLine( SKV_SERVER_BULK_INSERT_LOG )
      << "skv_server_bulk_insert_command_sm::"
      << " completing bulk-insert with status: " << skv_status_to_string( aRC )
      << EndLogLine;

    aCmpl->mStatus = aRC;

    aCmpl->EndianConvert() ;

    skv_status_t status = aEPState->Dispatch( aCommand,
                                              aSeqNo,
                                              aCommandOrdinal );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_bulk_insert_command_sm:: ERROR: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    return status;
  }

  static inline
  void bulk_insert_post_rdma( skv_server_ep_state_t *aEPState,
                              skv_local_kv_t *aLocalKV,
                              int aCommandOrdinal,
                              skv_cmd_bulk_insert_req_t *aReq,
                              skv_lmr_triplet_t *aNewRecordAllocRep,
                              int* aSeqNo,
                              int aMyRank )
  {
    /*******************************************************************
     * Issue an rdma read from the client
     ******************************************************************/
    skv_server_cookie_t Cookie;
    Cookie.Init( aEPState,
                 *aSeqNo,
                 aCommandOrdinal );

    it_dto_cookie_t* DtoCookie = (it_dto_cookie_t* ) & Cookie;
    it_dto_flags_t dto_flags = (it_dto_flags_t) ( IT_COMPLETION_FLAG | IT_NOTIFY_FLAG );

    gSKVServerBulk_InsertAboutToRDMARead.HitOE( SKV_SERVER_BULK_INSERT_TRACE,
                                                "SKVServerBulk_InsertRdmaRead",
                                                aMyRank,
                                                gSKVServerBulk_InsertAboutToRDMARead );

    // rdma_write the value
    it_status_t itstatus = it_post_rdma_read( aEPState->mEPHdl,
                                              aNewRecordAllocRep->GetTripletPtr(),
                                              1,
                                              *DtoCookie,
                                              dto_flags,
                                              (it_rdma_addr_t) aReq->mBuffer,
                                              aReq->mBufferRMR );

    AssertLogLine( itstatus == IT_SUCCESS )
      << "skv_server_bulk_insert_command_sm::Execute():: ERROR: "
      << " istatus: " << itstatus
      << EndLogLine;

    BegLogLine( SKV_SERVER_BULK_INSERT_LOG )
      << "skv_server_bulk_insert_command_sm::Execute():: called it_post_rdma_read() on: "
      << " EP: " << (void *) aEPState->mEPHdl
      << " Remote Buffer: " << (void *) aReq->mBuffer
      << " Remote RMR: " << (void *) aReq->mBufferRMR
      << " BufferSize: " << aReq->mBufferSize
      << " LocalBuffer: " << aReq->mBuffer
      << EndLogLine;
    /******************************************************************/
  }



  static skv_status_t
  Execute( skv_server_internal_event_manager_if_t* aEventQueueManager,
           skv_local_kv_t*                         aLocalKV,
           skv_server_ep_state_t*                  aEPState,
           int                                     aCommandOrdinal,
           skv_server_event_t*                     aEvent,
           int*                                    aSeqNo,
           int                                     aMyRank )
  {
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->mState;

    skv_server_event_type_t EventType = aEvent->mCmdEventType;

    BegLogLine( SKV_SERVER_BULK_INSERT_LOG )
      << "skv_server_bulk_insert_command_sm::Execute():: Entering "
      << " EP: " << (void *) aEPState->mEPHdl
      << " Command: " << (void *) Command
      << " State: " << skv_server_command_state_to_string( State )
      << " Event: " << skv_server_event_type_to_string( EventType )
      << EndLogLine;

    skv_status_t rc_status = SKV_SUCCESS;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_BULK_INSERT_CMD:
          {
            BegLogLine( SKV_SERVER_BULK_INSERT_LOG )
              << "skv_server_bulk_insert_command_sm::Execute():: Entering action block for "
              << " State: " << skv_server_command_state_to_string( State )
              << " Event: " << skv_server_event_type_to_string( EventType )
              << EndLogLine;

            skv_cmd_bulk_insert_req_t* Req = (skv_cmd_bulk_insert_req_t *) Command->GetSendBuff();

            Req->EndianConvert() ;

            AssertLogLine( ( ((void *) Req->mBuffer) != NULL ) &&
                           ( Req->mBufferSize >= 0 && Req->mBufferSize < SKV_BULK_INSERT_LIMIT ) )
              << "skv_server_bulk_insert_command_sm:: Execute():: ERROR: "
              << " Req->mBuffer: " << Req->mBuffer
              << " BufferSize: " << Req->mBufferSize
              << EndLogLine;

            // allocate a temporary buffer for RDMA transfer and then kick off the rdma
            skv_lmr_triplet_t NewRecordAllocRep;
            skv_status_t status = aLocalKV->Allocate( Req->mBufferSize,
                                                      & NewRecordAllocRep );
            BegLogLine(SKV_SERVER_BULK_INSERT_LOG)
              << "Allocated the temporary buffer"
              << EndLogLine ;

            if( status != SKV_SUCCESS )
            {
              command_completion( status,
                                  aEPState,
                                  (skv_cmd_insert_cmpl_t*)Command->GetSendBuff(),
                                  Command,
                                  aCommandOrdinal,
                                  aSeqNo );
              Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
              break;
            }

            create_multi_stage( aEPState, aLocalKV, Command, aCommandOrdinal, Req, &NewRecordAllocRep );
            bulk_insert_post_rdma( aEPState,
                                   aLocalKV,
                                   aCommandOrdinal,
                                   (skv_cmd_bulk_insert_req_t *)Command->GetSendBuff(),
                                   &NewRecordAllocRep,
                                   aSeqNo,
                                   aMyRank );
            Command->Transit( SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL );

            break;
          }
          default:
            {
            StrongAssertLogLine( 0 )
              << "skv_server_bulk_insert_command_sm:: Execute():: ERROR: State not recognized"
              << " State: " << State
              << " EventType: " << EventType
              << EndLogLine;

            break;
            }
        }

        break;
      }
      case SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_READ_CMPL:
          {

            gSKVServerBulk_InsertSendingRDMAReadAck.HitOE( SKV_SERVER_BULK_INSERT_TRACE,
                                                           "SKVServerBulk_InsertRdmaRead",
                                                           aMyRank,
                                                           gSKVServerBulk_InsertSendingRDMAReadAck );

            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );


#ifdef SKV_BULK_LOAD_CHECKSUM
  uint64_t  LocalBufferSize = Command->mCommandState.mCommandBulkInsert.mLocalBuffer.GetLen();
  char* LocalBufferAddr = (char*)Command->mCommandState.mCommandBulkInsert.mLocalBuffer.GetAddr();
  it_lmr_handle_t LocalBufferLMR = Command->mCommandState.mCommandBulkInsert.mLocalBuffer.GetLMRHandle();

  uint64_t  BufferChecksum       = 0;
  uint64_t  RemoteBufferChecksum = Command->mCommandState.mCommandBulkInsert.mRemoteBufferChecksum;
  for( int i=0; i < LocalBufferSize; i++)
  {
    BufferChecksum += LocalBufferAddr[ i ];
  }

  if( BufferChecksum != RemoteBufferChecksum )
  {
    BegLogLine( 1 )
      << "skv_server_bulk_insert_command_sm::Execute(): ERROR: "
      << " BufferChecksum: " << BufferChecksum
      << " RemoteBufferChecksum: " << RemoteBufferChecksum
      << " PDSId: " << Command->mCommandState.mCommandBulkInsert.mPDSId
      << " LocalBufferSize: " << LocalBufferSize
      << " LocalBuffer: " << (void *) LocalBufferAddr
      << " LocalBufferLMR: " << (void *) LocalBufferLMR
//      << " RemoteBufferRMR: " << (void *) RemoteBufferRMR
//      << " RemoteBufferAddr: " << (void *) RemoteBufferAddr
      << EndLogLine;

    int BytesProcessed = 0;
    char* BufferToReport = LocalBufferAddr;
    int RowsProcessed = 0;
    while( BytesProcessed < LocalBufferSize )
    {
      int KeySize   = -1;
      int ValueSize = -1;
      char* KeyPtr    = NULL;
      char* ValuePtr  = NULL;

      int RowLen = skv_bulk_insert_get_key_value_refs( BufferToReport,
                                                       &KeyPtr,
                                                       KeySize,
                                                       &ValuePtr,
                                                       ValueSize );

      int TotalSize = KeySize + ValueSize;

      int BytesInRow = skv_bulk_insert_get_total_len( BufferToReport );

      HexDump FxString( LocalBufferAddr, BytesInRow );

      BegLogLine( 1 )
        << "skv_server_bulk_insert_command_sm::Execute(): "
        << " TotalSize: " << TotalSize
        << " KeySize: " << KeySize
        << " ValueSize: " << ValueSize
        << " RowsProcessed: " << RowsProcessed
        << " BytesInRow: " << BytesInRow
        << " FxString: " << FxString
        << EndLogLine;

      RowsProcessed++;
      BufferToReport += BytesInRow;
      BytesProcessed += BytesInRow;
    }

    command_completion( SKV_ERRNO_CHECKSUM_MISMATCH,
                        aEPState,
                        (skv_cmd_insert_cmpl_t*)Command->GetSendBuff(),
                        Command,
                        aCommandOrdinal,
                        aSeqNo );
  }
#if 0
  StrongAssertLogLine( BufferChecksum == RemoteBufferChecksum )
    << "skv_server_bulk_insert_command_sm::Execute(): ERROR: "
    << " BufferChecksum: " << BufferChecksum
    << " RemoteBufferChecksum: " << RemoteBufferChecksum
    << " PDSId: " << Command->mCommandState.mCommandBulkInsert.mPDSId
    << " LocalBufferSize: " << LocalBufferSize
    << " LocalBuffer: " << (void *) LocalBufferAddr
    << " LocalBufferLMR: " << (void *) LocalBufferLMR
    << EndLogLine;
#endif
#endif


            skv_status_t status = aLocalKV->BulkInsert( Command->mCommandState.mCommandBulkInsert.mPDSId,
                                                        &Command->mCommandState.mCommandBulkInsert.mLocalBuffer,
                                                        cookie );

            switch( status )
            {
              case SKV_ERRNO_LOCAL_KV_EVENT:
                Command->Transit( SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP );
                status = SKV_SUCCESS;
                break;

              case SKV_ERRNO_COMMAND_LIMIT_REACHED:
                status = aEventQueueManager->Enqueue( aEvent );
                break;

              default:
              case SKV_SUCCESS:
                // Return the temporary buffer to the store
                BegLogLine(SKV_SERVER_BULK_INSERT_LOG)
                    << "Returning the temporary buffer"
                    << EndLogLine ;
                aLocalKV->Deallocate( & Command->mCommandState.mCommandBulkInsert.mLocalBuffer );
                BegLogLine(SKV_SERVER_BULK_INSERT_LOG)
                  << "Returned the temporary buffer"
                  << EndLogLine ;

                command_completion( status,
                                    aEPState,
                                    (skv_cmd_insert_cmpl_t*)Command->GetSendBuff(),
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
              << "skv_server_bulk_insert_command_sm::Execute(): ERROR:: EventType not recognized. "
              << " EventType: " << EventType
              << EndLogLine;
          }
        }

        break;
      }
      case SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP:
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:
          {
            // Return the temporary buffer to the store
            BegLogLine(SKV_SERVER_BULK_INSERT_LOG)
                << "Returning the temporary buffer"
                << EndLogLine ;
            aLocalKV->Deallocate( & Command->mCommandState.mCommandBulkInsert.mLocalBuffer );
            BegLogLine(SKV_SERVER_BULK_INSERT_LOG)
              << "Returned the temporary buffer"
              << EndLogLine ;

            command_completion( Command->mLocalKVrc,
                                aEPState,
                                (skv_cmd_insert_cmpl_t*)Command->GetSendBuff(),
                                Command,
                                aCommandOrdinal,
                                aSeqNo );

            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
            rc_status = SKV_ERRNO_STATE_MACHINE_ERROR;
            break;
        }
        break;
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_bulk_insert_command_sm:: Execute():: ERROR: State not recognized"
          << " State: " << State
          << EndLogLine;

        break;
      }
    }

    return rc_status;
  }
};
#endif
