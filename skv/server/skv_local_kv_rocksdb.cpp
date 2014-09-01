/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     lschneid - initial implementation
 *
 *  Created on: Jan 21, 2014
 */

#ifndef SKV_LOCAL_KV_BACKEND_LOG
#define SKV_LOCAL_KV_BACKEND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG
#define SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#include <common/skv_errno.hpp>
#include <common/skv_types.hpp>
#include <utils/skv_trace_clients.hpp>

#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>
#include <server/skv_server_cursor_manager_if.hpp>

#include <server/skv_local_kv_types.hpp>
#include <common/skv_mutex.hpp>
#include <server/skv_local_kv_request.hpp>
#include <server/skv_local_kv_request_queue.hpp>
#include <server/skv_local_kv_event_queue.hpp>
#include <server/skv_local_kv_rdma_data_buffer.hpp>
#include <server/skv_local_kv_rocksdb.hpp>


static
void RocksDBProcessing( skv_local_kv_rocksdb *aBackEnd )
{
  BegLogLine( SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG )
    << "AsyncProcessing: Entering thread"
    << EndLogLine;

  skv_local_kv_request_queue_t* RequestQueue = aBackEnd->GetRequestQueue();
  while( aBackEnd->KeepProcessing() )
  {
    skv_status_t status;
    skv_local_kv_request_t *nextRequest = RequestQueue->GetRequest();

    if( nextRequest )
    {
      BegLogLine( SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG )
        << "Fetched LocalKV request: " << skv_local_kv_request_type_to_string( nextRequest->mType )
        << EndLogLine;

      switch( nextRequest->mType )
      {
        case SKV_LOCAL_KV_REQUEST_TYPE_OPEN:
          status = aBackEnd->PerformOpen( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_INFO:
          status = aBackEnd->PerformStat( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_CLOSE:
          status = aBackEnd->PerformClose( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_GET_DISTRIBUTION:
          status = aBackEnd->PerformGetDistribution( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_INSERT:
          status = aBackEnd->PerformInsert( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_LOOKUP:
          status = aBackEnd->PerformLookup( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE:
          status = aBackEnd->PerformRetrieve( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_REMOVE:
          status = aBackEnd->PerformRemove( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_BULK_INSERT:
          status = aBackEnd->PerformBulkInsert( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE_N:
          status = aBackEnd->PerformRetrieveNKeys( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN:
        default:
          StrongAssertLogLine( 1 )
            << "skv_local_kv_asyncmem:AsyncProcessing(): ERROR, unknown/unexpected Request type: " << (int)nextRequest->mType
            << EndLogLine;
      }
      RequestQueue->AckRequest( nextRequest );
    }
    else
      usleep(10);
  }

  BegLogLine( SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG )
    << "AsyncProcessing: Exiting thread"
    << EndLogLine;
}



skv_status_t
skv_local_kv_rocksdb::Init( int aRank,
                            int aNodeCount,
                            skv_server_internal_event_manager_if_t *aInternalEventMgr,
                            it_pz_handle_t aPZ,
                            char* aCheckpointPath )
{
  mEventQueue.Init();
  mRequestQueue.Init();

  mDataBuffer.Init( aPZ, SKV_LOCAL_KV_RDMA_BUFFER_SIZE, SKV_LOCAL_KV_MAX_VALUE_SIZE );

  mKeepProcessing = true;
  mReqProcessor = new std::thread(RocksDBProcessing, this );

  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Exit()
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::CancelContext( skv_local_kv_req_ctx_t *aReqCtx )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::GetDistribution(skv_distribution_t **aDist,
                                      skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::PDS_Open( char *aPDSName,
                                skv_pds_priv_t aPrivs,
                                skv_cmd_open_flags_t aFlags,
                                skv_pds_id_t *aPDSId,
                                skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_rocksdb::PDS_Stat( skv_pdscntl_cmd_t aCmd,
                                skv_pds_attr_t *aPDSAttr,
                                skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_rocksdb::PDS_Close( skv_pds_attr_t *aPDSAttr,
                                 skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Lookup( skv_pds_id_t aPDSId,
                              char *aKeyPtr,
                              int aKeySize,
                              skv_cmd_RIU_flags_t aFlags,
                              skv_lmr_triplet_t *aStoredValueRep,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Insert( skv_cmd_RIU_req_t *aReq,
                              skv_status_t aCmdStatus,
                              skv_lmr_triplet_t *aStoredValueRep,
                              skv_lmr_triplet_t *aValueRDMADest,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Insert( skv_pds_id_t& aPDSId,
                              char* aRecordRep,
                              int aKeySize,
                              int aValueSize,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::InsertPostProcess( skv_local_kv_req_ctx_t *aReqCtx,
                                         skv_lmr_triplet_t *aValueRDMADest,
                                         skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::BulkInsert( skv_pds_id_t aPDSId,
                                  skv_lmr_triplet_t *aLocalBuffer,
                                  skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Retrieve( skv_pds_id_t aPDSId,
                                char* aKeyData,
                                int aKeySize,
                                int aValueOffset,
                                int aValueSize,
                                skv_cmd_RIU_flags_t aFlags,
                                skv_lmr_triplet_t* aStoredValueRep,
                                int *aTotalSize,
                                skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_rocksdb::RetrievePostProcess( skv_local_kv_req_ctx_t *aReqCtx )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::RetrieveNKeys( skv_pds_id_t aPDSId,
                                     char * aStartingKeyData,
                                     int aStartingKeySize,
                                     skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                                     int* aRetrievedKeysCount,
                                     int* aRetrievedKeysSizesSegsCount,
                                     int aListOfKeysMaxCount,
                                     skv_cursor_flags_t aFlags,
                                     skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Remove( skv_pds_id_t aPDSId,
                              char* aKeyData,
                              int aKeySize,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Lock( skv_pds_id_t *aPDSId,
                            skv_key_value_in_ctrl_msg_t *aKeyValue,
                            skv_rec_lock_handle_t *aRecLock )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_rocksdb::Unlock( skv_rec_lock_handle_t aLock )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::RDMABoundsCheck( const char* aContext,
                                       char* aMem,
                                       int aSize )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Allocate( int aBuffSize,
                                skv_lmr_triplet_t *aRDMARep )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::Deallocate( skv_lmr_triplet_t *aRDMARep )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::CreateCursor( char* aBuff,
                                    int aBuffSize,
                                    skv_server_cursor_hdl_t* aServCursorHdl,
                                    skv_local_kv_cookie_t* aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::DumpImage( char* aCheckpointPath )
{
  return SKV_SUCCESS;
}

skv_status_t skv_local_kv_rocksdb::PerformOpen( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformGetDistribution(skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformStat( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformClose( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformInsert( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformLookup( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformRetrieve( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformBulkInsert( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformRemove( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
skv_status_t skv_local_kv_rocksdb::PerformRetrieveNKeys( skv_local_kv_request_t *aReq ) { return SKV_ERRNO_NOT_IMPLEMENTED; }
