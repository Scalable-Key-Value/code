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
#include <memory>
#include <string>

#include <skv/common/skv_errno.hpp>
#include <skv/common/skv_types.hpp>
#include <skv/utils/skv_trace_clients.hpp>

#include <skv/common/skv_client_server_headers.hpp>
#include <skv/client/skv_client_server_conn.hpp>
#include <skv/common/skv_client_server_protocol.hpp>
#include <skv/server/skv_server_types.hpp>
#include <skv/server/skv_server_cursor_manager_if.hpp>

#include <skv/server/skv_local_kv_types.hpp>
#include <skv/common/skv_mutex.hpp>
#include <skv/server/skv_local_kv_request.hpp>
#include <skv/server/skv_local_kv_request_queue.hpp>
#include <skv/server/skv_local_kv_event_queue.hpp>
#include <skv/server/skv_local_kv_rdma_data_buffer.hpp>
#include <skv/server/skv_local_kv_rocksdb.hpp>

/*
 * RocksDB backend considerations
 * * processing thread similar to async inmem


 * * PDS management (target):
 *   * each owner node has container of existing PDS entries (including id, attr, etc)
 *   * pdsid is generated via hash_of_pds_name.srvRank
 *   * when open, clients get the pds-id from the owner's container and then
 *     servers need to do a broadcast of the open request to open the requested container
 * * PDS management (intermediate)
 *   * copy of the inmem approach: one large PDS that contains pdsids and keys
 *   * doesn't require the server broadcast
 */

skv_thread_id_map_t *skv_thread_id_map_t::mThreadMap = NULL;

void Run( skv_local_kv_rocksdb_worker_t *aWorker )
{
  BegLogLine( SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG )
    << "AsyncProcessing: Entering thread"
    << EndLogLine;

  skv_local_kv_rocksdb *Master = aWorker->GetMaster();
  skv_local_kv_request_queue_t *RequestQueue = aWorker->GetRequestQueue();
  skv_local_kv_request_queue_t *DedicatedQueue = aWorker->GetDedicatedQueue();

  // insert the data buffer into the thread-id-map to make sure the allocator finds the right buffer
  skv_thread_id_map_t *tm = skv_thread_id_map_t::GetThreadIdMap();
  tm->InsertRDB( aWorker->GetDataBuffer() );

  while( Master->KeepProcessing() )
  {
    skv_status_t status;
    skv_local_kv_request_queue_t *nextQueue = DedicatedQueue;
    skv_local_kv_request_t *nextRequest;

    // switch to global queue if dedicated request queue is empty
    if( nextQueue->IsEmpty() )
      nextQueue = RequestQueue;

    nextRequest = nextQueue->GetRequest();
    if( nextRequest )
    {
      BegLogLine( SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG )
        << "Fetched LocalKV request: " << skv_local_kv_request_type_to_string( nextRequest->mType )
        << " rctx:" << nextRequest->mReqCtx
        << " @" << (void*)nextRequest
        << EndLogLine;

      switch( nextRequest->mType )
      {
        case SKV_LOCAL_KV_REQUEST_TYPE_OPEN:
          status = aWorker->PerformOpen( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_INFO:
          status = aWorker->PerformStat( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_CLOSE:
          status = aWorker->PerformClose( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_GET_DISTRIBUTION:
          StrongAssertLogLine( 1 )
            << "skv_local_kv_rocksdb:AsyncProcessing(): ERROR, unexpected Request type: " << skv_local_kv_request_type_to_string( nextRequest->mType )
            << EndLogLine;
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_INSERT:
          status = aWorker->PerformInsert( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_LOOKUP:
          status = aWorker->PerformLookup( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE:
          status = aWorker->PerformRetrieve( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_REMOVE:
          status = aWorker->PerformRemove( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_BULK_INSERT:
          status = aWorker->PerformBulkInsert( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE_N:
          status = aWorker->PerformRetrieveNKeys( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_ASYNC_INSERT_CLEANUP:
          status = aWorker->PerformAsyncInsertCleanup( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_ASYNC_RETRIEVE_CLEANUP:
          status = aWorker->PerformAsyncRetrieveCleanup( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_ASYNC_RETRIEVE_NKEYS_CLEANUP:
          status = aWorker->PerformAsyncRetrieveNKeysCleanup( nextRequest );
          break;
        case SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN:
        default:
          StrongAssertLogLine( 1 )
            << "skv_local_kv_rocksdb:AsyncProcessing(): ERROR, unknown/unexpected Request type: " << (int)nextRequest->mType
            << EndLogLine;
      }
      nextQueue->AckRequest( nextRequest );
    }
  }

  BegLogLine( SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG )
    << "AsyncProcessing: Exiting thread"
    << EndLogLine;
}
skv_status_t skv_local_kv_rocksdb_worker_t::Init( skv_local_kv_rocksdb *aBackEnd, bool aThreaded )
{
  skv_status_t status;
  mMaster = aBackEnd;

  mDataBuffer = new skv_local_kv_rdma_data_buffer_t( );
  status = mDataBuffer->Init( mMaster->GetPZ(), SKV_LOCAL_KV_RDMA_BUFFER_SIZE, SKV_LOCAL_KV_MAX_VALUE_SIZE );
  if( status != SKV_SUCCESS )
    return status;

  mRequestQueue = mMaster->GetRequestQueue();
  status = mDedicatedQueue.Init();
  if( status != SKV_SUCCESS )
    return status;

  mDBAccess = mMaster->GetDBAccess();
  mEventQueue = mMaster->GetEventQueue();

  if( aThreaded )
    mRequestProcessor = new std::thread( Run, this );

  return status;
}

skv_status_t
skv_local_kv_rocksdb::Init( int aRank,
                            int aNodeCount,
                            skv_server_internal_event_manager_if_t *aInternalEventMgr,
                            it_pz_handle_t aPZ,
                            char* aCheckpointPath )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb::Init(): Entering..."
    << EndLogLine;

  skv_status_t status;
  mMyRank = aRank;
  skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

  status = mEventQueue.Init();
  if( status != SKV_SUCCESS )
    return status;

  status = mRequestQueue.Init();
  if( status != SKV_SUCCESS )
    return status;

  mKeepProcessing = true;
  mPZ = aPZ;

  status = mDistributionManager.Init( aNodeCount );
  if( status != SKV_SUCCESS )
    return status;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb::Init(): queues and distribution initialized..."
    << EndLogLine;

  status = mDBAccess.Init( config->GetServerPersistentFileLocalPath(), mMyRank );
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb::Init(): DB-Access initialized..."
    << EndLogLine;

  status = mMasterProcessing.Init( this );
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb::Init(): Masterbased worker initialized..."
    << EndLogLine;

  for( int w=0; ( w < SKV_LOCAL_KV_WORKER_POOL_SIZE ) && (status == SKV_SUCCESS ); w++ )
  {
    status = mWorkerPool[ w ].Init( this, true );
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb::Init(): Worker[ " << w << " ] initialized ..."
      << EndLogLine;
  }
  return status;
}

skv_status_t
skv_local_kv_rocksdb::Exit()
{
  skv_status_t status = mDBAccess.Exit();
  mDistributionManager.Finalize();
  return status;
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
  // dist manager is a SKV specific memory thing, so we can immediately return here
  *aDist = &mDistributionManager;
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_rocksdb::PDS_Open( char *aPDSName,
                                skv_pds_priv_t aPrivs,
                                skv_cmd_open_flags_t aFlags,
                                skv_pds_id_t *aPDSId,
                                skv_local_kv_cookie_t *aCookie )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb:PDS_Open Entering..."
    << EndLogLine;

  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_OPEN, aCookie );
  memcpy( &kvReq->mData[ 0 ], aPDSName, SKV_MAX_PDS_NAME_SIZE );
  kvReq->mRequest.mOpen.mPDSName = &kvReq->mData[ 0 ];
  kvReq->mRequest.mOpen.mPrivs = aPrivs;
  kvReq->mRequest.mOpen.mFlags = aFlags;

  mRequestQueue.QueueRequest( kvReq );

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: Open Request stored:"
    << " PDSName:" << kvReq->mRequest.mOpen.mPDSName
    << " Priv:" << kvReq->mRequest.mOpen.mPrivs
    << " Flags: " << kvReq->mRequest.mOpen.mFlags
    << EndLogLine;

  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_pds_id_t skv_local_kv_rocksdb_worker_t::PDSNameToID( std::string aPDSName )
{
  skv_pds_id_t pdsid;

  pdsid.mIdOnOwner = mMaster->GetHash( aPDSName.c_str(), aPDSName.length() );
  pdsid.mOwnerNodeId = mMaster->GetRank();

  return pdsid;
}
std::string skv_local_kv_rocksdb_worker_t::PDSIdToString( skv_pds_id_t aPDSId )
{
  std::string idstr( (const char*)&aPDSId, sizeof(skv_pds_id_t) );
  return idstr;
}


skv_status_t skv_local_kv_rocksdb_worker_t::PerformOpen( skv_local_kv_request_t *aReq )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_request_t::OPEN REQUEST"
    << " PDSName:" << aReq->mRequest.mOpen.mPDSName
    << " Priv:" << aReq->mRequest.mOpen.mPrivs
    << " Flags: " << aReq->mRequest.mOpen.mFlags
    << EndLogLine;

  skv_status_t status = SKV_SUCCESS;
  rocksdb::Slice pdsKey = rocksdb::Slice( aReq->mRequest.mOpen.mPDSName );
  std::string pdsDataString;
  skv_pds_attr_t *pdsData = NULL;
  skv_pds_id_t returnPDS;
  returnPDS.Init( -1, -1 );
  rocksdb::Options pdsOpts = rocksdb::Options();

  rocksdb::Status rs = mDBAccess->GetPDS( pdsKey, &pdsDataString );

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_request_t::OPEN REQUEST"
    << " rocksdb search for PDS was: " << rs.ToString().c_str()
    << EndLogLine;

  if( rs.ok() )
  {
    pdsData = (skv_pds_attr_t*)pdsDataString.data();

    if( aReq->mRequest.mOpen.mFlags & SKV_COMMAND_OPEN_FLAGS_EXCLUSIVE )
      status = SKV_ERRNO_PDS_ALREADY_EXISTS;  // exclusive creation - don't recreate
    else if ( aReq->mRequest.mOpen.mFlags & SKV_COMMAND_OPEN_FLAGS_CREATE )
      status = SKV_ERRNO_RECORD_ALREADY_EXISTS;  // recreate even if exists
    else
      status = SKV_SUCCESS;
  }
  else
    if( aReq->mRequest.mOpen.mFlags & SKV_COMMAND_OPEN_FLAGS_CREATE )
      status = SKV_ERRNO_ELEM_NOT_FOUND;  // just create a new pds
    else
      status = SKV_ERRNO_PDS_DOES_NOT_EXIST;  // don't create a new pds

  switch( status )
  {
    // pds already there and requested forced creation of pds
    case SKV_ERRNO_PDS_ALREADY_EXISTS:
      returnPDS.Init( -1, -1 );
      break;

    // pds already exists but requested to recreate/truncate
    case SKV_ERRNO_RECORD_ALREADY_EXISTS:
      // wipe out existing data and pds entries for this pds
      rs = mDBAccess->DeletePDS( pdsKey, PDSIdToString( pdsData->mPDSId ) );

      // NO break here: we want to create a new pds entry

    // pds does not exist and requested to create a new pds
    case SKV_ERRNO_ELEM_NOT_FOUND:
      pdsData = new skv_pds_attr_t;
      pdsData->mPrivs = aReq->mRequest.mOpen.mPrivs;
      strncpy( pdsData->mPDSName, aReq->mRequest.mOpen.mPDSName, SKV_MAX_PDS_NAME_SIZE );
      pdsData->mSize = 0;   // not a useful parameter right now
      pdsData->mPDSId = PDSNameToID( std::string( aReq->mRequest.mOpen.mPDSName, SKV_MAX_PDS_NAME_SIZE ) );
      returnPDS.Init( pdsData->mPDSId.mOwnerNodeId, pdsData->mPDSId.mIdOnOwner );

      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "skv_local_kv_request_t::OPEN REQUEST"
        << " creating new PDSId: " << returnPDS
        << EndLogLine;

      // insert the new PDS entry to the PDS table (name and reverse lookup entry)
      pdsDataString = std::string( (const char*) pdsData, sizeof(skv_pds_attr_t) );
      rs = mDBAccess->CreatePDS(pdsKey, PDSIdToString( pdsData->mPDSId ), rocksdb::Slice( pdsDataString ) );

      AssertLogLine( rs.ok() )
       << "skv_local_kv_rocksdb: Creation of new PDS entry failed."
       << EndLogLine;
      delete pdsData;

      status = SKV_SUCCESS;
      break;

    // doesn't exist and creation not requested - just fail...
    case SKV_ERRNO_PDS_DOES_NOT_EXIST:
      break;

    // found pds and flags are fine
    case SKV_SUCCESS:
      returnPDS = pdsData->mPDSId;
      break;

    default:
      AssertLogLine( 1 )
        << "skv_local_kv_rocksdb: OPEN REQUEST: unexpected state." << skv_status_to_string( status )
        << EndLogLine;
  }

  // permission check...

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "OPEN REQUEST COMPLETE. status: " << skv_status_to_string( status )
    << " creating event..."
    << EndLogLine;

  status = InitKVEvent( aReq->mCookie, returnPDS, status );
  return status;
}



skv_status_t
skv_local_kv_rocksdb::PDS_Stat( skv_pdscntl_cmd_t aCmd,
                                skv_pds_attr_t *aPDSAttr,
                                skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_INFO, aCookie );
  kvReq->mRequest.mStat.mCmd = aCmd;
  memcpy( &kvReq->mData[ 0 ], aPDSAttr, sizeof( skv_pds_attr_t ) );
  kvReq->mRequest.mStat.mPDSAttr = (skv_pds_attr_t*)&kvReq->mData[ 0 ];

  mRequestQueue.QueueRequest( kvReq );

  return SKV_ERRNO_LOCAL_KV_EVENT;
}
skv_status_t skv_local_kv_rocksdb_worker_t::PerformStat( skv_local_kv_request_t *aReq )
{
  skv_status_t status;
  skv_pds_id_t PDSId = aReq->mRequest.mStat.mPDSAttr->mPDSId;
  std::string attrStr;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "STAT REQUEST entering. PDSId:" << PDSId
    << EndLogLine;

  rocksdb::Status rs = mDBAccess->GetPDS( PDSIdToString( PDSId ), &attrStr );
  if( !rs.ok() )
    status = SKV_ERRNO_PDS_DOES_NOT_EXIST;
  else
  {
    AssertLogLine( attrStr.length() <= sizeof( skv_pds_attr_t ) )
      << "skv_local_kv_rocksdb: STAT REQUEST: stored data exceeds size of pds attribute type."
      << EndLogLine;
//    memcpy( &aReq->mRequest.mStat.mPDSAttr, attrStr.data(), attrStr.length() );
    status = SKV_SUCCESS;
  }

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "STAT REQUEST COMPLETE. status: " << skv_status_to_string( status )
    << " creating event..."
    << EndLogLine;

  status = InitKVEvent( aReq->mCookie, aReq->mRequest.mStat.mCmd, (skv_pds_attr_t*)attrStr.data(), status );
  return status;
}


skv_status_t
skv_local_kv_rocksdb::PDS_Close( skv_pds_attr_t *aPDSAttr,
                                 skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_CLOSE, aCookie );
  kvReq->mRequest.mStat.mCmd = SKV_PDSCNTL_CMD_CLOSE;
  kvReq->mRequest.mStat.mPDSAttr = aPDSAttr;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}
skv_status_t skv_local_kv_rocksdb_worker_t::PerformClose( skv_local_kv_request_t *aReq )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: CLOSE REQUEST. entering... "
    << EndLogLine;

  skv_status_t status;
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG ) << "skv_local_kv_rocksdb: CLOSE. aReq=" << aReq << EndLogLine;
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG ) << "skv_local_kv_rocksdb: CLOSE. aReq.mRequest.mStat=" << aReq->mRequest.mStat.mCmd << EndLogLine;
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG ) << "skv_local_kv_rocksdb: CLOSE. aReq.mRequest.mStat.mPDSAttr=" << aReq->mRequest.mStat.mPDSAttr << EndLogLine;
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG ) << "skv_local_kv_rocksdb: CLOSE. aReq.mRequest.mStat.mPDSAttr->mPDSId=" << aReq->mRequest.mStat.mPDSAttr->mPDSId << EndLogLine;
  rocksdb::Status rs = mDBAccess->ClosePDS( rocksdb::Slice( PDSIdToString( aReq->mRequest.mStat.mPDSAttr->mPDSId ) ));

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: CLOSE REQUEST. completed flush with: " << rs.ToString().c_str()
    << EndLogLine;

  skv_pds_attr_t PDSAttr;
  status = rocksdb_status_to_skv( rs );
  status = InitKVEvent( aReq->mCookie, aReq->mRequest.mStat.mCmd, &PDSAttr, status );

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
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_LOOKUP, aCookie );
  kvReq->mRequest.mLookup.mPDSId = aPDSId;
  memcpy( &kvReq->mData[ 0 ],
          aKeyPtr,
          aKeySize>SKV_CONTROL_MESSAGE_SIZE ? SKV_CONTROL_MESSAGE_SIZE : aKeySize );
  kvReq->mRequest.mLookup.mKeyData = &kvReq->mData[ 0 ];
  kvReq->mRequest.mLookup.mKeySize = aKeySize;
  kvReq->mRequest.mLookup.mFlags = aFlags;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t skv_local_kv_rocksdb_worker_t::PerformLookup( skv_local_kv_request_t *aReq )
{
  skv_status_t status = SKV_ERRNO_ELEM_NOT_FOUND;
  skv_lmr_triplet_t StoredValueRep;
  StoredValueRep.InitAbs( NULL, NULL, 0 );

  rocksdb::Slice key = MakeKey( aReq->mRequest.mLookup.mPDSId,
                                aReq->mRequest.mLookup.mKeyData,
                                aReq->mRequest.mLookup.mKeySize );

  std::string *value = new std::string();

  // Check if the key exists
  rocksdb::Status rs = mDBAccess->LookupData( key, value );

  if( rs.ok() )
  {
    //    if( ! aReq->mRequest.mLookup.mFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG )
//    {
//      status = mDataBuffer->AcquireDataArea( value.length(), &StoredValueRep );
//      memcpy( (char*)StoredValueRep.GetAddr(), value.data(), value.length());
//      //    StoredValueRep.InitAbs( mDataBuffer->GetLMR(), (char*)value->data(), value->length() );
//    }
    status = SKV_SUCCESS;
  }

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: lookup created LMR: " << StoredValueRep
    << " status=" << skv_status_to_string( status )
    << EndLogLine;

  status = InitKVEvent( aReq->mCookie, &StoredValueRep, status );
  delete value;
  ReleaseKey( key );
  return status;
}

skv_status_t
skv_local_kv_rocksdb::Insert( skv_cmd_RIU_req_t *aReq,
                              skv_status_t aCmdStatus,  // what the lookup status was
                              skv_lmr_triplet_t *aStoredValueRep,   // where old data is stored
                              skv_lmr_triplet_t *aValueRDMADest,    // where new data has to be stored
                              skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_INSERT, aCookie );
  memcpy( &kvReq->mData[ 0 ],
          aReq,
          SKV_CONTROL_MESSAGE_SIZE );
  kvReq->mRequest.mInsert.mReqData = (skv_cmd_RIU_req_t*)&kvReq->mData[ 0 ];
  kvReq->mRequest.mInsert.mCmdStatus = aCmdStatus;
  kvReq->mRequest.mInsert.mStoredValueRep = *aStoredValueRep;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}
skv_status_t skv_local_kv_rocksdb_worker_t::PerformInsert( skv_local_kv_request_t *aReq )
{
  skv_status_t status = SKV_ERRNO_NOT_DONE;
  skv_lmr_triplet_t ValueRDMADest;
  skv_key_value_in_ctrl_msg_t *kvData =  &aReq->mRequest.mInsert.mReqData->mKeyValue;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: Insert Command case: " << skv_status_to_string( aReq->mRequest.mInsert.mCmdStatus )
    << " smalldata: " << (int)(aReq->mRequest.mInsert.mReqData->mFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG)
    << EndLogLine;

  switch( aReq->mRequest.mInsert.mCmdStatus )
  {
    case SKV_SUCCESS:
      switch( aReq->mRequest.mInsert.mReqData->mFlags & (SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE
          | SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP
          | SKV_COMMAND_RIU_UPDATE
          | SKV_COMMAND_RIU_APPEND) )
      {
        case SKV_COMMAND_RIU_UPDATE:
          // just overwrite
          status = SKV_SUCCESS;
          break;
        case SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP:
        case SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE:
        case SKV_COMMAND_RIU_APPEND:
          status = SKV_ERRNO_NOT_IMPLEMENTED;
          break;
        case SKV_COMMAND_RIU_FLAGS_NONE:
          status = SKV_ERRNO_RECORD_ALREADY_EXISTS;
          break;
        default:
          status = SKV_ERRNO_NOT_IMPLEMENTED;
      }
      break;

    case SKV_ERRNO_ELEM_NOT_FOUND:
      // no old data, so we can just insert small or acquire a new rdma slot and return to SKV for data transfer for large
      status = SKV_SUCCESS;
      break;

    default:
      status = SKV_ERRNO_STATE_MACHINE_ERROR;
  }

  // exit if there were errors during prep
  if( status != SKV_SUCCESS )
    return InitKVEvent( aReq->mCookie, status );

  rocksdb::Slice *key = new rocksdb::Slice( MakeKey( aReq->mRequest.mInsert.mReqData->mPDSId,
                                                     kvData->mData,
                                                     kvData->mKeySize ) );
  skv_local_kv_rocksdb_reqctx_t *reqCtx = NULL;

  // actual data handling...
  if( aReq->mRequest.mInsert.mReqData->mFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG )
  {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: Inserting small data: pdsid:" << aReq->mRequest.mInsert.mReqData->mPDSId
      << " (int)key: " << *(unsigned int*)kvData->mData
      << " keySize: " << kvData->mKeySize
      << EndLogLine;

    rocksdb::Slice value = rocksdb::Slice( &(kvData->mData[ kvData->mKeySize ]), kvData->mValueSize );
    rocksdb::Status rs = mDBAccess->PutData( *key, value );
    ReleaseKey( *key );
    delete key;
  }
  else
  {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: Inserting large data: pdsid:" << aReq->mRequest.mInsert.mReqData->mPDSId
      << " (int)key: " << *(unsigned int*)kvData->mData
      << " keySize: " << kvData->mKeySize
      << " reqCtx: " << (skv_local_kv_req_ctx_t)key
      << EndLogLine;

    status = mDataBuffer->AcquireDataArea( aReq->mRequest.mInsert.mReqData->mKeyValue.mValueSize,
                                           &ValueRDMADest );
    status = SKV_ERRNO_NEED_DATA_TRANSFER;

    reqCtx = new skv_local_kv_rocksdb_reqctx_t;
    reqCtx->mWorker = this;
    reqCtx->mUserData = (void*)key;
  }

  status = InitKVRDMAEvent( aReq->mCookie, &ValueRDMADest, (skv_local_kv_req_ctx_t)reqCtx, 0, status );
  return status;
}

skv_status_t
skv_local_kv_rocksdb::Insert( skv_pds_id_t& aPDSId,
                              char* aRecordRep,
                              int aKeySize,
                              int aValueSize,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_ERRNO_NOT_IMPLEMENTED;
}

skv_status_t
skv_local_kv_rocksdb::InsertPostProcess( skv_local_kv_req_ctx_t aReqCtx,
                                         skv_lmr_triplet_t *aValueRDMADest,
                                         skv_local_kv_cookie_t *aCookie )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: completing insert: lmr:" << *aValueRDMADest
    << " reqCtx: " << aReqCtx
    << EndLogLine;

  skv_local_kv_rocksdb_reqctx_t *ReqCtx = (skv_local_kv_rocksdb_reqctx_t*)aReqCtx;
  rocksdb::Slice *key = (rocksdb::Slice*)ReqCtx->mUserData;
  rocksdb::Slice value = rocksdb::Slice( (const char*)(aValueRDMADest->GetAddr()), aValueRDMADest->GetLen() );

  rocksdb::Status rs = mDBAccess.PutData( *key, value);

  mMasterProcessing.ReleaseKey( *key );
  delete key;

  /* create an async request, make sure it goes to the worker thread
   * that created the data area to avoid locks on the databuffer
   */
  skv_local_kv_rocksdb_worker_t *DestWorker = ReqCtx->mWorker;

  // need to create a copy of the lmr because the main server insert command will destroy lmr (part of the ccb) immediately after return
  skv_lmr_triplet_t *lmr = new skv_lmr_triplet_t;
  *lmr = *aValueRDMADest;
  ReqCtx->mUserData = (void*)lmr;

  skv_status_t status = DestWorker->QueueDedicatedRequest( (skv_local_kv_req_ctx_t)ReqCtx,
                                                           SKV_LOCAL_KV_REQUEST_TYPE_ASYNC_INSERT_CLEANUP );

  return status;
}

skv_status_t
skv_local_kv_rocksdb::BulkInsert( skv_pds_id_t aPDSId,
                                  skv_lmr_triplet_t *aLocalBuffer,
                                  skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_BULK_INSERT, aCookie );
  kvReq->mRequest.mBulkInsert.mPDSId = aPDSId;
  kvReq->mRequest.mBulkInsert.mLocalBuffer = *aLocalBuffer;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t skv_local_kv_rocksdb_worker_t::PerformBulkInsert( skv_local_kv_request_t *aReq )
{
  struct skv_local_kv_bulkinsert_request_t *BIReq = &aReq->mRequest.mBulkInsert;
  int             LocalBufferSize = BIReq->mLocalBuffer.GetLen();
  char*           LocalBufferAddr = (char *)BIReq->mLocalBuffer.GetAddr();
  it_lmr_handle_t LocalBufferLMR = BIReq->mLocalBuffer.GetLMRHandle();

  BegLogLine(  SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem:: "
    << " PDSId: " << BIReq->mPDSId
    << " LocalBufferSize: " << LocalBufferSize
    << " LocalBuffer: " << (void *) LocalBufferAddr
    << " LocalBufferLMR: " << (void *) LocalBufferLMR
    << EndLogLine;

  // Layout of data in buffer { KeySize, ValueSize, Key, Value }
  int TotalProcessed = 0;
  skv_status_t LoopStatus = SKV_SUCCESS;
  static int RowsProcessed = 0;
  while( TotalProcessed < LocalBufferSize )
  {
    char* KeyPtr    = NULL;
    char* ValuePtr  = NULL;
    int   KeySize   = -1;
    int   ValueSize = -1;

    int RowLen = skv_bulk_insert_get_key_value_refs( LocalBufferAddr,
                                                     &KeyPtr,
                                                     KeySize,
                                                     &ValuePtr,
                                                     ValueSize );

    AssertLogLine( ( KeySize > 0 ) && ( KeySize <= SKV_KEY_LIMIT ) )
      << "skv_local_kv_asyncmem: ERROR: "
      << " KeySize: " << KeySize
      << " SKV_KEY_LIMIT: " << SKV_KEY_LIMIT
      << " TotalProcessed: " << TotalProcessed
      << " LocalBufferSize: "  << LocalBufferSize
      << EndLogLine;

    // Check if the key exists
    rocksdb::Slice *key = new rocksdb::Slice( MakeKey( aReq->mRequest.mBulkInsert.mPDSId,
                                                       KeyPtr,
                                                       KeySize ) );

    std::string value;
    rocksdb::Status rs = mDBAccess->LookupData( *key, &value );

    int TotalSize = KeySize + ValueSize;

 #if 0 // SKV_LOCAL_KV_BACKEND_LOG
    int BytesInRow = skv_bulk_insert_get_total_len( LocalBufferAddr );

    HexDump FxString( LocalBufferAddr, BytesInRow );

    BegLogLine(  SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_server_bulk_insert_command_sm::Execute(): "
      << " TotalSize: " << TotalSize
      << " KeySize: " << KeySize
      << " ValueSize: " << ValueSize
      << " RowsProcessed: " << RowsProcessed
      << " BytesInRow: " << BytesInRow
      << " FxString: " << FxString
      << EndLogLine;
#endif
    LocalBufferAddr += RowLen;
    TotalProcessed += RowLen;
    RowsProcessed++;

    if( rs.ok() )
    {
      static unsigned long long DupCount = 0;
      LoopStatus = SKV_ERRNO_RECORD_ALREADY_EXISTS;

      DupCount++;
      BegLogLine( 0 )
        << "skv_local_kv_asyncmem: DUP_COUNT: "
        << DupCount
        << EndLogLine;

      // SKV_SERVER_BULK_INSERT_DISPATCH_ERROR_RESP( SKV_ERRNO_RECORD_ALREADY_EXISTS, 0 );
      continue;
    }

    /****************************************************
     * Insert the record into local store.
     ****************************************************/
    rocksdb::Slice ins_value = rocksdb::Slice( ValuePtr, ValueSize );
    rs = mDBAccess->PutData( *key, ins_value );

    AssertLogLine( rs.ok() )
      << "skv_local_kv_asyncmem: ERROR:: "
      << " status: " << rs.ToString().c_str()
      << EndLogLine;

    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: BulkInserted: "
      << " key: " << key->ToString().c_str()
      << " keySize: " << key->size()
      << " vSize: " << ins_value.size()
      << " reqVSize: " << ValueSize
      << EndLogLine;
    /****************************************************/

    ReleaseKey( *key );
  }
  return InitKVEvent( aReq->mCookie, LoopStatus );
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
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE, aCookie );
  kvReq->mRequest.mRetrieve.mPDSId = aPDSId;
  memcpy( &kvReq->mData[ 0 ],
          aKeyData,
          aKeySize>SKV_CONTROL_MESSAGE_SIZE ? SKV_CONTROL_MESSAGE_SIZE : aKeySize );
  kvReq->mRequest.mRetrieve.mKeyData = &kvReq->mData[ 0 ];
  kvReq->mRequest.mRetrieve.mKeySize = aKeySize;
  kvReq->mRequest.mRetrieve.mValueOffset = aValueOffset;
  kvReq->mRequest.mRetrieve.mValueSize = aValueSize;
  kvReq->mRequest.mRetrieve.mFlags = aFlags;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}
skv_status_t skv_local_kv_rocksdb_worker_t::PerformRetrieve( skv_local_kv_request_t *aReq )
{
  skv_status_t status;
  size_t TotalSize = 0;
  skv_lmr_triplet_t StoredValueRep;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb:: retrieving: " << aReq->mRequest.mRetrieve.mValueSize
    << EndLogLine;

  rocksdb::Slice *key = new rocksdb::Slice( MakeKey( aReq->mRequest.mRetrieve.mPDSId,
                                                     aReq->mRequest.mRetrieve.mKeyData,
                                                     aReq->mRequest.mRetrieve.mKeySize ) );
  skv_local_kv_rocksdb_reqctx_t *reqCtx = NULL;


  skv_rdma_string *value;
  value = new skv_rdma_string();
  size_t expSize = aReq->mRequest.mRetrieve.mValueSize;

  rocksdb::Status rs;
  rs = mDBAccess->GetData( *key, value, expSize );

  if( rs.ok() ) {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: rocksdb.Get() complete. len=" << value->length()
      << EndLogLine;

    status = SKV_SUCCESS;

    TotalSize = value->length();
    if( aReq->mRequest.mRetrieve.mValueOffset > TotalSize )
    {
      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "skv_local_kv_rocksdb: retrieve offset out of range"
        << " vlen=" << TotalSize
        << " rsize=" << aReq->mRequest.mRetrieve.mValueSize
        << " roffs=" << aReq->mRequest.mRetrieve.mValueOffset
        << EndLogLine;
      status = SKV_ERRNO_VALUE_TOO_LARGE;
    }
  }
  else
  {
    status = SKV_ERRNO_ELEM_NOT_FOUND;
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: requested key not found: " << key->ToString().c_str()
      << EndLogLine;
  }

  ReleaseKey( *key );
  delete key;

//  if( status == SKV_SUCCESS )
//    status = mDataBuffer->AcquireDataArea( aReq->mRequest.mRetrieve.mValueSize, StoredValueRep );

  if( status == SKV_SUCCESS )
  {
    int RetrieveSize = aReq->mRequest.mRetrieve.mValueSize;
    // adjust the max retrieve size to fit the available data and the client candidate buffer
    if( RetrieveSize > TotalSize - aReq->mRequest.mRetrieve.mValueOffset )
      RetrieveSize = TotalSize - aReq->mRequest.mRetrieve.mValueOffset;


    StoredValueRep.InitAbs( mDataBuffer->GetLMR(),
                            (char*)((uintptr_t)value->data() + aReq->mRequest.mRetrieve.mValueOffset),
                            RetrieveSize );

    if( !(aReq->mRequest.mRetrieve.mFlags & SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG) )
    {
      status = SKV_ERRNO_NEED_DATA_TRANSFER;
    }
  }

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: LMR=" << StoredValueRep
    << " status=" << skv_status_to_string( status )
    << EndLogLine;

  reqCtx = new skv_local_kv_rocksdb_reqctx_t;
  reqCtx->mWorker= this;
  reqCtx->mUserData = value;

  status = InitKVRDMAEvent( aReq->mCookie,
                            &StoredValueRep,
                            (skv_local_kv_req_ctx_t)reqCtx,
                            TotalSize,
                            status );
  return status;
}
skv_status_t
skv_local_kv_rocksdb::RetrievePostProcess( skv_local_kv_req_ctx_t aReqCtx )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: completing retrieve:"
    << " reqCtx: " << (skv_lmr_triplet_t*)aReqCtx
    << EndLogLine;

  /* create an async request, make sure it goes to the worker thread
   * that created the data area to avoid locks on the databuffer
   */
  skv_local_kv_rocksdb_reqctx_t *ReqCtx = (skv_local_kv_rocksdb_reqctx_t*)aReqCtx;
  skv_local_kv_rocksdb_worker_t *DestWorker = ReqCtx->mWorker;
  skv_status_t status = DestWorker->QueueDedicatedRequest( aReqCtx,
                                                           SKV_LOCAL_KV_REQUEST_TYPE_ASYNC_RETRIEVE_CLEANUP );

  return status;
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
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE_N, aCookie );
  kvReq->mRequest.mRetrieveN.mPDSId = aPDSId;

  kvReq->mRequest.mRetrieveN.mRetrievedKeysSizesSegs = aRetrievedKeysSizesSegs;

  size_t copy_size = aStartingKeySize;
  // make sure the copy size is limited to the message size
  if( aStartingKeySize > SKV_CONTROL_MESSAGE_SIZE )
    copy_size = SKV_CONTROL_MESSAGE_SIZE;
  memcpy( &kvReq->mData[ 0 ],
          aStartingKeyData,
          copy_size );
  // startkeysize of zero indicates, no starting key
  kvReq->mRequest.mRetrieveN.mStartingKeyData = &kvReq->mData[ 0 ];
  kvReq->mRequest.mRetrieveN.mStartingKeySize = copy_size;
  kvReq->mRequest.mRetrieveN.mListOfKeysMaxCount = aListOfKeysMaxCount;
  kvReq->mRequest.mRetrieveN.mFlags = aFlags;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb:: retrieveNkeys storing request"
    << " MaxKeyCount: " << kvReq->mRequest.mRetrieveN.mListOfKeysMaxCount
    << " KeyData@: " << (void*)kvReq->mRequest.mRetrieveN.mStartingKeyData
    << " KeySize: " << kvReq->mRequest.mRetrieveN.mStartingKeySize
    << EndLogLine;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t
skv_local_kv_rocksdb_worker_t::PerformRetrieveNKeys( skv_local_kv_request_t *aReq )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_local_kv_retrieveN_request_t *RNReq = &aReq->mRequest.mRetrieveN;
  int RetrievedKeysCount = 0;
  int RetrievedKeysSizesSegsCount = 0;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb:: retrieveNkeys starting..."
    << " PDSid: " << RNReq->mPDSId
    << " MaxKeyCount: " << RNReq->mListOfKeysMaxCount
    << " KeyData@: " << (void*)RNReq->mStartingKeyData
    << " KeySize: " << RNReq->mStartingKeySize
    << " Flags: " << RNReq->mFlags
    << EndLogLine;

  rocksdb::Slice *startKey = NULL;

  if( !(RNReq->mFlags & SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG)
      || (RNReq->mFlags & SKV_CURSOR_WITH_STARTING_KEY_FLAG) )
  {
    startKey = new rocksdb::Slice( MakeKey( RNReq->mPDSId,
                                            RNReq->mStartingKeyData,
                                            RNReq->mStartingKeySize ) );
  }

  // create a prefix slice out of the PDSid to create an iterator that only iterates this single PDS
  rocksdb::Slice PDSPrefix = rocksdb::Slice( (const char*)&( RNReq->mPDSId ), sizeof( skv_pds_id_t) );
  rocksdb::Iterator *iter = mDBAccess->NewIterator( startKey, &PDSPrefix );

  StrongAssertLogLine( iter != NULL )
    << "skv_local_kv_rocksdb: iterator creation failed, cannot proceed."
    << EndLogLine;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: After starting iterator: "
    << " *StartingKeyPtr: " << (startKey!=NULL ? startKey->ToString( true ).c_str() : "EMPTY")
    << " (iter != end()): " << iter->Valid()
    << " aFlags: " << RNReq->mFlags
    << EndLogLine;

  if( !(RNReq->mFlags & SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG)
      || (RNReq->mFlags & SKV_CURSOR_WITH_STARTING_KEY_FLAG) )
    ReleaseKey( *startKey );


  // We do not need to send the starting key (unless it's the first key)
  if( iter->Valid() && !( RNReq->mFlags & SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG) )
    iter->Next();

  skv_lmr_triplet_t *keySizeLMR = new skv_lmr_triplet_t;
  int keySizeSpace = RNReq->mListOfKeysMaxCount * (sizeof(int) + SKV_KEY_LIMIT );
  status = mDataBuffer->AcquireDataArea( keySizeSpace, keySizeLMR );

  int IterCount = 0;
  while( (status == SKV_SUCCESS) &&
        iter->Valid() &&
        ( IterCount < RNReq->mListOfKeysMaxCount) )
  {
    int Index = 2 * IterCount;
    rocksdb::Slice key = iter->key();
    int pureKeySize = key.size() - sizeof( skv_pds_id_t );

    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb_worker_t: "
      << " Index: " << Index
      << " aListOfKeysMaxCount: " << RNReq->mListOfKeysMaxCount
      << " Key: " << iter->key().ToString( true ).c_str()
      << " keySize: " << key.size()
      << " pureKeySize: " << pureKeySize
      << EndLogLine;

    // need to skip unreleated PDS data until the prefix iterator is working
    if( *(skv_pds_id_t*)key.data() != RNReq->mPDSId )
    {
      iter->Next();
      continue;
    }

    char *keySizePtr = (char*)keySizeLMR->GetAddr();
    char *keyDataPtr = (char*)keySizeLMR->GetAddr() + sizeof( int );

    RNReq->mRetrievedKeysSizesSegs[Index].InitAbs( keySizeLMR->GetLMRHandle(),
                                                   keySizePtr,
                                                   sizeof( int ) );

    RNReq->mRetrievedKeysSizesSegs[Index + 1].InitAbs( keySizeLMR->GetLMRHandle(),
                                                       keyDataPtr,
                                                       pureKeySize );
    *(int*)keySizePtr = pureKeySize;
    memcpy( keyDataPtr,
            key.data() + sizeof( skv_pds_id_t ),
            pureKeySize );

    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: Initialized LMRs :a" << RNReq->mRetrievedKeysSizesSegs[ Index ]
      << " b" << RNReq->mRetrievedKeysSizesSegs[ Index + 1 ]
      << EndLogLine;

    IterCount++;
    iter->Next();
  }

  skv_local_kv_rocksdb_reqctx_t *reqCtx = new skv_local_kv_rocksdb_reqctx_t;
  reqCtx->mWorker= this;
  reqCtx->mUserData = keySizeLMR;

  RetrievedKeysCount = IterCount;
  RetrievedKeysSizesSegsCount = 2 * IterCount;

  if( !iter->Valid() )
    status = SKV_ERRNO_END_OF_RECORDS;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb:: retrieveNkeys completed"
    << " KeyCount: " << RetrievedKeysCount
    << " SegCount: " << RetrievedKeysSizesSegsCount
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  // cleanup resources if there was no key
  // state machine will not go into postprocess if no key retrieved
  if( RetrievedKeysCount == 0 )
  {
    mDataBuffer->ReleaseDataArea( keySizeLMR );
    delete reqCtx;
    reqCtx = NULL;
  }

  status = InitKVEvent( aReq->mCookie,
                        (skv_local_kv_req_ctx_t)reqCtx,
                        RNReq->mRetrievedKeysSizesSegs,
                        RetrievedKeysCount,
                        RetrievedKeysSizesSegsCount,
                        status );
  delete iter;
  return status;
}
skv_status_t
skv_local_kv_rocksdb::RetrieveNKeysPostProcess( skv_local_kv_req_ctx_t aReqCtx )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: completing retrieveN:"
    << " reqCtx: " << (void*)aReqCtx
    << EndLogLine;

  /* create an async request, make sure it goes to the worker thread
   * that created the data area to avoid locks on the databuffer
   */
  skv_local_kv_rocksdb_reqctx_t *ReqCtx = (skv_local_kv_rocksdb_reqctx_t*)aReqCtx;

  // need to create a copy of the lmr because the main server insert command will destroy lmr (part of the ccb) immediately after return
  skv_lmr_triplet_t *lmr = new skv_lmr_triplet_t;
  *lmr = *(skv_lmr_triplet_t*)ReqCtx->mUserData;

  ReqCtx->mUserData = (void*)lmr;
  skv_local_kv_rocksdb_worker_t *DestWorker = ReqCtx->mWorker;
  skv_status_t status = DestWorker->QueueDedicatedRequest( (skv_local_kv_req_ctx_t)ReqCtx,
                                                           SKV_LOCAL_KV_REQUEST_TYPE_ASYNC_RETRIEVE_NKEYS_CLEANUP );

  return status;
}


skv_status_t
skv_local_kv_rocksdb::Remove( skv_pds_id_t aPDSId,
                              char* aKeyData,
                              int aKeySize,
                              skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_REMOVE, aCookie );
  kvReq->mRequest.mRemove.mPDSId = aPDSId;
  memcpy( &kvReq->mData[ 0 ],
          aKeyData,
          aKeySize>SKV_CONTROL_MESSAGE_SIZE ? SKV_CONTROL_MESSAGE_SIZE : aKeySize );
  kvReq->mRequest.mRemove.mKeyData = &kvReq->mData[ 0 ];
  kvReq->mRequest.mRemove.mKeySize = aKeySize;

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}
skv_status_t skv_local_kv_rocksdb_worker_t::PerformRemove( skv_local_kv_request_t *aReq )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: deleting: pdsid=" << aReq->mRequest.mRemove.mPDSId
    << " key=" << *(int*)(aReq->mRequest.mRemove.mKeyData)
    << EndLogLine;

  rocksdb::Slice *key = new rocksdb::Slice( MakeKey( aReq->mRequest.mRemove.mPDSId,
                                                     aReq->mRequest.mRemove.mKeyData,
                                                     aReq->mRequest.mRemove.mKeySize ) );

  // rocksdb doesn't consider it an error to delete non-existing keys.
  // since SKV does, we need to check if the key is available and set the status accordingly


  std::string *value = new std::string;
  bool found_entry = ( mDBAccess->LookupData( *key, value ).ok() );

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_rocksdb: delete item precheck=" << found_entry
    << " mayex=" << mDBAccess->DataKeyMayExist( *key )
    << EndLogLine;

  if( found_entry )
  {
    rocksdb::Status rs = mDBAccess->DeleteData( *key );
    status = rocksdb_status_to_skv( rs );

    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_rocksdb: item deleted. status=" << skv_status_to_string( status )
      << EndLogLine;
  }
  else
    status = SKV_ERRNO_ELEM_NOT_FOUND;

  ReleaseKey( *key );
  delete key;
  delete value;

  status = InitKVEvent( aReq->mCookie, status );
  return status;
}

skv_status_t
skv_local_kv_rocksdb::Lock( skv_pds_id_t *aPDSId,
                            skv_key_value_in_ctrl_msg_t *aKeyValue,
                            skv_rec_lock_handle_t *aRecLock )
{
  return SKV_ERRNO_NOT_IMPLEMENTED;
}
skv_status_t
skv_local_kv_rocksdb::Unlock( skv_rec_lock_handle_t aLock )
{
  return SKV_ERRNO_NOT_IMPLEMENTED;
}

skv_status_t
skv_local_kv_rocksdb::RDMABoundsCheck( const char* aContext,
                                       char* aMem,
                                       int aSize )
{
  return SKV_ERRNO_NOT_IMPLEMENTED;
}

skv_status_t
skv_local_kv_rocksdb::Allocate( int aBuffSize,
                                skv_lmr_triplet_t *aRDMARep )
{
  return mMasterProcessing.GetDataBuffer()->AcquireDataArea( aBuffSize, aRDMARep );
}

skv_status_t
skv_local_kv_rocksdb::Deallocate( skv_lmr_triplet_t *aRDMARep )
{
  return mMasterProcessing.GetDataBuffer()->ReleaseDataArea( aRDMARep );
}

skv_status_t
skv_local_kv_rocksdb::CreateCursor( char* aBuff,
                                    int aBuffSize,
                                    skv_server_cursor_hdl_t* aServCursorHdl,
                                    skv_local_kv_cookie_t* aCookie )
{
  return SKV_ERRNO_NOT_IMPLEMENTED;
}

skv_status_t
skv_local_kv_rocksdb::DumpImage( char* aCheckpointPath )
{
  return SKV_ERRNO_NOT_IMPLEMENTED;
}

skv_status_t skv_local_kv_rocksdb_worker_t::PerformAsyncInsertCleanup( skv_local_kv_request_t *aReq )
{
  skv_local_kv_rocksdb_reqctx_t* rctx = (skv_local_kv_rocksdb_reqctx_t*)(aReq->mReqCtx);
  skv_lmr_triplet_t *ValueRDMADest = (skv_lmr_triplet_t*)rctx->mUserData;
  skv_status_t status = mDataBuffer->ReleaseDataArea( ValueRDMADest );

  delete ValueRDMADest;
  delete rctx;
  return status;
}

skv_status_t skv_local_kv_rocksdb_worker_t::PerformAsyncRetrieveCleanup( skv_local_kv_request_t *aReq )
{
  skv_local_kv_rocksdb_reqctx_t* rctx = (skv_local_kv_rocksdb_reqctx_t*)(aReq->mReqCtx);
  skv_rdma_string *str = (skv_rdma_string*)rctx->mUserData;

  delete str;
  delete rctx;
  return SKV_SUCCESS;
}

skv_status_t skv_local_kv_rocksdb_worker_t::PerformAsyncRetrieveNKeysCleanup( skv_local_kv_request_t *aReq )
{
  skv_local_kv_rocksdb_reqctx_t* rctx = (skv_local_kv_rocksdb_reqctx_t*)(aReq->mReqCtx);
  skv_lmr_triplet_t *keysLMR = (skv_lmr_triplet_t*)rctx->mUserData;

  mDataBuffer->ReleaseDataArea( keysLMR );

  delete keysLMR;
  delete rctx;
  return SKV_SUCCESS;
}
