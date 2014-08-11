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
 */

#ifndef SKV_LOCAL_KV_BACKEND_LOG
#define SKV_LOCAL_KV_BACKEND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_LOCAL_KV_ASYNCMEM_PROCESSING_LOG
#define SKV_LOCAL_KV_ASYNCMEM_PROCESSING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_INSERT_TRACE
#define SKV_SERVER_INSERT_TRACE ( 0 )
#endif

#include <unistd.h>
#include <common/skv_types.hpp>
#include <common/skv_mutex.hpp>
#include <utils/skv_trace_clients.hpp>

#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>

#include <server/skv_local_kv_types.hpp>
#include <server/skv_local_kv_request.hpp>
#include <server/skv_local_kv_request_queue.hpp>
#include <server/skv_local_kv_event_queue.hpp>

#include <server/skv_local_kv_asyncmem.hpp>

static
void AsyncProcessing( skv_local_kv_asyncmem *aBackEnd )
{
  BegLogLine( SKV_LOCAL_KV_ASYNCMEM_PROCESSING_LOG )
    << "AsyncProcessing: Entering thread"
    << EndLogLine;

  skv_local_kv_request_queue_t* RequestQueue = aBackEnd->GetRequestQueue();
  while( aBackEnd->KeepProcessing() )
  {
    skv_status_t status;
    skv_local_kv_request_t *nextRequest = RequestQueue->GetRequest();
    if( nextRequest )
    {
      BegLogLine( SKV_LOCAL_KV_ASYNCMEM_PROCESSING_LOG )
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

  BegLogLine( SKV_LOCAL_KV_ASYNCMEM_PROCESSING_LOG )
    << "AsyncProcessing: Exiting thread"
    << EndLogLine;
}

skv_status_t
skv_local_kv_asyncmem::Init( int aRank,
                          int aNodeCount,
                          skv_server_internal_event_manager_if_t *aInternalEventMgr,
                          it_pz_handle_t aPZ,
                          char* aCheckpointPath )
{
  skv_status_t status;
  mMyRank = aRank;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem::Init(): Entering..."
    << EndLogLine;

  mEventQueue.Init();
  mRequestQueue.Init();

  /************************************************************
   * Initialize the local partition dataset manager
   ***********************************************************/
  status = mPDSManager.Init( aRank,
                             aNodeCount,
                             aInternalEventMgr,
                             aPZ,
                             aCheckpointPath );
  StrongAssertLogLine( status == SKV_SUCCESS )
    << "skv_local_kv_asyncmem::Init():: ERROR:: mPDSManager.Init() failed. "
    << " status: " << skv_status_to_string( status )
    << " Rank: " << aRank
    << " PartitionSize: " << aNodeCount
    << EndLogLine;
  /***********************************************************/

  mKeepProcessing = true;
  mReqProcessor = new std::thread(AsyncProcessing, this );

  return status;
}

skv_status_t
skv_local_kv_asyncmem::Exit()
{
  mKeepProcessing = false;
  return mPDSManager.Finalize();
}

skv_status_t
skv_local_kv_asyncmem::CancelContext( skv_local_kv_req_ctx_t *aReqCtx )
{
  // allows potential cleanup of open resources in case of outside error
  return SKV_SUCCESS;
}


skv_status_t
skv_local_kv_asyncmem::GetDistribution(skv_distribution_t **aDist,
                                       skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_GET_DISTRIBUTION, aCookie );
  // getdistribution doesn't have further parameters

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t
skv_local_kv_asyncmem::PerformGetDistribution(skv_local_kv_request_t *aReq )
{
  skv_status_t status = SKV_SUCCESS;
  skv_distribution_t *dist = mPDSManager.GetDistribution();
  if( dist == NULL )
    status = SKV_ERRNO_NO_BUFFER_AVAILABLE;

  BegLogLine(1)
    << "GET_DIST REQUEST"
    << " dist:" << (void*)dist
    << EndLogLine;

  status = InitKVEvent( aReq->mCookie, dist, status );
  return status;
}


skv_status_t
skv_local_kv_asyncmem::PDS_Open( char *aPDSName,
                                 skv_pds_priv_t aPrivs,
                                 skv_cmd_open_flags_t aFlags,
                                 skv_pds_id_t *aPDSId,
                                 skv_local_kv_cookie_t *aCookie )
{
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
    << "skv_local_kv_asyncmem: Open Request stored:"
    << " PDSName:" << kvReq->mRequest.mOpen.mPDSName
    << " Priv:" << kvReq->mRequest.mOpen.mPrivs
    << " Flags: " << kvReq->mRequest.mOpen.mFlags
    << EndLogLine;

  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t
skv_local_kv_asyncmem::PerformOpen( skv_local_kv_request_t *aReq )
{
  skv_pds_id_t PDSId;
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "OPEN REQUEST"
    << " PDSName:" << aReq->mRequest.mOpen.mPDSName
    << " Priv:" << aReq->mRequest.mOpen.mPrivs
    << " Flags: " << aReq->mRequest.mOpen.mFlags
    << EndLogLine;
  skv_status_t status = mPDSManager.Open( aReq->mRequest.mOpen.mPDSName,
                                          aReq->mRequest.mOpen.mPrivs,
                                          aReq->mRequest.mOpen.mFlags,
                                          &PDSId );
  BegLogLine(1)
    << "OPEN REQUEST COMPLETE"
    << EndLogLine;

  status = InitKVEvent( aReq->mCookie, PDSId, status );
  return status;
}


skv_status_t
skv_local_kv_asyncmem::PDS_Stat( skv_pdscntl_cmd_t aCmd,
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

skv_status_t
skv_local_kv_asyncmem::PerformStat( skv_local_kv_request_t *aReq )
{
  skv_status_t status = mPDSManager.Stat( aReq->mRequest.mStat.mCmd, aReq->mRequest.mStat.mPDSAttr );
  status = InitKVEvent( aReq->mCookie, aReq->mRequest.mStat.mCmd, aReq->mRequest.mStat.mPDSAttr, status );
  return status;
}


skv_status_t
skv_local_kv_asyncmem::PDS_Close( skv_pds_attr_t *aPDSAttr,
                               skv_local_kv_cookie_t *aCookie )
{
  skv_local_kv_request_t *kvReq = mRequestQueue.AcquireRequestEntry();
  if( !kvReq )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  kvReq->InitCommon( SKV_LOCAL_KV_REQUEST_TYPE_CLOSE, aCookie );
  kvReq->mRequest.mStat.mCmd = SKV_PDSCNTL_CMD_CLOSE;
  memcpy( &kvReq->mData[ 0 ], aPDSAttr, sizeof( skv_pds_attr_t ) );
  kvReq->mRequest.mStat.mPDSAttr = (skv_pds_attr_t*)&kvReq->mData[ 0 ];

  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t
skv_local_kv_asyncmem::PerformClose( skv_local_kv_request_t *aReq )
{
  skv_status_t status = mPDSManager.Close( aReq->mRequest.mStat.mPDSAttr );
  status = InitKVEvent( aReq->mCookie, aReq->mRequest.mStat.mCmd, aReq->mRequest.mStat.mPDSAttr, status );
  return status;
}


/******************************************************
 * insertion helper functions
 */
static inline
skv_status_t
AllocateAndMoveKey( skv_cmd_RIU_req_t *aReq,
                    int aKeyValueSize,
                    skv_lmr_triplet_t *aNewRecordAllocRep,
                    skv_pds_manager_if_t *aPDSManager )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem::AllocateAndMoveKey(): allocating"
    << " KeyValueSize: " << aKeyValueSize
    << " RecordAddr: " << (void*)aNewRecordAllocRep->GetAddr()
    << " KeySize: " << aReq->mKeyValue.mKeySize
    << EndLogLine;

  skv_status_t status = aPDSManager->Allocate( aKeyValueSize,
                                               aNewRecordAllocRep );
  if( status != SKV_SUCCESS )
  {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_asyncmem::AllocateAndMoveKey(): Record allocation failed. status: " << status
      << EndLogLine;

    return status;
  }

  // Move the key
  memcpy( (char *)aNewRecordAllocRep->GetAddr(),
          aReq->mKeyValue.mData,
          aReq->mKeyValue.mKeySize );
  /******************************************/

  return status;
}

/*****************************************
 * Remove and deallocate the old from the local store
 *****************************************/
static inline
skv_status_t
RemoveLocal( skv_cmd_RIU_req_t *aReq,
             int aKeySize,
             char *aAddr,
             skv_pds_manager_if_t *aPDSManager )
{
  skv_status_t status = aPDSManager->Remove( aReq->mPDSId,
                                             aReq->mKeyValue.mData,
                                             aKeySize );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_local_kv_asyncmem::RemoveLocal():: ERROR: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  return status;
}

/****************************************************
 * Insert the record into local store.
 ****************************************************/
static inline
skv_status_t
InsertLocal( skv_cmd_RIU_req_t *aReq,
             char *aAddr,
             int aKeySize,
             int aTotalValueSize,
             skv_pds_manager_if_t *aPDSManager,
             int aMyRank )
{
  gSKVServerInsertInTreeStart.HitOE( SKV_SERVER_INSERT_TRACE,
                                     "SKVServerInsertInTree",
                                     aMyRank,
                                     gSKVServerInsertInTreeStart );

  skv_status_t status = aPDSManager->Insert( aReq->mPDSId,
                                             aAddr,
                                             aKeySize,
                                             aTotalValueSize );

  gSKVServerInsertInTreeFinis.HitOE( SKV_SERVER_INSERT_TRACE,
                                     "SKVServerInsertInTree",
                                     aMyRank,
                                     gSKVServerInsertInTreeFinis );

  AssertLogLine( status == SKV_SUCCESS )
    << "skv_local_kv_asyncmem::InsertLocal():: ERROR: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  return status;
}
/******************************************************
 * END: insertion helper functions
 */

skv_status_t
skv_local_kv_asyncmem::Insert( skv_cmd_RIU_req_t *aReq,
                            skv_status_t aCmdStatus,
                            skv_lmr_triplet_t *aStoredValueRep,
                            skv_lmr_triplet_t *aValueRDMADest,
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

skv_status_t
skv_local_kv_asyncmem::PerformInsert( skv_local_kv_request_t *aKVReq )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;
  skv_status_t CmdStatus = aKVReq->mRequest.mInsert.mCmdStatus;
  skv_cmd_RIU_req_t *Req = aKVReq->mRequest.mInsert.mReqData;
  skv_lmr_triplet_t *StoredValueRep = &aKVReq->mRequest.mInsert.mStoredValueRep;
  skv_lmr_triplet_t ValueRDMADest;

  int KeySize = Req->mKeyValue.mKeySize;
  int ValueSize = Req->mKeyValue.mValueSize;
  int TotalValueSize = ValueSize + Req->mOffset;

  AssertLogLine( TotalValueSize >= 0 &&
                 TotalValueSize < SKV_VALUE_LIMIT )
    << "skv_local_kv_asyncmem::Insert():: ERROR: "
    << "TotalValueSize: " << TotalValueSize
    << EndLogLine;

  // Check if there's enough space for Key/Value
  int KeyValueSize = KeySize + TotalValueSize;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG)
    << "skv_local_kv_asyncmem::Insert():: Entering with: "
    << " Key: " << *(int*)(Req->mKeyValue.mData)
    << " KeySize: " << KeySize
    << " ValueSize: " << ValueSize
    << " TotalValueSize: " << TotalValueSize
    << " KeyValueSize: " << KeyValueSize
    << " Flags: " << (void*)Req->mFlags
    << EndLogLine;

  int LocalValueSize = StoredValueRep->GetLen();

  /********************************************************
   * Record Exists
   ********************************************************/
  if( CmdStatus == SKV_SUCCESS )
  {
    switch( Req->mFlags & (SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE
        | SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP
        | SKV_COMMAND_RIU_UPDATE
        | SKV_COMMAND_RIU_APPEND) )
    {
      case SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP:
        /********************************************************
         * we overwrite WITHIN EXISTING RECORD BOUNDS ONLY
         ********************************************************/

        BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
            << "skv_local_kv_asyncmem::Insert():: OVERWRITE_ON_DUP"
            << " ValueSize: " << ValueSize
            << " offs: " << Req->mOffset
            << EndLogLine;

        if( LocalValueSize < TotalValueSize )
        {
          status = SKV_ERRNO_VALUE_TOO_LARGE;
          break;
        }

        ValueRDMADest.InitAbs( StoredValueRep->GetLMRHandle(),
                               (char *) StoredValueRep->GetAddr() + Req->mOffset,
                               ValueSize );
        status = SKV_SUCCESS;
        break;

      case SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE:
        /********************************************************
         * Record Exists, Expansion Allowed
         ********************************************************/

        // Expandable insert option is set and the record is found
        // Do we need to expand?

        BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
          << "skv_local_kv_asyncmem::Insert():: EXPAND_VALUE"
          << " LocalValueSize: " << LocalValueSize
          << " ValueSize: " << ValueSize
          << " TotalValueSize: " << TotalValueSize
          << EndLogLine;

        if( TotalValueSize > LocalValueSize )
        {
          if( !(Req->mFlags & SKV_COMMAND_RIU_INSERT_OVERLAPPING) )
          {
            BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
              << "skv_local_kv_asyncmem::Insert():: overlapping inserts not allowed "
              << " Req->mOffset: " << Req->mOffset
              << " LocalValueSize: " << LocalValueSize
              << " requires: SKV_COMMAND_RIU_INSERT_OVERLAPPING to be set"
              << EndLogLine;

            status = SKV_ERRNO_INSERT_OVERLAP;
            break;
          }

          /***************************************************************
           * Expand Value
           **************************************************************/
          // Allocate new
          skv_lmr_triplet_t NewRecordAllocRep;

          status = AllocateAndMoveKey( Req,
                                       KeyValueSize,
                                       &NewRecordAllocRep,
                                       &mPDSManager );

          if( status != SKV_SUCCESS )
            break;

          // copy existing value to new allocated record
          char* RecordPtr = (char *) NewRecordAllocRep.GetAddr() + KeySize;

          memcpy( RecordPtr,
                  (void *) StoredValueRep->GetAddr(),
                  LocalValueSize );

          RecordPtr += Req->mOffset;   // LocalValueSize;

          // Prepare LMR to cover the new to read fraction of the value only
          ValueRDMADest.InitAbs( NewRecordAllocRep.GetLMRHandle(),
                                 RecordPtr,
                                 ValueSize );

          // Remove old record and deallocate space
          status = RemoveLocal( Req,
                                KeySize,
                                (char *) StoredValueRep->GetAddr(),
                                &mPDSManager );

          // insert new record
          status = InsertLocal( Req,
                                (char *) NewRecordAllocRep.GetAddr(),
                                KeySize,
                                TotalValueSize,
                                &mPDSManager,
                                mMyRank );

          /*******************************************************************/
        }
        else
        {
          // Record was found, but no expansion is needed
          // Just make sure that the rdma_read is done to the
          // appropriate offset

          ValueRDMADest.InitAbs( StoredValueRep->GetLMRHandle(),
                                 (char *) StoredValueRep->GetAddr() + Req->mOffset,
                                 ValueSize );
          status = SKV_SUCCESS;
        }

        break;
        /********************************************************/

      case SKV_COMMAND_RIU_UPDATE:
        /********************************************************
         * Record exists and we update with POTENTIAL REALLOCATION of the record
         ********************************************************/

        // if the new size is exactly the previous size, there's nothing to adjust
        // just make sure the LMR is pointing to the right location
        if( ValueSize == StoredValueRep->GetLen() )
        {
          BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
            << "skv_local_kv_asyncmem::Insert():: UPDATE same length record, overwriting..."
            << " ValueSize: " << ValueSize
            << " offs: " << Req->mOffset
            << EndLogLine;

          ValueRDMADest.InitAbs( StoredValueRep->GetLMRHandle(),
                                 (char *) StoredValueRep->GetAddr() + Req->mOffset,
                                 ValueSize );
          status = SKV_SUCCESS;
        }
        // reallocate, copy and insert new updated record, if the size is different
        else
        {
          BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
            << "skv_local_kv_asyncmem::Insert():: UPDATE different length record, reallocating..."
            << " ValueSize: " << ValueSize
            << " offs: " << Req->mOffset
            << EndLogLine;

          status = RemoveLocal( Req,
                                KeySize,
                                (char *) StoredValueRep->GetAddr(),
                                &mPDSManager );

          skv_lmr_triplet_t NewRecordAllocRep;

          status = AllocateAndMoveKey( Req,
                                       KeyValueSize,
                                       &NewRecordAllocRep,
                                       &mPDSManager );

          if( status != SKV_SUCCESS )
            break;

          ValueRDMADest.InitAbs( NewRecordAllocRep.GetLMRHandle(),
                                 (char *) NewRecordAllocRep.GetAddr() + KeySize + Req->mOffset,
                                 ValueSize );

          status = InsertLocal( Req,
                                (char *) NewRecordAllocRep.GetAddr(),
                                KeySize,
                                TotalValueSize,
                                &mPDSManager,
                                mMyRank );

        }
        break;
        /********************************************************/

      case SKV_COMMAND_RIU_APPEND:
        /********************************************************
         * Record Exists, Append new data
         ********************************************************/

        TotalValueSize = LocalValueSize + ValueSize;

        KeyValueSize = KeySize + TotalValueSize;

        BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
          << "skv_local_kv_asyncmem::Insert():: APPEND "
          << " LocalValueSize: " << LocalValueSize
          << " ValueSize: " << ValueSize
          << " TotalValueSize: " << TotalValueSize
          << EndLogLine;

        if( TotalValueSize > LocalValueSize )
        {
          /***************************************************************
           * Expand Value
           **************************************************************/
          // Allocate new
          skv_lmr_triplet_t NewRecordAllocRep;

          status = AllocateAndMoveKey( Req,
                                       KeyValueSize,
                                       &NewRecordAllocRep,
                                       &mPDSManager );

          if( status != SKV_SUCCESS )
            break;

          char* RecordPtr = (char *) NewRecordAllocRep.GetAddr() + KeySize;

          memcpy( RecordPtr,
                  (void *) StoredValueRep->GetAddr(),
                  LocalValueSize );

          // setup to add new record
          RecordPtr += LocalValueSize;

          // rdma read only the new record data
          ValueRDMADest.InitAbs( NewRecordAllocRep.GetLMRHandle(),
                                 RecordPtr,
                                 ValueSize );

          status = RemoveLocal( Req,
                                KeySize,
                                (char *) StoredValueRep->GetAddr(),
                                &mPDSManager );

          status = InsertLocal( Req,
                                (char *) NewRecordAllocRep.GetAddr(),
                                KeySize,
                                TotalValueSize,
                                &mPDSManager,
                                mMyRank );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_local_kv_asyncmem::Insert():: ERROR: "
            << " status: " << skv_status_to_string( status )
            << EndLogLine;
          /*******************************************************************/
        }
        else
        {
          // Record was found, but no expansion is needed
          // Just make sure that the rdma_read is done to the
          // appropriate offset

          ValueRDMADest.InitAbs( StoredValueRep->GetLMRHandle(),
                                 (char *) StoredValueRep->GetAddr() + Req->mOffset,
                                 ValueSize );
        }

        break;
        /********************************************************/

      default:   // mFlags
        /********************************************************
         * Record Exists and no special flags provided: error response
         ********************************************************/

        BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
          << " skv_local_kv_asyncmem::Insert(): record exists, returning ERRNO"
          << EndLogLine;

        status = SKV_ERRNO_RECORD_ALREADY_EXISTS;

        break;
        /********************************************************/

    }
  }
  else
  {
    /********************************************************
     * Record Does NOT Exist, do a general insert
     ********************************************************/

    /****************************************
     * Allocate space for the key and value
     ****************************************/
    skv_lmr_triplet_t NewRecordAllocRep;

    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG)
      << "skv_local_kv_asyncmem::Insert():: "
      << " Req->mKeyValue: " << Req->mKeyValue
      << EndLogLine;

    status = AllocateAndMoveKey( Req,
                                 KeyValueSize,
                                 &NewRecordAllocRep,
                                 &mPDSManager );

    if( status == SKV_SUCCESS )
    {
      // if no EXPAND requested the requested offset has to be 0
      if( !(Req->mFlags & SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE) )
        AssertLogLine( Req->mOffset == 0 )
          << "skv_local_kv_asyncmem::Insert():: ERROR: "
          << " Req->mOffset: " << Req->mOffset
          << EndLogLine;

      ValueRDMADest.InitAbs( NewRecordAllocRep.GetLMRHandle(),
                             (char *) NewRecordAllocRep.GetAddr() + KeySize + Req->mOffset,
                             ValueSize );

      status = InsertLocal( Req,
                            (char *) NewRecordAllocRep.GetAddr(),
                            KeySize,
                            TotalValueSize,
                            &mPDSManager,
                            mMyRank );

    }
  }
  /***********************************************************************/

  if( status != SKV_SUCCESS )
    return InitKVRDMAEvent( aKVReq->mCookie, &ValueRDMADest, status );

  /*******************************************************************
   * Get the data copied or rdma'd into the new record
   ******************************************************************/
  if( Req->mFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG )
  {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_asyncmem::Insert():: inserting value with FIT_IN_CTL_MSG"
      << " len: " << ValueSize
      << EndLogLine;

    memcpy( (void *) ValueRDMADest.GetAddr(),
            &Req->mKeyValue.mData[KeySize],
            ValueSize );

  }
  else
    // signal that this command is going to require async data transfer
    status = SKV_ERRNO_NEED_DATA_TRANSFER;

  status = InitKVRDMAEvent( aKVReq->mCookie, &ValueRDMADest, status );
  return status;
}

skv_status_t
skv_local_kv_asyncmem::Insert( skv_pds_id_t& aPDSId,
                               char* aRecordRep,
                               int aKeySize,
                               int aValueSize,
                               skv_local_kv_cookie_t *aCookie )
{
  /* ONLY USED BY BULK-INSERT. No need to go async here. The caller is already in async mode when we get here.
   * - in general, a back-end impl will have to be able to go async. One way would be to extract the args to
   *   call the other insert command.
   */
  return mPDSManager.Insert( aPDSId,
                             aRecordRep,
                             aKeySize,
                             aValueSize );
}

skv_status_t
skv_local_kv_asyncmem::InsertPostProcess(  skv_local_kv_req_ctx_t *aReqCtx,
                                           skv_lmr_triplet_t *aValueRDMADest,
                                           skv_local_kv_cookie_t *aCookie )
{
  // we would have to do the localKV insert from the RDMAed data if we had a real storage back end
  // we could release any request state (aReqCtx) if needed (for this back-end, we don't need that

  return InitKVEvent( aCookie, SKV_SUCCESS );
}


skv_status_t
skv_local_kv_asyncmem::BulkInsert( skv_pds_id_t aPDSId,
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

skv_status_t
skv_local_kv_asyncmem::PerformBulkInsert( skv_local_kv_request_t *aReq )
{
  struct skv_local_kv_bulkinsert_request_t *BIReq = &aReq->mRequest.mBulkInsert;
  int             LocalBufferSize = BIReq->mLocalBuffer.GetLen();
  char*           LocalBufferAddr = (char *)BIReq->mLocalBuffer.GetAddr();
  it_lmr_handle_t LocalBufferLMR = BIReq->mLocalBuffer.GetLMRHandle();

// \todo Adjust checksum handling to be covered by the back-end API
#ifdef SKV_BULK_LOAD_CHECKSUM
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
      << " PDSId: " << PDSId
      << " LocalBufferSize: " << LocalBufferSize
      << " LocalBuffer: " << (void *) LocalBufferAddr
      << " LocalBufferLMR: " << (void *) LocalBufferLMR
      << " RemoteBufferRMR: " << (void *) RemoteBufferRMR
      << " RemoteBufferAddr: " << (void *) RemoteBufferAddr
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

    SKV_SERVER_BULK_INSERT_DISPATCH_ERROR_RESP( SKV_ERRNO_CHECKSUM_MISMATCH, BufferChecksum );
  }
#if 0
  StrongAssertLogLine( BufferChecksum == RemoteBufferChecksum )
    << "skv_server_bulk_insert_command_sm::Execute(): ERROR: "
    << " BufferChecksum: " << BufferChecksum
    << " RemoteBufferChecksum: " << RemoteBufferChecksum
    << " PDSId: " << PDSId
    << " LocalBufferSize: " << LocalBufferSize
    << " LocalBuffer: " << (void *) LocalBufferAddr
    << " LocalBufferLMR: " << (void *) LocalBufferLMR
    << " RemoteBufferRMR: " << (void *) RemoteBufferRMR
    << " RemoteBufferAddr: " << (void *) RemoteBufferAddr
    << EndLogLine;
#endif
#endif

  BegLogLine(  SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_inmem:: "
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
      << "skv_server_bulk_insert_command_sm::Execute(): ERROR: "
      << " KeySize: " << KeySize
      << " SKV_KEY_LIMIT: " << SKV_KEY_LIMIT
      << " TotalProcessed: " << TotalProcessed
      << " LocalBufferSize: "  << LocalBufferSize
      << EndLogLine;

    // Check if the key exists
    skv_lmr_triplet_t ValueRepInStore;
    skv_status_t status = mPDSManager.Retrieve( BIReq->mPDSId,
                                                KeyPtr,
                                                KeySize,
                                                0,
                                                0,
                                                (skv_cmd_RIU_flags_t)0,
                                                &ValueRepInStore );

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

    if( status == SKV_SUCCESS )
    {
      static unsigned long long DupCount = 0;

      Deallocate( &BIReq->mLocalBuffer );
      LoopStatus = SKV_ERRNO_RECORD_ALREADY_EXISTS;

      DupCount++;
      BegLogLine( 0 )
        << "skv_server_bulk_insert_command_sm::Execute(): DUP_COUNT: "
        << DupCount
        << EndLogLine;

      // SKV_SERVER_BULK_INSERT_DISPATCH_ERROR_RESP( SKV_ERRNO_RECORD_ALREADY_EXISTS, 0 );
      continue;
    }

    skv_lmr_triplet_t NewRecordAllocRep;
    status = Allocate( TotalSize,
                       & NewRecordAllocRep );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_bulk_insert_command_sm::Execute(): ERROR:: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    char* LocalStoreAddr = (char *) NewRecordAllocRep.GetAddr();
    memcpy( LocalStoreAddr,
            KeyPtr,
            KeySize );

    memcpy( & LocalStoreAddr[ KeySize ],
            ValuePtr,
            ValueSize );

    /****************************************************
     * Insert the record into local store.
     ****************************************************/
    status = Insert( BIReq->mPDSId,
                     LocalStoreAddr,
                     KeySize,
                     ValueSize,
                     (skv_local_kv_cookie_t*)NULL );

    AssertLogLine( status == SKV_SUCCESS )
      << "skv_server_bulk_insert_command_sm::Execute(): ERROR:: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;
    /****************************************************/
  }
  return InitKVEvent( aReq->mCookie, LoopStatus );
}


skv_status_t
skv_local_kv_asyncmem::Lookup( skv_pds_id_t aPDSId,
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

skv_status_t
skv_local_kv_asyncmem::PerformLookup( skv_local_kv_request_t *aReq )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem::Lookup():: Entering"
    << EndLogLine;
  skv_status_t status;

  gSKVServerInsertRetrieveFromTreeStart.HitOE( SKV_SERVER_INSERT_TRACE,
                                               "SKVServerInsertRetrieveFromTree",
                                               mMyRank,
                                               gSKVServerInsertRetrieveFromTreeStart );

  skv_lmr_triplet_t StoredValueRep;

  // Check if the key exists
  status = mPDSManager.Retrieve( aReq->mRequest.mLookup.mPDSId,
                                 aReq->mRequest.mLookup.mKeyData,
                                 aReq->mRequest.mLookup.mKeySize,
                                 0,
                                 0,
                                 aReq->mRequest.mLookup.mFlags,
                                 &StoredValueRep );

  gSKVServerInsertRetrieveFromTreeFinis.HitOE( SKV_SERVER_INSERT_TRACE,
                                               "SKVServerInsertRetrieveFromTree",
                                               mMyRank,
                                               gSKVServerInsertRetrieveFromTreeFinis );

  status = InitKVEvent( aReq->mCookie, &StoredValueRep, status );
  return status;
}


skv_status_t
skv_local_kv_asyncmem::Retrieve( skv_pds_id_t aPDSId,
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

skv_status_t
skv_local_kv_asyncmem::PerformRetrieve( skv_local_kv_request_t *aReq )
{
  size_t TotalSize;
  skv_lmr_triplet_t StoredValueRep;
  skv_local_kv_retrieve_request_t *Retrieve = &aReq->mRequest.mRetrieve;

  skv_status_t status = mPDSManager.Retrieve( Retrieve->mPDSId,
                                              Retrieve->mKeyData,
                                              Retrieve->mKeySize,
                                              Retrieve->mValueOffset,
                                              Retrieve->mValueSize,
                                              Retrieve->mFlags,
                                              &StoredValueRep );

  int ValueOversize = 0;
  if (status == SKV_SUCCESS)
  {
    TotalSize = StoredValueRep.GetLen();
    ValueOversize = StoredValueRep.GetLen() - Retrieve->mValueSize;
    if( ValueOversize > 0 )
    {
      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "skv_local_kv_asyncmem:: ADJUSTING rdma_write length to match smaller client buffer"
        << " client: " << Retrieve->mValueSize
        << " store: " << StoredValueRep.GetLen()
        << EndLogLine;

      StoredValueRep.SetLenIfSmaller( Retrieve->mValueSize );
    }

    if( !(Retrieve->mFlags & SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG) )
      status = SKV_ERRNO_NEED_DATA_TRANSFER;
  }
  else
    TotalSize = 0;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem:: storing valueRep:" << StoredValueRep
    << " status:" << skv_status_to_string( status )
    << EndLogLine;

  status = InitKVRDMAEvent( aReq->mCookie,
                            &StoredValueRep,
                            TotalSize,
                            status );
  return status;
}

skv_status_t
skv_local_kv_asyncmem::RetrievePostProcess(   skv_local_kv_req_ctx_t *aReqCtx )
{
  // with a real async backend, we would have to clean up state after the rdma transfer (e.g. release buffers, locks, etc...)
  skv_status_t status = SKV_SUCCESS;

  return status;
}


skv_status_t
skv_local_kv_asyncmem::RetrieveNKeys( skv_pds_id_t aPDSId,
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

  //  skv_lmr_triplet_t *RetrievedKeysSizesSegs = (skv_lmr_triplet_t*)new char( 2 * aListOfKeysMaxCount * sizeof(skv_lmr_triplet_t) );
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
    << "skv_local_kv_asyncmem:: retrieveNkeys storing request"
    << " MaxKeyCount: " << kvReq->mRequest.mRetrieveN.mListOfKeysMaxCount
    << " KeyData@: " << (void*)kvReq->mRequest.mRetrieveN.mStartingKeyData
    << " KeySize: " << kvReq->mRequest.mRetrieveN.mStartingKeySize
    << EndLogLine;


  mRequestQueue.QueueRequest( kvReq );
  return SKV_ERRNO_LOCAL_KV_EVENT;
}

skv_status_t
skv_local_kv_asyncmem::PerformRetrieveNKeys( skv_local_kv_request_t *aReq )
{
  skv_local_kv_retrieveN_request_t *RNReq = &aReq->mRequest.mRetrieveN;
  int RetrievedKeysCount = 0;
  int RetrievedKeysSizesSegsCount = 0;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem:: retrieveNkeys starting..."
    << " PDSid: " << RNReq->mPDSId
    << " MaxKeyCount: " << RNReq->mListOfKeysMaxCount
    << " KeyData@: " << (void*)RNReq->mStartingKeyData
    << " KeySize: " << RNReq->mStartingKeySize
    << " Flags: " << RNReq->mFlags
    << EndLogLine;

  skv_status_t status = mPDSManager.RetrieveNKeys( RNReq->mPDSId,
                                                   RNReq->mStartingKeyData,
                                                   RNReq->mStartingKeySize,
                                                   RNReq->mRetrievedKeysSizesSegs,
                                                   &RetrievedKeysCount,
                                                   &RetrievedKeysSizesSegsCount,
                                                   RNReq->mListOfKeysMaxCount,
                                                   RNReq->mFlags );
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_asyncmem:: retrieveNkeys completed"
    << " KeyCount: " << RetrievedKeysCount
    << " SegCount: " << RetrievedKeysSizesSegsCount
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  status = InitKVEvent( aReq->mCookie,
                        RNReq->mRetrievedKeysSizesSegs,
                        RetrievedKeysCount,
                        RetrievedKeysSizesSegsCount,
                        status );
  return status;
}


skv_status_t
skv_local_kv_asyncmem::Remove( skv_pds_id_t aPDSId,
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

skv_status_t
skv_local_kv_asyncmem::PerformRemove( skv_local_kv_request_t *aReq )
{
  skv_status_t status = mPDSManager.Remove( aReq->mRequest.mRemove.mPDSId,
                                            aReq->mRequest.mRemove.mKeyData,
                                            aReq->mRequest.mRemove.mKeySize );
  status = InitKVEvent( aReq->mCookie, status );
  return status;
}


skv_status_t
skv_local_kv_asyncmem::CreateCursor( char* aBuff,
                                  int aBuffSize,
                                  skv_server_cursor_hdl_t* aServCursorHdl,
                                  skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.CreateCursor( aBuff, aBuffSize, aServCursorHdl );
}

skv_status_t
skv_local_kv_asyncmem::Lock( skv_pds_id_t *aPDSId,
                          skv_key_value_in_ctrl_msg_t *aKeyValue,
                          skv_rec_lock_handle_t *aRecLock )
{
  return mPDSManager.LockRecord( *aPDSId,
                                 aKeyValue->mData,
                                 aKeyValue->mKeySize,
                                 &(*aRecLock) );
}

skv_status_t
skv_local_kv_asyncmem::Unlock( skv_rec_lock_handle_t aLock )
{
  return mPDSManager.UnlockRecord( aLock );
}


skv_status_t
skv_local_kv_asyncmem::RDMABoundsCheck( const char* aContext,
                                     char* aMem,
                                     int aSize )
{
  return mPDSManager.InBoundsCheck(aContext, aMem, aSize);
}

skv_status_t
skv_local_kv_asyncmem::Allocate( int aBuffSize,
                              skv_lmr_triplet_t *aRDMARep )
{
  return mPDSManager.Allocate( aBuffSize, aRDMARep );
}

skv_status_t
skv_local_kv_asyncmem::Deallocate ( skv_lmr_triplet_t *aRDMARep )
{
   return mPDSManager.Deallocate( aRDMARep );
}

skv_status_t
skv_local_kv_asyncmem::DumpImage( char* aCheckpointPath )
{
  return mPDSManager.DumpPersistenceImage( aCheckpointPath );
}
