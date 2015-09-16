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

#ifndef SKV_LOCAL_KV_ROCKSDB_HPP_
#define SKV_LOCAL_KV_ROCKSDB_HPP_

#ifndef SKV_LOCAL_KV_BACKEND_LOG
#define SKV_LOCAL_KV_BACKEND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG
#define SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

// Worker Settings: note that the configured total rdma buffer size
// will be divided by the worker pool size and the max value size
#define SKV_LOCAL_KV_MIN_OUTSTANDING_REQUESTS ( 16 )
#define SKV_LOCAL_KV_MAX_VALUE_SIZE ( 1 * 1048576ul )
#define SKV_LOCAL_KV_WORKER_POOL_SIZE ( 24 )

#include <thread>
#include <rocksdb/db.h>

#include <skv/server/skv_rdma_buffer_allocator.hpp>
#include <skv/server/skv_local_kv_rocksdb_access.hpp>

static inline skv_status_t rocksdb_status_to_skv( rocksdb::Status &aRS )
{
  if( aRS.ok() ) return SKV_SUCCESS;
  if( aRS.IsNotFound() ) return SKV_ERRNO_ELEM_NOT_FOUND;
  if( aRS.IsNotSupported() ) return SKV_ERRNO_NOT_IMPLEMENTED;
  if( aRS.IsInvalidArgument() ) return SKV_ERRNO_NOT_DONE;

  // all other cases:
  return SKV_ERRNO_UNSPECIFIED_ERROR;
}

class skv_local_kv_rocksdb;
class skv_local_kv_rocksdb_worker_t;

struct skv_local_kv_rocksdb_reqctx_t
{
  skv_local_kv_rocksdb_worker_t *mWorker;
  void *mUserData;
};

struct skv_local_kv_rocksdb_worker_settings_t {
  uint64_t mThreadRank;
  uint64_t mRDMABufferSize;
  uint64_t mMaxRequests;
  uint64_t mMaxValueSize;
};

class skv_local_kv_rocksdb_worker_t {
  skv_local_kv_request_queue_t mDedicatedQueue;
  skv_local_kv_request_queue_t mRequestQueue;
  skv_local_kv_rocksdb_access_t *mDBAccess;
  skv_local_kv_rocksdb *mMaster;
  std::thread *mRequestProcessor;
  skv_local_kv_rdma_data_buffer_t *mDataBuffer;
  skv_local_kv_event_queue_t *mEventQueue;
  skv_local_kv_request_t *mStalledCommand;

  skv_local_kv_rocksdb_worker_settings_t mSettings;

public:
  skv_local_kv_rocksdb_worker_t( const uint32_t aQueueLengths )
    : mStalledCommand( NULL ),
      mRequestQueue( aQueueLengths ),
  // worst case: each worker gets a request that got initially processed by one worker
  // note that constructor limits the length to max lenght
      mDedicatedQueue( aQueueLengths * SKV_LOCAL_KV_WORKER_POOL_SIZE )
    {
    };
  ~skv_local_kv_rocksdb_worker_t() {};

  skv_status_t Init( const skv_local_kv_rocksdb_worker_settings_t &aSettings,
                     skv_local_kv_rocksdb *aBackEnd, bool aThreaded = false );

  skv_status_t PerformOpen( skv_local_kv_request_t *aReq );
  skv_status_t PerformStat( skv_local_kv_request_t *aReq );
  skv_status_t PerformClose( skv_local_kv_request_t *aReq );
  skv_status_t PerformInsert( skv_local_kv_request_t *aReq );
  skv_status_t PerformLookup( skv_local_kv_request_t *aReq );
  skv_status_t PerformRetrieve( skv_local_kv_request_t *aReq );
  skv_status_t PerformBulkInsert( skv_local_kv_request_t *aReq );
  skv_status_t PerformRemove( skv_local_kv_request_t *aReq );
  skv_status_t PerformRetrieveNKeys( skv_local_kv_request_t *aReq );
  skv_status_t PerformAsyncInsertCleanup( skv_local_kv_request_t *aReq );
  skv_status_t PerformAsyncRetrieveCleanup( skv_local_kv_request_t *aReq );
  skv_status_t PerformAsyncRetrieveNKeysCleanup( skv_local_kv_request_t *aReq );

  skv_local_kv_rocksdb* GetMaster() const
  {
    return mMaster;
  }
  uint64_t GetThreadRank() const
  {
    return mSettings.mThreadRank;
  }
  skv_local_kv_request_queue_t* GetRequestQueue()
  {
    return &mRequestQueue;
  }
  skv_local_kv_request_queue_t* GetDedicatedQueue()
  {
    return &mDedicatedQueue;
  }
  skv_local_kv_request_t* GetStalledRequest() const
  {
    return mStalledCommand;
  }
  void ResetStalledRequest( )
  {
    mStalledCommand = NULL;
  }
  skv_local_kv_rdma_data_buffer_t* GetDataBuffer() const
  {
    return mDataBuffer;
  }
  skv_status_t QueueDedicatedRequest( skv_local_kv_req_ctx_t aReqCtx, skv_local_kv_request_type_t aType )
  {
    skv_local_kv_request_t *DedicatedRequest = mDedicatedQueue.AcquireRequestEntry();
    if( DedicatedRequest == NULL )
    {
      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "FATAL ERROR: ran out of request entries for dedicate queue."
        << EndLogLine;
      return SKV_ERRNO_OUT_OF_MEMORY;
    }

    DedicatedRequest->InitCommon( aType, NULL, aReqCtx );

    mDedicatedQueue.QueueRequest( DedicatedRequest );
    return SKV_SUCCESS;
  }

private:
  inline skv_server_ccb_t* RetrieveCCB( skv_local_kv_cookie_t *aCookie )
  {
    skv_server_ccb_t *retval = NULL;
    skv_server_ep_state_t *EP = aCookie->GetEPState();
    if( EP && ( EP->mEPState_status == SKV_SERVER_ENDPOINT_STATUS_ACTIVE ) )
      retval = EP->GetCommandForOrdinal( aCookie->GetOrdinal() );

    return retval;
  }

  inline skv_status_t InitKVEvent( skv_local_kv_cookie_t *aCookie,
                                   skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }
  inline skv_status_t InitKVEvent( skv_local_kv_cookie_t *aCookie,
                                   skv_pds_id_t aPDSId,
                                   skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVData.mPDSOpen.mPDSId = aPDSId;
    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }
  inline skv_status_t InitKVEvent( skv_local_kv_cookie_t *aCookie,
                                   skv_pdscntl_cmd_t aCntlCmd,
                                   skv_pds_attr_t *aPDSAttr,
                                   skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVData.mPDSStat.mPDSAttr = *aPDSAttr;
    ccb->mLocalKVData.mPDSStat.mCntlCmd = aCntlCmd;
    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }
  inline skv_status_t InitKVEvent( skv_local_kv_cookie_t *aCookie,
                                   skv_distribution_t *aDist,
                                   skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVData.mDistribution.mDist = aDist;
    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }
  inline skv_status_t InitKVEvent( skv_local_kv_cookie_t *aCookie,
                                   skv_lmr_triplet_t *aValueRepInStore,
                                   skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVData.mLookup.mValueRepInStore = *aValueRepInStore;
    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }
  inline skv_status_t InitKVEvent( skv_local_kv_cookie_t *aCookie,
                                   skv_local_kv_req_ctx_t aReqCtx,
                                   skv_lmr_triplet_t *aKeysSizesSegs,
                                   int aKeysCount,
                                   int aKeysSizesSegsCount,
                                   skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVData.mRetrieveNKeys.mKeysSizesSegs= aKeysSizesSegs;
    ccb->mLocalKVData.mRetrieveNKeys.mKeysCount = aKeysCount;
    ccb->mLocalKVData.mRetrieveNKeys.mKeysSizesSegsCount = aKeysSizesSegsCount;
    ccb->mLocalKVData.mRetrieveNKeys.mReqCtx = aReqCtx;
    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }
  inline skv_status_t InitKVRDMAEvent( skv_local_kv_cookie_t *aCookie,
                                       skv_lmr_triplet_t *aValueRDMADest,
                                       skv_local_kv_req_ctx_t aReqCtx,
                                       int aValueSize,
                                       skv_status_t aRC )
  {
    skv_server_ccb_t *ccb = RetrieveCCB( aCookie );
    if( !ccb )
      return SKV_ERRNO_CONN_FAILED;

    ccb->mLocalKVData.mRDMA.mValueRDMADest= *aValueRDMADest;
    ccb->mLocalKVData.mRDMA.mReqCtx = aReqCtx;
    ccb->mLocalKVData.mRDMA.mSize = aValueSize;
    ccb->mLocalKVrc = aRC;

    return mEventQueue->QueueEvent( aCookie );
  }

public:
  rocksdb::Slice MakeKey( const skv_pds_id_t& aPDSId,
                          const char* aKeyData,
                          const int aKeySize )
  {
    size_t keySize = sizeof(skv_pds_id_t) + aKeySize;
    char *keyData = new char[ keySize ];
    memcpy( keyData, &aPDSId, sizeof(skv_pds_id_t) );
    memcpy( keyData+sizeof(skv_pds_id_t), aKeyData, aKeySize );

    rocksdb::Slice key = rocksdb::Slice( keyData, keySize );
    return key;
  }
  void ReleaseKey( rocksdb::Slice &aKey )
  {
    const char *keyData = aKey.data();
    delete keyData;
    aKey.clear();
  }

private:
  skv_pds_id_t PDSNameToID( std::string aPDSName );
  std::string PDSIdToString( skv_pds_id_t aPDSId );

};

class skv_local_kv_rocksdb {
  int mMyRank;

  skv_local_kv_event_queue_t mEventQueue;
  skv_local_kv_request_queue_list_t mRequestQueueList;
  it_pz_handle_t mPZ;
  skv_distribution_t mDistributionManager;

  volatile bool mKeepProcessing;
  skv_local_kv_rocksdb_worker_t *mMasterProcessing;
  skv_local_kv_rocksdb_worker_t *mWorkerPool[ SKV_LOCAL_KV_WORKER_POOL_SIZE ];

  skv_local_kv_rocksdb_access_t mDBAccess;

public:
  skv_local_kv_rocksdb() : mRequestQueueList( SKV_LOCAL_KV_WORKER_POOL_SIZE )
  {
    mMyRank = -1;
  }
  ~skv_local_kv_rocksdb();
  /****************************************************************************
   * SKV BACKEND API ROUTINES
   */

  skv_status_t Init( int aRank,
                     int aNodeCount,
                     skv_server_internal_event_manager_if_t *aInternalEventMgr,
                     it_pz_handle_t aPZ,
                     char* aCheckpointPath );

  skv_status_t Exit();

  skv_local_kv_event_t* GetEvent()
  {
    return mEventQueue.GetEvent();
  }
  skv_status_t AckEvent( skv_local_kv_event_t *aEvent )
  {
    return mEventQueue.AckEvent( aEvent );
  }

  skv_status_t CancelContext( skv_local_kv_req_ctx_t *aReqCtx );

  skv_status_t GetDistribution( skv_distribution_t**,
                                skv_local_kv_cookie_t * );

  skv_status_t PDS_Open( char *aPDSName,
                         skv_pds_priv_t aPrivs,
                         skv_cmd_open_flags_t aFlags,
                         skv_pds_id_t *aPDSId,
                         skv_local_kv_cookie_t *aCookie );
  skv_status_t PDS_Stat( skv_pdscntl_cmd_t aCmd,
                         skv_pds_attr_t *aPDSAttr,
                         skv_local_kv_cookie_t *aCookie );
  skv_status_t PDS_Close( skv_pds_attr_t *aPDSAttr,
                          skv_local_kv_cookie_t *aCookie );

  skv_status_t Lookup( skv_pds_id_t aPDSId,
                       char *aKeyPtr,
                       int aKeySize,
                       skv_cmd_RIU_flags_t aFlags,
                       skv_lmr_triplet_t *aStoredValueRep,
                       skv_local_kv_cookie_t *aCookie );

  skv_status_t Insert( skv_cmd_RIU_req_t *aReq,
                       skv_status_t aCmdStatus,
                       skv_lmr_triplet_t *aStoredValueRep,
                       skv_lmr_triplet_t *aValueRDMADest,
                       skv_local_kv_cookie_t *aCookie );

  skv_status_t Insert( skv_pds_id_t& aPDSId,
                       char* aRecordRep,
                       int aKeySize,
                       int aValueSize,
                       skv_local_kv_cookie_t *aCookie );

  skv_status_t InsertPostProcess( skv_local_kv_req_ctx_t aReqCtx,
                                  skv_lmr_triplet_t *aValueRDMADest,
                                  skv_local_kv_cookie_t *aCookie );

  skv_status_t BulkInsert( skv_pds_id_t aPDSId,
                             skv_lmr_triplet_t *aLocalBuffer,
                             skv_local_kv_cookie_t *aCookie );

  skv_status_t Retrieve( skv_pds_id_t aPDSId,
                         char* aKeyData,
                         int aKeySize,
                         int aValueOffset,
                         int aValueSize,
                         skv_cmd_RIU_flags_t aFlags,
                         skv_lmr_triplet_t* aStoredValueRep,
                         int *aTotalSize,
                         skv_local_kv_cookie_t *aCookie );
  skv_status_t RetrievePostProcess( skv_local_kv_req_ctx_t aReqCtx );

  skv_status_t RetrieveNKeys( skv_pds_id_t aPDSId,
                              char * aStartingKeyData,
                              int aStartingKeySize,
                              skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                              int* aRetrievedKeysCount,
                              int* aRetrievedKeysSizesSegsCount,
                              int aListOfKeysMaxCount,
                              skv_cursor_flags_t aFlags,
                              skv_local_kv_cookie_t *aCookie );
  skv_status_t RetrieveNKeysPostProcess( skv_local_kv_req_ctx_t aReqCtx );

  skv_status_t Remove( skv_pds_id_t aPDSId,
                       char* aKeyData,
                       int aKeySize,
                       skv_local_kv_cookie_t *aCookie );


  skv_status_t RDMABoundsCheck( const char* aContext,
                                char* aMem,
                                int aSize );

  skv_status_t Allocate( int aBuffSize,
                         skv_lmr_triplet_t *aRDMARep );

  skv_status_t Deallocate ( skv_lmr_triplet_t *aRDMARep );

  skv_status_t CreateCursor( char* aBuff,
                             int aBuffSize,
                             skv_server_cursor_hdl_t* aServCursorHdl,
                             skv_local_kv_cookie_t* aCookie );

  skv_status_t DumpImage( char* aCheckpointPath );

  /****************************************************************************
   * END SKV BACKEND API ROUTINES
   */

  /******************************/
  /* NON-BACK-END API functions */
  bool KeepProcessing() { return mKeepProcessing; }
  skv_local_kv_event_queue_t* GetEventQueue() { return &mEventQueue; }
  it_pz_handle_t GetPZ() { return mPZ; }
  skv_local_kv_rocksdb_access_t* GetDBAccess() { return &mDBAccess; }
  int GetRank() { return mMyRank; }
  int GetHash( const char* aData, size_t aLen ) { return mDistributionManager.mHashFunc.GetHash( aData, aLen ); }

};



#endif /* SKV_LOCAL_KV_ROCKSDB_HPP_ */
