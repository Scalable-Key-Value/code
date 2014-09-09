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

#define SKV_LOCAL_KV_MAX_OUTSTANDING_REQUESTS ( 16 )
#define SKV_LOCAL_KV_MAX_VALUE_SIZE ( 128 * 1048576ul )
#define SKV_LOCAL_KV_RDMA_BUFFER_SIZE ( size_t(SKV_LOCAL_KV_MAX_VALUE_SIZE * SKV_LOCAL_KV_MAX_OUTSTANDING_REQUESTS) )

#include <thread>

class skv_local_kv_rocksdb {
  int mMyRank;

  skv_local_kv_event_queue_t mEventQueue;
  skv_local_kv_request_queue_t mRequestQueue;
  skv_local_kv_rdma_data_buffer_t mDataBuffer;

  volatile bool mKeepProcessing;
  std::thread *mReqProcessor;

public:
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

  skv_status_t InsertPostProcess( skv_local_kv_req_ctx_t *aReqCtx,
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
  skv_status_t RetrievePostProcess(   skv_local_kv_req_ctx_t *aReqCtx );

  skv_status_t RetrieveNKeys( skv_pds_id_t aPDSId,
                              char * aStartingKeyData,
                              int aStartingKeySize,
                              skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                              int* aRetrievedKeysCount,
                              int* aRetrievedKeysSizesSegsCount,
                              int aListOfKeysMaxCount,
                              skv_cursor_flags_t aFlags,
                              skv_local_kv_cookie_t *aCookie );

  skv_status_t Remove( skv_pds_id_t aPDSId,
                       char* aKeyData,
                       int aKeySize,
                       skv_local_kv_cookie_t *aCookie );

  skv_status_t Lock( skv_pds_id_t *aPDSId,
                     skv_key_value_in_ctrl_msg_t *aKeyValue,
                     skv_rec_lock_handle_t *aRecLock );
  skv_status_t Unlock( skv_rec_lock_handle_t aLock );


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

  /* NON-BACK-END API functions */
  bool KeepProcessing()
  {
    return mKeepProcessing;
  }
  skv_local_kv_request_queue_t* GetRequestQueue()
  {
    return &mRequestQueue;
  }

  skv_status_t PerformOpen( skv_local_kv_request_t *aReq );
  skv_status_t PerformGetDistribution(skv_local_kv_request_t *aReq );
  skv_status_t PerformStat( skv_local_kv_request_t *aReq );
  skv_status_t PerformClose( skv_local_kv_request_t *aReq );
  skv_status_t PerformInsert( skv_local_kv_request_t *aReq );
  skv_status_t PerformLookup( skv_local_kv_request_t *aReq );
  skv_status_t PerformRetrieve( skv_local_kv_request_t *aReq );
  skv_status_t PerformBulkInsert( skv_local_kv_request_t *aReq );
  skv_status_t PerformRemove( skv_local_kv_request_t *aReq );
  skv_status_t PerformRetrieveNKeys( skv_local_kv_request_t *aReq );
};



#endif /* SKV_LOCAL_KV_ROCKSDB_HPP_ */
