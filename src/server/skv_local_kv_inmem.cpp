/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 *************************************************/

/* Contributors:
 *     lschneid - initial implementation
 *
 * Created on: Jan 13, 2014
 */

#ifndef SKV_LOCAL_KV_BACKEND_LOG
#define SKV_LOCAL_KV_BACKEND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_INSERT_TRACE
#define SKV_SERVER_INSERT_TRACE ( 0 )
#endif

#include <common/skv_types.hpp>
#include <utils/skv_trace_clients.hpp>

#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>

#include <server/skv_local_kv_types.hpp>
#include <server/skv_local_kv_inmem.hpp>

skv_status_t
skv_local_kv_inmem::Init( int aRank,
                          int aNodeCount,
                          skv_server_internal_event_manager_if_t *aInternalEventMgr,
                          it_pz_handle_t aPZ,
                          char* aCheckpointPath )
{
  skv_status_t status;
  mMyRank = aRank;

  /************************************************************
   * Initialize the local partition dataset manager
   ***********************************************************/
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_inmem::Init(): Entering..."
    << EndLogLine;

  status = mPDSManager.Init( aRank,
                             aNodeCount,
                             aInternalEventMgr,
                             aPZ,
                             aCheckpointPath );
  StrongAssertLogLine( status == SKV_SUCCESS )
    << "skv_local_kv_inmem::Init():: ERROR:: mPDSManager.Init() failed. "
    << " status: " << skv_status_to_string( status )
    << " Rank: " << aRank
    << " PartitionSize: " << aNodeCount
    << EndLogLine;
  /***********************************************************/
  return status;
}

skv_status_t
skv_local_kv_inmem::Exit()
{
  return mPDSManager.Finalize();
}

skv_local_kv_event_t*
skv_local_kv_inmem::GetEvent()
{
  //  this kv backend doesn't create events
  return NULL;
}


skv_status_t
skv_local_kv_inmem::GetDistribution(skv_distribution_t **aDist,
                                    skv_local_kv_cookie_t *aCookie )
{
  *aDist = mPDSManager.GetDistribution();
  return ( *aDist != NULL ) ? SKV_SUCCESS : SKV_ERRNO_NO_BUFFER_AVAILABLE;
}

skv_status_t
skv_local_kv_inmem::PDS_Open( char *aPDSName,
                              skv_pds_priv_t aPrivs,
                              skv_cmd_open_flags_t aFlags,
                              skv_pds_id_t *aPDSId,
                              skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.Open( aPDSName,
                           aPrivs,
                           aFlags,
                           aPDSId );
}

skv_status_t
skv_local_kv_inmem::PDS_Stat( skv_pdscntl_cmd_t aCmd,
                              skv_pds_attr_t *aPDSAttr,
                              skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.Stat( aCmd, aPDSAttr );
}

skv_status_t
skv_local_kv_inmem::PDS_Close( skv_pds_attr_t *aPDSAttr,
                               skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.Close( aPDSAttr );
}


static inline
skv_status_t
AllocateAndMoveKey( skv_cmd_RIU_req_t *aReq,
                    int aKeyValueSize,
                    skv_lmr_triplet_t *aNewRecordAllocRep,
                    skv_pds_manager_if_t *aPDSManager )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_inmem::AllocateAndMoveKey(): allocating"
    << " KeyValueSize: " << aKeyValueSize
    << " RecordAddr: " << (void*)aNewRecordAllocRep->GetAddr()
    << " KeySize: " << aReq->mKeyValue.mKeySize
    << EndLogLine;

  skv_status_t status = aPDSManager->Allocate( aKeyValueSize,
                                               aNewRecordAllocRep );
  if( status != SKV_SUCCESS )
  {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_inmem::AllocateAndMoveKey(): Record allocation failed. status: " << status
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
    << "skv_local_kv_inmem::RemoveLocal():: ERROR: "
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
    << "skv_local_kv_inmem::InsertLocal():: ERROR: "
    << " status: " << skv_status_to_string( status )
    << EndLogLine;

  return status;
}



skv_status_t
skv_local_kv_inmem::Insert( skv_cmd_RIU_req_t *aReq,
                            skv_status_t aCmdStatus,
                            skv_lmr_triplet_t *aStoredValueRep,
                            skv_lmr_triplet_t *aValueRDMADest,
                            skv_local_kv_cookie_t *aCookie )
{
  skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;

  int KeySize = aReq->mKeyValue.mKeySize;
  int ValueSize = aReq->mKeyValue.mValueSize;
  int TotalValueSize = ValueSize + aReq->mOffset;

  AssertLogLine( TotalValueSize >= 0 &&
                 TotalValueSize < SKV_VALUE_LIMIT )
    << "skv_local_kv_inmem::Insert():: ERROR: "
    << "TotalValueSize: " << TotalValueSize
    << EndLogLine;

  // Check if there's enough space for Key/Value
  int KeyValueSize = KeySize + TotalValueSize;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG)
    << "skv_local_kv_inmem::Insert():: Entering with: "
    << " KeySize: " << KeySize
    << " ValueSize: " << ValueSize
    << " TotalValueSize: " << TotalValueSize
    << " KeyValueSize: " << KeyValueSize
    << " Flags: " << (void*)aReq->mFlags
    << EndLogLine;

  int LocalValueSize = aStoredValueRep->GetLen();

  /********************************************************
   * Record Exists
   ********************************************************/
  if( aCmdStatus == SKV_SUCCESS )
  {
    switch( aReq->mFlags & (SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE
        | SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP
        | SKV_COMMAND_RIU_UPDATE
        | SKV_COMMAND_RIU_APPEND) )
    {
      case SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP:
        /********************************************************
         * we overwrite WITHIN EXISTING RECORD BOUNDS ONLY
         ********************************************************/

        BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
            << "skv_local_kv_inmem::Insert():: OVERWRITE_ON_DUP"
            << " ValueSize: " << ValueSize
            << " offs: " << aReq->mOffset
            << EndLogLine;

        if( LocalValueSize < TotalValueSize )
        {
          status = SKV_ERRNO_VALUE_TOO_LARGE;
          break;
        }

        aValueRDMADest->InitAbs( aStoredValueRep->GetLMRHandle(),
                                 (char *) aStoredValueRep->GetAddr() + aReq->mOffset,
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
          << "skv_local_kv_inmem::Insert():: EXPAND_VALUE"
          << " LocalValueSize: " << LocalValueSize
          << " ValueSize: " << ValueSize
          << " TotalValueSize: " << TotalValueSize
          << EndLogLine;

        if( TotalValueSize > LocalValueSize )
        {
          if( !(aReq->mFlags & SKV_COMMAND_RIU_INSERT_OVERLAPPING) )
          {
            BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
              << "skv_local_kv_inmem::Insert():: overlapping inserts not allowed "
              << " Req->mOffset: " << aReq->mOffset
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

          status = AllocateAndMoveKey( aReq,
                                       KeyValueSize,
                                       &NewRecordAllocRep,
                                       &mPDSManager );

          if( status != SKV_SUCCESS )
            break;

          // copy existing value to new allocated record
          char* RecordPtr = (char *) NewRecordAllocRep.GetAddr() + KeySize;

          memcpy( RecordPtr,
                  (void *) aStoredValueRep->GetAddr(),
                  LocalValueSize );

          RecordPtr += aReq->mOffset;   // LocalValueSize;

          // Prepare LMR to cover the new to read fraction of the value only
          aValueRDMADest->InitAbs( NewRecordAllocRep.GetLMRHandle(),
                                   RecordPtr,
                                   ValueSize );

          // Remove old record and deallocate space
          status = RemoveLocal( aReq,
                                KeySize,
                                (char *) aStoredValueRep->GetAddr(),
                                &mPDSManager );

          // insert new record
          status = InsertLocal( aReq,
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

          aValueRDMADest->InitAbs( aStoredValueRep->GetLMRHandle(),
                                   (char *) aStoredValueRep->GetAddr() + aReq->mOffset,
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
        if( (uint64_t)ValueSize == aStoredValueRep->GetLen() )
        {
          BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
            << "skv_local_kv_inmem::Insert():: UPDATE same length record, overwriting..."
            << " ValueSize: " << ValueSize
            << " offs: " << aReq->mOffset
            << EndLogLine;

          aValueRDMADest->InitAbs( aStoredValueRep->GetLMRHandle(),
                                   (char *) aStoredValueRep->GetAddr() + aReq->mOffset,
                                   ValueSize );
          status = SKV_SUCCESS;
        }
        // reallocate, copy and insert new updated record, if the size is different
        else
        {
          BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
            << "skv_local_kv_inmem::Insert():: UPDATE different length record, reallocating..."
            << " ValueSize: " << ValueSize
            << " offs: " << aReq->mOffset
            << EndLogLine;

          status = RemoveLocal( aReq,
                                KeySize,
                                (char *) aStoredValueRep->GetAddr(),
                                &mPDSManager );

          skv_lmr_triplet_t NewRecordAllocRep;

          status = AllocateAndMoveKey( aReq,
                                       KeyValueSize,
                                       &NewRecordAllocRep,
                                       &mPDSManager );

          if( status != SKV_SUCCESS )
            break;

          aValueRDMADest->InitAbs( NewRecordAllocRep.GetLMRHandle(),
                                   (char *) NewRecordAllocRep.GetAddr() + KeySize + aReq->mOffset,
                                   ValueSize );

          status = InsertLocal( aReq,
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
          << "skv_local_kv_inmem::Insert():: APPEND "
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

          status = AllocateAndMoveKey( aReq,
                                       KeyValueSize,
                                       &NewRecordAllocRep,
                                       &mPDSManager );

          if( status != SKV_SUCCESS )
            break;

          char* RecordPtr = (char *) NewRecordAllocRep.GetAddr() + KeySize;

          memcpy( RecordPtr,
                  (void *) aStoredValueRep->GetAddr(),
                  LocalValueSize );

          // setup to add new record
          RecordPtr += LocalValueSize;

          // rdma read only the new record data
          aValueRDMADest->InitAbs( NewRecordAllocRep.GetLMRHandle(),
                                   RecordPtr,
                                   ValueSize );

          status = RemoveLocal( aReq,
                                KeySize,
                                (char *) aStoredValueRep->GetAddr(),
                                &mPDSManager );

          status = InsertLocal( aReq,
                                (char *) NewRecordAllocRep.GetAddr(),
                                KeySize,
                                TotalValueSize,
                                &mPDSManager,
                                mMyRank );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_local_kv_inmem::Insert():: ERROR: "
            << " status: " << skv_status_to_string( status )
            << EndLogLine;
          /*******************************************************************/
        }
        else
        {
          // Record was found, but no expansion is needed
          // Just make sure that the rdma_read is done to the
          // appropriate offset

          aValueRDMADest->InitAbs( aStoredValueRep->GetLMRHandle(),
                                   (char *) aStoredValueRep->GetAddr() + aReq->mOffset,
                                   ValueSize );
        }

        break;
        /********************************************************/

      default:   // mFlags
        /********************************************************
         * Record Exists and no special flags provided: error response
         ********************************************************/

        BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
          << " skv_local_kv_inmem::Insert(): record exists, returning ERRNO"
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
      << "skv_local_kv_inmem::Insert():: "
      << " Req->mKeyValue: " << aReq->mKeyValue
      << EndLogLine;

    status = AllocateAndMoveKey( aReq,
                                 KeyValueSize,
                                 &NewRecordAllocRep,
                                 &mPDSManager );

    if( status == SKV_SUCCESS )
    {
      // if no EXPAND requested the requested offset has to be 0
      if( !(aReq->mFlags & SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE) )
        AssertLogLine( aReq->mOffset == 0 )
          << "skv_local_kv_inmem::Insert():: ERROR: "
          << " Req->mOffset: " << aReq->mOffset
          << EndLogLine;

      aValueRDMADest->InitAbs( NewRecordAllocRep.GetLMRHandle(),
                               (char *) NewRecordAllocRep.GetAddr() + KeySize + aReq->mOffset,
                               ValueSize );

      status = InsertLocal( aReq,
                            (char *) NewRecordAllocRep.GetAddr(),
                            KeySize,
                            TotalValueSize,
                            &mPDSManager,
                            mMyRank );

    }
  }
  /***********************************************************************/

  if( status != SKV_SUCCESS )
    return status;

  /*******************************************************************
   * Get the data copied or rdma'd into the new record
   ******************************************************************/
  if( aReq->mFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG )
  {
    BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
      << "skv_local_kv_inmem::Insert():: inserting value with FIT_IN_CTL_MSG"
      << " len: " << ValueSize
      << EndLogLine;

    memcpy( (void *) aValueRDMADest->GetAddr(),
            &aReq->mKeyValue.mData[KeySize],
            ValueSize );

  }
  else
    // signal that this command is going to require async data transfer
    status = SKV_ERRNO_NEED_DATA_TRANSFER;

  return status;
}

skv_status_t
skv_local_kv_inmem::Insert( skv_pds_id_t& aPDSId,
                            char* aRecordRep,
                            int aKeySize,
                            int aValueSize,
                            skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.Insert( aPDSId,
                             aRecordRep,
                             aKeySize,
                             aValueSize );
}




skv_status_t
skv_local_kv_inmem::Lookup( skv_pds_id_t aPDSId,
                            char *aKeyPtr,
                            int aKeySize,
                            skv_cmd_RIU_flags_t aFlags,
                            skv_lmr_triplet_t *aStoredValueRep,
                            skv_local_kv_cookie_t *aCookie )
{
  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_inmem::Lookup():: Entering"
    << EndLogLine;

  gSKVServerInsertRetrieveFromTreeStart.HitOE( SKV_SERVER_INSERT_TRACE,
                                               "SKVServerInsertRetrieveFromTree",
                                               mMyRank,
                                               gSKVServerInsertRetrieveFromTreeStart );

  // Check if the key exists
  skv_status_t status = mPDSManager.Retrieve( aPDSId,
                                              aKeyPtr,
                                              aKeySize,
                                              0,
                                              0,
                                              aFlags,
                                              aStoredValueRep );



  gSKVServerInsertRetrieveFromTreeFinis.HitOE( SKV_SERVER_INSERT_TRACE,
                                               "SKVServerInsertRetrieveFromTree",
                                               mMyRank,
                                               gSKVServerInsertRetrieveFromTreeFinis );


  return status;
}

skv_status_t
skv_local_kv_inmem::Retrieve( skv_pds_id_t aPDSId,
                              char* aKeyData,
                              int aKeySize,
                              int aValueOffset,
                              int aValueSize,
                              skv_cmd_RIU_flags_t aFlags,
                              skv_lmr_triplet_t* aStoredValueRep,
                              int *aTotalSize,
                              skv_local_kv_cookie_t *aCookie )
{
  skv_status_t status = mPDSManager.Retrieve( aPDSId,
                                              aKeyData,
                                              aKeySize,
                                              aValueOffset,
                                              aValueSize,
                                              aFlags,
                                              aStoredValueRep );
  int ValueOversize = 0;
  if ((status == SKV_SUCCESS) || (status == SKV_ERRNO_NEED_DATA_TRANSFER))
  {
    *aTotalSize = aStoredValueRep->GetLen();
    ValueOversize = aStoredValueRep->GetLen() - aValueSize;
    if( ValueOversize > 0 )
    {
      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "skv_local_kv_inmem:: ADJUSTING rdma_write length to match smaller client buffer"
        << " client: " << aValueSize
        << " store: " << aStoredValueRep->GetLen()
        << EndLogLine;

      aStoredValueRep->SetLenIfSmaller( aValueSize );
    }

    if( !(aFlags & SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG) )
      status = SKV_ERRNO_NEED_DATA_TRANSFER;
  }
  else
    *aTotalSize = 0;

  BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
    << "skv_local_kv_inmem:: storing valueRep:" << *aStoredValueRep
    << " status:" << skv_status_to_string( status )
    << EndLogLine;

  return status;
}

skv_status_t
skv_local_kv_inmem::RetrieveNKeys( skv_pds_id_t aPDSId,
                                   char * aStartingKeyData,
                                   int aStartingKeySize,
                                   skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                                   int* aRetrievedKeysCount,
                                   int* aRetrievedKeysSizesSegsCount,
                                   int aListOfKeysMaxCount,
                                   skv_cursor_flags_t aFlags,
                                   skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.RetrieveNKeys( aPDSId,
                                    aStartingKeyData,
                                    aStartingKeySize,
                                    aRetrievedKeysSizesSegs,
                                    aRetrievedKeysCount,
                                    aRetrievedKeysSizesSegsCount,
                                    aListOfKeysMaxCount,
                                    aFlags );
}


skv_status_t
skv_local_kv_inmem::Remove( skv_pds_id_t aPDSId,
                            char* aKeyData,
                            int aKeySize,
                            skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.Remove( aPDSId,
                             aKeyData,
                             aKeySize );
}

skv_status_t
skv_local_kv_inmem::BulkInsert( skv_pds_id_t aPDSId,
                                skv_lmr_triplet_t *aLocalBuffer,
                                skv_local_kv_cookie_t *aCookie )
{
  int             LocalBufferSize = aLocalBuffer->GetLen();
  char*           LocalBufferAddr = (char *)aLocalBuffer->GetAddr();
  it_lmr_handle_t LocalBufferLMR = aLocalBuffer->GetLMRHandle();

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
    << " PDSId: " << aPDSId
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
    skv_status_t status = mPDSManager.Retrieve( aPDSId,
                                                KeyPtr,
                                                KeySize,
                                                0,
                                                0,
                                                (skv_cmd_RIU_flags_t)0,
                                                &ValueRepInStore );

    int TotalSize = KeySize + ValueSize;

#if 0 //SKV_LOCAL_KV_BACKEND_LOG
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

      Deallocate( aLocalBuffer );
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
    status = Insert( aPDSId,
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
  return LoopStatus;
}


skv_status_t
skv_local_kv_inmem::CreateCursor( char* aBuff,
                                  int aBuffSize,
                                  skv_server_cursor_hdl_t* aServCursorHdl,
                                  skv_local_kv_cookie_t *aCookie )
{
  return mPDSManager.CreateCursor( aBuff, aBuffSize, aServCursorHdl );
}

skv_status_t
skv_local_kv_inmem::Lock( skv_pds_id_t *aPDSId,
                          skv_key_value_in_ctrl_msg_t *aKeyValue,
                          skv_rec_lock_handle_t *aRecLock )
{
  return mPDSManager.LockRecord( *aPDSId,
                                 aKeyValue->mData,
                                 aKeyValue->mKeySize,
                                 &(*aRecLock) );
}

skv_status_t
skv_local_kv_inmem::Unlock( skv_rec_lock_handle_t aLock )
{
  return mPDSManager.UnlockRecord( aLock );
}


skv_status_t
skv_local_kv_inmem::RDMABoundsCheck( const char* aContext,
                                     char* aMem,
                                     int aSize )
{
  return mPDSManager.InBoundsCheck(aContext, aMem, aSize);
}

skv_status_t
skv_local_kv_inmem::Allocate( int aBuffSize,
                              skv_lmr_triplet_t *aRDMARep )
{
  return mPDSManager.Allocate( aBuffSize, aRDMARep );
}

skv_status_t
skv_local_kv_inmem::Deallocate ( skv_lmr_triplet_t *aRDMARep )
{
   return mPDSManager.Deallocate( aRDMARep );
}

skv_status_t
skv_local_kv_inmem::DumpImage( char* aCheckpointPath )
{
  return mPDSManager.DumpPersistenceImage( aCheckpointPath );
}
