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

#ifndef __PARTITIONED_DATA_SET_MANAGER_IF_HPP__
#define __PARTITIONED_DATA_SET_MANAGER_IF_HPP__

#include <common/skv_types.hpp>

template<class PartitionedDataSetManagerT>
class skv_partitioned_data_set_manager_if_t
{
  PartitionedDataSetManagerT mPartitionedDataSetManager;

public:
  skv_partitioned_data_set_manager_if_t()
  {
  }

  /***
   * Desc: Initites storage manager's state
   * input: none  
   * returns: 0 on success, or error code
   ***/
  skv_status_t Init( int aNodeId,
                     int aNodeCount,
                     skv_server_internal_event_manager_if_t* aInternalEventQueue,
                     it_pz_handle_t aPZ_Hdl,
                     char* aCheckpointPath )
  {
    return mPartitionedDataSetManager.Init( aNodeId, aNodeCount, aInternalEventQueue, aPZ_Hdl, aCheckpointPath );
  }

  /***
   * Desc: Closes the storage manager
   * input: none  
   * returns: 0 on success, or error code
   ***/
  skv_status_t Finalize()
  {
    return mPartitionedDataSetManager.Finalize();
  }

  /***
   * Operations on a partition data sets (PDS)
   ***/
  skv_status_t Open( char* aPDSName, skv_pds_priv_t aPrivs, skv_cmd_open_flags_t aFlags, skv_pds_id_t* aPDSId )
  {
    return mPartitionedDataSetManager.Open( aPDSName, aPrivs, aFlags, aPDSId );
  }

  skv_status_t Stat( skv_pdscntl_cmd_t aCmd,
                     skv_pds_attr_t *aPDSAttr )
  {
    return mPartitionedDataSetManager.Stat( aCmd, aPDSAttr );
  }

  skv_status_t Close( skv_pds_attr_t *aPDSAttr )
  {
    return mPartitionedDataSetManager.Close( aPDSAttr );
  }

  int GetMaxDataLoad()
  {
    return mPartitionedDataSetManager.GetMaxDataLoad();
  }

  skv_status_t
  Remove( skv_pds_id_t aPDSId,
          char* aKeyData,
          int aKeySize )
  {
    return mPartitionedDataSetManager.Remove( aPDSId,
                                              aKeyData,
                                              aKeySize );
  }

  skv_status_t
  Insert( skv_pds_id_t& aPDSId,
          char* aRecordRep,
          int aKeySize,
          int aValueSize )
  {
    return mPartitionedDataSetManager.Insert( aPDSId,
                                              aRecordRep,
                                              aKeySize,
                                              aValueSize );
  }

  skv_status_t
  Allocate( int aSize,
            skv_lmr_triplet_t* aRemMemRep )
  {
    return mPartitionedDataSetManager.Allocate( aSize, aRemMemRep );
  }

  skv_status_t
  Deallocate( skv_lmr_triplet_t* aRemMemRep )
  {
    return mPartitionedDataSetManager.Deallocate( aRemMemRep );
  }

  skv_status_t Retrieve( skv_pds_id_t aPDSId,
                         char* aKeyData,
                         int aKeySize,
                         int aValueOffset,
                         int aValueSize,
                         skv_cmd_RIU_flags_t aFlags,
                         skv_lmr_triplet_t* aRemMemRepValue )
  {
    return mPartitionedDataSetManager.Retrieve( aPDSId,
                                                aKeyData,
                                                aKeySize,
                                                aValueOffset,
                                                aValueSize,
                                                aFlags,
                                                aRemMemRepValue );
  }

  skv_distribution_t* GetDistribution()
  {
    return mPartitionedDataSetManager.GetDistribution();
  }

  skv_status_t RetrieveNKeys( skv_pds_id_t aPDSId,
                              char * aStartingKeyData,
                              int aStartingKeySize,
                              skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                              int* aRetrievedKeysCount,
                              int* aRetrievedKeysSizesSegsCount,
                              int aListOfKeysMaxCount,
                              skv_cursor_flags_t aFlags )
  {
    return mPartitionedDataSetManager.RetrieveNKeys( aPDSId,
                                                     aStartingKeyData,
                                                     aStartingKeySize,
                                                     aRetrievedKeysSizesSegs,
                                                     aRetrievedKeysCount,
                                                     aRetrievedKeysSizesSegsCount,
                                                     aListOfKeysMaxCount,
                                                     aFlags );
  }

  skv_status_t
  UnlockRecord( skv_rec_lock_handle_t aRecLock )
  {
    return mPartitionedDataSetManager.UnlockRecord( aRecLock );

  }

  skv_status_t
  LockRecord( skv_pds_id_t aPDSId,
              char* aKeyData,
              int aKeySize,
              skv_rec_lock_handle_t* aRecLock )
  {
    return mPartitionedDataSetManager.LockRecord( aPDSId,
                                                  aKeyData,
                                                  aKeySize,
                                                  aRecLock );
  }

  skv_status_t
  CreateCursor( char* aBuff,
                int aBuffSize,
                skv_server_cursor_hdl_t* aServCursorHdl )
  {
    return mPartitionedDataSetManager.CreateCursor( aBuff,
                                                    aBuffSize,
                                                    aServCursorHdl );
  }

  skv_status_t
  DumpPersistenceImage( char* aPath )
  {
    return mPartitionedDataSetManager.DumpPersistenceImage( aPath );
  }

  skv_status_t
  InBoundsCheck( const char* aContext,
                 char* aMem,
                 int aSize )
  {
    return mPartitionedDataSetManager.InBoundsCheck( aContext, aMem, aSize );
  }
};
#endif
