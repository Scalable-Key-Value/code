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

#ifndef __SKV_UBER_PDS_HPP__
#define __SKV_UBER_PDS_HPP__

#include <common/skv_types.hpp>
#include <common/skv_distribution_manager.hpp>
#include <server/skv_server_tree_based_container.hpp>

/***
 * Placeholder for dealing with access permissions on a PDS
 ***/
struct AccessPerms
{
  int mOwnerId;
  int mGroupId;
};

/***
 * Metadata associated with a PDS
 ***/
struct PDSMetadata
{
  AccessPerms mAccessPerms;
};

class skv_uber_pds_t
{
  /***
   * Single container containing all the partitioned data sets
   * Note on storing PDS Meta information:
   * The PDSName is treated as a key, with the value
   * being equal to PDSMetadata. It's treated no differently
   * then any other PDS. This implies that it's contents
   * would go through the same hash function and data
   * redistribution procedures.
   ***/
  skv_tree_based_container_t    mLocalData;

  skv_distribution_t            mDistributionManager;

  int                           mMyNodeId;

  // int                           mLocalPDSCount;

  //  typedef   std::map<string, skv_pds_attr_t>   skv_pds_name_table_t;

  // skv_pds_name_table_t*                        mPDSNameTable;  

protected:
  int Rebalance();

public:
  skv_uber_pds_t() {}
  ~skv_uber_pds_t() {}

  skv_status_t Init( int                                      aNodeId, 
                     int                                      aNodeCount,
                     skv_server_internal_event_manager_if_t* aInternalEventQueue,
                     it_pz_handle_t                           aPZ_Hdl,
                     char*                                    aCheckpointPath );

  skv_status_t Finalize();

  /***
   * Operations on a partition data sets ( PDS )
   ***/

  /***
   * Every PDS has a distribution.
   * For the case of the UberPDS, the distribution is the same
   * for all PDS, since every PDS is kept in one container
   ***/
  skv_distribution_t* GetDistribution();

  skv_status_t Open( char*                  aPDSName, 
                     skv_pds_priv_t        aPrivs,
                     skv_cmd_open_flags_t  aFlags,
                     skv_pds_id_t*         aPDSId );

  skv_status_t Stat( skv_pdscntl_cmd_t  aCmd, 
                     skv_pds_attr_t    *aPDSAttr );
  skv_status_t Close( skv_pds_attr_t   *aPDSAttr );


  // 
  int GetMaxDataLoad();

  skv_status_t Remove( skv_pds_id_t             aPDSId, 
                       char*                     aKeyData,
                       int                       aKeySize );

  skv_status_t Insert( skv_pds_id_t             aPDSId, 
                       char*                     aRecordRep,
                       int                       aKeySize,
                       int                       aValueSize );

  skv_status_t Allocate( int                       aSize,
                         skv_lmr_triplet_t*       aMemRep );

  skv_status_t Deallocate( skv_lmr_triplet_t*     aMemRep );

  skv_status_t Retrieve( skv_pds_id_t             aPDSId, 
                         char*                     aKeyData,
                         int                       aKeySize,
                         int                       aValueOffset,
                         int                       aValueSize,
                         skv_cmd_RIU_flags_t      aFlags,
                         skv_lmr_triplet_t*       aRemMemRepValue );


  skv_status_t RetrieveNKeys( skv_pds_id_t       aPDSId, 
                              char *              aStartingKeyData,
                              int                 aStartingKeySize,
                              skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                              int*                aRetrievedKeysCount,
                              int*                aRetrievedKeysSizesSegsCount,
                              int                 aListOfKeysMaxCount,
                              skv_cursor_flags_t aFlags );

  skv_status_t UnlockRecord( skv_rec_lock_handle_t   aRecLock );

  skv_status_t LockRecord( skv_pds_id_t             aPDSId,
                           char*                     aKeyData,
                           int                       aKeySize,
                           skv_rec_lock_handle_t*   aRecLock );

  skv_status_t CreateCursor( char*                     aBuff,
                             int                       aBuffSize,
                             skv_server_cursor_hdl_t* aServCursorHdl );

  skv_status_t  FillCursorBuffer( skv_server_cursor_hdl_t   aServerCursorHandle,
                                  char*                      aBuffer,
                                  int                        aBufferMaxLen,
                                  int*                       aFilledSize,
                                  skv_cursor_flags_t        aFlags );

  skv_status_t  FillCursorBufferForIndex( skv_server_cursor_hdl_t   aServerCursorHandle,
                                          skv_server_ep_state_t*    aEPState,
                                          char*                      aBuffer,
                                          int                        aBufferMaxLen,
                                          int*                       aFilledSize,
                                          skv_cursor_flags_t        aFlags );

  skv_status_t DumpPersistenceImage( char* aPath );

  skv_status_t InBoundsCheck( const char* aContext,
                              char* aMem,
                              int   aSize );


};
#endif
