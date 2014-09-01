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

#ifndef __SKV_TREE_BASED_CONTAINER__
#define __SKV_TREE_BASED_CONTAINER__
#include <common/skv_types.hpp>

#include <server/skv_server_heap_manager.hpp>
#include <server/skv_server_tree_based_container_key.hpp>
#include <server/skv_server_cursor_manager_if.hpp>

// class skv_server_pds_compare_t
//   {
//   bool operator()(const skv_pds_id_t& lhs, const skv_pds_id_t& rhs) const
//     {
//     return lhs<rhs;
//     }
//   };

class skv_tree_based_container_t
{
  skv_data_container_t*                         mDataMap;
  // In bytes
  int                                           mMaxDataLoad;

  // Memory for local data 
  char*                                         mStartOfDataField;
  size_t                                        mDataFieldLen;

  it_lmr_handle_t                               mDataLMR;
  it_rmr_context_t                              mDataRMR;

  // This is used to get an iterator to the records associated 
  // to a skv_pds_id_t. Assumes the rest of the records follow in sorted order
  skv_tree_based_container_key_t* MakeMagicKey( skv_pds_id_t* );
  skv_tree_based_container_key_t* MakeKey( skv_pds_id_t& aPDSId, skv_key_t* aKey );

  // ASSUME: Since the SKV Server is single threaded, this is safe
  skv_tree_based_container_key_t mTempKeyBuffer;

  skv_server_cursor_manager_if_t mServCursorMgrIF;

  skv_server_internal_event_manager_if_t* mInternalEventManager;
  it_pz_handle_t mPZ_Hdl;

  int* mLocalPDSCountPtr;

  typedef std::basic_string<char,
      std::char_traits<char>,
      skv_allocator_t<char> > skv_server_string_t;

  typedef std::map<skv_server_string_t,
      skv_pds_id_t,
      less<skv_server_string_t>,
      skv_allocator_t<pair<skv_server_string_t, skv_pds_id_t> > > skv_pds_name_table_t;

  skv_pds_name_table_t* mPDSNameTable;

  typedef std::map<skv_pds_id_t,
      skv_pds_attr_t,
      less<skv_pds_id_t>,
      skv_allocator_t<pair<skv_pds_id_t, skv_pds_attr_t> > > skv_pds_id_table_t;

  skv_pds_id_table_t* mPDSIdTable;

  int mMyNodeId;

  skv_server_persistance_heap_hdr_t* mHeapHdr;

public:
  skv_tree_based_container_t()
  {
  }
  ~skv_tree_based_container_t()
  {
  }

  skv_status_t
  Open( char* aPDSName,
        skv_pds_priv_t aPrivs,
        skv_cmd_open_flags_t aFlags,
        skv_pds_id_t* aPDSId );

  skv_status_t
  Stat( skv_pdscntl_cmd_t aCmd,
        skv_pds_attr_t *aPDSAttr );

  skv_status_t
  Close( skv_pds_attr_t *aPDSAttr );

  int GetMaxDataLoad();

  skv_status_t Init( it_pz_handle_t aPZ_Hdl,
                     skv_server_internal_event_manager_if_t* aInternalEventManager,
                     int aMyNodeId,
                     char* aRestartImagePath,
                     skv_persistance_flag_t aFlag );

  skv_status_t Finalize();

  skv_status_t Remove( skv_pds_id_t aPDSId,
                       char* aKeyData,
                       int aKeySize );

  skv_status_t Insert( skv_pds_id_t aPDSId,
                       char* aRecordRep,
                       int aKeySize,
                       int aValueSize );

  skv_status_t Allocate( int aSize,
                         skv_lmr_triplet_t* aRemMemRep );

  skv_status_t Deallocate( skv_lmr_triplet_t* aRemMemRep );

  skv_status_t Retrieve( skv_pds_id_t aPDSId,
                         char* aKeyData,
                         int aKeySize,
                         int aValueOffset,
                         int aValueSize,
                         skv_cmd_RIU_flags_t aFlags,
                         skv_lmr_triplet_t* aMemRepValue );

  skv_status_t RetrieveNKeys( skv_pds_id_t aPDSId,
                              char * aStartingKeyData,
                              int aStartingKeySize,
                              skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                              int* aRetrievedKeysCount,
                              int* aRetrievedKeysSizesSegsCount,
                              int aListOfKeysMaxCount,
                              skv_cursor_flags_t aFlags );

  skv_status_t UnlockRecord( skv_rec_lock_handle_t aRecLock );

  skv_status_t LockRecord( skv_pds_id_t aPDSId,
                           char* aKeyData,
                           int aKeySize,
                           skv_rec_lock_handle_t* aRecLock );

  skv_status_t CreateCursor( char* aBuff,
                             int aBuffSize,
                             skv_server_cursor_hdl_t* aServCursorHdl );

  skv_status_t FillCursorBuffer( skv_server_cursor_hdl_t aServerCursorHandle,
                                 char* aBuffer,
                                 int aBufferMaxLen,
                                 int* aFilledSize,
                                 skv_cursor_flags_t aFlags );

  skv_status_t FillCursorBufferForIndex( skv_server_cursor_hdl_t aServerCursorHandle,
                                         skv_server_ep_state_t* aEPState,
                                         char* aBuffer,
                                         int aBufferMaxLen,
                                         int* aFilledSize,
                                         skv_cursor_flags_t aFlags );

  skv_status_t PredicateAndProject( char* aRow,
                                    skv_server_cursor_hdl_t aServerCursorHandle,
                                    char* aBuffer,
                                    int aBufferMaxLen,
                                    int* aFilledSize );

  skv_status_t DumpPersistenceImage( char* aPath );

  skv_status_t InBoundsCheck( const char* aContext,
                              char* aMem,
                              int aSize );

};
#endif

