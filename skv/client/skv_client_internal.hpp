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

#ifndef __SKV_CLIENT_INTERNAL_HPP__
#define __SKV_CLIENT_INTERNAL_HPP__

#ifndef SKV_CLIENT_UNI
#include <mpi.h>
#endif

#include <common/skv_config.hpp>

#include <common/skv_client_server_headers.hpp>

#include <client/skv_client_server_conn.hpp>

#include <common/skv_client_server_protocol.hpp>

#include <client/skv_client_conn_manager_if.hpp>

#include <client/skv_client_command_manager_if.hpp>

#include <client/skv_client_cursor_manager_if.hpp>

#include <client/skv_client_bulk_inserter_manager_if.hpp>

/*
 * Upon IA creation, a Verbs Provider is selected based on the VP name
 * of a preloaded kVP module.
 */
#define VP_NAME		"vp_softrdma"

class skv_client_internal_t
  {
    skv_configuration_t *mSKVConfiguration;

    int mMyRank;
    int mClientGroupCount;
    skv_client_group_id_t mCommGroupId;

    int mFlags;

#ifndef SKV_CLIENT_UNI
    MPI_Comm mComm;
#endif

    it_ia_handle_t mIA_Hdl;
    it_pz_handle_t mPZ_Hdl;

    skv_distribution_t mDistribution;

    skv_client_command_manager_if_t mCommandMgrIF;

    skv_client_conn_manager_if_t mConnMgrIF;
    skv_client_ccb_manager_if_t mCCBMgrIF;

    skv_client_state_t mState;

    skv_pds_id_t mSKVSystemPds;

    skv_client_cursor_manager_if_t mCursorManagerIF;

    skv_status_t RetrieveDistribution(skv_distribution_t* aDist);

    skv_status_t RetrieveNextCachedKey(skv_client_cursor_handle_t aCursorHdl,
                                       char* aRetrievedKeyBuffer,
                                       int* aRetrievedKeySize,
                                       int aRetrievedKeyMaxSize,
                                       char* aRetrievedValueBuffer,
                                       int* aRetrievedValueSize,
                                       int aRetrievedValueMaxSize,
                                       skv_cursor_flags_t aFlags);

    skv_status_t RetrieveNKeys(skv_client_cursor_handle_t aCursorHdl,
                               char* aStartingKeyBuffer,
                               int aStartingKeyBufferSize,
                               skv_cursor_flags_t aFlags);

    skv_status_t C2S_ActiveBroadcast(skv_c2s_active_broadcast_func_type_t aFuncType,
                                     char* aBuff,
                                     int aBuffSize,
                                     void* aIncommingDataMgrIF);

    skv_status_t iSendActiveBcastReq(int aNodeId,
                                     skv_c2s_active_broadcast_func_type_t aFuncType,
                                     char* aBuff,
                                     int aBuffSize,
                                     void* aIncommingDataMgrIF,
                                     it_pz_handle_t aPZHdl,
                                     skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iBulkInsert(int aNodeId,
                             skv_pds_id_t* aPDSId,
                             char* aBuffer,
                             int aBufferSize,
                             it_lmr_handle_t aBufferLMR,
                             it_rmr_context_t aBufferRMR,
                             skv_client_ccb_t** aCCB);

  public:
    skv_client_internal_t()
      {
      }

    int GetRank()
      {
        return mMyRank;
      }

    // Initializes server's state
#ifndef SKV_CLIENT_UNI
    skv_status_t Init(skv_client_group_id_t aCommGroupId, MPI_Comm aComm, int aFlags, const char* aConfigFile = NULL );

    skv_status_t SetCommunicator(MPI_Comm aComm);
#else
    skv_status_t Init( skv_client_group_id_t aCommGroupId, int aFlags, const char* aConfigFile = NULL );
#endif

    // Removes state and shuts down the server
    skv_status_t Finalize();

    skv_status_t Connect(const char* aConfigFile, int aFlags);
    skv_status_t Disconnect();

    /***
     * Once a connection is established
     * Operations on a partition data sets (PDS)
     ***/

    // Blocking interface
    skv_status_t Open(char* aPDSName, skv_pds_priv_t aPrivs, skv_cmd_open_flags_t aFlags, skv_pds_id_t* aPDSId);

    skv_status_t Retrieve(skv_pds_id_t* aPDSId,
                          char* aKeyBuffer,
                          int aKeyBufferSize,
                          char* aValueBuffer,
                          int aValueBufferSize,
                          int* aValueRetrievedSize,
                          int aOffset,
                          skv_cmd_RIU_flags_t aFlags);

    skv_status_t Update(skv_pds_id_t* aPDSId,
                        char* aKeyBuffer,
                        int aKeyBufferSize,
                        char* aValueBuffer,
                        int aValueUpdateSize,
                        int aOffset,
                        skv_cmd_RIU_flags_t aFlags);

    skv_status_t Insert(skv_pds_id_t* aPDSId,
                        char* aKeyBuffer,
                        int aKeyBufferSize,
                        char* aValueBuffer,
                        int aValueBufferSize,
                        int aValueBufferOffset,
                        skv_cmd_RIU_flags_t aFlags);

    skv_status_t Remove(skv_pds_id_t* aPDSId, char* aKeyBuffer, int aKeyBufferSize, skv_cmd_remove_flags_t aFlags);

    skv_status_t Close(skv_pds_id_t* aPDSId);

    skv_status_t PDScntl(skv_pdscntl_cmd_t aCmd, skv_pds_attr_t *aPDSAttr);

    // Async interface
    skv_status_t iOpen(char* aPDSName,
                       skv_pds_priv_t aPrivs,
                       skv_cmd_open_flags_t aFlags,
                       skv_pds_id_t* aPDSId,
                       skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iRetrieve(skv_pds_id_t* aPDSId,
                           char* aKeyBuffer,
                           int aKeyBufferSize,
                           char* aValueBuffer,
                           int aValueBufferSize,
                           int* aValueRetrievedSize,
                           int aOffset,
                           skv_cmd_RIU_flags_t aFlags,
                           skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iUpdate(skv_pds_id_t* aPDSId,
                         char* aKeyBuffer,
                         int aKeyBufferSize,
                         char* aValueBuffer,
                         int aValueUpdateSize,
                         int aOffset,
                         skv_cmd_RIU_flags_t aFlags,
                         skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iInsert(skv_pds_id_t* aPDSId,
                         char* aKeyBuffer,
                         int aKeyBufferSize,
                         char* aValueBuffer,
                         int aValueBufferSize,
                         int aValueBufferOffset,
                         skv_cmd_RIU_flags_t aFlags,
                         skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iRemove(skv_pds_id_t* aPDSId,
                         char* aKeyBuffer,
                         int aKeyBufferSize,
                         skv_cmd_remove_flags_t aFlags,
                         skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iClose(skv_pds_id_t* aPDSId, skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t iPDScntl(skv_pdscntl_cmd_t aCmd, skv_pds_attr_t *aPDSAttr, skv_client_cmd_hdl_t *aCmdHdl);

    // Wait

    // Returns a handle that has completed
    skv_status_t WaitAny(skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t Wait(skv_client_cmd_hdl_t aCmdHdl);

    // Test
    // Returns a completed handle on success
    skv_status_t TestAny(skv_client_cmd_hdl_t* aCmdHdl);

    skv_status_t Test(skv_client_cmd_hdl_t aCmdHdl);

    /******************************************************************************
     * Pure Key / Value Cursor Interface
     *****************************************************************************/
    // Local Cursor Interface
    skv_status_t GetLocalServerRanks(int **aLocalServers, int *aCount);

    skv_status_t OpenLocalCursor(int aNodeId, skv_pds_id_t* aPDSId, skv_client_cursor_handle_t* aCursorHdl);

    skv_status_t CloseLocalCursor(skv_client_cursor_handle_t aCursorHdl);

    skv_status_t GetFirstLocalElement(skv_client_cursor_handle_t aCursorHdl,
                                      char* aRetrievedKeyBuffer,
                                      int* aRetrievedKeySize,
                                      int aRetrievedKeyMaxSize,
                                      char* aRetrievedValueBuffer,
                                      int* aRetrievedValueSize,
                                      int aRetrievedValueMaxSize,
                                      skv_cursor_flags_t aFlags);

    skv_status_t GetNextLocalElement(skv_client_cursor_handle_t aCursorHdl,
                                     char* aRetrievedKeyBuffer,
                                     int* aRetrievedKeySize,
                                     int aRetrievedKeyMaxSize,
                                     char* aRetrievedValueBuffer,
                                     int* aRetrievedValueSize,
                                     int aRetrievedValueMaxSize,
                                     skv_cursor_flags_t aFlags);

    // Global Cursor
    skv_status_t OpenCursor(skv_pds_id_t* aPDSId, skv_client_cursor_handle_t* aCursorHdl);

    skv_status_t CloseCursor(skv_client_cursor_handle_t aCursorHdl);

    skv_status_t GetFirstElement(skv_client_cursor_handle_t aCursorHdl,
                                 char* aRetrievedKeyBuffer,
                                 int* aRetrievedKeySize,
                                 int aRetrievedKeyMaxSize,
                                 char* aRetrievedValueBuffer,
                                 int* aRetrievedValueSize,
                                 int aRetrievedValueMaxSize,
                                 skv_cursor_flags_t aFlags);

    skv_status_t GetNextElement(skv_client_cursor_handle_t aCursorHdl,
                                char* aRetrievedKeyBuffer,
                                int* aRetrievedKeySize,
                                int aRetrievedKeyMaxSize,
                                char* aRetrievedValueBuffer,
                                int* aRetrievedValueSize,
                                int aRetrievedValueMaxSize,
                                skv_cursor_flags_t aFlags);
    /*****************************************************************************/

    /******************************************************************************
     * Bulk Insert Interface
     *****************************************************************************/
    skv_status_t CreateBulkInserter(skv_pds_id_t* aPDSId,
                                    skv_bulk_inserter_flags_t aFlags,
                                    skv_client_bulk_inserter_hdl_t* aBulkInserterHandle);

    skv_status_t Insert(skv_client_bulk_inserter_hdl_t aBulkInserterHandle,
                        char* aKeyBuffer,
                        int aKeyBufferSize,
                        char* aValueBuffer,
                        int aValueBufferSize,
                        // int aValueBufferOffset ??? This can be supported later
                        skv_bulk_inserter_flags_t aFlags);

    skv_status_t Flush(skv_client_bulk_inserter_hdl_t aBulkInserterHandle);

    skv_status_t CloseBulkInserter(skv_client_bulk_inserter_hdl_t aBulkInserterHandle);
    /*****************************************************************************/

    skv_status_t LockPDS(skv_pds_id_t* aPDSId, int flags);
    skv_status_t LockPDSElement(skv_pds_id_t* aPDSId, skv_key_t* aKey, int flags);

    // Debugging
    skv_status_t DumpPDS(skv_pds_id_t aPDSId, int aMaxKeySize, int aMaxValueSize);

    skv_status_t DumpPersistentImage(char* aPath);
  };
#endif
