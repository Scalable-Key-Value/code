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

#ifndef __SKV_CLIENT_HPP__
#define __SKV_CLIENT_HPP__

#ifndef SKV_CLIENT_UNI
#include <mpi.h>
#endif

#include <common/skv_types_ext.hpp>

class skv_client_t
{
  void* mSKVClientInternalPtr;

public:
  skv_client_t() {}

  /**
   * Initializes clients's state
   */
#ifdef SKV_CLIENT_UNI
  skv_status_t Init( skv_client_group_id_t aCommGroupId,
      int aFlags,
      const char* aConfigFile = NULL );
#else
  skv_status_t Init( skv_client_group_id_t aCommGroupId,
                     MPI_Comm aComm,
                     int aFlags,
                     const char* aConfigFile = NULL );
  #endif

  /**
   * Releases client's state
   */
  skv_status_t Finalize();

  /* optional config file name or NULL to point to the skv_server.conf file or equivalent */
  skv_status_t Connect( const char* aConfigFile, int aFlags );
  skv_status_t Disconnect();

  /***
   * Once a connection is established
   * Operations on a partition data sets (PDS)
   ***/

  // Blocking interface
  skv_status_t Open( char* aPDSName,
                     skv_pds_priv_t aPrivs,
                     skv_cmd_open_flags_t aFlags,
                     skv_pds_id_t* aPDSId );

  skv_status_t Retrieve( skv_pds_id_t* aPDSId,
                         char* aKeyBuffer,
                         int aKeyBufferSize,
                         char* aValueBuffer,
                         int aValueBufferSize,
                         int* aValueRetrievedSize,
                         int aOffset,
                         skv_cmd_RIU_flags_t aFlags );

  skv_status_t Update( skv_pds_id_t* aPDSId,
                       char* aKeyBuffer,
                       int aKeyBufferSize,
                       char* aValueBuffer,
                       int aValueUpdateSize,
                       int aOffset,
                       skv_cmd_RIU_flags_t aFlags );

  skv_status_t Insert( skv_pds_id_t* aPDSId,
                       char* aKeyBuffer,
                       int aKeyBufferSize,
                       char* aValueBuffer,
                       int aValueBufferSize,
                       int aValueBufferOffset,
                       skv_cmd_RIU_flags_t aFlags );

  skv_status_t Remove( skv_pds_id_t* aPDSId,
                       char* aKeyBuffer,
                       int aKeyBufferSize,
                       skv_cmd_remove_flags_t aFlags );

  skv_status_t Close( skv_pds_id_t* aPDSId );

  skv_status_t PDScntl( skv_pdscntl_cmd_t aCmd,
                        skv_pds_attr_t *aPDSAttr );

  // Async interface
  skv_status_t iOpen( char* aPDSName,
                      skv_pds_priv_t aPrivs,
                      skv_cmd_open_flags_t aFlags,
                      skv_pds_id_t* aPDSId,
                      skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t iRetrieve( skv_pds_id_t* aPDSId,
                          char* aKeyBuffer,
                          int aKeyBufferSize,
                          char* aValueBuffer,
                          int aValueBufferSize,
                          int* aValueRetrievedSize,
                          int aOffset,
                          skv_cmd_RIU_flags_t aFlags,
                          skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t iUpdate( skv_pds_id_t* aPDSId,
                        char* aKeyBuffer,
                        int aKeyBufferSize,
                        char* aValueBuffer,
                        int aValueUpdateSize,
                        int aOffset,
                        skv_cmd_RIU_flags_t aFlags,
                        skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t iInsert( skv_pds_id_t* aPDSId,
                        char* aKeyBuffer,
                        int aKeyBufferSize,
                        char* aValueBuffer,
                        int aValueBufferSize,
                        int aValueBufferOffset,
                        skv_cmd_RIU_flags_t aFlags,
                        skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t iRemove( skv_pds_id_t* aPDSId,
                        char* aKeyBuffer,
                        int aKeyBufferSize,
                        skv_cmd_remove_flags_t aFlags,
                        skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t iClose( skv_pds_id_t* aPDSId,
                       skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t iPDScntl( skv_pdscntl_cmd_t aCmd,
                         skv_pds_attr_t *aPDSAttr,
                         skv_client_cmd_ext_hdl_t *aCmdHdl );
  // Wait

  // Returns a handle that has completed
  skv_status_t WaitAny( skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t Wait( skv_client_cmd_ext_hdl_t aCmdHdl );

  // Test
  // Returns a completed handle on success
  skv_status_t TestAny( skv_client_cmd_ext_hdl_t* aCmdHdl );

  skv_status_t Test( skv_client_cmd_ext_hdl_t aCmdHdl );

  /******************************************************************************
   * Pure Key / Value Cursor Interface
   *****************************************************************************/
  // Local Cursor Interface
  skv_status_t GetLocalServerRanks( int **aLocalServers, int *aCount );

  skv_status_t OpenLocalCursor( int aNodeId,
                                skv_pds_id_t* aPDSId,
                                skv_client_cursor_ext_hdl_t* aCursorHdl );

  skv_status_t CloseLocalCursor( skv_client_cursor_ext_hdl_t aCursorHdl );

  skv_status_t GetFirstLocalElement( skv_client_cursor_ext_hdl_t aCursorHdl,
                                     char* aRetrievedKeyBuffer,
                                     int* aRetrievedKeySize,
                                     int aRetrievedKeyMaxSize,
                                     char* aRetrievedValueBuffer,
                                     int* aRetrievedValueSize,
                                     int aRetrievedValueMaxSize,
                                     skv_cursor_flags_t aFlags );

  skv_status_t GetNextLocalElement( skv_client_cursor_ext_hdl_t aCursorHdl,
                                    char* aRetrievedKeyBuffer,
                                    int* aRetrievedKeySize,
                                    int aRetrievedKeyMaxSize,
                                    char* aRetrievedValueBuffer,
                                    int* aRetrievedValueSize,
                                    int aRetrievedValueMaxSize,
                                    skv_cursor_flags_t aFlags );

  // Global Cursor
  skv_status_t OpenCursor( skv_pds_id_t* aPDSId,
                           skv_client_cursor_ext_hdl_t* aCursorHdl );

  skv_status_t CloseCursor( skv_client_cursor_ext_hdl_t aCursorHdl );

  skv_status_t GetFirstElement( skv_client_cursor_ext_hdl_t aCursorHdl,
                                char* aRetrievedKeyBuffer,
                                int* aRetrievedKeySize,
                                int aRetrievedKeyMaxSize,
                                char* aRetrievedValueBuffer,
                                int* aRetrievedValueSize,
                                int aRetrievedValueMaxSize,
                                skv_cursor_flags_t aFlags );

  skv_status_t GetNextElement( skv_client_cursor_ext_hdl_t aCursorHdl,
                               char* aRetrievedKeyBuffer,
                               int* aRetrievedKeySize,
                               int aRetrievedKeyMaxSize,
                               char* aRetrievedValueBuffer,
                               int* aRetrievedValueSize,
                               int aRetrievedValueMaxSize,
                               skv_cursor_flags_t aFlags );
  /*****************************************************************************/

  /******************************************************************************
   * Bulk Insert Interface
   *****************************************************************************/
  skv_status_t CreateBulkInserter( skv_pds_id_t* aPDSId,
                                   skv_bulk_inserter_flags_t aFlags,
                                   skv_client_bulk_inserter_ext_hdl_t* aBulkInserterHandle );

  skv_status_t Insert( skv_client_bulk_inserter_ext_hdl_t aBulkInserterHandle,
                       char* aKeyBuffer,
                       int aKeyBufferSize,
                       char* aValueBuffer,
                       int aValueBufferSize,
                       // int                                                  aValueBufferOffset ??? This can be supported later
                       skv_bulk_inserter_flags_t aFlags
                       );

  skv_status_t Flush( skv_client_bulk_inserter_ext_hdl_t aBulkInserterHandle );

  skv_status_t CloseBulkInserter( skv_client_bulk_inserter_ext_hdl_t aBulkInserterHandle );
  /*****************************************************************************/

  /******************************************************************************
   * Persistence
   *****************************************************************************/
  skv_status_t DumpPersistentImage( char* aPath );
  /*****************************************************************************/

  // Debugging
  skv_status_t DumpPDS( skv_pds_id_t aPDSId,
                        int aMaxKeySize,
                        int aMaxValueSize );
};
#endif
