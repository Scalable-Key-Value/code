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

#ifndef __SKV_H__
#define __SKV_H__

#include <inttypes.h>
#include <common/skv_errno.hpp>

#if defined(__cplusplus)
extern "C" 
  {
#endif

#define SKV_MAX_PDS_NAME_SIZE  ( 64 )

// pds-control command flags
  typedef enum
    {
    SKV_PDSCNTL_CMD_STAT_SET,
    SKV_PDSCNTL_CMD_STAT_GET,
    SKV_PDSCNTL_CMD_CLOSE
    } skv_pdscntl_cmd_t;


// Open() flags 
  typedef enum
    {
    SKV_COMMAND_OPEN_FLAGS_NONE       = 0x0000, 

    // Create if does not exist
    SKV_COMMAND_OPEN_FLAGS_CREATE     = 0x0001,

    // Return error if already exists    
    SKV_COMMAND_OPEN_FLAGS_EXCLUSIVE  = 0x0002,

    // PDS has duplicates
    SKV_COMMAND_OPEN_FLAGS_DUP        = 0x0003

    } skv_cmd_open_flags_t;

  typedef enum
    {
    SKV_PDS_READ                      = 0x0001,

    SKV_PDS_WRITE                     = 0x0002

    } skv_pds_priv_t;

  typedef enum
    {
    SKV_COMMAND_RIU_FLAGS_NONE                       = 0x0000, 
    
    /***
     * IMPORTANT: 
     * only one of the following flags can be set per request
     * SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE
     * SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP
     * SKV_COMMAND_RIU_UPDATE
     * SKV_COMMAND_RIU_APPEND
     * because they define different insert cases (i.e. represent different types of insert command)
     ***/

    /***
     * If this flag is specified, the insert 
     * will create a buffer equal to Offset+Len, if neccessary
     * Note: Every time a new buffer is created a memcpy 
     * of the already present data is performed
     ***/
    SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE             = 0x0001,

    SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS          = 0x0002,

    SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG       = 0x0004,

    SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG  = 0x0008,

    SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG    = 0x0010,

    // Overwrite value on dup
    // NOTE: This option can't be used with the EXPANDS_VALUE option
    SKV_COMMAND_RIU_INSERT_OVERWRITE_VALUE_ON_DUP    = 0x0020,

    // Retrieve specified value length only. 
    SKV_COMMAND_RIU_RETRIEVE_SPECIFIC_VALUE_LEN      = 0x0040,    

    // update and reallocation if needed
    SKV_COMMAND_RIU_UPDATE                           = 0x0080,

    // append new value to end of record
    SKV_COMMAND_RIU_APPEND                           = 0x0100,

    // enable overlapping inserts (e.g. for EXPANDS_VALUE case)
    SKV_COMMAND_RIU_INSERT_OVERLAPPING               = 0x0200
    } skv_cmd_RIU_flags_t;

  typedef enum
    {
    SKV_COMMAND_REMOVE_FLAGS_NONE                = 0x0000, 

    SKV_COMMAND_REMOVE_KEY_FITS_IN_CTL_MSG       = 0x0001,

    } skv_cmd_remove_flags_t;


  typedef enum 
    {
    SKV_CURSOR_NONE_FLAG                   = 0x0000,

    // For Pure Key/Value Cursor
    SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG = 0x0001,    
    SKV_CURSOR_WITH_STARTING_KEY_FLAG      = 0x0002,

    // For Relational Cursor
    /*
     * Ordered. The order is defined by the index fields. 
     * i.e. arguments to CreateIndex()
     */    
    SKV_CURSOR_USE_ORDERED_STREAM_FLAG         = 0x0004,


    SKV_CURSOR_USE_RANDOM_STREAM_FLAG          = 0x0008,
    SKV_CURSOR_USE_ROUND_ROBIN_STREAM_FLAG     = 0x0010,   
    SKV_CURSOR_USE_HASH_STREAM_FLAG            = 0x0020,
    SKV_CURSOR_USE_SHARED_STREAM_FLAG		= 0x0040
    } skv_cursor_flags_t;

// Index related structures
  typedef enum
    {
    SKV_INDEX_FLAGS_NONE         = 0x0000,

    /*
     * Hash. Iterator is setup by the server to send
     * records to node ids according to the hash value of the 
     * index fields. This is used in the join.
     */
    SKV_INDEX_FLAGS_HASH_ORDERED = 0x0001,

    /* 
     * Shared iterator allows the clients to dynamically fetch the
     * next small set of records from the servers.  Servers just hold
     * a single list and ignore client id (requires shared cursor flag
     * too)
     */

    SKV_INDEX_FLAGS_SHARED	  = 0x0002
    } skv_index_flags_t;

// Bulk Inserter related structures
  typedef enum
    {
    SKV_BULK_INSERTER_FLAGS_NONE  = 0x0000
    } skv_bulk_inserter_flags_t;

  typedef char skv_pdsname_string_t;

  typedef struct
    {
    uint64_t              mSize;
    skv_pds_priv_t       mPrivs;
    skv_pdsname_string_t mPDSName[ SKV_MAX_PDS_NAME_SIZE ];
    uint64_t              Reserved;
    } skv_pds_attr_ext_t;



  typedef int   skv_client_group_id_t;
  typedef void* skv_hdl_t;
  typedef void* skv_pds_hdl_t;


  typedef void* skv_client_cmd_ext_hdl_t;
  typedef void* skv_client_cursor_ext_hdl_t;
  typedef void* skv_client_relational_cursor_ext_hdl_t;
  typedef void* skv_client_index_ext_hdl_t;
  typedef void* skv_client_bulk_inserter_ext_hdl_t;


/**
 * Initializes clients's state
 */
#if defined( SKV_NON_MPI ) || defined ( SKV_CLIENT_UNI )

  skv_status_t
  SKV_Init( skv_client_group_id_t aCommGroupId,
            int aFlags,
            const char* aConfigFile,
            skv_hdl_t *aClient );
#else

  skv_status_t
  SKV_Init( skv_client_group_id_t aCommGroupId,
             MPI_Comm aComm,
             int aFlags,
             const char* aConfigFile,
             skv_hdl_t *aClient );
#endif


/**
 * Releases client's state
 */
  skv_status_t
  SKV_Finalize( skv_hdl_t aClient );

  skv_status_t
  SKV_Connect( skv_hdl_t  aClient,
                const char *aServerGroupFile,
                int         aFlags );

  skv_status_t
  SKV_Disconnect( skv_hdl_t aClient );

/***
 * Once a connection is established
 * Operations on a partition data sets (PDS)
 ***/

// Blocking interface
  skv_status_t
  SKV_Open( skv_hdl_t             aClient,
             char                  *aPDSName, 
             skv_pds_priv_t        aPrivs, 
             skv_cmd_open_flags_t  aFlags, 
             skv_pds_hdl_t        *aPDSId );

  skv_status_t
  SKV_Retrieve( skv_hdl_t           aClient,
                 skv_pds_hdl_t       aPDSId, 
                 char                *aKeyBuffer,
                 int                  aKeyBufferSize,
                 char                *aValueBuffer,
                 int                  aValueBufferSize,
                 int                 *aValueRetrievedSize,
                 int                  aOffset, 
                 skv_cmd_RIU_flags_t aFlags );

  skv_status_t
  SKV_Update( skv_hdl_t             aClient,
               skv_pds_hdl_t         aPDSId, 
               char                  *aKeyBuffer,
               int                    aKeyBufferSize,
               char                  *aValueBuffer,
               int                    aValueUpdateSize,
               int                    aOffset,
               skv_cmd_RIU_flags_t   aFlags );

  skv_status_t
  SKV_Insert( skv_hdl_t            aClient,
               skv_pds_hdl_t        aPDSId, 
               char                 *aKeyBuffer,
               int                   aKeyBufferSize,
               char                 *aValueBuffer,
               int                   aValueBufferSize,
               int                   aValueBufferOffset,
               skv_cmd_RIU_flags_t  aFlags );

  skv_status_t
  SKV_Remove( skv_hdl_t               aClient,
               skv_pds_hdl_t           aPDSId, 
               char                    *aKeyBuffer,
               int                      aKeyBufferSize,
               skv_cmd_remove_flags_t  aFlags );


  skv_status_t
  SKV_Close( skv_hdl_t     aClient,
              skv_pds_hdl_t aPDSId );

  skv_status_t
  SKV_PDSCntl( skv_hdl_t           aClient,
                skv_pds_hdl_t       aPDSId,
                skv_pdscntl_cmd_t   aCmd,
                skv_pds_attr_ext_t *aPDSAttr );

// Async interface
  skv_status_t
  SKV_Iopen( skv_hdl_t                 aClient,
              char                      *aPDSName, 
              skv_pds_priv_t            aPrivs, 
              skv_cmd_open_flags_t      aFlags, 
              skv_pds_hdl_t            *aPDSId, 
              skv_client_cmd_ext_hdl_t *aCmdHdl );

  skv_status_t
  SKV_Iretrieve( skv_hdl_t                  aClient,
                  skv_pds_hdl_t              aPDSId,
                  char                       *aKeyBuffer,
                  int                         aKeyBufferSize,
                  char                       *aValueBuffer,
                  int                         aValueBufferSize,
                  int                        *aValueRetrievedSize,
                  int                         aOffset, 
                  skv_cmd_RIU_flags_t        aFlags,
                  skv_client_cmd_ext_hdl_t  *aCmdHdl );

  skv_status_t
  SKV_Iupdate( skv_hdl_t                  aClient,
                skv_pds_hdl_t              aPDSId,
                char                       *aKeyBuffer,
                int                         aKeyBufferSize,
                char                       *aValueBuffer,
                int                         aValueUpdateSize,
                int                         aOffset,
                skv_cmd_RIU_flags_t        aFlags,
                skv_client_cmd_ext_hdl_t  *aCmdHdl );

  skv_status_t
  SKV_Iinsert( skv_hdl_t                  aClient,
                skv_pds_hdl_t              aPDSId,
                char                       *aKeyBuffer,
                int                         aKeyBufferSize,
                char                       *aValueBuffer,
                int                         aValueBufferSize,
                int                         aValueBufferOffset,
                skv_cmd_RIU_flags_t        aFlags,
                skv_client_cmd_ext_hdl_t  *aCmdHdl );

  skv_status_t
  SKV_Iremove( skv_hdl_t                 aClient,
                skv_pds_hdl_t             aPDSId, 
                char                      *aKeyBuffer,
                int                        aKeyBufferSize,
                skv_cmd_remove_flags_t    aFlags,
                skv_client_cmd_ext_hdl_t *aCmdHdl );

  skv_status_t
  SKV_Iclose( skv_hdl_t                 aClient,
               skv_pds_hdl_t             aPDSId,
               skv_client_cmd_ext_hdl_t *aCmdHdl );


// Wait

// Returns a handle that has completed
  skv_status_t
  SKV_WaitAny( skv_hdl_t                 aClient,
                skv_client_cmd_ext_hdl_t *aCmdHdl );

  skv_status_t
  SKV_Wait( skv_hdl_t                    aClient,
             skv_client_cmd_ext_hdl_t     aCmdHdl );

// Test
// Returns a completed handle on success
  skv_status_t
  SKV_TestAny( skv_hdl_t                 aClient,
                skv_client_cmd_ext_hdl_t *aCmdHdl );

  skv_status_t
  SKV_Test( skv_hdl_t                 aClient,
             skv_client_cmd_ext_hdl_t  aCmdHdl );


/******************************************************************************
 * Pure Key / Value Cursor Interface
 *****************************************************************************/
// Local Cursor Interface
  skv_status_t
  SKV_OpenLocalCursor( int                           aNodeId, 
                        skv_pds_hdl_t                aPDSId, 
                        skv_client_cursor_ext_hdl_t *aCursorHdl );

  skv_status_t
  SKV_CloseLocalCursor( skv_client_cursor_ext_hdl_t aCursorHdl );

  skv_status_t
  GetFirstLocalElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                        char                         *aRetrievedKeyBuffer,
                        int                          *aRetrievedKeySize,
                        int                           aRetrievedKeyMaxSize,
                        char                         *aRetrievedValueBuffer,
                        int                          *aRetrievedValueSize,
                        int                           aRetrievedValueMaxSize,
                        skv_cursor_flags_t           aFlags );

  skv_status_t
  SKV_GetNextLocalElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                            char                         *aRetrievedKeyBuffer,
                            int                          *aRetrievedKeySize,
                            int                           aRetrievedKeyMaxSize,
                            char                         *aRetrievedValueBuffer,
                            int                          *aRetrievedValueSize,
                            int                           aRetrievedValueMaxSize,
                            skv_cursor_flags_t           aFlags );

// Global Cursor
  skv_status_t
  SKV_OpenCursor( skv_pds_hdl_t                aPDSId, 
                   skv_client_cursor_ext_hdl_t *aCursorHdl );

  skv_status_t
  SKV_CloseCursor( skv_client_cursor_ext_hdl_t aCursorHdl );

  skv_status_t
  SKV_GetFirstElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                        char                         *aRetrievedKeyBuffer,
                        int                          *aRetrievedKeySize,
                        int                           aRetrievedKeyMaxSize,
                        char                         *aRetrievedValueBuffer,
                        int                          *aRetrievedValueSize,
                        int                           aRetrievedValueMaxSize,
                        skv_cursor_flags_t           aFlags );

  skv_status_t
  SKV_GetNextElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                       char                         *aRetrievedKeyBuffer,
                       int                          *aRetrievedKeySize,
                       int                           aRetrievedKeyMaxSize,
                       char                         *aRetrievedValueBuffer,
                       int                          *aRetrievedValueSize,
                       int                           aRetrievedValueMaxSize,
                       skv_cursor_flags_t           aFlags );
/*****************************************************************************/






/******************************************************************************
 * Bulk Insert Interface
 *****************************************************************************/
  skv_status_t
  SKV_CreateBulkInserter( skv_pds_hdl_t                           aPDSId,
                           skv_bulk_inserter_flags_t               aFlags,
                           skv_client_bulk_inserter_ext_hdl_t     *aBulkInserterHandle );

  skv_status_t
  SKV_Bulk_Insert( skv_client_bulk_inserter_ext_hdl_t                  aBulkInserterHandle,
                    char                                                *aKeyBuffer,
                    int                                                  aKeyBufferSize,
                    char                                                *aValueBuffer,
                    int                                                  aValueBufferSize,			
                    skv_bulk_inserter_flags_t                           aFlags );

  skv_status_t
  SKV_Flush( skv_client_bulk_inserter_ext_hdl_t                   aBulkInserterHandle );

  skv_status_t
  SKV_CloseBulkInserter(  skv_client_bulk_inserter_ext_hdl_t      aBulkInserterHandle );  
/*****************************************************************************/


/******************************************************************************
 * Persistence
 *****************************************************************************/
  skv_status_t
  DumpPersistentImage( char* aPath );
/*****************************************************************************/

// Debugging
  skv_status_t DumpPDS( skv_pds_hdl_t aPDSId,
                         int            aMaxKeySize,
                         int            aMaxValueSize );

#if defined(__cplusplus)
  };  // extern "C"
#endif


#endif
