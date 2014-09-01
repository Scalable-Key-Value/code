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

#if !defined( SKV_NON_MPI ) && !defined ( SKV_CLIENT_UNI )
#include "mpi.h"
#endif

#include <skv.h>
#include <client/skv_client_internal.hpp>

#if defined( SKV_NON_MPI ) || defined ( SKV_CLIENT_UNI )

skv_status_t
SKV_Init( skv_client_group_id_t aCommGroupId,
           int aFlags,
           const char* aConfigFile,
           skv_hdl_t *aClient )
  {
  *aClient = malloc( sizeof( skv_client_internal_t ));
  StrongAssertLogLine( *aClient != NULL )
    << "skv_client_t::Init():: ERROR:: Couldn't allocate client handle "
    << EndLogLine;

  skv_status_t status = ((skv_client_internal_t *)(*aClient))->Init( aCommGroupId,  
                                                                     aFlags,
                                                                     aConfigFile );
  return status;
  }
#else

skv_status_t
SKV_Init( skv_client_group_id_t aCommGroupId,
           MPI_Comm aComm,
           int aFlags,
           const char* aConfigFile,
           skv_hdl_t *aClient )
  {
  *aClient = malloc( sizeof( skv_client_internal_t ));
  StrongAssertLogLine( *aClient != NULL )
    << "skv_client_t::Init():: ERROR:: Couldn't allocate client handle "
    << EndLogLine;

  skv_status_t status = ((skv_client_internal_t *)(*aClient))->Init( aCommGroupId,  
                                                                     aComm,
                                                                     aFlags,
                                                                     aConfigFile );
  return status;
  }
#endif


/**
 * Releases client's state
 */
skv_status_t
SKV_Finalize( skv_hdl_t aClient )
  {
  skv_status_t ret = ((skv_client_internal_t *)aClient)->Finalize();
  free( aClient );
  return ret;
  }


skv_status_t
SKV_Connect( skv_hdl_t  aClient,
              const char *aServerGroupFile,
              int         aFlags )
  {
  return ((skv_client_internal_t *)aClient)->Connect( aServerGroupFile,
                                                      aFlags );
  }


skv_status_t
SKV_Disconnect( skv_hdl_t aClient )
  {
  return SKV_SUCCESS;
  }


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
           skv_pds_hdl_t        *aPDSId )
  {
  skv_pds_id_t *MyPDSId = new skv_pds_id_t;
  *aPDSId = (skv_pds_hdl_t)MyPDSId;
  return ((skv_client_internal_t *)aClient)->Open( aPDSName, 
                                                    aPrivs, 
                                                    aFlags, 
                                                    MyPDSId );
  }


skv_status_t
SKV_Retrieve( skv_hdl_t           aClient,
               skv_pds_hdl_t       aPDSId, 
               char                *aKeyBuffer,
               int                  aKeyBufferSize,
               char                *aValueBuffer,
               int                  aValueBufferSize,
               int                 *aValueRetrievedSize,
               int                  aOffset, 
               skv_cmd_RIU_flags_t aFlags )
  {
  return ((skv_client_internal_t *)aClient)->Retrieve( (skv_pds_id_t*)aPDSId, 
                                                        aKeyBuffer,
                                                        aKeyBufferSize,
                                                        aValueBuffer,
                                                        aValueBufferSize,
                                                        aValueRetrievedSize,
                                                        aOffset, 
                                                        aFlags );
  }


skv_status_t
SKV_Update( skv_hdl_t             aClient,
             skv_pds_hdl_t         aPDSId, 
             char                  *aKeyBuffer,
             int                    aKeyBufferSize,
             char                  *aValueBuffer,
             int                    aValueUpdateSize,
             int                    aOffset,
             skv_cmd_RIU_flags_t   aFlags )
  {
  return ((skv_client_internal_t *)aClient)->Update( (skv_pds_id_t*)aPDSId,
                                                      aKeyBuffer,
                                                      aKeyBufferSize,
                                                      aValueBuffer,
                                                      aValueUpdateSize,
                                                      aOffset,
                                                      aFlags );
  }


skv_status_t
SKV_Insert( skv_hdl_t            aClient,
             skv_pds_hdl_t        aPDSId, 
             char                 *aKeyBuffer,
             int                   aKeyBufferSize,
             char                 *aValueBuffer,
             int                   aValueBufferSize,
             int                   aValueBufferOffset,
             skv_cmd_RIU_flags_t  aFlags )
  {
  return ((skv_client_internal_t *)aClient)->Insert( (skv_pds_id_t*)aPDSId, 
                                                      aKeyBuffer,
                                                      aKeyBufferSize,
                                                      aValueBuffer,
                                                      aValueBufferSize,
                                                      aValueBufferOffset,
                                                      aFlags );
  }


skv_status_t
SKV_Remove( skv_hdl_t               aClient,
             skv_pds_hdl_t           aPDSId, 
             char                    *aKeyBuffer,
             int                      aKeyBufferSize,
             skv_cmd_remove_flags_t  aFlags )
  {
  return ((skv_client_internal_t *)aClient)->Remove( (skv_pds_id_t*)aPDSId, 
                                                      aKeyBuffer,
                                                      aKeyBufferSize,
                                                      aFlags );
  }



skv_status_t
SKV_Close( skv_hdl_t       aClient,
            skv_pds_hdl_t   aPDSId )
  {
  skv_status_t ret = ((skv_client_internal_t *)aClient)->Close( (skv_pds_id_t*)aPDSId );
  delete (skv_pds_id_t*)aPDSId;
  return ret;
  }


skv_status_t
SKV_PDSCntl( skv_hdl_t           aClient,
              skv_pds_hdl_t       aPDSId,
              skv_pdscntl_cmd_t   aCmd,
              skv_pds_attr_ext_t *aPDSAttr )
  {
  skv_pds_attr_t *intAttr = (skv_pds_attr_t*) aPDSAttr;
  intAttr->mPDSId = *((skv_pds_id_t *)aPDSId);

  skv_status_t ret = ((skv_client_internal_t *)aClient)->PDScntl( aCmd,
                                                                    intAttr );

  aPDSAttr->Reserved = 0;
  return ret;
  }



// Async interface
skv_status_t
SKV_Iopen( skv_hdl_t                 aClient,
            char                      *aPDSName, 
            skv_pds_priv_t            aPrivs, 
            skv_cmd_open_flags_t      aFlags, 
            skv_pds_hdl_t            *aPDSId, 
            skv_client_cmd_ext_hdl_t *aCmdHdl )
  {
  skv_pds_id_t  *MyPDSId = new skv_pds_id_t;
  *aPDSId = (skv_pds_hdl_t)MyPDSId;
  return ((skv_client_internal_t *)aClient)->iOpen( aPDSName, 
                                                     aPrivs, 
                                                     aFlags, 
                                                     MyPDSId, 
                                                     (skv_client_cmd_hdl_t*) aCmdHdl );
  }


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
                skv_client_cmd_ext_hdl_t  *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->iRetrieve( (skv_pds_id_t*)aPDSId,
                                                         aKeyBuffer,
                                                         aKeyBufferSize,
                                                         aValueBuffer,
                                                         aValueBufferSize,
                                                         aValueRetrievedSize,
                                                         aOffset, 
                                                         aFlags,
                                                         (skv_client_cmd_hdl_t *) aCmdHdl );
  }


skv_status_t
SKV_Iupdate( skv_hdl_t                  aClient,
              skv_pds_hdl_t              aPDSId,
              char                       *aKeyBuffer,
              int                         aKeyBufferSize,
              char                       *aValueBuffer,
              int                         aValueUpdateSize,
              int                         aOffset,
              skv_cmd_RIU_flags_t        aFlags,
              skv_client_cmd_ext_hdl_t  *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->iUpdate( (skv_pds_id_t*)aPDSId,
                                                       aKeyBuffer,
                                                       aKeyBufferSize,
                                                       aValueBuffer,
                                                       aValueUpdateSize,
                                                       aOffset, 
                                                       aFlags,
                                                       (skv_client_cmd_hdl_t *) aCmdHdl );
  }


skv_status_t
SKV_Iinsert( skv_hdl_t                  aClient,
              skv_pds_hdl_t              aPDSId,
              char                       *aKeyBuffer,
              int                         aKeyBufferSize,
              char                       *aValueBuffer,
              int                         aValueBufferSize,
              int                         aValueBufferOffset,
              skv_cmd_RIU_flags_t        aFlags,
              skv_client_cmd_ext_hdl_t  *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->iInsert( (skv_pds_id_t*)aPDSId,
                                                       aKeyBuffer,
                                                       aKeyBufferSize,
                                                       aValueBuffer,
                                                       aValueBufferSize,
                                                       aValueBufferOffset,
                                                       aFlags,
                                                       (skv_client_cmd_hdl_t *) aCmdHdl );
  }


skv_status_t
SKV_Iremove( skv_hdl_t                 aClient,
              skv_pds_hdl_t             aPDSId, 
              char                      *aKeyBuffer,
              int                        aKeyBufferSize,
              skv_cmd_remove_flags_t    aFlags,
              skv_client_cmd_ext_hdl_t *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->iRemove( (skv_pds_id_t*)aPDSId, 
                                                       aKeyBuffer,
                                                       aKeyBufferSize,
                                                       aFlags,
                                                       (skv_client_cmd_hdl_t *)aCmdHdl );
  }


skv_status_t
SKV_Iclose( skv_hdl_t                 aClient,
             skv_pds_hdl_t             aPDSId,
             skv_client_cmd_ext_hdl_t *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->iClose( (skv_pds_id_t*)aPDSId,
                                                      (skv_client_cmd_hdl_t *) aCmdHdl );
  }



// Wait

// Returns a handle that has completed
skv_status_t
SKV_WaitAny( skv_hdl_t                 aClient,
              skv_client_cmd_ext_hdl_t *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->WaitAny( (skv_client_cmd_hdl_t *) aCmdHdl );
  }


skv_status_t
SKV_Wait( skv_hdl_t                    aClient,
           skv_client_cmd_ext_hdl_t     aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->Wait( (skv_client_cmd_hdl_t) aCmdHdl );
  }


// Test
// Returns a completed handle on success
skv_status_t
SKV_TestAny( skv_hdl_t                 aClient,
              skv_client_cmd_ext_hdl_t *aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->TestAny( (skv_client_cmd_hdl_t *) aCmdHdl );
  }


skv_status_t
SKV_Test( skv_hdl_t                 aClient,
           skv_client_cmd_ext_hdl_t  aCmdHdl )
  {
  return ((skv_client_internal_t *)aClient)->Test( (skv_client_cmd_hdl_t)   aCmdHdl );
  }



/******************************************************************************
 * Pure Key / Value Cursor Interface
 *****************************************************************************/
// Local Cursor Interface
skv_status_t
SKV_OpenLocalCursor( int                           aNodeId, 
                      skv_pds_hdl_t                aPDSId, 
                      skv_client_cursor_ext_hdl_t *aCursorHdl )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_CloseLocalCursor( skv_client_cursor_ext_hdl_t aCursorHdl )
  {
  return SKV_SUCCESS;
  }


skv_status_t
GetFirstLocalElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                      char                         *aRetrievedKeyBuffer,
                      int                          *aRetrievedKeySize,
                      int                           aRetrievedKeyMaxSize,
                      char                         *aRetrievedValueBuffer,
                      int                          *aRetrievedValueSize,
                      int                           aRetrievedValueMaxSize,
                      skv_cursor_flags_t           aFlags )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_GetNextLocalElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                          char                         *aRetrievedKeyBuffer,
                          int                          *aRetrievedKeySize,
                          int                           aRetrievedKeyMaxSize,
                          char                         *aRetrievedValueBuffer,
                          int                          *aRetrievedValueSize,
                          int                           aRetrievedValueMaxSize,
                          skv_cursor_flags_t           aFlags )
  {
  return SKV_SUCCESS;
  }


// Global Cursor
skv_status_t
SKV_OpenCursor( skv_pds_hdl_t                aPDSId, 
                 skv_client_cursor_ext_hdl_t *aCursorHdl )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_CloseCursor( skv_client_cursor_ext_hdl_t aCursorHdl )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_GetFirstElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                      char                         *aRetrievedKeyBuffer,
                      int                          *aRetrievedKeySize,
                      int                           aRetrievedKeyMaxSize,
                      char                         *aRetrievedValueBuffer,
                      int                          *aRetrievedValueSize,
                      int                           aRetrievedValueMaxSize,
                      skv_cursor_flags_t           aFlags )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_GetNextElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                     char                         *aRetrievedKeyBuffer,
                     int                          *aRetrievedKeySize,
                     int                           aRetrievedKeyMaxSize,
                     char                         *aRetrievedValueBuffer,
                     int                          *aRetrievedValueSize,
                     int                           aRetrievedValueMaxSize,
                     skv_cursor_flags_t           aFlags )
  {
  return SKV_SUCCESS;
  }

/*****************************************************************************/






/******************************************************************************
 * Bulk Insert Interface
 *****************************************************************************/
skv_status_t
SKV_CreateBulkInserter( skv_pds_hdl_t                           aPDSId,
                         skv_bulk_inserter_flags_t               aFlags,
                         skv_client_bulk_inserter_ext_hdl_t     *aBulkInserterHandle )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_Bulk_Insert( skv_client_bulk_inserter_ext_hdl_t                  aBulkInserterHandle,
                  char                                                *aKeyBuffer,
                  int                                                  aKeyBufferSize,
                  char                                                *aValueBuffer,
                  int                                                  aValueBufferSize,			
                  skv_bulk_inserter_flags_t                           aFlags )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_Flush( skv_client_bulk_inserter_ext_hdl_t                   aBulkInserterHandle )
  {
  return SKV_SUCCESS;
  }


skv_status_t
SKV_CloseBulkInserter(  skv_client_bulk_inserter_ext_hdl_t      aBulkInserterHandle )
  {
  return SKV_SUCCESS;
  }

/*****************************************************************************/


/******************************************************************************
 * Persistence
 *****************************************************************************/
skv_status_t
DumpPersistentImage( char* aPath )
  {
  return SKV_SUCCESS;
  }

/*****************************************************************************/

// Debugging
skv_status_t DumpPDS( skv_pds_hdl_t aPDSId,
                       int            aMaxKeySize,
                       int            aMaxValueSize )
  {
  return SKV_SUCCESS;
  }

