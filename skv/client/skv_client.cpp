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

#include <skv/client/skv_client.hpp>
#include <skv/client/skv_client_internal.hpp>
#include <skv/common/skv_init.hpp>

#ifndef SKV_CLIENT_INSERT_TRACE
#define SKV_CLIENT_INSERT_TRACE ( 1 )
#endif

TraceClient SKVClientInsertStart;
TraceClient SKVClientInsertFinis;

// Used to prevent mixed mpi/non-mpi linking of the same library. This library
// is compiled in different variants. Each variant has its own instance of
// initialized, therefore will trip the skv_common_init which may only be called
// once
static bool _checkLinking()
{
  const bool commonInit = skv_common_init();
  StrongAssertLogLine( commonInit )
    << "skv_client:: ERROR:: Another client library version in use (MPI/non-MPI mix?) "
    << EndLogLine;
  return true;
}
static bool initialized = _checkLinking();

skv_client_t::~skv_client_t()
{
  delete mSKVClientInternalPtr;
}


/***
 * skv_client_t::Init::
 * Desc: Initializes the state of the skv_client
 * Gets the client ready to establish a  connection
 * with the serverp
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
#ifdef SKV_CLIENT_UNI
skv_status_t skv_client_t::Init( skv_client_group_id_t aCommGroupId,
                                 int aFlags, const char* aConfigFile )
#else
skv_status_t skv_client_t::Init( skv_client_group_id_t aCommGroupId,
                                 MPI_Comm aComm, int aFlags,
                                 const char* aConfigFile )
#endif
{
  mSKVClientInternalPtr = new skv_client_internal_t;
#ifdef SKV_CLIENT_UNI
  return mSKVClientInternalPtr->Init( aCommGroupId, aFlags, aConfigFile );
#else
  return mSKVClientInternalPtr->Init( aCommGroupId, aComm, aFlags, aConfigFile );
#endif
}

skv_status_t
skv_client_t::
Disconnect()
{
  return mSKVClientInternalPtr->Disconnect();
}


/***
 * skv_client_t::Connect::
 * Desc: Connect to the SKV Server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Connect( const char* aConfigFile,
         int         aFlags )
{
  return mSKVClientInternalPtr->Connect( aConfigFile, aFlags );
}

/***
 * skv_client_t::iOpen::
 * Desc: Async interface to opening a PDS
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iOpen( char*                 aPDSName,
       skv_pds_priv_t        aPrivs,
       skv_cmd_open_flags_t  aFlags,
       skv_pds_id_t*         aPDSId,
       skv_client_cmd_ext_hdl_t* aCmdHdl )
{
  return mSKVClientInternalPtr->iOpen( aPDSName, aPrivs, aFlags, aPDSId,
                                       (skv_client_cmd_hdl_t*) aCmdHdl );
}

/***
 * skv_client_t::iRetrieve::
 * Desc: Async interface to retrieving a record
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iRetrieve( skv_pds_id_t*          aPDSId,
           char*                  aKeyBuffer,
           int                    aKeyBufferSize,
           char*                  aValueBuffer,
           int                    aValueBufferSize,
           int*                   aValueRetrievedSize,
           int                    aOffset,
           skv_cmd_RIU_flags_t    aFlags,
           skv_client_cmd_ext_hdl_t*  aCmdHdl )
{
  return mSKVClientInternalPtr->iRetrieve( aPDSId,
                                           aKeyBuffer, aKeyBufferSize,
                                           aValueBuffer, aValueBufferSize,
                                           aValueRetrievedSize, aOffset, aFlags,
                                           (skv_client_cmd_hdl_t *) aCmdHdl );
}

/***
 * skv_client_t::iUpdate::
 * Desc: Async interface to updating a record
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iUpdate( skv_pds_id_t*          aPDSId,
         char*                  aKeyBuffer,
         int                    aKeyBufferSize,
         char*                  aValueBuffer,
         int                    aValueUpdateSize,
         int                    aOffset,
         skv_cmd_RIU_flags_t    aFlags,
         skv_client_cmd_ext_hdl_t*  aCmdHdl )
{
  return mSKVClientInternalPtr->iUpdate( aPDSId,
                                         aKeyBuffer, aKeyBufferSize,
                                         aValueBuffer, aValueUpdateSize,
                                         aOffset, aFlags,
                                         (skv_client_cmd_hdl_t *) aCmdHdl );
}

/***
 * skv_client_t::iInsert::
 * Desc: Async interface to inserting a record
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iInsert( skv_pds_id_t*          aPDSId,
         char*                  aKeyBuffer,
         int                    aKeyBufferSize,
         char*                  aValueBuffer,
         int                    aValueBufferSize,
         int                    aValueBufferOffset,
         skv_cmd_RIU_flags_t    aFlags,
         skv_client_cmd_ext_hdl_t*  aCmdHdl )
{
  return mSKVClientInternalPtr->iInsert( aPDSId,
                                         aKeyBuffer, aKeyBufferSize,
                                         aValueBuffer, aValueBufferSize,
                                         aValueBufferOffset, aFlags,
                                         (skv_client_cmd_hdl_t *) aCmdHdl );
}


/***
 * skv_client_t::iRemove::
 * Desc: Async interface to remove an item
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iRemove( skv_pds_id_t*          aPDSId,
         char*                  aKeyBuffer,
         int                    aKeyBufferSize,
         skv_cmd_remove_flags_t aFlags,
         skv_client_cmd_ext_hdl_t*  aCmdHdl )
{
  return mSKVClientInternalPtr->iRemove( aPDSId,
                                         aKeyBuffer, aKeyBufferSize,
                                         aFlags,
                                         (skv_client_cmd_hdl_t *)aCmdHdl );
}



/***
 * skv_client_t::iClose::
 * Desc: Async interface to closing a PDS
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iClose( skv_pds_id_t*             aPDSId,
        skv_client_cmd_ext_hdl_t* aCmdHdl )
{
  return mSKVClientInternalPtr->iClose( aPDSId,
                                        (skv_client_cmd_hdl_t *) aCmdHdl );
}


/***
 * skv_client_t::iPDScntl::
 * Desc: Async interface to stat a PDS
 * input: pdsID,
 * inout: attributes
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
iPDScntl( skv_pdscntl_cmd_t         aCmd,
          skv_pds_attr_t           *aPDSAttr,
          skv_client_cmd_ext_hdl_t *aCmdHdl )
  {
  return mSKVClientInternalPtr->iPDScntl( aCmd, aPDSAttr,
                                          (skv_client_cmd_hdl_t *) aCmdHdl );
  }

/***
 * skv_client_t::TestAny::
 * Desc: Check if any command is done
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
TestAny( skv_client_cmd_ext_hdl_t* aCmdHdl )
{
  return mSKVClientInternalPtr->TestAny( (skv_client_cmd_hdl_t *) aCmdHdl );
}

/***
 * skv_client_t::Test::
 * Desc: Check if a command is done
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Test( skv_client_cmd_ext_hdl_t aCmdHdl )
{
  return mSKVClientInternalPtr->Test( (skv_client_cmd_hdl_t)   aCmdHdl );
}

/***
 * skv_client_t::WaitAny::
 * Desc: Wait on any command handle
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
WaitAny( skv_client_cmd_ext_hdl_t* aCmdHdl )
{
  return mSKVClientInternalPtr->WaitAny( (skv_client_cmd_hdl_t *) aCmdHdl );
}

/***
 * skv_client_t::Wait::
 * Desc: Wait on a command handle
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Wait( skv_client_cmd_ext_hdl_t aCmdHdl )
{
  return mSKVClientInternalPtr->Wait( (skv_client_cmd_hdl_t) aCmdHdl );
}

/***
 * skv_client_t::Open::
 * Desc: Create or open a new PDS (partition data set)
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Open( char*                  aPDSName,
      skv_pds_priv_t         aPrivs,
      skv_cmd_open_flags_t   aFlags,
      skv_pds_id_t*          aPDSId )
{
  return mSKVClientInternalPtr->Open( aPDSName, aPrivs, aFlags, aPDSId );
}

/***
 * skv_client_t::Close::
 * Desc: Close the pds
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Close( skv_pds_id_t* aPDSId )
{
  return mSKVClientInternalPtr->Close( aPDSId );
}

/***
 * skv_client_t::iPDScntl::
 * Desc: Sync interface to stat a PDS
 * input: pdsID,
 * inout: attributes
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
PDScntl( skv_pdscntl_cmd_t  aCmd,
         skv_pds_attr_t    *aPDSAttr )
  {
  return mSKVClientInternalPtr->PDScntl( aCmd, aPDSAttr );
  }


/***
 * skv_client_t::Retrieve::
 * Desc: Retrieve a record from the skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Retrieve( skv_pds_id_t*       aPDSId,
          char*               aKeyBuffer,
          int                 aKeyBufferSize,
          char*               aValueBuffer,
          int                 aValueBufferSize,
          int*                aValueRetrievedSize,
          int                 aOffset,
          skv_cmd_RIU_flags_t aFlags  )
{
  return mSKVClientInternalPtr->Retrieve( aPDSId,
                                          aKeyBuffer, aKeyBufferSize,
                                          aValueBuffer, aValueBufferSize,
                                          aValueRetrievedSize, aOffset, aFlags );
}

/***
 * skv_client_t::Update::
 * Desc: Update a record on the skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Update( skv_pds_id_t*         aPDSId,
        char*                 aKeyBuffer,
        int                   aKeyBufferSize,
        char*                 aValueBuffer,
        int                   aValueUpdateSize,
        int                   aOffset,
        skv_cmd_RIU_flags_t   aFlags )
{
  return mSKVClientInternalPtr->Update( aPDSId,
                                        aKeyBuffer, aKeyBufferSize,
                                        aValueBuffer, aValueUpdateSize,
                                        aOffset, aFlags );
}

/***
 * skv_client_t::Insert::
 * Desc: Insert a record into the skv server
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
Insert( skv_pds_id_t*       aPDSId,
        char*               aKeyBuffer,
        int                 aKeyBufferSize,
        char*               aValueBuffer,
        int                 aValueBufferSize,
        int                 aValueBufferOffset,
        skv_cmd_RIU_flags_t aFlags )
{
  SKVClientInsertStart.HitOE( SKV_CLIENT_INSERT_TRACE,
                              "SKVClientInsert",
                              mSKVClientInternalPtr->GetRank(),
                              SKVClientInsertStart );

  skv_status_t status  = mSKVClientInternalPtr->Insert( aPDSId,
                                                        aKeyBuffer, aKeyBufferSize,
                                                        aValueBuffer, aValueBufferSize,
                                                        aValueBufferOffset,
                                                        aFlags );

  SKVClientInsertFinis.HitOE( SKV_CLIENT_INSERT_TRACE,
                              "SKVClientInsert",
                              mSKVClientInternalPtr->GetRank(),
                              SKVClientInsertFinis );

  return status;
}

/**********************************************************
 * Local Cursor Interface
 **********************************************************/

skv_status_t
skv_client_t::
GetLocalServerRanks( int **aLocalServers, int *aCount )
{
  return mSKVClientInternalPtr->GetLocalServerRanks( aLocalServers, aCount );
}

/***
 * skv_client_t::OpenLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
OpenLocalCursor( int                          aNodeId,
                 skv_pds_id_t*                aPDSId,
                 skv_client_cursor_ext_hdl_t* aCursorHdl )
{
  return mSKVClientInternalPtr->OpenLocalCursor( aNodeId, aPDSId,
                                                 (skv_client_cursor_handle_t *) aCursorHdl );
}

/***
 * skv_client_t::CloseLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
CloseLocalCursor( skv_client_cursor_ext_hdl_t  aCursorHdl )
{
  return mSKVClientInternalPtr->CloseLocalCursor( (skv_client_cursor_handle_t) aCursorHdl );
}


/***
 * skv_client_t::GetFirstLocalElement::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
GetFirstLocalElement( skv_client_cursor_ext_hdl_t aCursorHdl,
                      char*                       aRetrievedKeyBuffer,
                      int*                        aRetrievedKeySize,
                      int                         aRetrievedKeyMaxSize,
                      char*                       aRetrievedValueBuffer,
                      int*                        aRetrievedValueSize,
                      int                         aRetrievedValueMaxSize,
                      skv_cursor_flags_t          aFlags )
{
  return mSKVClientInternalPtr->GetFirstLocalElement(
      (skv_client_cursor_handle_t) aCursorHdl,
      aRetrievedKeyBuffer, aRetrievedKeySize, aRetrievedKeyMaxSize,
      aRetrievedValueBuffer, aRetrievedValueSize, aRetrievedValueMaxSize,
      aFlags );
}

/***
 * skv_client_t::GetNextLocalElement::
 * Desc: Get the next element in the cursor pointed to a node id
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
GetNextLocalElement(  skv_client_cursor_ext_hdl_t   aCursorHdl,
                      char*                         aRetrievedKeyBuffer,
                      int*                          aRetrievedKeySize,
                      int                           aRetrievedKeyMaxSize,
                      char*                         aRetrievedValueBuffer,
                      int*                          aRetrievedValueSize,
                      int                           aRetrievedValueMaxSize,
                      skv_cursor_flags_t            aFlags )
{
  return mSKVClientInternalPtr->GetNextLocalElement( (skv_client_cursor_handle_t) aCursorHdl,
                                                                                aRetrievedKeyBuffer,
                                                                                aRetrievedKeySize,
                                                                                aRetrievedKeyMaxSize,
                                                                                aRetrievedValueBuffer,
                                                                                aRetrievedValueSize,
                                                                                aRetrievedValueMaxSize,
                                                                                aFlags );
}

/**********************************************************
 * Global Cursor Interface
 **********************************************************/
#if 1
/***
 * skv_client_t::OpenCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
OpenCursor( skv_pds_id_t*                aPDSId,
            skv_client_cursor_ext_hdl_t* aCursorHdl )
{
  return mSKVClientInternalPtr->OpenCursor( aPDSId,
                                            (skv_client_cursor_handle_t * ) aCursorHdl);
}
#endif

/***
 * skv_client_t::CloseLocalCursor::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
CloseCursor( skv_client_cursor_ext_hdl_t aCursorHdl )
{
  return mSKVClientInternalPtr->CloseCursor( (skv_client_cursor_handle_t) aCursorHdl);
}


skv_status_t
skv_client_t::
Remove( skv_pds_id_t*          aPDSId,
        char*                  aKeyBuffer,
        int                    aKeyBufferSize,
        skv_cmd_remove_flags_t aFlags )
{
  skv_status_t status  = mSKVClientInternalPtr->Remove( aPDSId,
                                                        aKeyBuffer,
                                                        aKeyBufferSize,
                                                        aFlags );
  return status;
}

skv_status_t
skv_client_t::
Finalize()
{
  return mSKVClientInternalPtr->Finalize();
}


/***
 * skv_client_t::GetFirstElement::
 * Desc: Get the first element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
GetFirstElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                 char*                        aRetrievedKeyBuffer,
                 int*                         aRetrievedKeySize,
                 int                          aRetrievedKeyMaxSize,
                 char*                        aRetrievedValueBuffer,
                 int*                         aRetrievedValueSize,
                 int                          aRetrievedValueMaxSize,
                 skv_cursor_flags_t           aFlags )
{
  return mSKVClientInternalPtr->GetFirstElement( (skv_client_cursor_handle_t) aCursorHdl,
                                                                            aRetrievedKeyBuffer,
                                                                            aRetrievedKeySize,
                                                                            aRetrievedKeyMaxSize,
                                                                            aRetrievedValueBuffer,
                                                                            aRetrievedValueSize,
                                                                            aRetrievedValueMaxSize,
                                                                            aFlags );
}

/***
 * skv_client_t::GetNextElement::
 * Desc: Get the next global element in the cursor
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
GetNextElement( skv_client_cursor_ext_hdl_t  aCursorHdl,
                char*                        aRetrievedKeyBuffer,
                int*                         aRetrievedKeySize,
                int                          aRetrievedKeyMaxSize,
                char*                        aRetrievedValueBuffer,
                int*                         aRetrievedValueSize,
                int                          aRetrievedValueMaxSize,
                skv_cursor_flags_t           aFlags )
{
  return mSKVClientInternalPtr->GetNextElement( (skv_client_cursor_handle_t) aCursorHdl,
                                                                           aRetrievedKeyBuffer,
                                                                           aRetrievedKeySize,
                                                                           aRetrievedKeyMaxSize,
                                                                           aRetrievedValueBuffer,
                                                                           aRetrievedValueSize,
                                                                           aRetrievedValueMaxSize,
                                                                           aFlags );
}

/***
 * skv_client_t::DumpPDS::
 * Desc:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_t::
DumpPDS( skv_pds_id_t aPDSId,
         int          aMaxKeySize,
         int          aMaxValueSize )
{
  return mSKVClientInternalPtr->DumpPDS( aPDSId, aMaxKeySize, aMaxValueSize );
}


/******************************************************************************
 * Bulk Insert Interface
 *****************************************************************************/
skv_status_t
skv_client_t::
CreateBulkInserter( skv_pds_id_t*                           aPDSId,
                    skv_bulk_inserter_flags_t               aFlags,
                    skv_client_bulk_inserter_ext_hdl_t*     aBulkInserterHandle )
{
  return mSKVClientInternalPtr->CreateBulkInserter( aPDSId,
                                                    aFlags,
                                                    (skv_client_bulk_inserter_hdl_t *) aBulkInserterHandle );
}

skv_status_t
skv_client_t::
Insert( skv_client_bulk_inserter_ext_hdl_t  aBulkInserterHandle,
        char*                               aKeyBuffer,
        int                                 aKeyBufferSize,
        char*                               aValueBuffer,
        int                                 aValueBufferSize,
        // int                              aValueBufferOffset ??? This can be supported later
        skv_bulk_inserter_flags_t           aFlags )
{
  return mSKVClientInternalPtr->Insert( (skv_client_bulk_inserter_hdl_t) aBulkInserterHandle,
                                        aKeyBuffer, aKeyBufferSize,
                                        aValueBuffer, aValueBufferSize,
                                        aFlags );
}

skv_status_t
skv_client_t::
Flush( skv_client_bulk_inserter_ext_hdl_t aBulkInserterHandle )
{
  return mSKVClientInternalPtr->Flush( (skv_client_bulk_inserter_hdl_t) aBulkInserterHandle );
}

skv_status_t
skv_client_t::
CloseBulkInserter( skv_client_bulk_inserter_ext_hdl_t aBulkInserterHandle )
{
  return mSKVClientInternalPtr->CloseBulkInserter( (skv_client_bulk_inserter_hdl_t) aBulkInserterHandle );
}

skv_status_t
skv_client_t::
DumpPersistentImage( char* aPath )
{
  return mSKVClientInternalPtr->DumpPersistentImage( aPath );
}
/*****************************************************************************/
