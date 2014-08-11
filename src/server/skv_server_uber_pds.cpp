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

#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_uber_pds.hpp>

#ifndef SKV_SERVER_UBER_PDS_LOG
#define SKV_SERVER_UBER_PDS_LOG ( 0 | SKV_LOGGING_ALL  )
#endif

int 
skv_uber_pds_t::
GetMaxDataLoad()
{
  return mLocalData.GetMaxDataLoad();
}

skv_status_t 
skv_uber_pds_t::
Remove( skv_pds_id_t             aPDSId, 
        char*                    aKeyData,
        int                      aKeySize )
{
  return mLocalData.Remove( aPDSId,
                            aKeyData,
                            aKeySize );
}

skv_status_t 
skv_uber_pds_t::
Insert( skv_pds_id_t             aPDSId, 
        char*                    aRecordRep,
        int                      aKeySize,
        int                      aValueSize )
{
  return mLocalData.Insert( aPDSId,
                            aRecordRep,
                            aKeySize,
                            aValueSize );

}

skv_status_t 
skv_uber_pds_t::
Allocate( int                aSize,
          skv_lmr_triplet_t* aRemMemRep )
{  
  return mLocalData.Allocate( aSize,
                              aRemMemRep );
}

skv_status_t 
skv_uber_pds_t::
Deallocate( skv_lmr_triplet_t* aRemMemRep )
{
  return mLocalData.Deallocate( aRemMemRep );  
}


skv_status_t 
skv_uber_pds_t::
Retrieve( skv_pds_id_t             aPDSId, 
          char*                    aKeyData,
          int                      aKeySize,
          int                      aValueOffset,
          int                      aValueSize,
          skv_cmd_RIU_flags_t      aFlags,
          skv_lmr_triplet_t*       aMemRepValue )
{
  return mLocalData.Retrieve( aPDSId, 
                              aKeyData,
                              aKeySize,
                              aValueOffset,
                              aValueSize,
                              aFlags,
                              aMemRepValue );
}

skv_status_t 
skv_uber_pds_t::
UnlockRecord( skv_rec_lock_handle_t   aRecLock )
{
  return mLocalData.UnlockRecord( aRecLock );
}

skv_status_t
skv_uber_pds_t::
CreateCursor( char*                    aBuff,
              int                      aBuffSize,
              skv_server_cursor_hdl_t* aServCursorHdl )
{
  return mLocalData.CreateCursor( aBuff,
                                  aBuffSize,
                                  aServCursorHdl );
}

skv_status_t 
skv_uber_pds_t::
LockRecord( skv_pds_id_t             aPDSId,
            char*                    aKeyData,
            int                      aKeySize,
            skv_rec_lock_handle_t*   aRecLock )
{
  return mLocalData.LockRecord( aPDSId,
                                aKeyData,
                                aKeySize,
                                aRecLock);
}

skv_status_t 
skv_uber_pds_t::
RetrieveNKeys( skv_pds_id_t       aPDSId, 
               char *             aStartingKeyData,
               int                aStartingKeySize,
               skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
               int*               aRetrievedKeysCount,
               int*               aRetrievedKeysSizesSegsCount,
               int                aListOfKeysMaxCount,
               skv_cursor_flags_t aFlags )
{
  return mLocalData.RetrieveNKeys( aPDSId, 
                                   aStartingKeyData,
                                   aStartingKeySize,
                                   aRetrievedKeysSizesSegs,
                                   aRetrievedKeysCount,
                                   aRetrievedKeysSizesSegsCount,
                                   aListOfKeysMaxCount,
                                   aFlags );
}



/***
 * skv_uber_pds_t::Open::
 * Desc: Opens a partitioned data set
 * input: 
 * aPDSName -> Name of the partitioned data set (PDS)
 * aFlags -> 
 *          SKV_CREATE: Create the PDS if it doesn't exist
 *          default:     Assumes that the PDS is already created
 * aPDSId -> The pointer is filled in as a result of the Open()
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_uber_pds_t::
Open( char*                 aPDSName,
      skv_pds_priv_t        aPrivs, 
      skv_cmd_open_flags_t  aFlags, 
      skv_pds_id_t*         aPDSId )
{
  return mLocalData.Open( aPDSName, aPrivs, aFlags, aPDSId );
}

skv_status_t 
skv_uber_pds_t::
Stat( skv_pdscntl_cmd_t  aCmd, 
      skv_pds_attr_t    *aPDSAttr )
{
  return mLocalData.Stat( aCmd, aPDSAttr );
}

skv_status_t 
skv_uber_pds_t::
Close( skv_pds_attr_t *aPDSAttr )
{
  return mLocalData.Close( aPDSAttr );
}


/***
 * skv_uber_pds_t::Init::
 * Desc: Initializes the state of the skv_uber_pds_t
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_uber_pds_t::
Init( int            aNodeId, 
      int            aNodeCount,
      skv_server_internal_event_manager_if_t* aInternalEventQueue,
      it_pz_handle_t aPZ_Hdl,
      char*          aCheckpointPath )
{
  BegLogLine( SKV_SERVER_UBER_PDS_LOG )
    << "skv_uber_pds_t::Init():: Entering... "
    << " aNodeId: "    << aNodeId
    << " aNodeCount: " << aNodeCount
    << " aPZ_Hdl: "    << (void *) aPZ_Hdl
    << EndLogLine;

  //   mPDSNameTable = new skv_pds_name_table_t;

  //   StrongAssertLogLine( mPDSNameTable != NULL )
  //     << "skv_uber_pds_t::Init:: ERROR:: "
  //     << " mPDSNameTable != NULL"
  //     << EndLogLine;  

  mMyNodeId = aNodeId;

  skv_persistance_flag_t persistanceFlag = SKV_PERSISTANCE_FLAG_INIT;

  if( aCheckpointPath != NULL )
    persistanceFlag = SKV_PERSISTANCE_FLAG_RESTART;

  skv_status_t rc = mLocalData.Init( aPZ_Hdl, aInternalEventQueue, aNodeId, aCheckpointPath, persistanceFlag );
  if( rc )
    return rc;

  rc = mDistributionManager.Init( aNodeCount );
  if( rc )
    return rc; 

  return rc;
}

/***
 * skv_uber_pds_t::Finalize::
 * Desc: Takes down the state of skv_uber_pds_t
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_uber_pds_t::
Finalize()
{
  skv_status_t rc = mLocalData.Finalize();
  if( rc ) 
    return rc;

  rc = mDistributionManager.Finalize();
  if( rc ) 
    return rc;

  return rc;
}




/***
 * skv_uber_pds_t::GetDistribution::
 * Desc: Return the distribution of the skv_uber_pds_t
 * Note, there's one distribution for all PDSIds

 * returns: SKV_SUCCESS on success or error code
 ***/
skv_distribution_t*
skv_uber_pds_t::
GetDistribution()
{     
  return & mDistributionManager;
}

skv_status_t 
skv_uber_pds_t::
DumpPersistenceImage( char* aPath )
{
  return mLocalData.DumpPersistenceImage( aPath );
}

skv_status_t 
skv_uber_pds_t::
InBoundsCheck( const char* aContext,
               char* aMem, 
               int   aSize )
{
  return mLocalData.InBoundsCheck( aContext, aMem, aSize );
}
