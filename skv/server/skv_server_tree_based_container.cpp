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
#include <server/skv_server_tree_based_container.hpp>

#ifndef SKV_SERVER_TREE_BASED_CONTAINER_LOG
#define SKV_SERVER_TREE_BASED_CONTAINER_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_TREE_BASED_CONTAINER_INIT_LOG
#define SKV_SERVER_TREE_BASED_CONTAINER_INIT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG
#define SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG ( 0 | SKV_LOGGING_ALL )
#endif

skv_status_t 
skv_tree_based_container_t::
Open( char*                 aPDSName,
      skv_pds_priv_t        aPrivs, 
      skv_cmd_open_flags_t  aFlags, 
      skv_pds_id_t*         aPDSId )
{
  skv_status_t status = SKV_SUCCESS;

  skv_server_string_t PdsNameStr( aPDSName );

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
    << "skv_tree_based_container_t::Open():: Entering"
    << " aPDSName: >" << aPDSName << "<"
    << " PdsNameStr: >" << PdsNameStr.c_str() << "<"
    << " aPrivs: " << aPrivs
    << " aFlags: " << aFlags
    << EndLogLine;

  skv_pds_name_table_t::iterator iter = mPDSNameTable->find( PdsNameStr );  

  if( iter != mPDSNameTable->end() )
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
      << "skv_tree_based_container_t::Open():: "
      << " aPDSName: >" << aPDSName << "<"
      << " IS found!"
      << EndLogLine;

    // Entry exists
    if( aFlags & SKV_COMMAND_OPEN_FLAGS_EXCLUSIVE )
    {
      return SKV_ERRNO_PDS_ALREADY_EXISTS;
    }

    skv_pds_id_table_t::iterator iterId = mPDSIdTable->find( iter->second );

    // Check permissions
    if( iterId->second.mPrivs == aPrivs )
      *aPDSId = iterId->second.mPDSId;
    else
      return SKV_ERRNO_PERMISSION_DENIED;
  }
  else
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
      << "skv_tree_based_container_t::Open():: "
      << " aPDSName: >" << aPDSName << "<"
      << " IS NOT found!"
      << EndLogLine;

    // PDS does NOT exist
    if( aFlags & SKV_COMMAND_OPEN_FLAGS_CREATE )
    {
      // first create entry in pds name table
      skv_pds_id_t PDSId;

      PDSId.mOwnerNodeId = mMyNodeId;
      PDSId.mIdOnOwner = *(mLocalPDSCountPtr);
      *mLocalPDSCountPtr = *mLocalPDSCountPtr + 1;

      int rc = mPDSNameTable->insert( std::make_pair( PdsNameStr, PDSId ) ).second;

      BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
        << "skv_tree_based_container_t::Open():: Just inserted a new PdsName: "
        << " PdsNameStr: >" << PdsNameStr.c_str() << "<"
        << " PDSId: " << PDSId
        << EndLogLine;

      StrongAssertLogLine( rc == 1 )
        << "skv_tree_based_container_t::Open():: ERROR:: "
        << " rc: " << rc
        << EndLogLine;

      *aPDSId = PDSId;

      // next create entry in pds id table
      skv_pds_attr_t PDSAttr;
      PDSAttr.mPDSId = PDSId;
      PDSAttr.mPrivs = aPrivs;
      PDSAttr.mSize  = 0;
      strncpy( PDSAttr.mPDSName, aPDSName, SKV_MAX_PDS_NAME_SIZE );

      rc = mPDSIdTable->insert( std::make_pair( PDSId, PDSAttr ) ).second;

      BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
        << "skv_tree_based_container_t::Open():: Just inserted a new PdsId: "
        << " PdsNameStr: >" << PdsNameStr.c_str() << "<"
        << " PDSAttr: " << PDSAttr
        << EndLogLine;
    }
    else
    {
      return SKV_ERRNO_PDS_DOES_NOT_EXIST;
    }
  }

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
    << "skv_tree_based_container_t::Open():: Leaving"
    << " aPDSName: " << aPDSName
    << " status: " << skv_status_to_string( status )
    << " aPDSId: " << *aPDSId
    << EndLogLine;

  return status;
}

skv_status_t
skv_tree_based_container_t::
Stat( skv_pdscntl_cmd_t     aCmd,
      skv_pds_attr_t       *aPDSAttr )
{
  skv_status_t status = SKV_SUCCESS;
  skv_pds_id_t PDSId = aPDSAttr->mPDSId;

  // find the pds based on pds-id
  skv_pds_id_table_t::iterator iter = mPDSIdTable->find( PDSId );

  if( iter != mPDSIdTable->end() )
  {
    switch( aCmd )
    {
      case SKV_PDSCNTL_CMD_STAT_GET:
        *aPDSAttr = iter->second;
        break;

      case SKV_PDSCNTL_CMD_STAT_SET:
        iter->second.mSize = aPDSAttr->mSize;
        break;

      default:
        AssertLogLine( 0 )
          << "skv_tree_based_container_t::Stat():: "
          << " unrecognized PDSCNTL command: " << aCmd
          << EndLogLine;
    }

    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
      << "skv_tree_based_container_t::Stat():: "
      << " PDSId: >" << PDSId << "<"
      << " IS found!"
      << " Attr: " << *aPDSAttr
      << EndLogLine;
  }
  else
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
      << "skv_tree_based_container_t::Stat():: "
      << " PDSId: >" << PDSId << "<"
      << " IS NOT found!"
      << EndLogLine;

    // PDS does NOT exist
    return SKV_ERRNO_PDS_DOES_NOT_EXIST;
  }

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
    << "skv_tree_based_container_t::Stat():: Leaving"
    << " status: "   << skv_status_to_string( status )
    << " aAttr: "    << *aPDSAttr
    << EndLogLine;

  return status;
}


skv_status_t
skv_tree_based_container_t::
Close( skv_pds_attr_t      *aPDSAttr )
{
  skv_status_t status = SKV_SUCCESS;
  skv_pds_id_t PDSId  = aPDSAttr->mPDSId;

  // find the pds based on pds-id
  skv_pds_id_table_t::iterator iter = mPDSIdTable->find( PDSId );

  if( iter != mPDSIdTable->end() )
  {
    iter->second.mSize = aPDSAttr->mSize;

    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
      << "skv_tree_based_container_t::Close():: "
      << " PDSId: >" << PDSId << "<"
      << " IS found!"
      << " Attr: " << iter->second
      << EndLogLine;
  }
  else
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
      << "skv_tree_based_container_t::Close():: "
      << " PDSId: >" << PDSId << "<"
      << " IS NOT found!"
      << EndLogLine;

    // PDS does NOT exist
    return SKV_ERRNO_PDS_DOES_NOT_EXIST;
  }

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_OPEN_LOG )
    << "skv_tree_based_container_t::Close():: Leaving"
    << " status: "   << skv_status_to_string( status )
    << " aAttr: "    << iter->second
    << EndLogLine;

  return status;
}


int 
skv_tree_based_container_t::
GetMaxDataLoad()
{
  return mMaxDataLoad;
}

skv_status_t 
skv_tree_based_container_t::
InBoundsCheck( const char* aContext, 
               char* aMem,
               int   aSize )
{
  BegLogLine( 0 )
    << "InBoundsCheck(): "
    << " aContext: " << aContext
    << " aMem: " << (void *) aMem
    << " aSize: " << aSize
    << " mDataFieldLen: " << mDataFieldLen
    << " mStartOfDataField: " << (void *) mStartOfDataField
    << EndLogLine;  

  uint64_t req_end = (uint64_t)aMem + (uint64_t)aSize;
  uint64_t sto_end = (uint64_t)mStartOfDataField + (uint64_t)mDataFieldLen;

  StrongAssertLogLine( aMem >= mStartOfDataField && 
                       req_end < sto_end )
    << "ERROR: "
    << " aContext: " << aContext
    << " aMem: " << (void *) aMem
    << " aSize: " << aSize
    << " mDataFieldLen: " << mDataFieldLen
    << " mStartOfDataField: " << (void *) mStartOfDataField
    << EndLogLine;  

  return SKV_SUCCESS;
}

skv_status_t 
skv_tree_based_container_t::
Allocate( int                   aSize,
          skv_lmr_triplet_t*   aRemMemRep )
{   
  char* Mem = (char *) skv_server_heap_manager_t::Allocate( aSize );

  if( Mem == NULL )
    return SKV_ERRNO_OUT_OF_MEMORY;

  // Check that the address is within bounds 
  InBoundsCheck( "Allocate1", Mem, aSize );

  aRemMemRep->InitAbs( mDataLMR, Mem, aSize );

  return SKV_SUCCESS;
}

skv_status_t 
skv_tree_based_container_t::
Deallocate( skv_lmr_triplet_t* aRemMemRep )
{  
  char* MemAddr = (char *) aRemMemRep->GetAddr();
  if( MemAddr != NULL )
  {
    skv_server_heap_manager_t::Free( MemAddr );
    aRemMemRep->SetAddr( NULL );
  }

  return SKV_SUCCESS;
}

skv_status_t 
skv_tree_based_container_t::
Insert( skv_pds_id_t         aPDSId, 
        char*                 aRowData,
        int                   aKeySize,
        int                   aValueSize )
{
  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Insert():: Entering"
    << EndLogLine;

  skv_key_t UserKey;
  UserKey.Init( aRowData, 
                aKeySize );

  skv_tree_based_container_key_t* key = MakeKey( aPDSId, &UserKey );

  key->SetValueSize( aValueSize );

  int RowSize   = key->GetRecordSize();
  char* RowData = key->GetRecordPtr();

#if 0
  for( int i=0; i<RowSize; i++ )
  {
    mHeapHdr->mRowDataChecksum += RowData[ i ];
  }
#endif

  BegLogLine(0)
    << "INSERT: " 
    << " key: "     << (void*)key
    << " dataMap: " << (void*)mDataMap
    << EndLogLine;

  int rc = mDataMap->insert( *key ).second;

  skv_status_t status = SKV_SUCCESS;

  if( ! rc )
    status = SKV_ERRNO_RECORD_ALREADY_EXISTS;

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Insert():: Leaving"
    << " status: " << skv_status_to_string ( status )
    << EndLogLine;

  return status;
}

template<class streamclass>
static
streamclass& 
operator<<(streamclass& os, const skv_tree_based_container_key_t& aKey )
{
  os << "skv_tree_based_container_t::Key [ "
     << " mPDSId: " << aKey.mPDSId
     << " mUserKey: " << aKey.mUserKey
     << " mLockState: " << aKey.mLockState
     << " ]";

  return os;
}


skv_status_t 
skv_tree_based_container_t::
RetrieveNKeys( skv_pds_id_t       aPDSId, 
               char *              aStartingKeyData,
               int                 aStartingKeySize,
               skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
               int*                aRetrievedKeysCount,
               int*                aRetrievedKeysSizesSegsCount,
               int                 aListOfKeysMaxCount,
               skv_cursor_flags_t aFlags )
{
  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::RetrieveNKeys():: Entering "
    << " aFlags: " << aFlags
    << " aStartingKeyData: " << *((int *)aStartingKeyData)
    << " aStartingKeySize: " << aStartingKeySize
    << EndLogLine;

  skv_tree_based_container_key_t* StartingKeyPtr;

  skv_key_t StartingUserKey;  
  StartingUserKey.Init( aStartingKeyData, aStartingKeySize );

  if( (aFlags & SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG)
      && !(aFlags & SKV_CURSOR_WITH_STARTING_KEY_FLAG) )
  {
    StartingKeyPtr = MakeMagicKey( &aPDSId );
  }
  else
  {
    StartingKeyPtr = MakeKey( aPDSId, &StartingUserKey );
  }

  *aRetrievedKeysCount = 0;
  skv_data_container_t::iterator iter = mDataMap->lower_bound( *StartingKeyPtr );

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::RetrieveNKeys():: After mDataMap->lower_bound():: "    
    << " *StartingKeyPtr: " << *StartingKeyPtr
    << " (iter != mDataMap->end()): " << (iter != mDataMap->end())
    << " mDataMap->size(): " << mDataMap->size() 
    << " aFlags: " << aFlags
    << EndLogLine;

  if( iter != mDataMap->end() )
  {
    // We do not need to send the starting key (unless it's the first key)
    if( !(aFlags & SKV_CURSOR_RETRIEVE_FIRST_ELEMENT_FLAG) )
      iter++;

    // Check if the iterator is pointing to a queried PDSId
    skv_tree_based_container_key_t * key = (skv_tree_based_container_key_t *) &(*iter);
    skv_pds_id_t* PDSIdInKey = key->GetPDSId();

    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
      << "skv_tree_based_container_t::RetrieveNKeys():: "
      << " PDSIdInKey: " << *PDSIdInKey
      << EndLogLine;

    if( !(*PDSIdInKey == aPDSId) )
    {
      BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
        << "skv_tree_based_container_t::RetrieveNKeys():: Leaving with SKV_ERRNO_END_OF_RECORDS"
        << EndLogLine;

      return SKV_ERRNO_END_OF_RECORDS;
    }

    int IterCount = 0;
    while( (iter != mDataMap->end()) &&
           (IterCount < aListOfKeysMaxCount) &&
           (*(key->GetPDSId()) == aPDSId) )
    {
      int Index = 2 * IterCount;

      BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
        << "skv_tree_based_container_t::RetrieveNKeys():: "
        << " Index: " << Index
        << " aListOfKeysMaxCount: " << aListOfKeysMaxCount
        << " Key: " << *key
        << EndLogLine;

      aRetrievedKeysSizesSegs[Index].InitAbs( mDataLMR,
                                              (char *) &key->mUserKey.mSize,
                                              sizeof(int) );

      aRetrievedKeysSizesSegs[Index + 1].InitAbs( mDataLMR,
                                                  key->mUserKey.GetData(),
                                                  key->mUserKey.GetSize() );

      IterCount++;
      iter++;
      key = (skv_tree_based_container_key_t *) &(*iter);
    }

    *aRetrievedKeysCount          =     IterCount;
    *aRetrievedKeysSizesSegsCount = 2 * IterCount;
  }
  else
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
      << "skv_tree_based_container_t::RetrieveNKeys():: Leaving with SKV_ERRNO_END_OF_RECORDS"
      << EndLogLine;

    return SKV_ERRNO_END_OF_RECORDS;
  }

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::RetrieveNKeys():: Leaving with " << *aRetrievedKeysCount << " Keys."
    << EndLogLine;

  if( *aRetrievedKeysCount == 0 )
    return SKV_ERRNO_END_OF_RECORDS;
  else
    return SKV_SUCCESS;
}



/***
 * skv_tree_based_container_t::Remove::
 * Desc: Given a [ KeyData, KeySize ] remove
 * from the local store (STL map)
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_tree_based_container_t::
Remove( skv_pds_id_t             aPDSId, 
        char*                     aKeyData,
        int                       aKeySize )
{
  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Remove():: Entering... "
    << " aPDSId: " << aPDSId
    << " aKeySize: " << aKeySize
    << EndLogLine;

  skv_key_t UserKey;
  UserKey.Init( aKeyData, aKeySize );

  // create key and find entry
  skv_tree_based_container_key_t* key = MakeKey( aPDSId, & UserKey );

  skv_data_container_t::iterator iter = mDataMap->find( *key );

  if( iter == mDataMap->end() )
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
      << "skv_tree_based_container_t::Remove():: Leaving with SKV_ERRNO_ELEM_NOT_FOUND"
      << EndLogLine;

    return SKV_ERRNO_ELEM_NOT_FOUND;
  }

  // set key, get addr of record and deallocate potential RMR
  key = (skv_tree_based_container_key_t *) &(*iter);

  if( key->GetLockState() == SKV_LOCKED_STATE )
  {
    return SKV_ERRNO_RECORD_IS_LOCKED;
  }

  char* RecordPtr = key->GetRecordPtr();
  int KeySize = key->GetKeySize();

  int rc = mDataMap->erase( *key );

  if( rc != 1 )
  {
    return SKV_ERRNO_ELEM_NOT_FOUND;
  }

  skv_lmr_triplet_t TmpRMR;
  TmpRMR.SetAddr( (char *) RecordPtr );

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Remove():: Deallocating RMR and Leaving... "
    << EndLogLine;

   return Deallocate( & TmpRMR );
}


/***
 * skv_tree_based_container_t::Retrieve::
 * Desc: Given a [ KeyData, KeySize ] retrieve 
 * from [ aOffset, up to aValueSizeMax ]
 * from the local store (STL map)
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_tree_based_container_t::
Retrieve( skv_pds_id_t             aPDSId, 
          char*                     aKeyData,
          int                       aKeySize,
          int                       aValueOffset,
          int                       aValueSizeMax,
          skv_cmd_RIU_flags_t      aFlags,
          skv_lmr_triplet_t*       aMemRepValue )
{  
  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Retrieve():: Entering... "
    << EndLogLine;

  skv_key_t UserKey;
  UserKey.Init( aKeyData, aKeySize );

  skv_tree_based_container_key_t* key = MakeKey( aPDSId, & UserKey );

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Retrieve():: "
    << " key: " << *key
    << EndLogLine;

#if 0
  int count = 0;
  for( skv_data_container_t::iterator iter1 = mDataMap->begin();
      iter1 != mDataMap->end();
      iter1++ )
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
      << "skv_tree_based_container_t::Retrieve():: "
      << " iter[ " << count << " ].Key: " << iter1->first
      << " iter[ " << count << " ].Value: " << iter1->second
      << EndLogLine;

    count++;
  }
#endif

  skv_data_container_t::iterator iter = mDataMap->find( *key );

  if( iter == mDataMap->end() )
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
      << "skv_tree_based_container_t::Retrieve():: Leaving with SKV_ERRNO_ELEM_NOT_FOUND"
      << EndLogLine;

    return SKV_ERRNO_ELEM_NOT_FOUND;
  }

  key = (skv_tree_based_container_key_t *) &(*iter);

  char* RecordPtr = key->GetRecordPtr();
  int   KeySize   = key->GetKeySize();

  AssertLogLine( KeySize == aKeySize )
    << "skv_tree_based_container_t::Retrieve(): ERROR: "
    << " KeySize: " << KeySize
    << " aKeySize: " << aKeySize
    << EndLogLine;

  char* StartOfValueInStore = RecordPtr + KeySize;

  AssertLogLine( StartOfValueInStore != NULL )
    << "skv_tree_based_container_t::Retrieve:: ERROR: "
    << " StartOfValueInStore != NULL "
    << EndLogLine;

  int SizeInStore = key->GetValueSize();

  AssertLogLine( SizeInStore >= 0 && SizeInStore < SKV_VALUE_LIMIT )
    << "skv_tree_based_container_t::Retrieve:: ERROR: "
    << " SizeInStore: " << SizeInStore
    << EndLogLine;

  // Note: This requires both the Offset and Size
  // of the request to be provided. A flag which 
  // would specify from Offset to the end of the 
  // record could be implemented if a use case arises
  int RequestedOffset   = aValueOffset;

  AssertLogLine( RequestedOffset < SizeInStore )
    << "skv_tree_based_container_t::Retrieve:: ERROR: RequestedOffset < SizeInStore "
    << " RequestedOffset: " << RequestedOffset
    << " SizeInStore: " << SizeInStore
    << EndLogLine;

  int SizeToReturn = 0;

  if( aFlags & SKV_COMMAND_RIU_RETRIEVE_SPECIFIC_VALUE_LEN )
  {
    int MaxCandidateSize = SizeInStore - RequestedOffset;

    if( aValueSizeMax > MaxCandidateSize )
    {
      BegLogLine( 1 )
      // AssertLogLine( aValueSizeMax <= MaxCandidateSize )
        << "skv_tree_based_container_t::Retrieve:: ERROR: aValueSizeMax <= MaxCandidateSize "
        << " aValueSizeMax: " << aValueSizeMax
        << " MaxCandidateSize: " << MaxCandidateSize
        << EndLogLine;

      return SKV_ERRNO_VALUE_TOO_LARGE;
    }

    SizeToReturn = aValueSizeMax;
  }
  else
  {
    SizeToReturn = SizeInStore - RequestedOffset;
  }

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Retrieve():: "
    << " !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    << " RequestedOffset: "  << RequestedOffset
    << " SizeInStore: "      << SizeInStore
    << " SizeToReturn: "     << SizeToReturn
    << " StartOfValueInStore: " << (void *) StartOfValueInStore
    << EndLogLine;  

  // NOTE: Retrieve does not return a copy
  aMemRepValue->mLMRTriplet.lmr      = mDataLMR;
  aMemRepValue->mLMRTriplet.addr.abs = (StartOfValueInStore + RequestedOffset);
  aMemRepValue->mLMRTriplet.length   = SizeToReturn;

  if( aFlags & SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS )
  {
    if( key->GetLockState() == SKV_LOCKED_STATE )
    {
      return SKV_ERRNO_RECORD_IS_LOCKED;
    }
  }

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::Retrieve():: Leaving with SKV_SUCCESS"
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t
skv_tree_based_container_t::
DumpPersistenceImage( char* aPath )
{
  skv_server_heap_manager_t::Dump( aPath );

  return SKV_SUCCESS;
}

/***
 * skv_tree_based_container_t::Init::
 * Desc: Initializes the state of the container
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_tree_based_container_t::
Init( it_pz_handle_t                            aPZ_Hdl,
      skv_server_internal_event_manager_if_t*  aInternalEventManager,
      int                                       aMyNodeId,
      char*                                     aRestartImagePath,
      skv_persistance_flag_t                   aFlag )
{    
  mMyNodeId             = aMyNodeId;
  mPZ_Hdl               = aPZ_Hdl;
  mInternalEventManager = aInternalEventManager;

  // mMaxDataLoad = SKV_MAX_DATA_LOAD * 1024 * 1024;  
  char* PersistanceHdr = NULL;
  skv_server_heap_manager_t::Init( SKV_MAX_DATA_LOAD, & PersistanceHdr, aRestartImagePath, aFlag );

  mHeapHdr = (skv_server_persistance_heap_hdr_t *) PersistanceHdr;

  if( aFlag & SKV_PERSISTANCE_FLAG_INIT )
  {
    // Construction of the
    mDataMap = (skv_data_container_t *) skv_server_heap_manager_t::Allocate( sizeof( skv_data_container_t ) );

    StrongAssertLogLine( mDataMap != NULL )
      << "Init(): ERROR: Not enough memory for: "
      << " size: " << sizeof( skv_data_container_t )
      << EndLogLine;

    // Call the constructor
    new((void*)mDataMap)skv_data_container_t();

    mHeapHdr->mDataMap = mDataMap;

    mPDSNameTable = (skv_pds_name_table_t *) skv_server_heap_manager_t::Allocate( sizeof( skv_pds_name_table_t ) );

    StrongAssertLogLine( mPDSNameTable != NULL )
      << "Init(): ERROR: Not enough memory for: "
      << " size: " << sizeof( skv_pds_name_table_t )
      << EndLogLine;

    // Call the constructor
    new ( (void*) mPDSNameTable ) skv_pds_name_table_t();

    mHeapHdr->mPDSNameTable = mPDSNameTable;
    mHeapHdr->mLocalPDSCount = 0;
    mHeapHdr->mRowDataChecksum = 0;

    mPDSIdTable = (skv_pds_id_table_t *) skv_server_heap_manager_t::Allocate( sizeof(skv_pds_id_table_t) );

    StrongAssertLogLine( mPDSIdTable != NULL )
      << "Init(): ERROR: Not enough memory for: "
      << " size: " << sizeof( skv_pds_id_table_t )
      << EndLogLine;

    // Call the constructor
    new ( (void*) mPDSIdTable ) skv_pds_id_table_t();

    mHeapHdr->mPDSIdTable = mPDSIdTable;
  }
  else if( aFlag & SKV_PERSISTANCE_FLAG_RESTART )
  {
    mDataMap = (skv_data_container_t *) mHeapHdr->mDataMap;
    mPDSNameTable = (skv_pds_name_table_t *) mHeapHdr->mPDSNameTable;

    skv_pds_name_table_t::iterator iter = mPDSNameTable->begin();
    skv_pds_name_table_t::iterator iter_end = mPDSNameTable->end();

    while( iter != iter_end )
    {
      fxlogger_string_t PdsName;
      PdsName.Init( (char*) iter->first.c_str(), iter->first.size() );

      BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_INIT_LOG )
        << "PDSName: " << PdsName
        << " PDSAttr: " << iter->second
        << EndLogLine;

      iter++;
    }

    skv_data_container_t::iterator iter_data = mDataMap->begin();
    skv_data_container_t::iterator iter_data_end = mDataMap->end();

    int RowCount = 0;
    unsigned long long checksum = 0;
    unsigned long long TotalSize = 0;
    while( iter_data != iter_data_end )
    {
      skv_tree_based_container_key_t * KeyPtr = (skv_tree_based_container_key_t *) &(*iter_data);

      int RowSize = KeyPtr->GetRecordSize();
      TotalSize += RowSize;
      char* RowData = KeyPtr->GetRecordPtr();

      HexDump hd( RowData, RowSize );

      BegLogLine( 0 )
        << "Row: " << RowCount
        << " Ptr: " << (void *) RowData
        << " Size: " << RowSize
        << " hd: " << hd
        << EndLogLine;

      for( int i = 0; i < RowSize; i++ )
      {
        checksum += RowData[i];
      }

      iter_data++;
      RowCount++;
    }

    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_INIT_LOG )
      << "Init() RowCount: " << RowCount
      << " TotalSize: " << TotalSize
      << " "
      << EndLogLine;

    StrongAssertLogLine( checksum == mHeapHdr->mRowDataChecksum )
      << "Init(): ERROR: Checksum mismatch "
      << " checksum: " << checksum
      << " mHeapHdr->mRowDataChecksum: " << mHeapHdr->mRowDataChecksum
      << EndLogLine;
  }
  else
  {
    StrongAssertLogLine( 0 )
      << "Init(): ERROR: Flag not recognized. "
      << " aFlag: " << aFlag
      << EndLogLine;
  }

  mLocalPDSCountPtr = &(mHeapHdr->mLocalPDSCount);

  skv_server_heap_manager_t::GetDataStartAndLen( &mStartOfDataField, (size_t *) &mDataFieldLen );

  mMaxDataLoad = mDataFieldLen;

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_INIT_LOG )
    << "skv_tree_based_container_t::Init:: "
    << " mStartOfDataField: " << (void *) mStartOfDataField
    << " mDataFieldLen: " << mDataFieldLen
    << EndLogLine;

  it_mem_priv_t privs     = (it_mem_priv_t) ( IT_PRIV_LOCAL | IT_PRIV_REMOTE );
  it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

  it_status_t itstatus = it_lmr_create( aPZ_Hdl,
                                        mStartOfDataField,
                                        NULL,
                                        mDataFieldLen,
                                        IT_ADDR_MODE_ABSOLUTE, 
                                        privs, 
                                        lmr_flags,
                                        0,
                                        & mDataLMR,
                                        & mDataRMR );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_tree_based_container_t::Init:: ERROR:: itstatus == IT_SUCCESS "    
    << " itstatus: " << itstatus
    << EndLogLine;

  return SKV_SUCCESS;
}

/***
 * skv_tree_based_container_t::Finalize::
 * Desc: Takes down the state of the container
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_tree_based_container_t::
Finalize()
{
#if 0
  if( mDataMap != NULL )
  {
    mDataMap->clear();

    delete mDataMap;
    mDataMap = NULL;
  }
#endif

  return SKV_SUCCESS;
}

/***
 * skv_tree_based_container_t::MakeKey::
 * Desc: Given a aPDSId put the next element in aKey and aValue
 * input: 
 * aPDSId -> Representation of the PDSId
 * aKey   -> Representation of user defined key
 * returns: Pointer to a key structure
 ***/
skv_tree_based_container_key_t*
skv_tree_based_container_t::
MakeKey( skv_pds_id_t & aPDSId, skv_key_t* aKey )
{
  AssertLogLine( aKey != NULL )
    << "skv_tree_based_container_t::MakeKey():: ERROR: " 
    << " aKey != NULL "
    << EndLogLine;

  AssertLogLine( (aKey->GetSize() > 0) && 
                 (aKey->GetSize() <= SKV_KEY_LIMIT ) )
    << "skv_tree_based_container_t::MakeKey():: ERROR: "
    << " aKey->GetSize(): " << aKey->GetSize()
    << " SKV_KEY_LIMIT: " << SKV_KEY_LIMIT
    << EndLogLine;

  mTempKeyBuffer.SetPDSId( aPDSId );
  mTempKeyBuffer.SetUserKey( aKey );
  mTempKeyBuffer.SetLockState( SKV_UNLOCKED_STATE );

  return &mTempKeyBuffer;
}

/***
 * skv_tree_based_container_t::MakeMagicKey::
 * Desc: Create a special key to be used in 
 * picking out the records associated with a aPDSId
 * NOTE: This key is for temporary use only, designed
 * for the temporary need of a lookup 
 * input: 
 * aPDSId -> Representation of the PDSId
 * returns: Pointer to a key structure
 ***/
skv_tree_based_container_key_t*
skv_tree_based_container_t::
MakeMagicKey( skv_pds_id_t* aPDSId )
{
  AssertLogLine( aPDSId != NULL )
    << "skv_tree_based_container_t::MakeMagicKey():: ERROR: " 
    << " aPDSId != NULL"
    << EndLogLine;

  mTempKeyBuffer.MakeMagicKey( aPDSId );

  return &mTempKeyBuffer;
}


skv_status_t 
skv_tree_based_container_t::
UnlockRecord( skv_rec_lock_handle_t   aRecLock )
{
  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::UnlockRecord():: Entering... "
    << EndLogLine;

  *aRecLock = SKV_UNLOCKED_STATE;

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::UnlockRecord():: Leaving "
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t 
skv_tree_based_container_t::
LockRecord( skv_pds_id_t             aPDSId, 
            char*                     aKeyData,
            int                       aKeySize,	  
            skv_rec_lock_handle_t*   aRecLock )
{
  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::LockRecord():: Entering... "
    << EndLogLine;

  skv_key_t UserKey;
  UserKey.Init( aKeyData, aKeySize );

  skv_tree_based_container_key_t* key = MakeKey( aPDSId, & UserKey );

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::LockRecord():: "
    << " key: " << *key
    << EndLogLine;

  skv_data_container_t::iterator iter = mDataMap->find( *key );

  if( iter == mDataMap->end() )
  {
    BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
      << "skv_tree_based_container_t::LockRecord():: Leaving with SKV_ERRNO_ELEM_NOT_FOUND"
      << EndLogLine;

    return SKV_ERRNO_ELEM_NOT_FOUND;
  }

  skv_tree_based_container_key_t* IterKey = (skv_tree_based_container_key_t *) & (*iter);

  IterKey->SetLockState( SKV_LOCKED_STATE ) ;

  *aRecLock = IterKey->GetLockStatePtr();

  BegLogLine( SKV_SERVER_TREE_BASED_CONTAINER_LOG )
    << "skv_tree_based_container_t::LockRecord():: Leaving"
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t 
skv_tree_based_container_t::
CreateCursor( char*                              aBuff,	      
              int                                aBuffSize,	      
              skv_server_cursor_hdl_t*          aServCursorHdl )
{
  return mServCursorMgrIF.InitHandle( aBuff,				      
                                      aBuffSize,		      
                                      mPZ_Hdl,
                                      mInternalEventManager,
                                      aServCursorHdl );
}

#ifndef SKV_SERVER_FILL_CURSOR_BUFFER_TRACE
#define SKV_SERVER_FILL_CURSOR_BUFFER_TRACE ( 0 )
#endif

TraceClient gFillCursorBufferStart;
TraceClient gFillCursorBufferFinis;

TraceClient gFillCursorBufferProjectTupleStart;
TraceClient gFillCursorBufferProjectTupleFinis;

#include <mpi.h>

int TraceReportCounter = 0;
int MyRankForTracing = 0;
#define  SKV_SERVER_FILL_CURSOR_BUFFER_TRACE_REPORT_MODULO 1000
