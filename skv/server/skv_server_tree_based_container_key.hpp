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

#ifndef __SKV_SERVER_TREE_BASED_CONTAINER_KEY__
#define __SKV_SERVER_TREE_BASED_CONTAINER_KEY__

#ifndef SKV_TREE_BASED_CONTAINER_KEY_LOG 
#define SKV_TREE_BASED_CONTAINER_KEY_LOG  ( 0 | SKV_LOGGING_ALL )
#endif

#include <server/skv_server_heap_manager.hpp>

class skv_tree_based_container_key_t
{
public:

  /***************************************************
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   *
   * NOTE:: THIS DATASTRUCTURE NEEDS TO BE COMPRESSED
   *
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   * OVERHEAD PER RECORD OVERHEAD PER RECORD
   ***************************************************/ 

  skv_pds_id_t  mPDSId;
  skv_key_t     mUserKey;
  int            mValueSize; 
  char           mLockState; 



#define SKV_MAGIC_VALUE  (-1)

  skv_tree_based_container_key_t()
  {
  }
  ~skv_tree_based_container_key_t()
  {
  }

  // This assumes that the key ptr is the start of a record
  // ValuePtr = KeyPtr + KeySize
  char*
  GetRecordPtr()
  {
    return GetUserKey()->GetData();
  }

  int
  GetRecordSize()
  {
    return mValueSize + mUserKey.GetSize();
  }

  int
  GetKeySize()
  {
    return mUserKey.GetSize();
  }

  char *
  GetLockStatePtr()
  {
    return &mLockState;
  }

  char
  GetLockState()
  {
    return mLockState;
  }

  void
  SetLockState( char aState )
  {
    mLockState = aState;
  }

  void
  SetValueSize( int aValueSize )
  {
    mValueSize = aValueSize;
  }

  int
  GetValueSize()
  {
    return mValueSize;
  }

  skv_key_t*
  GetUserKey()
  {
    return &mUserKey;
  }

  skv_pds_id_t*
  GetPDSId()
  {
    return &mPDSId;
  }

  void
  SetMagicValue()
  {
    mUserKey.mSize = SKV_MAGIC_VALUE;
    mUserKey.mData = NULL;
  }

  int
  IsMagicValueSet() const
  {
    return (mUserKey.mSize == SKV_MAGIC_VALUE);
  }

  void
  MakeMagicKey( skv_pds_id_t* aPDSId )
  {
    mPDSId = *aPDSId;
    SetMagicValue();
  }

  void
  SetPDSId( skv_pds_id_t& aPDSId )
  {
    mPDSId = aPDSId;
  }

  void
  SetUserKey( skv_key_t* aUserKey )
  {
    mUserKey = *aUserKey;
  }

  bool
  operator==( const skv_tree_based_container_key_t& aKey ) const
  {      
    // If the magic value is set, then only compare PDSIds
    if( aKey.IsMagicValueSet() || IsMagicValueSet() )
    {
      return( aKey.mPDSId == mPDSId  );
    }
    else
    {
      if( (aKey.mPDSId == mPDSId) )
        return (mUserKey == aKey.mUserKey);
      else
        return 0;
    }
  }

  bool 
  operator<( const skv_tree_based_container_key_t& aKey ) const
  {      
    if( mPDSId < aKey.mPDSId )
    {
      BegLogLine( SKV_TREE_BASED_CONTAINER_KEY_LOG )
        << "skv_tree_based_container_t::Key::operator<():: "
        << " mPDSId: " << mPDSId
        << " aKey.mPDSId: " << aKey.mPDSId
        << EndLogLine;

      return 1;
    }
    else if( mPDSId == aKey.mPDSId )
    {
      // If this is a magic key, then it should always be less then
      // since we're looking for the first row for a given pds

      if( aKey.IsMagicValueSet() )
        return 0;

      if( IsMagicValueSet() )
        return 1;

      int rc = (mUserKey < aKey.mUserKey);

      BegLogLine( SKV_TREE_BASED_CONTAINER_KEY_LOG )
        << "skv_tree_based_container_t::Key::operator<():: "
        << " mUserKey: " << mUserKey
        << " aKey.mUserKey: " << aKey.mUserKey
        << " rc: " << rc
        << EndLogLine;

      return rc;
    }
    else
    {
      BegLogLine( SKV_TREE_BASED_CONTAINER_KEY_LOG )
        << "skv_tree_based_container_t::Key::operator<():: "
        << " mPDSId: " << mPDSId
        << " aKey.mPDSId: " << aKey.mPDSId
        << EndLogLine;

      return 0;
    }
  }
};

typedef std::set< skv_tree_based_container_key_t,
                  less< skv_tree_based_container_key_t >,
                  skv_allocator_t< skv_tree_based_container_key_t > > skv_data_container_t;

#endif
