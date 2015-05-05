/************************************************
 * Copyright (c) IBM Corp. 2014-2015
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_server_lock_manager.hpp
 *
 *  Created on: May 5, 2015
 *      Author: lschneid
 */

#ifndef SKV_SERVER_SKV_SERVER_LOCK_MANAGER_HPP_
#define SKV_SERVER_SKV_SERVER_LOCK_MANAGER_HPP_

#ifndef SKV_SERVER_RECORD_LOCK_MGR_LOG
#define SKV_SERVER_RECORD_LOCK_MGR_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#include <set>

class skv_server_record_lock_key_t
{
  skv_pds_id_t mPDSId;
  skv_key_t mKey;

public:
  skv_server_record_lock_key_t()
  {
    mPDSId.Init( 0, 0 );
    mKey.Init( NULL, 0 );
    mKey.mData = new char[ SKV_CONTROL_MESSAGE_SIZE ];
  }
  skv_server_record_lock_key_t( const skv_pds_id_t &aPDSId,
                                const skv_key_t &aKey )
  {
    setPdsId( aPDSId );
    mKey.mData = new char[ SKV_CONTROL_MESSAGE_SIZE ];
    setKey( aKey );
  }
  ~skv_server_record_lock_key_t()
  {
  // deleting mKey.mData caused double-delete error
  }

  const skv_key_t& getKey() const
  {
    return mKey;
  }

  void setKey( const skv_key_value_in_ctrl_msg_t &aKeyVal )
  {
    mKey.mSizeBE = aKeyVal.mKeySize;
    if( mKey.mSizeBE < SKV_CONTROL_MESSAGE_SIZE )
      memcpy( mKey.mData, aKeyVal.mData, mKey.mSizeBE );
    else
      StrongAssertLogLine( 0 )
        << "KeySize too large (check client request or potential endian issue)."
        << " size=" << mKey.mSizeBE
        << " in hex=0x" << (void*)mKey.mSizeBE
        << EndLogLine;
  }
  void setKey( const skv_key_t& aKey )
  {
    mKey = aKey;
  }

  skv_pds_id_t getPdsId() const
  {
    return mPDSId;
  }

  void setPdsId( const skv_pds_id_t &aPDSId )
  {
    mPDSId.Init( aPDSId.mOwnerNodeId, aPDSId.mIdOnOwner );
  }

  bool
  operator==( const skv_server_record_lock_key_t &aLockRec ) const
  {
    if( (aLockRec.mPDSId == mPDSId) )
      return (mKey == aLockRec.mKey);
    else
      return 0;
  }

  bool
  operator<( const skv_server_record_lock_key_t &aLockRec ) const
  {
    if( mPDSId < aLockRec.mPDSId )
    {
      return 1;
    }
    else if( mPDSId == aLockRec.mPDSId )
    {
      int rc = (mKey < aLockRec.mKey);

      return rc;
    }
    else
    {
      return 0;
    }
  }

};


struct skv_server_record_lock_t
{
  skv_server_record_lock_key_t mKey;
  skv_server_ccb_t *mLockOwner;
};

// once there's a decision to accept C++11 features by default, this could be changed to an unordered map
typedef std::map< skv_server_record_lock_key_t,
                  skv_server_ccb_t*,
                  less< skv_server_record_lock_key_t >,
                  skv_allocator_t< std::pair< skv_server_record_lock_key_t, skv_server_record_lock_t* >> > skv_server_record_lock_vault_t;;

class skv_server_lock_manager_t
{
  skv_server_record_lock_vault_t mActiveLocks;
  skv_server_record_lock_t mTmpRecord;
  skv_mutex_t mSerializer;

public:
  skv_server_lock_manager_t()
  {
  }
  ~skv_server_lock_manager_t()
  {
  }

  skv_status_t Lock( const skv_pds_id_t *aPDSId,
                     const skv_key_value_in_ctrl_msg_t *aKeyValue,
                     const skv_server_ccb_t *aOwner,
                     skv_rec_lock_handle_t *aRecLock )
  {
    skv_status_t status = SKV_SUCCESS;
    mTmpRecord.mKey.setPdsId( *aPDSId );
    mTmpRecord.mKey.setKey( *aKeyValue );

    // note: operator[] inserts new element if key isn't found...
    skv_server_record_lock_vault_t::iterator iter = mActiveLocks.find( mTmpRecord.mKey );

    if( iter != mActiveLocks.end() )
    {
      skv_server_ccb_t *currentOwner = iter->second;
      if( currentOwner != aOwner )
      {
        BegLogLine( SKV_SERVER_RECORD_LOCK_MGR_LOG )
          << "This record is already locked and we 0x" << (void*)aOwner
          << " are NOT the owner 0x" << (void*)currentOwner
          << EndLogLine;
        status = SKV_ERRNO_RECORD_IS_LOCKED;
      }
      else
      {
        BegLogLine( SKV_SERVER_RECORD_LOCK_MGR_LOG )
          << "This record is already locked and we're the owner 0x" << (void*)aOwner
          << EndLogLine;
        *aRecLock = (skv_rec_lock_handle_t)&(iter->first);
        status = SKV_SUCCESS;
      }
    }
    else
    {
      BegLogLine( SKV_SERVER_RECORD_LOCK_MGR_LOG )
        << "This record didn't exist. Creating record lock for owner 0x" << (void*)aOwner
        << EndLogLine;
      std::pair<skv_server_record_lock_vault_t::iterator,bool> retval = mActiveLocks.insert( std::make_pair( mTmpRecord.mKey, (skv_server_ccb_t*)aOwner ) );
      if( retval.second == true )
      {
        *aRecLock = (skv_rec_lock_handle_t)&(retval.first->first);
        status = SKV_SUCCESS;
      }
      else
        StrongAssertLogLine( 0 )
          << "Lock creation failed while inserting key into map. Sorry, can't continue..."
          << EndLogLine;
    }

    return status;
  }

  skv_status_t Unlock( const skv_rec_lock_handle_t aLock,
                       const skv_server_ccb_t *aOwner )
  {
    skv_status_t status = SKV_SUCCESS;
    skv_server_record_lock_key_t *lockPtr = (skv_server_record_lock_key_t*)aLock;
    skv_server_record_lock_vault_t::iterator iter = mActiveLocks.find( *lockPtr );
    if( iter != mActiveLocks.end() )
      if( iter->second == aOwner )
      {
        BegLogLine( SKV_SERVER_RECORD_LOCK_MGR_LOG )
          << "This record is locked and we're the owner 0x" << (void*)aOwner
          << ". Unlocking..."
          << EndLogLine;
        mActiveLocks.erase( *lockPtr );
        status = SKV_SUCCESS;
      }
      else
      {
        BegLogLine( SKV_SERVER_RECORD_LOCK_MGR_LOG )
          << "This record is locked and we 0x" << (void*)aOwner
          << " are NOT the owner 0x" << (void*)iter->second
          << ". Nice try..."
          << EndLogLine;
        status = SKV_ERRNO_RECORD_IS_LOCKED;
      }
    else
    {
      BegLogLine( SKV_SERVER_RECORD_LOCK_MGR_LOG )
        << "This record isn't locked. No-Op."
        << EndLogLine;
      status = SKV_SUCCESS;
    }

    return status;
  }
};



#endif /* SKV_SERVER_SKV_SERVER_LOCK_MANAGER_HPP_ */
