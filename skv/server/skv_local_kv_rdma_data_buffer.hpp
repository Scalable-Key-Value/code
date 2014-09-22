/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_local_kv_data_mover.hpp
 *
 *  Created on: May 21, 2014
 *      Author: lschneid
 */

#ifndef SKV_LOCAL_KV_RDMA_DATA_BUFFER_HPP_
#define SKV_LOCAL_KV_RDMA_DATA_BUFFER_HPP_

#ifndef SKV_LOCAL_KV_MAX_REQUESTS
#define SKV_LOCAL_KV_MAX_REQUESTS ( 1024 )
#endif

#ifndef SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG
#define SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG ( 0 | SKV_LOGGING_ALL )
#endif

/* class implements a wrapping ringbuffer pointer and provides several arithmetic and boolean operators */
class skv_ringbuffer_ptr {
  char *mBase;   // start address of memory
  char *mPtr;    // current location
  size_t mSize;  // length of the memory
  size_t mGranularity; // minimum alignment for pointers and sizes
  bool mWrapped; // double buffering to allow comparison of addresses beyond the wrap-around

  inline char* alignedPtr( const char *a )
  {
    return ((uintptr_t)a % mGranularity) ? (char*)( (uintptr_t)a + (mGranularity- ( (uintptr_t)a % mGranularity)) ) : (char*)a;
  }
  inline size_t alignedSize( const size_t s )
  {
    return (s % mGranularity) ? s + ( mGranularity - ( s % mGranularity )) : s;
  }
public:
  skv_ringbuffer_ptr( const char *aBase=NULL, size_t aSize=0, size_t aGranularity=sizeof(uintptr_t) )
  {
    Init( aBase, aSize, aGranularity );
  }
  ~skv_ringbuffer_ptr() {}
  void Init( const char *aBase=NULL, size_t aSize=0, size_t aGranularity=sizeof(uintptr_t) )
  {
    StrongAssertLogLine( ((uintptr_t)aBase % aGranularity == 0) && (aSize % aGranularity == 0) )
      << "skv_ringbuffer_ptr(): unaligned memory base pointer: " << (void*)aBase
      << " requested granularity: " << aGranularity
      << EndLogLine;
    mGranularity = aGranularity;
    mBase = (char*)aBase;
    mPtr = mBase;
    mSize = aSize;
    mWrapped = false;
  }
  void Reset()
  {
    mPtr = mBase;
    mWrapped = false;
  }

  // operators
  skv_ringbuffer_ptr& operator+( const int aAdd )
  {
    size_t addSize = alignedSize( aAdd );
    bool towrap = (( intptr_t(mPtr-mBase) + (intptr_t)addSize) >= (intptr_t)mSize);
    mWrapped = ( mWrapped != towrap );
    mPtr = mBase + (GetOffset() + addSize) % mSize;
    return (*this);
  }
  skv_ringbuffer_ptr& operator-( const int aSub )
  {
    size_t subSize = alignedSize( aSub );
    bool towrap = ( intptr_t(mPtr - mBase) < (intptr_t)subSize );
    mWrapped = ( mWrapped != towrap );
    mPtr = mBase + (( (intptr_t)GetOffset() + 2 * mSize ) - subSize) % mSize;
    return (*this);
  }
  skv_ringbuffer_ptr& operator-( const skv_ringbuffer_ptr &aSub )
  {
    StrongAssertLogLine( ( aSub.mPtr - aSub.mBase ) % mGranularity == 0 )
      << "skv_ringbuffer_ptr:operator-(): unaligned ptr in operand (maybe incompatible ringbuffer pointers with different granularity) "
      << " offset: " << aSub.mPtr - aSub.mBase
      << " gran: " << mGranularity
      << EndLogLine;

    bool towrap = ( (mPtr - mBase) < (aSub.mPtr - aSub.mBase) );
    mWrapped = ( mWrapped != towrap );
    mPtr = mBase + ( mPtr + mSize - aSub.mPtr ) % mSize;
    return (*this);
  }
  intptr_t diff( const skv_ringbuffer_ptr &a )
  {
    if( a.mWrapped != mWrapped )
      return ( mPtr + mSize - a.mPtr );
    return ( mPtr - a.mPtr );
  }
  skv_ringbuffer_ptr& operator=( const skv_ringbuffer_ptr &in )
  {
    mBase = in.mBase;
    mSize = in.mSize;
    mPtr = in.mPtr;
    mWrapped = in.mWrapped;
    return (*this);
  }
  bool operator==( const skv_ringbuffer_ptr &a )
  {
    return ( a.mPtr == mPtr) && ( a.mWrapped == mWrapped );
  }
  bool operator<( const skv_ringbuffer_ptr &other )
  {
    // if wrap-level is equal, then operator evaluates to true if a<b
    // if wrap-level is different, then operator evaluates to false if a<b (i.e. true if a>=b)
    return ( ( mPtr < other.mPtr ) == ( mWrapped == other.mWrapped ) );
  }
  bool operator>( const skv_ringbuffer_ptr &other )
  {
    return ( ( mPtr > other.mPtr ) == ( mWrapped == other.mWrapped ) );
  }
  bool operator>=( const skv_ringbuffer_ptr &other )
  {
    return ( ( mPtr >= other.mPtr ) == ( mWrapped == other.mWrapped ) );
  }
  bool operator<=( const skv_ringbuffer_ptr &other )
  {
    return ( ( mPtr <= other.mPtr ) == ( mWrapped == other.mWrapped ) );
  }

  // helper functions
  inline char* GetPtr()
  {
    return mPtr;
  }
  inline size_t GetOffset()
  {
    return (size_t)(mPtr - mBase);
  }
  inline size_t GetSpace()
  {
    return (size_t)(mSize - GetOffset());
  }
  inline void Wrap()
  {
    mPtr = mBase;
    mWrapped = !mWrapped;
  }
  inline bool GetWrapState()
  {
    return mWrapped;
  }
};

enum skv_lmr_wait_queue_state_t {
  SKV_LMR_STATE_FREE = 0ul,
  SKV_LMR_STATE_BUSY = 1ul,
  SKV_LMR_STATE_TORELEASE = 2ul
};

struct skv_lmr_wait_queue_t {
  size_t mSize;
  skv_lmr_wait_queue_state_t mState;
};

class skv_local_kv_rdma_data_buffer_t {
  char *mDataArea;
  size_t mChunkSize;
  size_t mAlignment;
  skv_lmr_triplet_t mLMR;
  skv_rmr_triplet_t mRMR;

  skv_ringbuffer_ptr mFirstFree;
  skv_ringbuffer_ptr mLastBusy;
  skv_mutex_t mSerializer;

  int mHeadSpace;

public:
  skv_local_kv_rdma_data_buffer_t()
  {
    mDataArea = NULL;
    mChunkSize = 0;
    mAlignment = 0;
    mHeadSpace = sizeof( skv_lmr_wait_queue_t );
    mRMR.Init( (it_rmr_context_t)0, 0, 0 );
    mLMR.InitAbs( (it_lmr_handle_t)0, 0, 0 );
  }
  ~skv_local_kv_rdma_data_buffer_t()
  {
    if( mLMR.GetAddr() != 0 )
    {
      it_lmr_free( mLMR.GetLMRHandle() );
      mRMR.Init( (it_rmr_context_t)0, NULL, 0 );
      mLMR.InitAbs( (it_lmr_handle_t)0, NULL, 0 );
    }
    if( mDataArea != NULL )
      delete [] mDataArea;
  }
  skv_status_t Init( it_pz_handle_t aPZ_Hdl,
                     size_t aDataAreaSize,
                     size_t aDataChunkSize,
                     size_t aAlignment=sizeof(uintptr_t) )
  {
    StrongAssertLogLine( aDataAreaSize % aAlignment == 0 )
      << "skv_local_kv_rdma_data_buffer_t::Init(): Area size has to be multiple of Alignment"
      << " size=" << aDataAreaSize
      << " align=" << aAlignment
      << EndLogLine;

    mAlignment = aAlignment;
    size_t pad = sizeof( skv_lmr_wait_queue_t) - (sizeof( skv_lmr_wait_queue_t ) % aAlignment);
    if( pad==aAlignment ) pad = 0;
    mHeadSpace = sizeof( skv_lmr_wait_queue_t) + pad;

    mDataArea = new char[ aDataAreaSize ];
    StrongAssertLogLine( mDataArea != NULL )
      << "skv_local_kv_rdma_data_buffer_t::Init(): Failed to allocate data area of size: " << aDataAreaSize
      << EndLogLine;

    mChunkSize = aDataChunkSize;

    it_mem_priv_t privs     = (it_mem_priv_t) ( IT_PRIV_LOCAL | IT_PRIV_REMOTE );
    it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;
    it_lmr_handle_t lmr;
    it_rmr_context_t rmr;

    it_status_t itstatus = it_lmr_create( aPZ_Hdl,
                                          mDataArea,
                                          NULL,
                                          aDataAreaSize,
                                          IT_ADDR_MODE_ABSOLUTE,
                                          privs,
                                          lmr_flags,
                                          0,
                                          & lmr,
                                          & rmr );

    StrongAssertLogLine( itstatus == IT_SUCCESS )
      << "skv_tree_based_container_t::Init:: ERROR:: itstatus == IT_SUCCESS "
      << " itstatus: " << itstatus
      << EndLogLine;

    mLMR.InitAbs( lmr, mDataArea, aDataAreaSize );
    mRMR.Init( rmr, mDataArea, aDataAreaSize );

    mFirstFree.Init( mDataArea, aDataAreaSize, mAlignment );
    mLastBusy.Init( mDataArea, aDataAreaSize, mAlignment );

    BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
      << "skv_local_kv_rdma_data_buffer_t: FF[" << (uintptr_t)mFirstFree.GetPtr() << "]"
      << " LB[" << (uintptr_t)mLastBusy.GetPtr() << "]"
      << " mHeadSpace=" << mHeadSpace
      << " mAlignment=" << mAlignment
      << EndLogLine;


    return SKV_SUCCESS;
  }
  inline size_t GetSize()
  {
    return mLMR.GetLen()-mHeadSpace;
  }
  inline size_t GetAllocSize()
  {
    return mChunkSize;
  }
  inline it_lmr_handle_t GetLMR()
  {
    return mLMR.GetLMRHandle();
  }
  inline bool IsEmpty()
  {
    return mLastBusy == mFirstFree;
  }
  inline bool IsFull()
  {
    return ( mLastBusy > mFirstFree );
  }
  skv_status_t AcquireDataArea( size_t aSize, skv_lmr_triplet_t *aLMR )
  {
    if( aSize <= 0 )
      return SKV_ERRNO_NOT_DONE;

    if( aSize > GetSize() )
      return SKV_ERRNO_VALUE_TOO_LARGE;

    if( mFirstFree.GetSpace() <= (aSize + mHeadSpace) )
    {
      // create a fake entry to cause the release process to wrap around...
      skv_lmr_wait_queue_t *wrapEntry = (skv_lmr_wait_queue_t*)mFirstFree.GetPtr();
      wrapEntry->mSize = mFirstFree.GetSpace() - mHeadSpace;
      wrapEntry->mState = SKV_LMR_STATE_TORELEASE;

      BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
        << "AcquireDataArea: created fake entry for wrap-around. "
        << " FE[" << (uintptr_t)wrapEntry << ":" << wrapEntry->mSize << ":" << wrapEntry->mState << "]"
        << " newFE[" << (uintptr_t)mFirstFree.GetPtr()
        << EndLogLine;

      mFirstFree.Wrap();
    }

    skv_ringbuffer_ptr newFirst = mFirstFree;
    newFirst = newFirst + (aSize + mHeadSpace);

    skv_lmr_wait_queue_t *lmrState = (skv_lmr_wait_queue_t*)(mFirstFree.GetPtr());

    // this comparison looks wrong, but given the "wrap-around"
    if( mLastBusy > newFirst )
    {
      BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
        << " AcquireDataArea FAILstats:"
        << " FF.ptr=" << (uintptr_t)mFirstFree.GetPtr()
        << " FF.Offs=" << mFirstFree.GetOffset()
        << " FF.Space=" << mFirstFree.GetSpace()
        << " LB.ptr=" << (uintptr_t)mLastBusy.GetPtr()
        << " NF.ptr=" << (uintptr_t)newFirst.GetPtr()
        << " size=" << aSize
        << EndLogLine;
      return SKV_ERRNO_NO_BUFFER_AVAILABLE;
    }

    aLMR->InitAbs( mLMR.GetLMRHandle(), mFirstFree.GetPtr()+mHeadSpace, aSize );
//    memset( (void*)aLMR->GetAddr(), 0x5a, aSize );
    lmrState->mSize = aSize;
    lmrState->mState = SKV_LMR_STATE_BUSY;

    BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
      << " AcquireDataArea stats: lmr=" << (uintptr_t)aLMR->GetAddr()
      << " FF.ptr=" << (uintptr_t)mFirstFree.GetPtr()
      << " FF.Offs=" << mFirstFree.GetOffset()
      << " FF.Space=" << mFirstFree.GetSpace()
      << " LB.ptr=" << (uintptr_t)mLastBusy.GetPtr()
      << " NF.ptr=" << (uintptr_t)newFirst.GetPtr()
      << " lmr.size=" << aLMR->GetLen()
      << " lmrstate@" << (uintptr_t)lmrState << "[" << lmrState->mSize << ":" << lmrState->mState << "]"
      << " lmr: " << *aLMR
      << EndLogLine;

    mFirstFree = newFirst;
    if( !IsFull() )
    {
      // make sure the following entry is marked free;
      lmrState = (skv_lmr_wait_queue_t*)(mFirstFree.GetPtr());
      lmrState->mSize=5555;
      lmrState->mState=SKV_LMR_STATE_FREE;
    }
    return SKV_SUCCESS;
  }
  skv_status_t ReleaseDataArea( skv_lmr_triplet_t *aLMR )
  {
    if( ( IsEmpty() ) || ( aLMR == NULL ) )
      return SKV_ERRNO_ELEM_NOT_FOUND;

    bool released;
    skv_lmr_wait_queue_t *lmrState = (skv_lmr_wait_queue_t*)(aLMR->GetAddr() - mHeadSpace);

    BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
      << "ReleaseDataArea: About to release: [" << aLMR->GetAddr()-mHeadSpace << ":" << aLMR->GetLen() << "]"
      << " NIL[" << (uintptr_t)mLastBusy.GetPtr() << ":" << ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mSize << "]"
      << EndLogLine;

    StrongAssertLogLine( ((lmrState->mSize == aLMR->GetLen()) || (lmrState->mState == SKV_LMR_STATE_BUSY)) )
      << "skv_local_kv_rdma_data_buffer_t::ReleaseDataArea():  Protocol mismatch. LMR.len (" << aLMR->GetLen()
      << ") doesn't match stored buffer len (" << lmrState->mSize << "). entry@" << (uintptr_t)lmrState
      << " state: " << lmrState->mState << " != " << SKV_LMR_STATE_BUSY
      << " NIL[" << (uintptr_t)mLastBusy.GetPtr() << ":" << ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mSize << "]"
      << EndLogLine;

    // if LMR matches the oldest data buffer entry, release it, otherwise push it to the wait queue
    released = (( (char*)lmrState == mLastBusy.GetPtr() ) || ( ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mState != SKV_LMR_STATE_BUSY ));
    if( !released )
    {
      BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
        << "ReleaseDataArea: deferring release of LMR[" << aLMR->GetAddr() << ":" << aLMR->GetLen() << "]"
        << " NIL[" << (uintptr_t)mLastBusy.GetPtr() << ":" << ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mSize << "]"
        << EndLogLine;
    }
    lmrState->mState = SKV_LMR_STATE_TORELEASE;

    while( released )
    {
      skv_lmr_wait_queue_t *lmrToRelease = (skv_lmr_wait_queue_t*)mLastBusy.GetPtr();

      released = (( lmrToRelease->mState == SKV_LMR_STATE_TORELEASE ));
      if( released )
      {
        BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
          << "ReleaseDataArea: oldEntry[" << (uintptr_t)lmrToRelease << ":" << lmrToRelease->mSize << "]"
          << " NIL[" << (uintptr_t)mLastBusy.GetPtr() << ":" << ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mSize << "]"
          << " mod_align:" << (lmrToRelease->mSize + mHeadSpace)%mAlignment
          << EndLogLine;
        mLastBusy = mLastBusy + (lmrToRelease->mSize + mHeadSpace);
        BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
          << "ReleaseDataArea: inc:" << (lmrToRelease->mSize + mHeadSpace) << "; newLast[" << (uintptr_t)mLastBusy.GetPtr()+mHeadSpace << ":" << ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mSize << "]"
          << EndLogLine;
        lmrToRelease->mState = SKV_LMR_STATE_FREE;
        lmrToRelease->mSize = 6666;
      }
//      else
//        BegLogLine( SKV_LOCAL_KV_RDMA_DATA_BUFFER_LOG )
//          << "ReleaseDataArea: LB.state:" << lmrToRelease->mState
//          << " oldEntry[" << (uintptr_t)lmrToRelease+mHeadSpace << ":" << lmrToRelease->mSize << "]"
//          << " NIL[" << (uintptr_t)mLastBusy.GetPtr() << ":" << ((skv_lmr_wait_queue_t*)mLastBusy.GetPtr())->mSize << "]"
//          << " mod_align:" << (lmrToRelease->mSize + mHeadSpace)%mAlignment
//          << EndLogLine;

    }

    return SKV_SUCCESS;
  }
  skv_status_t AcquireDataAreaPtr( size_t aSize, char** aPointer )
  {
    skv_lmr_triplet_t lmr;
    skv_status_t status = AcquireDataArea( aSize, &lmr );
    StrongAssertLogLine( status == SKV_SUCCESS )
      << "skv_local_kv_rdma_data_buffer_t: error while allocating data buffer. status: " << skv_status_to_string( status )
      << EndLogLine;

    *aPointer = (char*)lmr.GetAddr();

    StrongAssertLogLine( ((uintptr_t)*aPointer >= mLMR.GetAddr()) && ((uintptr_t)*aPointer< mLMR.GetAddr()+mLMR.GetLen()-mHeadSpace) )
      << "skv_local_kv_rdma_data_buffer_t: BUG in AcquireDataArea! Acquired buffer out of range "
      << (uintptr_t)*aPointer << " < " << mLMR.GetAddr()
      << EndLogLine;

    return status;
  }
  skv_status_t ReleaseDataAreaPtr( size_t aSize, char* aPointer )
  {
    skv_lmr_triplet_t lmr;

    skv_lmr_wait_queue_t *entry = (skv_lmr_wait_queue_t*)(aPointer - mHeadSpace);
    StrongAssertLogLine( ((uintptr_t)entry >= mLMR.GetAddr()) && ((uintptr_t)entry < mLMR.GetAddr()+mLMR.GetLen()-mHeadSpace) )
      << "skv_local_kv_rdma_data_buffer_t: Release attempt out of range "
      << (uintptr_t)entry << " < " << mLMR.GetAddr()
      << EndLogLine;

    lmr.InitAbs( mLMR.GetLMRHandle(), aPointer, aSize );
    return ReleaseDataArea( &lmr );
  }

#ifdef SKV_UNIT_TEST
  size_t UnitTest_GetHeadOffset()
  {
    return mFirstFree.GetOffset();
  }
  size_t UnitTest_GetTailOffset()
  {
    return mLastBusy.GetOffset();
  }
#endif

};

#endif /* SKV_LOCAL_KV_RDMA_DATA_BUFFER_HPP_ */
