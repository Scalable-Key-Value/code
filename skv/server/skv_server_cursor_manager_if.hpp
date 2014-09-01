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

#ifndef __SKV_SERVER_CURSOR_MANAGER_IF_HPP__
#define __SKV_SERVER_CURSOR_MANAGER_IF_HPP__

#include <common/skv_types.hpp>
#include <common/skv_utils.hpp>
#include <server/skv_server_tree_based_container_key.hpp>

#ifndef SKV_SERVER_CURSOR_MANAGER_IF_LOG
#define SKV_SERVER_CURSOR_MANAGER_IF_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_CURSOR_RESERVE_RELEASE_BUF_LOG
#define SKV_SERVER_CURSOR_RESERVE_RELEASE_BUF_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#define SKV_SERVER_CURSOR_BUFFER_DATA_SIZE ( 512 * 1024 )
#define SKV_SERVER_CURSOR_BUFFER_COUNT ( 32 )

struct skv_server_cursor_buffer_t
{  
  int                           mSize;
  it_lmr_handle_t               mLMR;

  // Needed for performance tuning
  int                           mFilledSize;
  unsigned long long            mStartTime;

  skv_server_cursor_buffer_t*   mNext;
  skv_server_cursor_buffer_t*   mPrev;

  void*                         mPrivateArg;

  char                          mDataPtr[ 0 ];

  void
  Reset()
  {
    SetStartTime();
    mFilledSize = 0;
  }

  unsigned long long
  GetStartTime()
  {
    return mStartTime;
  }

  void
  SetStartTime()
  {
    mStartTime = PkTimeGetNanos();
  }

  int
  GetFilledSize()
  {
    return mFilledSize;
  }

  void
  SetFilledSize( int aFilledSize )
  {
    mFilledSize = aFilledSize;
  }

  void*
  GetPrivateArg()
  {
    return mPrivateArg;
  }

  void
  Init( int             aSize,
        it_lmr_handle_t aLMR,
        void*           aPrivateArg )
  {
    mPrivateArg = aPrivateArg;

    mSize       = aSize;
    mLMR        = aLMR;

    mNext = NULL;
    mPrev = NULL;
  }

  it_lmr_handle_t
  GetLMR()
  {
    return mLMR;
  }

  char*
  GetDataPtr()
  {
    return & mDataPtr[ 0 ];
  }

  int
  GetSize()
  {
    return mSize;
  }
};

typedef enum
{
  SKV_SERVER_CURSOR_STATE_RUNNING = 0x0001,
  SKV_SERVER_CURSOR_STATE_DONE    = 0x0002
} skv_server_cursor_state_t;

static
const char* 
skv_server_cursor_state_to_string( skv_server_cursor_state_t aA )
{
  switch( aA )
  {
    case SKV_SERVER_CURSOR_STATE_RUNNING: { return "SKV_SERVER_CURSOR_STATE_RUNNING"; }
    case SKV_SERVER_CURSOR_STATE_DONE:    { return "SKV_SERVER_CURSOR_STATE_DONE"; }
    default:
      StrongAssertLogLine( 0 ) 
        << "skv_server_cursor_state_to_string(): ERROR:: State not recognized "
        << " aA: " << aA
        << EndLogLine;
  }
  return "SKV_SERVER_CURSOR_STATE_UNRECOGNIZED";
} 

struct skv_server_cursor_index_iterator_positions_t
{
  int* mPositionIndecies;
  int  mPositionsCount;

  int
  GetPositionInIndex( int aClientGroupOrd )
  {
    AssertLogLine( aClientGroupOrd >= 0 && aClientGroupOrd < mPositionsCount )
      << "skv_server_cursor_index_iterator_positions_t::GetPositionInIndex(): ERROR: "
      << " aClientGroupOrd: " << aClientGroupOrd
      << " mPositionsCount: " << mPositionsCount
      << EndLogLine;

    return mPositionIndecies[ aClientGroupOrd ];
  }

  void
  SetPositionInIndex( int aClientGroupOrd, int aPos )
  {
    AssertLogLine( aClientGroupOrd >= 0 && aClientGroupOrd < mPositionsCount )
      << "skv_server_cursor_index_iterator_positions_t::SetPositionInIndex(): ERROR: "
      << " aClientGroupOrd: " << aClientGroupOrd
      << " mPositionsCount: " << mPositionsCount
      << EndLogLine;

    mPositionIndecies[ aClientGroupOrd ] = aPos;
  }

  void
  Init( int aCount )
  {
    mPositionsCount = aCount;

    int PositionIndeciesSize = sizeof( int ) * mPositionsCount;
    mPositionIndecies = (int*) malloc( PositionIndeciesSize );
    StrongAssertLogLine( mPositionIndecies != NULL )
      << "skv_server_cursor_index_iterator_positions_t::Init(): ERROR: "
      << " PositionIndeciesSize: " << PositionIndeciesSize
      << EndLogLine;

    for( int i=0; i < mPositionsCount; i++ )
    {
      mPositionIndecies[ i ] = 0;
    }
  }  

  void
  Finalize()
  {
    free( mPositionIndecies );
    mPositionIndecies = NULL;
  }

};


#include <server/skv_server_types.hpp>

class skv_server_cursor_control_block_t 
{
public:
  skv_data_id_t                     mDataId;
  int32_t*                           mProjColDesc;
  int                                mProjColDescCount;

  skv_cursor_flags_t                 mFlags;    

  skv_data_container_t::iterator                mCursorPos;
  skv_server_cursor_index_iterator_positions_t  mIndexCursorPos;

  /// typedef STL_STACK( skv_server_cursor_buffer_t * ) skv_server_cursor_buffer_freelist_t;
  typedef skv_stack_t<skv_server_cursor_buffer_t> skv_server_cursor_buffer_freelist_t;
  skv_server_cursor_buffer_freelist_t*            mFreeBufferList;

  typedef skv_queue_t<skv_server_cursor_buffer_t> skv_server_cursor_buffer_pendinglist_t;
  skv_server_cursor_buffer_pendinglist_t         mPendingBufferList;

  // Store waiting events on the buffer here for now.
  typedef STL_QUEUE( skv_server_event_t ) skv_server_cursor_buffer_event_waitlist_t;
  skv_server_cursor_buffer_event_waitlist_t*      mEventWaitList;

  int                          mWorkingBufferSize;
  char*                        mWorkingBuffer;
  it_lmr_handle_t              mWorkingBufferLMR;

  skv_server_internal_event_manager_if_t*        mInternalEventQueue;

  skv_server_cursor_state_t                      mState;

public:

  int
  GetPositionInIndex( int aClientGroupOrd )
  {
    return mIndexCursorPos.GetPositionInIndex( aClientGroupOrd );
  }

  void
  SetPositionInIndex( int aClientGroupOrd, int aPos )
  {
    return mIndexCursorPos.SetPositionInIndex( aClientGroupOrd, aPos );
  }

  skv_data_id_t*
  GetDataIdPtr()
  {
    return & mDataId;
  }

  int
  IsIndexCursor()
  {
    return ( mDataId.mType == SKV_DATA_ID_TYPE_INDEX_ID );
  }

  skv_server_cursor_state_t
  GetState()
  {
    return mState;
  }

  void
  Transit( skv_server_cursor_state_t aState )
  {
    mState = aState;
  }

  void
  AddToWaitingList( skv_server_event_t* aEvent )
  {
    mEventWaitList->push( *aEvent );

    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG ) 
      << "skv_server_cursor_control_block_t::AddToWaitingList(): "
      << " now size: " << mEventWaitList->size()
      << EndLogLine;
  }

  void
  ProcessWaitingList()
  {
    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG ) 
      << "skv_server_cursor_control_block_t::ProcessWaitingList(): "
      << " size: " << mEventWaitList->size()
      << EndLogLine;

    if( mEventWaitList->size() > 0 )
    {
      skv_server_event_t Event = mEventWaitList->front();

      BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG )
        << "skv_server_cursor_control_block_t::ProcessWaitingList(): "
        << " Event: " << Event
        << EndLogLine;

      mInternalEventQueue->Enqueue( & Event );
      mEventWaitList->pop();
    }
  }

  skv_status_t
  Init( it_pz_handle_t                            aPZ_Hdl,
        skv_server_internal_event_manager_if_t*   aInternalEventQueue,
        char*                                     aBuff,
        int                                       aBuffSize )
  {
    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG )
      << " skv_server_cursor_control_block_t::Init(): "
      << " aBuff: " << (void *) aBuff
      << " aBuffSize: " << aBuffSize
      << " this: " << (void *) this
      << EndLogLine;

    Transit( SKV_SERVER_CURSOR_STATE_RUNNING );
    mInternalEventQueue = aInternalEventQueue;

    // \todo: check if this can completely go
    //    int ClientGroupCount = 0;

    int SizePerBufferMetadata = sizeof( skv_server_cursor_buffer_t );
    int SizePerBufferData     = SKV_SERVER_CURSOR_BUFFER_DATA_SIZE * sizeof( char );
    int SizePerBuffer         = SizePerBufferMetadata + SizePerBufferData;    

    mWorkingBufferSize = SKV_SERVER_CURSOR_BUFFER_COUNT * SizePerBuffer;

    mWorkingBuffer = (char *) malloc( mWorkingBufferSize );
    StrongAssertLogLine( mWorkingBuffer != NULL ) 
      << "skv_server_cursor_control_block_t::Init():: ERROR:: "
      << " mWorkingBufferSize: " << mWorkingBufferSize
      << EndLogLine;

    it_mem_priv_t privs     = (it_mem_priv_t) ( IT_PRIV_LOCAL | IT_PRIV_REMOTE );
    it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

    it_rmr_context_t TempRMR;

    it_status_t istatus = it_lmr_create( aPZ_Hdl, 
                                         mWorkingBuffer,
                                         NULL,
                                         mWorkingBufferSize,
                                         IT_ADDR_MODE_ABSOLUTE,
                                         privs,
                                         lmr_flags,
                                         0,
                                         & mWorkingBufferLMR,
                                         & TempRMR );

    StrongAssertLogLine( istatus == IT_SUCCESS )
      << "skv_server_cursor_control_block_t::Init():: ERROR:: "
      << " istatus: " << istatus
      << EndLogLine;

    mPendingBufferList.Init();

    mFreeBufferList = new skv_server_cursor_buffer_freelist_t;
    StrongAssertLogLine( mFreeBufferList ) << EndLogLine;

    mEventWaitList = new skv_server_cursor_buffer_event_waitlist_t;
    StrongAssertLogLine( mEventWaitList ) << EndLogLine;

    for( int i = 0; i < SKV_SERVER_CURSOR_BUFFER_COUNT; i++ )
    {
      skv_server_cursor_buffer_t* Buffer = (skv_server_cursor_buffer_t *) (mWorkingBuffer + (i * SizePerBuffer) );

      Buffer->Init( SizePerBufferData,
                    mWorkingBufferLMR,
                    (void *) this );

      mFreeBufferList->push( Buffer );
    }

    return SKV_SUCCESS;
  }

  skv_server_cursor_buffer_t *
  RemoveFromFreeList()
  {
    if( mFreeBufferList->size() == 0 )
      return NULL;

    skv_server_cursor_buffer_t * rc = mFreeBufferList->top();
    mFreeBufferList->pop();

    rc->mNext = NULL;
    rc->mPrev = NULL;

    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG )
      << " skv_server_cursor_control_block_t::RemoveFromFreeList(): "
      << " Buffer: " << (void *) rc
      << " BufferSize: " << rc->GetSize()
      << " RemFreeBufs: " << mFreeBufferList->size()
      << EndLogLine;

    return rc;
  }

  void
  PrintFreeList()
  {
    mFreeBufferList->print("Freelist");
  }

  void
  PrintPendingList()
  {
    mPendingBufferList.print("PendingList");
  }

  void
  Print()
  {
    PrintFreeList();
    PrintPendingList();    
  }

  void
  AddToFreeList( skv_server_cursor_buffer_t * aBuffer )
  {
    AssertLogLine( aBuffer != NULL ) << EndLogLine;

    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG )
      << " skv_server_cursor_control_block_t::AddToFreeList(): "
      << " aBuffer: " << (void *) aBuffer
      << " aBuffer->GetSize(): " << aBuffer->GetSize()
      << EndLogLine;

    mFreeBufferList->push( aBuffer );
  }

  void
  AddToPendingList( skv_server_cursor_buffer_t * aBuffer )
  {
    AssertLogLine( aBuffer != NULL ) << EndLogLine;

    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG )
      << " skv_server_cursor_control_block_t::AddToPendingList(): Entering..."
      << " aBuffer: " << (void *) aBuffer
      << " aBuffer->GetSize(): " << aBuffer->GetSize()
      << EndLogLine;

    mPendingBufferList.enqueue( aBuffer );
  }

  void
  RemoveFromPendingList( skv_server_cursor_buffer_t * aBuffer )
  {
    AssertLogLine( aBuffer != NULL ) << EndLogLine;

    int rc = mPendingBufferList.remove( aBuffer );    
    AssertLogLine( rc != -1 )
      << "skv_server_cursor_control_block_t::RemoveFromPendingList(): ERROR: "
      << " rc: " << rc
      << " aBuffer: " << (void *) aBuffer
      << EndLogLine;

    BegLogLine( SKV_SERVER_CURSOR_MANAGER_IF_LOG )
      << " skv_server_cursor_control_block_t::RemoveFromPendingList(): "
      << " aBuffer: " << (void *) aBuffer
      << " aBuffer->GetSize(): " << aBuffer->GetSize()
      << EndLogLine;    
  }

  skv_server_cursor_buffer_t *
  ReserveBuffer()
  {
    skv_server_cursor_buffer_t * FreeBuffer = RemoveFromFreeList();

    if(FreeBuffer == NULL)
      return NULL;

    AddToPendingList( FreeBuffer );

    BegLogLine( SKV_SERVER_CURSOR_RESERVE_RELEASE_BUF_LOG )
      << " skv_server_cursor_control_block_t::ReserveBuffer(): "
      << " FreeBuffer: " << (void *) FreeBuffer
      << " FreeBuffer->GetSize(): " << FreeBuffer->GetSize()
      << EndLogLine;

    FreeBuffer->Reset();

    return FreeBuffer;
  }

  void
  ReleaseBuffer( skv_server_cursor_buffer_t * aBuffer )
  {
    unsigned long long FinishTime = PkTimeGetNanos();

    int FilledSize = aBuffer->GetFilledSize();
    double Bandwidth = ((FilledSize * 1e9) / (1024.0*1024.0)) / (FinishTime-aBuffer->GetStartTime());

    BegLogLine( SKV_SERVER_CURSOR_RESERVE_RELEASE_BUF_LOG )
      << " skv_server_cursor_control_block_t::ReleaseBuffer(): "
      << " aBuffer: " << (void *) aBuffer
      << " aBuffer->GetSize(): " << aBuffer->GetSize()
      << " FilledSize: " << FilledSize
      << " Bandwidth (MB): " << Bandwidth
      << EndLogLine;

    RemoveFromPendingList( aBuffer );
    AddToFreeList( aBuffer );
  }

  void
  Finalize()
  {
    if( IsIndexCursor() )
    {
      mIndexCursorPos.Finalize();
    }

    if( mProjColDescCount > 0 )
    {
      free( mProjColDesc );
      mProjColDesc = NULL;
    }

    it_lmr_free( mWorkingBufferLMR );

    if( mWorkingBuffer != NULL )
    {
      free( mWorkingBuffer );
      mWorkingBuffer = NULL;
    }

    if( mFreeBufferList )
    {
      delete mFreeBufferList;
      mFreeBufferList = NULL;
    }

    if( mEventWaitList )
    {
      delete mEventWaitList;
      mEventWaitList = NULL;
    }
  }
};

typedef skv_server_cursor_control_block_t* skv_server_cursor_hdl_t;

class skv_server_cursor_manager_if_t
{
public:
  skv_status_t
  InitHandle( char*                                       aBuff,
              int                                         aBuffSize,
              it_pz_handle_t                              aPZ_Hdl,
              skv_server_internal_event_manager_if_t*     aInternalEventManager,
              skv_server_cursor_hdl_t*                    aServCursorHandle )
  {
    skv_server_cursor_control_block_t * ServCursorCCB =
        (skv_server_cursor_control_block_t *) malloc( sizeof( skv_server_cursor_control_block_t ) );
    StrongAssertLogLine( ServCursorCCB != NULL )
      << "skv_server_cursor_manager_if_t::InitHandle():: ERROR:: "
      << " not enough memory for: " << sizeof( skv_server_cursor_control_block_t )
      << " bytes"
      << EndLogLine;

    skv_status_t status = ServCursorCCB->Init( aPZ_Hdl, 
                                               aInternalEventManager,
                                               aBuff,
                                               aBuffSize );

    *aServCursorHandle = ServCursorCCB;

    return status;
  }
};

#endif
