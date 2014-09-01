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

#ifndef __SKV_CLIENT_TYPES_HPP__
#define __SKV_CLIENT_TYPES_HPP__

#ifndef SKV_CLIENT_FREE_QUEUE_LOG
#define SKV_CLIENT_FREE_QUEUE_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_DONE_QUEUE_LOG
#define SKV_CLIENT_DONE_QUEUE_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_CCB_MANAGER_LOG
#define SKV_CLIENT_CCB_MANAGER_LOG ( 0 | SKV_LOGGING_ALL )
#endif

/** \brief This is the max number of non-blocking commands that a client can issue without checking or waiting for completeness
 *  \note It has nothing to do the number of outstanding operations posted to the IT_API Endpoint.
 */
#define SKV_CLIENT_COMMAND_LIMIT ( 128 * 1024 )
//#define SKV_CLIENT_COMMAND_LIMIT ( 128 )

// The number of keys to prefetch
#define SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE   ( 2 )

#include <common/skv_types.hpp>
#include <common/skv_distribution_manager.hpp>
#include <client/skv_c2s_active_broadcast.hpp>

struct skv_client_command_open_t  
{
  skv_pds_id_t* mPDSId;
};

struct skv_client_command_insert_t 
{
  skv_cmd_RIU_flags_t   mFlags;
  it_lmr_handle_t       mValueLMR;
};

struct skv_client_command_retrieve_t
{
  skv_cmd_RIU_flags_t   mFlags;

  union
  {
    it_lmr_handle_t     mValueLMR;
    void*               mValueAddr;
  } mValueBufferRef;

  int*                  mValueRetrievedSize;
  int                   mValueRequestedSize;
};

struct skv_client_command_retrieve_dist_t
{
  skv_distribution_t*   mDist;  
};

struct skv_client_command_update_t
{
  it_lmr_handle_t       mValueLMR;
};

struct skv_client_command_pdscntl_t
{
  skv_pdscntl_cmd_t     mCmd;
  skv_pds_attr_t       *mPDSAttr;
};

struct skv_client_command_remove_t
{
  skv_cmd_remove_flags_t mFlags;
};

struct skv_client_command_bulk_insert_t
{
  // Needed for debugging
  char*         mBuffer;
  int           mBufferSize;
  uint64_t      mBufferChecksum;
};

struct skv_client_command_retrieve_n_keys_t
{
  int           mCachedKeysCountMax;
  int*          mCachedKeysCountPtr;

  skv_key_t*    mCachedKeys;
};

struct skv_client_command_active_bcast_t
{
  skv_c2s_active_broadcast_func_type_t  mFuncType;
  int                                   mNodeId;
  void                                 *mIncommingDataMgrIF;
  it_lmr_handle_t                       mBufferLMRHdl;
};

struct skv_client_command_cursor_prefetch_t
{
  int           *mBufferSizePtr;
  skv_status_t  *mStatusPtr;
  char          *mBufferPtr;
  // skv_client_index_handle_t                   mIndexHandle;
  void          *mIndexHandle;
  int            mNodeId;
  int           *mOutstandingBuffersPtr;
  void          *mBuffer;
  void          *mBufferChainMgr;
};

struct skv_client_command_t
{
  skv_command_type_t mType;    

  union
  {
    skv_client_command_open_t              mCommandOpen;
    skv_client_command_insert_t            mCommandInsert;
    skv_client_command_bulk_insert_t       mCommandBulkInsert;
    skv_client_command_retrieve_t          mCommandRetrieve;
    skv_client_command_retrieve_n_keys_t   mCommandRetrieveNKeys;
    skv_client_command_retrieve_dist_t     mCommandRetrieveDist;
    skv_client_command_update_t            mCommandUpdate;
    skv_client_command_remove_t            mCommandRemove;
    skv_client_command_pdscntl_t           mCommandPDScntl;
    skv_client_command_active_bcast_t      mCommandActiveBcast;
    skv_client_command_cursor_prefetch_t   mCommandCursorPrefetch;
  } mCommandBundle;  
};

typedef enum
{
  SKV_CLIENT_EVENT_CMD_NONE     = 0,
  SKV_CLIENT_EVENT_CMD_COMPLETE = 1,
  SKV_CLIENT_EVENT_ERROR,
  //    SKV_CLIENT_EVENT_VALUE_MOCK_RDMA_READ_REQ,
  //    SKV_CLIENT_EVENT_KEY_MOCK_RDMA_READ_REQ,
  SKV_CLIENT_EVENT_RDMA_WRITE_VALUE_ACK
} skv_client_event_t;

static
const char*
skv_client_event_to_string( skv_client_event_t aEvent )
{
  switch( aEvent )
  {
    case SKV_CLIENT_EVENT_CMD_NONE:             { return "SKV_CLIENT_EVENT_CMD_NONE"; }
    case SKV_CLIENT_EVENT_CMD_COMPLETE:         { return "SKV_CLIENT_EVENT_CMD_COMPLETE"; }
    case SKV_CLIENT_EVENT_ERROR:                { return "SKV_CLIENT_EVENT_ERROR"; }
      // case SKV_CLIENT_EVENT_VALUE_MOCK_RDMA_READ_REQ: { return "SKV_CLIENT_EVENT_VALUE_MOCK_RDMA_READ_REQ";}
      // case SKV_CLIENT_EVENT_KEY_MOCK_RDMA_READ_REQ:   { return "SKV_CLIENT_EVENT_KEY_MOCK_RDMA_READ_REQ";}
    case SKV_CLIENT_EVENT_RDMA_WRITE_VALUE_ACK: { return "SKV_CLIENT_EVENT_RDMA_WRITE_VALUE_ACK"; }
    default:
      {
      StrongAssertLogLine( 0 ) << "skv_client_event_to_string:: "
          << " aEvent: " << aEvent << " Not recognized"
          << EndLogLine
      break;
    }
  }
  return "SKV_CLIENT_EVENT_CMD_UNKNOWN";
}

typedef enum
{
  SKV_CLIENT_COMMAND_STATE_IDLE = 1,
  SKV_CLIENT_COMMAND_STATE_PENDING,
  SKV_CLIENT_COMMAND_STATE_DONE,
  // SKV_CLIENT_COMMAND_STATE_WAITING_TO_RDMA_WRITE_KEY,
  // SKV_CLIENT_COMMAND_STATE_WAITING_TO_RDMA_WRITE_VALUE,
  SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL,
  SKV_CLIENT_COMMAND_STATE_WAITING_FOR_VALUE_TX_ACK
} skv_client_command_state_t;

static
const char*
skv_client_command_state_to_string( skv_client_command_state_t aState )
{
  switch( aState )
  {
    case SKV_CLIENT_COMMAND_STATE_IDLE:                         { return "SKV_CLIENT_COMMAND_STATE_IDLE"; }
    case SKV_CLIENT_COMMAND_STATE_PENDING:                      { return "SKV_CLIENT_COMMAND_STATE_PENDING"; } 
    case SKV_CLIENT_COMMAND_STATE_DONE:                         { return "SKV_CLIENT_COMMAND_STATE_DONE"; } 
      //case SKV_CLIENT_COMMAND_STATE_WAITING_TO_RDMA_WRITE_KEY:    { return "SKV_CLIENT_COMMAND_STATE_WAITING_TO_RDMA_WRITE_KEY"; } 
      //case SKV_CLIENT_COMMAND_STATE_WAITING_TO_RDMA_WRITE_VALUE:  { return "SKV_CLIENT_COMMAND_STATE_WAITING_TO_RDMA_WRITE_VALUE"; } 
    case SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL:             { return "SKV_CLIENT_COMMAND_STATE_WAITING_FOR_CMPL"; } 
    case SKV_CLIENT_COMMAND_STATE_WAITING_FOR_VALUE_TX_ACK:     { return "SKV_CLIENT_COMMAND_STATE_WAITING_FOR_VALUE_TX_ACK"; } 
    default:
    {
      StrongAssertLogLine( 0 )
          << "skv_client_command_state_to_string:: "
          << " aState: " << aState
          << " Not recognized."
          << EndLogLine
          break;
    }
  }
  return "SKV_CLIENT_COMMAND_STATE_UNKNOWN";
}

typedef enum
{
  SKV_CLIENT_STATE_CONNECTED = 1,
  SKV_CLIENT_STATE_DISCONNECTED
} skv_client_state_t;

static
const char*
skv_client_state_to_string( skv_client_state_t aState )
{
  switch( aState )
    {
    case SKV_CLIENT_STATE_CONNECTED:    { return "SKV_CLIENT_STATE_CONNECTED";    }
    case SKV_CLIENT_STATE_DISCONNECTED: { return "SKV_CLIENT_STATE_DISCONNECTED"; }
    default:
      {
        StrongAssertLogLine( 0 )
          << "skv_client_state_to_string:: "
          << " aState: " << aState
          << " Not recognized."	  
          << EndLogLine;
        break;
      }
    }
  return "SKV_CLIENT_STATE_UNKNOWN";
}

class skv_client_ccb_manager_if_t;

struct skv_client_ccb_t
{
  char                          mSendCtrlMsgBuff[ SKV_CONTROL_MESSAGE_SIZE ];
  // char                         mRecvCtrlMsgBuff[ SKV_CONTROL_MESSAGE_SIZE ];
  char                         *mRecvCtrlMsgBuff;
  it_lmr_handle_t               mBaseLMR;
  skv_client_command_t          mCommand;
  skv_client_command_state_t    mState;
  skv_client_ccb_t*             mNext;
  skv_client_ccb_t*             mPrev;

  // This status is only valid after the command 
  // has completed
  skv_status_t                  mStatus;
  int                           mCmdOrd;
  skv_client_ccb_manager_if_t*  mCCBMgrIF;
  unsigned char                 mCmdReadyForDone;

public:
  void
  SetCmdOrd( int aCmdOrd )
  {
    mCmdOrd = aCmdOrd;
  }

  int
  GetCmdOrd()
  {
    return mCmdOrd;
  }

  skv_client_command_state_t
  GetState()
  {
    return mState;
  }

  void
  Transit( skv_client_command_state_t aState )
  {
    mState = aState;
  }

  char*
  GetRecvBuff()
  {
    return mRecvCtrlMsgBuff;
  }

  void
  SetRecvBuff( char *aNewBuff )
  {
    mRecvCtrlMsgBuff = aNewBuff;
  }

  char*
  GetSendBuff()
  {
    return mSendCtrlMsgBuff;
  }

  void SetSendWasFirst() { mCmdReadyForDone = 1; }
  void SetRecvWasFirst() { mCmdReadyForDone = 1; }
  int  CheckSendIsComplete() { return (int)mCmdReadyForDone; }
  int  CheckRecvIsComplete() { return (int)mCmdReadyForDone; }

  void
  Init( it_lmr_handle_t aBaseLMR, skv_client_ccb_manager_if_t* aCCBMgrIF )
  {
    mBaseLMR = aBaseLMR;

    bzero( mSendCtrlMsgBuff, SKV_CONTROL_MESSAGE_SIZE );
    // bzero( mRecvCtrlMsgBuff, SKV_CONTROL_MESSAGE_SIZE );
    mRecvCtrlMsgBuff = mSendCtrlMsgBuff;

    mNext = NULL;
    mPrev = NULL;

    mCCBMgrIF = aCCBMgrIF;

    Reset();
  }

  void 
  Finalize()
  {

  }

  void
  Reset()
  {
    mState         = SKV_CLIENT_COMMAND_STATE_IDLE;    
    mCommand.mType = SKV_COMMAND_NONE;
    mCmdOrd        = -1;
    mCmdReadyForDone = 0;
    mRecvCtrlMsgBuff = NULL;
  }  
};

class skv_client_ccb_manager_if_t
{
  skv_client_ccb_t*                        mFreeCommandCtrlBlocks;

  /***
   * NEED: This has to be properly managed to enable
   * clean reconnect after a disconnect
   ***/
  skv_client_ccb_t*                        mDoneCommandCtrlBlocks;

  skv_client_ccb_t                         mCommandCtrlBlockHeap[ SKV_CLIENT_COMMAND_LIMIT ];

  it_lmr_handle_t                           mBaseHandle;
  

public:

  void
  Init( it_pz_handle_t aPZ_Hdl )
  {

    it_mem_priv_t privs         = (it_mem_priv_t) (IT_PRIV_LOCAL_READ | IT_PRIV_LOCAL_WRITE);
    it_lmr_flag_t lmr_flags     = IT_LMR_FLAG_NON_SHAREABLE;

    int SizeToLMR = sizeof( mCommandCtrlBlockHeap );

    BegLogLine( SKV_CLIENT_CCB_MANAGER_LOG )
      << "skv_client_ccb_manager_if_t::Init(): Before it_lmr_create(): "
      << " SizeToLMR: " << SizeToLMR
      << EndLogLine;

    it_status_t istatus = it_lmr_create( aPZ_Hdl, 
                                         mCommandCtrlBlockHeap,
                                         NULL,
                                         SizeToLMR,
                                         IT_ADDR_MODE_ABSOLUTE, 
                                         privs, 
                                         lmr_flags,
                                         0,
                                         & mBaseHandle,
                                         NULL );

    StrongAssertLogLine( istatus == IT_SUCCESS )
      << "skv_client_ccb_manager_if_t::Init(): ERROR:: after it_lmr_create()"
      << " status: " << istatus 
      << EndLogLine;

    StrongAssertLogLine( SKV_CLIENT_COMMAND_LIMIT >=2 )
      << "skv_client_ccb_manager_if_t::Init(): ERROR:: "
      << " SKV_CLIENT_COMMAND_LIMIT: " << SKV_CLIENT_COMMAND_LIMIT
      << EndLogLine;

    BegLogLine( SKV_CLIENT_CCB_MANAGER_LOG )
      << "skv_client_ccb_manager_if_t::Init(): client baseLMR: "
      << " SizeToLMR: " << SizeToLMR
      << " BaseLMR: " << ( void * )mBaseHandle
      << EndLogLine;


    mCommandCtrlBlockHeap[ 0 ].Init( mBaseHandle, this );
    mCommandCtrlBlockHeap[ 0 ].mPrev = NULL;
    mCommandCtrlBlockHeap[ 0 ].mNext = & mCommandCtrlBlockHeap[ 1 ];

    for( int i = 1; i < SKV_CLIENT_COMMAND_LIMIT-1; i++ )
    {
      mCommandCtrlBlockHeap[ i ].Init( mBaseHandle, this );

      mCommandCtrlBlockHeap[ i ].mNext = & mCommandCtrlBlockHeap[ i+1 ];
      mCommandCtrlBlockHeap[ i ].mPrev = & mCommandCtrlBlockHeap[ i-1 ];
    }

    mCommandCtrlBlockHeap[ SKV_CLIENT_COMMAND_LIMIT-1 ].Init( mBaseHandle, this );
    mCommandCtrlBlockHeap[ SKV_CLIENT_COMMAND_LIMIT-1 ].mPrev = & mCommandCtrlBlockHeap[ SKV_CLIENT_COMMAND_LIMIT-2 ];
    mCommandCtrlBlockHeap[ SKV_CLIENT_COMMAND_LIMIT-1 ].mNext = NULL;

    mDoneCommandCtrlBlocks = NULL;

    mFreeCommandCtrlBlocks = & mCommandCtrlBlockHeap[ 0 ];
  }

  /***
   * AddToDoneCCBQueue::
   * Desc: 
   * input: 
   * returns: Returns a free command control block
   * Or NULL if none are available
   ***/
  void
  AddToDoneCCBQueue( skv_client_ccb_t* aCCB )
  {
    BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::AddToDoneCCBQueue(): Entering "
      << " aCCB: " << (void *) aCCB
      << EndLogLine;

    if( mDoneCommandCtrlBlocks == NULL )
    {
      mDoneCommandCtrlBlocks = aCCB;
    }
    else
    {
      mDoneCommandCtrlBlocks->mPrev = aCCB;
      aCCB->mNext = mDoneCommandCtrlBlocks;
      mDoneCommandCtrlBlocks = aCCB;
    }

    BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::AddToDoneCCBQueue(): Leaving "
      << EndLogLine;
  }

  /***
   * skv_client_ccb_manager_if_t::RemoveFromDoneCCBQueue::
   * Desc: 
   * input: 
   * returns: Returns a free command control block
   * Or NULL if none are available
   ***/
  void
  RemoveFromDoneCCBQueue( skv_client_ccb_t* aCCB )
  {
    BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::RemoveFromDoneCCBQueue(): Entering "
      << " aCCB: " << (void *) aCCB
      << EndLogLine;

    if( mDoneCommandCtrlBlocks == aCCB )
    {
      mDoneCommandCtrlBlocks = aCCB->mNext;

      if( mDoneCommandCtrlBlocks != NULL )
        mDoneCommandCtrlBlocks->mPrev = NULL;
    }

    // Remove the CCB from the Done queue
    if( (aCCB->mPrev != NULL) && (aCCB->mNext != NULL) )
    {
      aCCB->mPrev->mNext = aCCB->mNext;
      aCCB->mNext->mPrev = aCCB->mPrev;
    }
    else if( aCCB->mPrev != NULL )
    {
      aCCB->mPrev->mNext = aCCB->mNext;
    }
    else if( aCCB->mNext != NULL )
    {
      aCCB->mNext->mPrev = aCCB->mPrev;
    }
    else
    {
      // Both Prev and Next are NULL
    }

    aCCB->mPrev = NULL;
    aCCB->mNext = NULL;

    BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::RemoveFromDoneCCBQueue(): Leaving "
      << EndLogLine;

    return;
  }

  /***
   * skv_client_ccb_manager_if_t::AddToFreeCCBQueue::
   * Desc: 
   * input: 
   * returns: Returns a free command control block
   * Or NULL if none are available
   ***/
  void
  AddToFreeCCBQueue( skv_client_ccb_t* aCCB )
  {
    BegLogLine( SKV_CLIENT_FREE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::AddToFreeCCBQueue(): Entering "
      << EndLogLine;

    // AssertLogLine( aCCB->mPrev == NULL )
    //   << "skv_client_ccb_manager_if_t::AddToFreeCCBQueue(): ERROR: "
    //   << " aCCB->mPrev: " << (void *) aCCB->mPrev
    //   << EndLogLine;

    // AssertLogLine( aCCB->mNext == NULL )
    //   << "skv_client_ccb_manager_if_t::AddToFreeCCBQueue(): ERROR: "
    //   << " aCCB->mNext: " << (void *) aCCB->mNext
    //   << EndLogLine;

    if( mFreeCommandCtrlBlocks == NULL )
    {
      mFreeCommandCtrlBlocks = aCCB;
    }
    else
    {
      mFreeCommandCtrlBlocks->mPrev = aCCB;

      aCCB->mNext = mFreeCommandCtrlBlocks;

      mFreeCommandCtrlBlocks = aCCB;

      AssertLogLine( mFreeCommandCtrlBlocks->mPrev == NULL )
        << "skv_client_ccb_manager_if_t::AddToFreeCCBQueue(): ERROR: "
        << " mFreeCommandCtrlBlocks->mPrev: " << (void *) mFreeCommandCtrlBlocks->mPrev
        << EndLogLine;
    }

    // Bring back to idle state
    aCCB->Reset();

    BegLogLine( SKV_CLIENT_FREE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::AddToFreeCCBQueue(): Leaving "
      << EndLogLine;
  }

  /***
   * skv_client_ccb_manager_if_t::RemoveFromFrontDoneCCBQueue::
   * Desc: 
   * input/output: aCCB  
   * returns: 
   * Or NULL if none are available
   ***/
  skv_client_ccb_t*
  RemoveFromFrontDoneCCBQueue()
  {
    BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontDoneCCBQueue(): Entering "
      << EndLogLine;

    if( mDoneCommandCtrlBlocks == NULL )
    {
      BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
        << "skv_client_ccb_manager_if_t::RemoveFromFrontDoneCCBQueue(): Leaving "
        << EndLogLine;

      return NULL;
    }

    skv_client_ccb_t* DoneBlock = mDoneCommandCtrlBlocks;

    AssertLogLine( DoneBlock->mPrev == NULL )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontDoneCCBQueue(): "
      << " DoneBlock: " << (void *) DoneBlock
      << EndLogLine;

    mDoneCommandCtrlBlocks = mDoneCommandCtrlBlocks->mNext;

    if( mDoneCommandCtrlBlocks != NULL )
      mDoneCommandCtrlBlocks->mPrev = NULL;

    DoneBlock->mNext = NULL;

    AssertLogLine( DoneBlock->mState == SKV_CLIENT_COMMAND_STATE_DONE )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontDoneCCBQueue(): "
      << " DoneBlock->mState: " << DoneBlock->mState
      << EndLogLine;  

    BegLogLine( SKV_CLIENT_DONE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontDoneCCBQueue(): Leaving "
      << EndLogLine;

    return DoneBlock;
  }

  /***
   * skv_client_ccb_manager_if_t::RemoveFromFrontFreeCCBQueue::
   * Desc: 
   * input/output: aCCB  
   * returns: 
   * Or NULL if none are available
   ***/
  skv_client_ccb_t*
  RemoveFromFrontFreeCCBQueue()
  {
    BegLogLine( SKV_CLIENT_FREE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontFreeCCBQueue(): Entering "
      << EndLogLine;

    if( mFreeCommandCtrlBlocks == NULL )
    {
      BegLogLine( SKV_CLIENT_FREE_QUEUE_LOG )
        << "skv_client_ccb_manager_if_t::RemoveFromFrontFreeCCBQueue(): FreeCommandCtrlBlocks is empty. Leaving... "
        << EndLogLine;

      return NULL;
    }

    skv_client_ccb_t* FreeBlock = mFreeCommandCtrlBlocks;

    mFreeCommandCtrlBlocks = mFreeCommandCtrlBlocks->mNext;

    AssertLogLine( FreeBlock->mPrev == NULL )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontFreeCCBQueue(): "
      << " FreeBlock: " << (void *) FreeBlock
      << EndLogLine;

    // Detach the first block from the list
    FreeBlock->mPrev = NULL;
    FreeBlock->mNext = NULL;

    if( mFreeCommandCtrlBlocks != NULL )
      mFreeCommandCtrlBlocks->mPrev = NULL;

    AssertLogLine( FreeBlock->mState == SKV_CLIENT_COMMAND_STATE_IDLE )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontFreeCCBQueue(): "
      << " FreeBlock->mState: " << FreeBlock->mState
      << EndLogLine;  

    BegLogLine( SKV_CLIENT_FREE_QUEUE_LOG )
      << "skv_client_ccb_manager_if_t::RemoveFromFrontFreeCCBQueue(): Leaving "
      << EndLogLine;

    return FreeBlock;
  }  

  void
  Finalize()
  {
    it_lmr_free( mBaseHandle );
  }
};

#endif
