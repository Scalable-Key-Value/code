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

#ifndef __SKV_CLIENT_SERVER_CONN_HPP__
#define __SKV_CLIENT_SERVER_CONN_HPP__

#include <cstdlib>
#include <queue>
#include <skv/common/skv_array_stack.hpp>
#include <skv/common/skv_client_server_headers.hpp>

#ifndef SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG
#define SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RESPONSE_POLLING_LOG
#define SKV_CLIENT_RESPONSE_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_REQUEST_COALESCING_LOG
#define SKV_CLIENT_REQUEST_COALESCING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifdef SKV_COALESCING_STATISTICS
#include <sstream>
static uint64_t gClientCoalescCount[ SKV_MAX_COALESCED_COMMANDS + 1 ];
static uint64_t gClientRequestCount = 0;
static uint64_t gClientCoalescSum = 0;
#endif

//#define DISABLE_CLIENT_COALESCING

typedef enum
{
  SKV_CLIENT_CONN_DISCONNECTED = 1,
  SKV_CLIENT_CONN_CONNECTED
} skv_client_conn_state_t;

#define ALIGNMENT ( 256 )
#define SKV_MAX_UNRETIRED_CMDS ( SKV_MAX_COMMANDS_PER_EP * SKV_MAX_COALESCED_COMMANDS )

struct skv_client_server_conn_t;

struct skv_client_cookie_t
{
  skv_client_server_conn_t*  mConn;
  skv_client_ccb_t*          mCCB;
  uint64_t                    mSeq;
};

template<class streamclass>
static
streamclass&
operator<<(streamclass& os, const skv_client_cookie_t& aHdr)
{
  os << "skv_client_cookie_t [ "
     << (void *) aHdr.mConn << ' '
     << (void *) aHdr.mCCB << ' '
     << aHdr.mSeq
     << " ]";

  return(os);
}

struct skv_client_queued_command_rep_t
{
  // sendseg is hold in separate array to allow post_rdma() without extra copy of segment list
  skv_client_ccb_t *mCCB;
  uint64_t mSeqNo;
};
 struct skv_client_server_conn_t
  {
    skv_client_conn_state_t mState;
    it_ep_handle_t mEP;
    skv_rmr_triplet_t mServerCommandMem;
    int mCurrentServerRecvSlot;
    skv_client_queued_command_rep_t mQueuedCommands[ SKV_SERVER_COMMAND_SLOTS ];  // hold request metadata for completion processing
    it_lmr_triplet_t mQueuedSegments[ SKV_SERVER_COMMAND_SLOTS ]; // hold request segments for post_rmda_write() call
    int mSendSegsCount;
    int mInFlightWriteCount;   // number of posted rdma_write()
    int mOutStandingRequests;  // number of uncompleted requests ( rdma_write() might post more than one request )

    int mUnretiredRecvCount;

    typedef std::queue<skv_client_ccb_t *> skv_command_overflow_queue_t;

    skv_command_overflow_queue_t* mOverflowCommands;   // queue to hold deferred commands that couldn't be posted to the server

    int mSeqNo;

    int mCommandSlotBusyFlags[ SKV_MAX_UNRETIRED_CMDS];

    // typedef std::stack<int>                          skv_command_ordinal_stack_t;
    typedef skv_array_stack_t<int, SKV_MAX_UNRETIRED_CMDS> skv_command_ordinal_stack_t;
    skv_command_ordinal_stack_t* mOrdinalStack;

    char *mResponseSlotBuffer;
    it_lmr_handle_t mResponseLMRHandle;
    skv_rmr_triplet_t mResponseRMR;
    int mCurrentResponseSlot;
    bool mServerIsLocal;

    int
    ReserveCmdOrdinal()
      {
        if( mUnretiredRecvCount >= SKV_MAX_UNRETIRED_CMDS )
          return -1;

        AssertLogLine( mUnretiredRecvCount >= 0 && mUnretiredRecvCount < SKV_MAX_UNRETIRED_CMDS )
          << "skv_client_server_conn_t::ReserveCommandOrdinal():: ERROR:: "
          << " mUnretiredRecvCount: " << mUnretiredRecvCount
          << " SKV_MAX_UNRETIRED_CMDS: " << SKV_MAX_UNRETIRED_CMDS
          << EndLogLine;

        mUnretiredRecvCount++;

        AssertLogLine( mOrdinalStack->size() > 0 )
          << "skv_client_server_conn_t::ReserveCommandOrdinal():: ERROR:: "
          << " mOrdinalStack->size(): " << mOrdinalStack->size()
          << EndLogLine;

        int CmdOrd = mOrdinalStack->top();
        mOrdinalStack->pop();

        AssertLogLine( CmdOrd >= 0 && CmdOrd < SKV_MAX_UNRETIRED_CMDS )
          << "ERROR: "
          << " CmdOrd: " << CmdOrd
          << " SKV_MAX_UNRETIRED_CMDS: " << SKV_MAX_UNRETIRED_CMDS
          << EndLogLine;

        AssertLogLine( mCommandSlotBusyFlags[ CmdOrd ] == 0 )
          << "ERROR: Expect the command slot to be free "
          << EndLogLine;

        mCommandSlotBusyFlags[ CmdOrd ] = 1;

        return CmdOrd;
      }

    void
    ReleaseCmdOrdinal(int aCmdOrd)
      {
        mUnretiredRecvCount--;
        mOutStandingRequests--;

        AssertLogLine(( mUnretiredRecvCount >= 0 ) && ( mOutStandingRequests >= 0 ))
          << "skv_client_server_conn_t::ReleaseCmdOrdinal():: ERROR:: "
          << " mUnretiredRecvCount: " << mUnretiredRecvCount
          << EndLogLine;

        mOrdinalStack->push( aCmdOrd );

        AssertLogLine( aCmdOrd >= 0 && aCmdOrd < SKV_MAX_UNRETIRED_CMDS )
          << "ERROR: "
          << " aCmdOrd: " << aCmdOrd
          << " SKV_MAX_UNRETIRED_CMDS: " << SKV_MAX_UNRETIRED_CMDS
          << EndLogLine;

        mCommandSlotBusyFlags[ aCmdOrd ] = 0;

        return;
      }

    /** \brief Get the serverside slot address for the next command to write
     */
    uintptr_t GetCurrentServerRecvAddress()
      {
        uintptr_t address = mServerCommandMem.GetAddr();

        address += mCurrentServerRecvSlot * SKV_CONTROL_MESSAGE_SIZE;

        BegLogLine( SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG )
          << "skv_client_server_conn_t::GetNextServerRecvAddress():"
          << " EP: " << ( void* )this
          << " slot#: " << mCurrentServerRecvSlot
          << " address to write: " << (void*)address
          << EndLogLine;

        return address;
      }

    it_lmr_handle_t
    GetResponseLMR()
      {
        return mResponseLMRHandle;
      }
    void*
    GetResponseRMRAddr()
      {
        return (void*) mResponseRMR.GetAddr();
      }
    uint64_t
    GetResponseRMRLength()
      {
        return mResponseRMR.GetLen();
      }
    it_rmr_context_t&
    GetResponseRMRContext()
      {

        return mResponseRMR.GetRMRContext();
      }

    /******************************************************************/
    // Recv-Slot Polling
    skv_server_to_client_cmd_hdr_t*
    CheckForNewResponse() const
      {
        const int Index = mCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE;

        skv_header_as_cmd_buffer_t* NewResponse = (skv_header_as_cmd_buffer_t*) (&mResponseSlotBuffer[Index]);

        const bool found = ((NewResponse->mData.mRespHdr.mCmdType != SKV_COMMAND_NONE) &&
                       NewResponse->GetCheckSum() == NewResponse->mData.mRespHdr.CheckSum());
        if( found )
        {
          return &NewResponse->mData.mRespHdr;
        }

        return NULL;
      }

    void
    ResponseSlotAdvance()
      {
        // reset the current slot first
        int Index = mCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE;
        char* OldResponse = (char*) (&mResponseSlotBuffer[Index]);

        BegLogLine( SKV_CLIENT_RESPONSE_POLLING_LOG )
          << "skv_client_server_conn_t::ResponseSlotAdvance():: Slot from: " << mCurrentResponseSlot
          << " @: " << (void*)OldResponse
          << EndLogLine;

        ((skv_header_as_cmd_buffer_t*)OldResponse)->SetCheckSum( SKV_CTRL_MSG_FLAG_EMPTY );

        // advance to next slot
        mCurrentResponseSlot = (mCurrentResponseSlot + 1) % SKV_SERVER_COMMAND_SLOTS;

        BegLogLine( SKV_CLIENT_RESPONSE_POLLING_LOG )
          << "skv_client_server_conn_t::ResponseSlotAdvance()::"
          << " EP: " << ( void* )this
          << " next Slot: " << mCurrentResponseSlot
          << " @: "         << (void*)( & mResponseSlotBuffer[ mCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE ] )
          << EndLogLine;
      }

    // int
    // GetCurrentResponseSlot()
    // {
    //   return mCurrentResponseSlot;
    // }

    inline
    bool DynamicCoalescingCondition()
    {
      return ( mInFlightWriteCount * mInFlightWriteCount < mSendSegsCount );
    }

    skv_server_to_client_cmd_hdr_t*
    GetCurrentResponseAddress()
    {
      return (skv_server_to_client_cmd_hdr_t*) (&mResponseSlotBuffer[mCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE]);
    }
    inline
    skv_status_t RequestCompletion( skv_client_ccb_t *aCCB )
    {
      if( aCCB->CheckRequestWithWrite() )
        mInFlightWriteCount--;

      bool needpost = ( (( mSendSegsCount > 0) && DynamicCoalescingCondition() ) ||
          ( mSendSegsCount >= SKV_MAX_COALESCED_COMMANDS ) );

      BegLogLine( SKV_CLIENT_REQUEST_COALESCING_LOG )
        << "RequestCompletion: EP: " << (void*)this
        << " needpost: " << needpost
        << " inFlight: " << mInFlightWriteCount
        << " mSegsCount: " << mSendSegsCount
        << " mUnretired: " << mUnretiredRecvCount
        << EndLogLine;

      if( needpost )
      {
        return PostRequest( );
      }
      return SKV_SUCCESS;
    }
    inline
    skv_status_t PostRequest( )
    {
      it_status_t status = IT_SUCCESS;

      it_rdma_addr_t dest_recv_slot_addr = ( it_rdma_addr_t )( GetCurrentServerRecvAddress() );
      int active_srv_slot = mCurrentServerRecvSlot + mSendSegsCount -1;  // the last slot we're going to write
      skv_client_cookie_t cookie;
      cookie.mCCB = mQueuedCommands[ active_srv_slot ].mCCB;
      cookie.mSeq = mQueuedCommands[ active_srv_slot ].mSeqNo;
      cookie.mConn = this;

      it_dto_cookie_t *ITCookie = (it_dto_cookie_t*) &cookie;

      // if the server slot is 0 the do a signalled
      it_dto_flags_t dto_write_flags = (it_dto_flags_t) (0);
      if( mCurrentServerRecvSlot == 0 )
        dto_write_flags = (it_dto_flags_t) (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG);

#ifdef SKV_COALESCING_STATISTICS
      gClientCoalescCount[ mSendSegsCount ]++;
      gClientRequestCount = (gClientRequestCount+1) & 0xFFF;
      gClientCoalescSum += mSendSegsCount;
      if( gClientRequestCount == 0 )
      {
//        std::string buckets;
        std::stringstream buckets;
        for ( int i=1; i<SKV_MAX_COALESCED_COMMANDS+1; i++ )
        {
           buckets << "["<< i << ":" << gClientCoalescCount[ i ] << "] ";
        }
        BegLogLine( 1 )
          << "Client Request Coalescing after " << 0xFFF << " Requests: "
          << buckets.str().c_str()
          << " Avg: " << (double)gClientCoalescSum/(double)0x1000
          << EndLogLine;
        memset( gClientCoalescCount, 0, sizeof(uint64_t) * (SKV_MAX_COALESCED_COMMANDS + 1) );
        gClientCoalescSum = 0;
      }

#endif

      BegLogLine( SKV_CLIENT_REQUEST_COALESCING_LOG )
        << "PostRequest(): EP: " << (void*)mEP
        << " mSegsCount: " << mSendSegsCount
        << " inFlight: " << mInFlightWriteCount
        << EndLogLine;

      status = it_post_rdma_write( mEP,
                                   &(mQueuedSegments[ mCurrentServerRecvSlot ]),
                                   mSendSegsCount,
                                   *ITCookie,
                                   dto_write_flags,
                                   dest_recv_slot_addr,
                                   mServerCommandMem.GetRMRContext() );

      // if we don't use completion events, we need to signal that we are "ready"
      if( dto_write_flags != 0 )
        for( int n=mSendSegsCount-1; n>=0; n-- )
        {
          mQueuedCommands[ mCurrentServerRecvSlot + n ].mCCB->SetSendWasFirst();
        }

      mCurrentServerRecvSlot = (mCurrentServerRecvSlot + mSendSegsCount) % SKV_SERVER_COMMAND_SLOTS;

      if( status == IT_SUCCESS )
      {
        mQueuedCommands[ active_srv_slot ].mCCB->SetRequestWithWrite();
        mInFlightWriteCount++;
        mOutStandingRequests += mSendSegsCount;
        mSendSegsCount = 0;
      }
      else
        return SKV_ERRNO_IT_POST_SEND_FAILED;
      return SKV_SUCCESS;
    }

    // create a queue of posts that has the sendseg, CCB and seq number
    // we'll need the CCB in case a delayed write requires a signal - the cookie needs the CCB for completion processing
    skv_status_t PostOrStoreRdmaWrite( it_lmr_triplet_t aSendSeg, skv_client_ccb_t *aCCB, uint64_t aSeqNo )
    {
      skv_status_t status = SKV_SUCCESS;

      // requests cannot be combined when server buffer would wrap around
      int base_srv_slot = mCurrentServerRecvSlot ;

      AssertLogLine( base_srv_slot + mSendSegsCount < SKV_SERVER_COMMAND_SLOTS )
        << "skv_client_server_conn_t:: protocol failure. server-command-slot and send segment counter out of range. "
        << " base_slot: " << base_srv_slot
        << " segm. count: " << mSendSegsCount
        << EndLogLine;

      // store the request metadata and send segments for potentially deferred post_rdma_write
      mQueuedCommands[ base_srv_slot + mSendSegsCount ].mCCB = aCCB;
      mQueuedCommands[ base_srv_slot + mSendSegsCount ].mSeqNo = aSeqNo;
      mQueuedSegments[ base_srv_slot + mSendSegsCount ] = aSendSeg;
      mSendSegsCount++;

      /* post an actual rdma request if:
       * - the pipeline isn't full yet
       * - the max number of send-segments is reached
       * - we hit the last server mem slot - the remote data placement can't wrap the circular buffer
       */
#ifdef DISABLE_CLIENT_COALESCING
      bool needpost = true;
#else
      bool needpost = ( DynamicCoalescingCondition() ||
          ( mSendSegsCount >= SKV_MAX_COALESCED_COMMANDS ) ||
          (( base_srv_slot + mSendSegsCount ) == SKV_SERVER_COMMAND_SLOTS ) );
#endif

      BegLogLine( SKV_CLIENT_REQUEST_COALESCING_LOG )
        << "PostOrStoreRdmaWrite: EP: " << (void*)this
        << " needpost: " << needpost
        << " inFlight: " << mInFlightWriteCount
        << " mSegsCount: " << mSendSegsCount
        << " mUnretired: " << mUnretiredRecvCount
        << " base_srv_slot: " << base_srv_slot
        << " aSeqNo: " << aSeqNo
        << EndLogLine;

#if ( SKV_CTRLMSG_DATA_LOG!=0 )
      HexDump CtrlMsgData( aSendSeg.addr.abs, SKV_CONTROL_MESSAGE_SIZE );
      BegLogLine( 1 )
        << "OUTMSG:@" << aSendSeg.addr.abs
        << " Data: " << CtrlMsgData
        << EndLogLine;
#endif
      if( needpost )
      {
        status = PostRequest( );
      }
      return status;
    }
    void
    Init(it_pz_handle_t aPZ_Hdl)
      {

        bzero( mCommandSlotBusyFlags, sizeof(int) * SKV_MAX_UNRETIRED_CMDS );

        mState = SKV_CLIENT_CONN_DISCONNECTED;
        mUnretiredRecvCount = 0;
        mOutStandingRequests = 0;

        mSeqNo = 0;
        mServerIsLocal = false;

        mOverflowCommands = new skv_command_overflow_queue_t;

        StrongAssertLogLine( mOverflowCommands != NULL )
          << "skv_client_server_conn_t::Init():: "
          << " mOverflowCommands != NULL"
          << " sizeof( skv_command_overflow_queue_t ): "
          << sizeof( skv_command_overflow_queue_t )
          << EndLogLine;

        mOrdinalStack = new skv_command_ordinal_stack_t;
        StrongAssertLogLine( mOrdinalStack != NULL )
          << "skv_client_server_conn_t::Init():: "
          << " mOrdinalStack != NULL"
          << " sizeof( skv_command_ordinal_stack_t ): "
          << sizeof( skv_command_ordinal_stack_t )
          << EndLogLine;

        for( int i = 0; i < SKV_MAX_UNRETIRED_CMDS; i++ )
          {
            mOrdinalStack->push( i );
          }

        mServerCommandMem.Init( 0, 0, 0 );
        mCurrentServerRecvSlot = 0;

        // Create contigous area to place responses
        if( posix_memalign( (void**)(&mResponseSlotBuffer), ALIGNMENT, SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE ) )
          StrongAssertLogLine( 1 )
            << "Unable to allocate aligned memory for response buffers"
            << EndLogLine;
        memset( mResponseSlotBuffer, 0, SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE );
        for( int i = 0; i < SKV_SERVER_COMMAND_SLOTS; i++ )
        {
          skv_server_to_client_cmd_hdr_t* resp = (skv_server_to_client_cmd_hdr_t*) (&mResponseSlotBuffer[ i * SKV_CONTROL_MESSAGE_SIZE ]);
          resp->Reset();
          ((skv_header_as_cmd_buffer_t*)resp)->SetCheckSum( SKV_CTRL_MSG_FLAG_EMPTY );
        }

        StrongAssertLogLine( mResponseSlotBuffer != NULL )
          << "skv_client_ccb_manager_if_t::Init(): ERROR:: "
          << " allocating response buffer"
          << EndLogLine;

        mCurrentResponseSlot = 0;

        it_mem_priv_t privs = (it_mem_priv_t) (IT_PRIV_LOCAL | IT_PRIV_REMOTE_WRITE);
        it_lmr_flag_t lmr_flags = IT_LMR_FLAG_SHARED;

        it_rmr_context_t RMR_Context;

        it_status_t istatus = it_lmr_create( aPZ_Hdl,
                                             mResponseSlotBuffer,
                                             NULL,
                                             SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE,
                                             IT_ADDR_MODE_ABSOLUTE,
                                             privs,
                                             lmr_flags,
                                             0,
                                             &mResponseLMRHandle,
                                             &RMR_Context );

        StrongAssertLogLine( istatus == IT_SUCCESS )
          << "skv_client_ccb_manager_if_t::Init(): ERROR:: after it_lmr_create( ResponseBuffer)"
          << " status: " << istatus
          << EndLogLine;

        // initializse RMR-data for later use
        mResponseRMR.Init( RMR_Context,
                           mResponseSlotBuffer,
                           SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE );

        mSendSegsCount = 0;
        memset( mQueuedCommands, 0, sizeof( skv_client_queued_command_rep_t ) * SKV_SERVER_COMMAND_SLOTS );
        memset( mQueuedSegments, 0, sizeof( it_lmr_triplet_t ) * SKV_SERVER_COMMAND_SLOTS );
        mInFlightWriteCount = 0;
      }

    void
    Finalize()
      {
        if( mOverflowCommands != NULL )
          {
            delete mOverflowCommands;
            mOverflowCommands = NULL;
          }

        if( mOrdinalStack != NULL )
          {
            delete mOrdinalStack;
            mOrdinalStack = NULL;
          }
        it_lmr_free( mResponseLMRHandle );
        if( mResponseSlotBuffer != NULL )
          {
            free( mResponseSlotBuffer );
            mResponseSlotBuffer = NULL;
          }
      }
  };

#endif // __SKV_CLIENT_SERVER_CONN_HPP__
