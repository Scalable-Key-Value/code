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

#include <queue>
#include <common/skv_array_stack.hpp>
#include <common/skv_client_server_headers.hpp>

#ifndef SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG
#define SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RESPONSE_POLLING_LOG
#define SKV_CLIENT_RESPONSE_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

typedef enum
  {
  SKV_CLIENT_CONN_DISCONNECTED = 1,
  SKV_CLIENT_CONN_CONNECTED
  } skv_client_conn_state_t;

#define SKV_MAX_UNRETIRED_CMDS ( SKV_MAX_COMMANDS_PER_EP )

struct skv_client_server_conn_t
  {
    skv_client_conn_state_t mState;
    it_ep_handle_t mEP;
    skv_rmr_triplet_t mServerCommandMem;
    int mCurrentServerRecvSlot;

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

        AssertLogLine( mUnretiredRecvCount >= 0 )
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

    /** \brief Get and advance the serverside slot address for the next command to write
     */
    uintptr_t GetNextServerRecvAddress()
      {
        uintptr_t address = mServerCommandMem.GetAddr();

        mCurrentServerRecvSlot = (mCurrentServerRecvSlot + 1) % SKV_SERVER_COMMAND_SLOTS;

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
    CheckForNewResponse()
      {
        int Index = mCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE;

        skv_server_to_client_cmd_hdr_t* NewResponse = (skv_server_to_client_cmd_hdr_t*) (&mResponseSlotBuffer[Index]);

        bool found = ((NewResponse->mCmdType != SKV_COMMAND_NONE) &&
                      (((char*) NewResponse)[SKV_CONTROL_MESSAGE_SIZE - 1] == NewResponse->CheckSum())
            );

        if( found )
          {

// #define HEXLOG( x )  (  (void*) (*((uint64_t*) &(x)) ) )
            // BegLogLine( 1 )
            //   << "responseBuf: " << HEXLOG(mResponseSlotBuffer[ Index ])
            //   << " " << HEXLOG(mResponseSlotBuffer[ Index + sizeof(void*) * 1])
            //   << " " << HEXLOG(mResponseSlotBuffer[ Index + sizeof(void*) * 2])
            //   << " " << HEXLOG(mResponseSlotBuffer[ Index + sizeof(void*) * 3])
            //   << " " << HEXLOG(mResponseSlotBuffer[ Index + sizeof(void*) * 4])
            //   << EndLogLine;

//         BegLogLine( SKV_CLIENT_RESPONSE_POLLING_LOG )
//           << "skv_client_server_conn_t::CheckForNewResponse()::"
//           << " EP: "        << ( void* )this
//           << " found rsp: " << skv_command_type_to_string( NewResponse->mCmdType )
//           << " in slot: "   << mCurrentResponseSlot
//           << " event: "     << skv_client_event_to_string( NewResponse->mEvent )
//           << " ChSum: "     << (int)( ((char*)NewResponse)[SKV_CONTROL_MESSAGE_SIZE-1] )
//           << EndLogLine;

            return NewResponse;
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

        OldResponse[SKV_CONTROL_MESSAGE_SIZE - 1] = SKV_CTRL_MSG_FLAG_EMPTY;

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

    skv_server_to_client_cmd_hdr_t*
    GetCurrentResponseAddress()
      {
        return (skv_server_to_client_cmd_hdr_t*) (&mResponseSlotBuffer[mCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE]);
      }

    void
    Init(it_pz_handle_t aPZ_Hdl)
      {

        bzero( mCommandSlotBusyFlags, sizeof(int) * SKV_MAX_UNRETIRED_CMDS );

        mState = SKV_CLIENT_CONN_DISCONNECTED;
        mUnretiredRecvCount = 0;

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
        mCurrentServerRecvSlot = -1;

        // Create contigous area to place responses
        mResponseSlotBuffer = (char*) malloc( SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE );
        memset( mResponseSlotBuffer, 0, SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE );

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
