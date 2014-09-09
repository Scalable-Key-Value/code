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

#ifndef __SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_HPP__
#define __SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_HPP__

#ifndef SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG 
#define SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG  ( 0 | SKV_LOGGING_ALL  )
#endif

#define SKV_SERVER_ACTIVE_BCAST_DISPATCH_ERROR_RESP( ErrorCode )       \
  {                                                                     \
    skv_cmd_err_resp_t* ErrResp = (skv_cmd_err_resp_t *) Command->GetSendBuff(); \
                                                                        \
    ErrResp->mHdr.mEvent   = SKV_CLIENT_EVENT_ERROR;                   \
                                                                        \
    ErrResp->mStatus       = ErrorCode;                                 \
                                                                        \
    skv_status_t dstatus = aEPState->Dispatch( Command,                \
                                                aSeqNo,                 \
                                                aCommandOrdinal );      \
                                                                        \
    AssertLogLine( dstatus == SKV_SUCCESS )                            \
      << "skv_server_active_bcast_command_sm::Execute():: ERROR: "     \
      << " status: " << skv_status_to_string( dstatus )                \
      << EndLogLine;                                                    \
                                                                        \
    Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );                 \
    break;                                                              \
  }


class skv_server_active_bcast_command_sm
{
public:
  static skv_status_t
  Execute( skv_local_kv_t*              aLocalKV,
           skv_server_ep_state_t*       aEPState,
           int                          aCommandOrdinal,
           skv_server_event_t*          aEvent,
           int*                         aSeqNo,
           it_pz_handle_t               aPZHdl )
  {
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->mState;

    skv_server_event_type_t EventType = aEvent->mCmdEventType;

    BegLogLine( SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG )
      << "skv_server_active_bcast_command_sm::Execute():: Entering "
      << " Command: " << (void *) Command
      << " State: " << skv_server_command_state_to_string( State )
      << " Event: " << skv_server_event_type_to_string( EventType )
      << EndLogLine;

    skv_status_t status = SKV_SUCCESS;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL:
      {
        skv_c2s_active_broadcast_func_type_t FuncType =
            Command->mCommandState.mCommandActiveBcast.mFuncType;

        char* Buff = Command->mCommandState.mCommandActiveBcast.mBufferPtr;
        int BuffSize = Command->mCommandState.mCommandActiveBcast.mBufferSize;

        BegLogLine( SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG )
          << "skv_server_active_bcast_command_sm::Execute():: "
          << " BuffSize: " << BuffSize
          << " Buff: " << (void *) Buff
          << " FuncType: " << skv_c2s_active_broadcast_func_type_to_string( FuncType )
          << EndLogLine;

        switch( FuncType )
        {
          case SKV_ACTIVE_BCAST_DUMP_PERSISTENCE_IMAGE_FUNC_TYPE:
          {
            aLocalKV->DumpImage( Buff );

            BegLogLine( SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG )
              << "skv_server_active_bcast_command_sm::Execute():: "
              << " FuncType: " << skv_c2s_active_broadcast_func_type_to_string( FuncType )
              << " Persistent Image Path: " << Buff
              << EndLogLine;

            break;
          }
          case SKV_ACTIVE_BCAST_CREATE_CURSOR_FUNC_TYPE:
          {
            skv_server_cursor_hdl_t ServCursorHdl;

            skv_local_kv_cookie_t *cookie = &Command->mLocalKVCookie;
            cookie->Set( aCommandOrdinal, aEPState );
            skv_status_t cstatus = aLocalKV->CreateCursor( Buff,
                                                           BuffSize,
                                                           & ServCursorHdl,
                                                           cookie );

            skv_cmd_active_bcast_resp_t* Resp = (skv_cmd_active_bcast_resp_t *) Command->GetSendBuff();
            Resp->mHdr                = Command->mCommandState.mCommandActiveBcast.mHdr;
            Resp->mHdr.mEvent         = SKV_CLIENT_EVENT_CMD_COMPLETE;

            Resp->mServerHandle = (uint64_t) ServCursorHdl;

            Resp->mStatus = cstatus;

            aEPState->AddToAssociatedState( SKV_SERVER_FINALIZABLE_ASSOCIATED_EP_STATE_CREATE_CURSOR_TYPE,
                                            ServCursorHdl );

            BegLogLine( SKV_SERVER_ACTIVE_BCAST_COMMAND_SM_LOG )
              << "skv_server_active_bcast_command_sm::Execute():: "
              << " FuncType: " << skv_c2s_active_broadcast_func_type_to_string( FuncType )
              << " ServCursorHdl: " << (void *) ServCursorHdl
              << " cstatus: " << skv_status_to_string( cstatus )
              << EndLogLine;

            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_server_active_bcast_command_sm:: Execute():: ERROR: FuncType not recognized"
              << " FuncType: " << FuncType
              << EndLogLine;

            break;
          }
        }

        skv_status_t status = aEPState->Dispatch( Command,
                                                  aSeqNo,
                                                  aCommandOrdinal );

        AssertLogLine( status == SKV_SUCCESS )
          << "skv_server_active_bcast_command_sm::Execute():: ERROR: "
          << " status: " << status
          << EndLogLine;

        /*******************************************************************
         * Free buffer resources
         ******************************************************************/
        it_status_t itstatus = it_lmr_free( Command->mCommandState.mCommandActiveBcast.mBufferLMR );

        AssertLogLine( itstatus == IT_SUCCESS )
            << "skv_server_active_bcast_command_sm::Execute(): ERROR:: "
            << " itstatus: " << itstatus
            << EndLogLine;

        free( Buff );
        /******************************************************************/

        Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );

        break;
      }
      case SKV_SERVER_COMMAND_STATE_INIT:
        {
          switch( EventType )
          {
            case SKV_SERVER_EVENT_TYPE_ACTIVE_BCAST_CMD:
            {
              skv_cmd_active_bcast_req_t* Req = (skv_cmd_active_bcast_req_t *) Command->GetSendBuff();

              /*******************************************************************
               * LMR create a new memory region (Don't forget to it_lmr_free( ) )
               ******************************************************************/
              int BufferSize = Req->mBufferRep.GetLen();
              char* BufferData = (char *) malloc( BufferSize );
              if( BufferData == NULL )
                SKV_SERVER_ACTIVE_BCAST_DISPATCH_ERROR_RESP( SKV_ERRNO_OUT_OF_MEMORY );

              it_lmr_handle_t lmrHandle;
              it_rmr_context_t rmrHandle;

              it_mem_priv_t privs = (it_mem_priv_t) (IT_PRIV_LOCAL | IT_PRIV_REMOTE);
              it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

              it_status_t status = it_lmr_create( aPZHdl,
                                                  BufferData,
                                                  NULL,
                                                  BufferSize,
                                                  IT_ADDR_MODE_ABSOLUTE,
                                                  privs,
                                                  lmr_flags,
                                                  0,
                                                  &lmrHandle,
                                                  &rmrHandle );

              AssertLogLine( status == IT_SUCCESS )
                 << "skv_server_active_bcast_command_sm::Execute(): ERROR:: "
                 << " status: " << status
                 << EndLogLine;
              /******************************************************************/

              /*******************************************************************
               * Save local command state
               ******************************************************************/
              Command->mCommandState.mCommandActiveBcast.mHdr        = Req->mHdr;
              Command->mCommandState.mCommandActiveBcast.mBufferLMR  = lmrHandle;
              Command->mCommandState.mCommandActiveBcast.mFuncType   = Req->mFuncType;
              Command->mCommandState.mCommandActiveBcast.mBufferSize = BufferSize;
              Command->mCommandState.mCommandActiveBcast.mBufferPtr  = BufferData;
              /******************************************************************/

              /*******************************************************************
               * Issue an rdma read from the client
               ******************************************************************/
              skv_server_cookie_t Cookie;
              Cookie.Init( aEPState,
                           *aSeqNo,
                           aCommandOrdinal );

              it_dto_cookie_t* DtoCookie = (it_dto_cookie_t*) &Cookie;

              it_dto_flags_t dto_flags = (it_dto_flags_t) (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG);

              it_lmr_triplet_t Seg;
              Seg.length   = BufferSize;
              Seg.addr.abs = BufferData;
              Seg.lmr      = lmrHandle;

              it_status_t itstatus = it_post_rdma_read( aEPState->mEPHdl,
                                                        &Seg,
                                                        1,
                                                        *DtoCookie,
                                                        dto_flags,
                                                        (it_rdma_addr_t) Req->mBufferRep.GetAddr(),
                                                        Req->mBufferRep.GetRMRContext() );

              AssertLogLine( itstatus == IT_SUCCESS )
                << "skv_server_insert_command_sm::Execute():: ERROR: "
                << " itstatus: " << itstatus
                << EndLogLine;
              /******************************************************************/

              aEPState->ReplaceAndInitCommandBuffer( Command, aCommandOrdinal );

              Command->Transit( SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL );
              break;
            } // SKV_SERVER_EVENT_TYPE_ACTIVE_BCAST_CMD
            default:
            {
              StrongAssertLogLine( 0 )
                << "skv_server_active_bcast_command_sm:: Execute():: ERROR: Event not recognized"
                << " State: " << State
                << " EventType: " << EventType
                << EndLogLine;

              break;
            }
          }
          break;
        } // SKV_SERVER_COMMAND_STATE_INIT
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_active_bcast_command_sm:: Execute():: ERROR: State not recognized"
          << " State: " << State
          << EndLogLine;

        break;
      }
    }

    return status;
  }
};
#endif
