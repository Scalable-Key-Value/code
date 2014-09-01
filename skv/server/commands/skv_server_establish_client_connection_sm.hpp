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

#ifndef __SKV_SERVER_ESTABLISH_CLIENT_CONNECTION_FLOW_HPP__
#define __SKV_SERVER_ESTABLISH_CLIENT_CONNECTION_FLOW_HPP__

#ifndef SKV_SERVER_CLIENT_CONN_EST_LOG
#define SKV_SERVER_CLIENT_CONN_EST_LOG      ( 0 )
#endif

#include <common/skv_types.hpp>

class skv_server_establish_client_connection_sm
{
public:

  static skv_status_t
  Execute( skv_server_ep_state_t  *aEPState,
           int                     aCommandOrdinal,
           skv_server_event_t     *aEvent,
           int                    *aSeqNo )
  {
    skv_server_ccb_t* Command = aEPState->GetCommandForOrdinal( aCommandOrdinal );

    skv_server_command_state_t State = Command->mState;

    skv_server_event_type_t EventType = aEvent->mEventType;

    BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
      << "skv_establish_client_connection_sm::Execute():: Entering "
      << " State: " << skv_server_command_state_to_string( State )
      << " EventType: " << skv_server_event_type_to_string( EventType )
      << " aCommandOrdinal: " << aCommandOrdinal
      << " Command: " << (void *) Command
      << EndLogLine;

    switch( State )
    {
      case SKV_SERVER_COMMAND_STATE_CONN_EST:
      {
        switch( EventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_DISCONNECT:
          {
            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_establish_client_connection_sm::Execute():: "
              << " Disconnecting "
              << " EP: " << (void *) aEPState->mEPHdl
              << EndLogLine;

            // aEPState->Finalize();

            Command->Transit( SKV_SERVER_COMMAND_STATE_INIT );
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_establish_client_connection_op:: Execute():: ERROR: State not recognized"
              << " EventType: " << aEvent->mEventType
              << EndLogLine;

            break;
          }
        }

        break;
      }
      case SKV_SERVER_COMMAND_STATE_CONN_EST_PENDING:
      {
        switch( aEvent->mEventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_ESTABLISHED:
          {
            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_establish_client_connection_sm::Execute():: "
              << " Connection established on "
              << " EP: " << (void *) aEPState->mEPHdl
              << EndLogLine;

            Command->Transit( SKV_SERVER_COMMAND_STATE_CONN_EST );
            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_establish_client_connection_op:: Execute():: ERROR: State not recognized"
              << " EventType: " << aEvent->mEventType
              << EndLogLine;

            break;
          }
        }
        break;
      }
      case SKV_SERVER_COMMAND_STATE_INIT:
      {
        switch( aEvent->mEventType )
        {
          case SKV_SERVER_EVENT_TYPE_IT_CMR_CONN_REQUEST:
          {
            /*
             * IT-API mandates that at least one Receive be posted
             * prior to calling it_ep_accept().
             */

            it_cn_est_identifier_t conn_est_id       = aEvent->mEventMetadata.mConnFinder.mConnEstId;
            int                    ClientOrdInGroup  = aEvent->mEventMetadata.mConnFinder.mClientOrdInGroup;
            skv_client_group_id_t  ClientGroupId     = aEvent->mEventMetadata.mConnFinder.mClientGroupId;
            it_rmr_triplet_t      *ClientResponseRMR = &aEvent->mEventMetadata.mConnFinder.mResponseRMR;

            it_status_t status = itx_bind_ep_to_device( aEPState->mEPHdl, conn_est_id );
            StrongAssertLogLine( status == IT_SUCCESS )
              << "skv_establish_client_connection_op:: Execute():: "
              << "ERROR: after itx_bind_ep_to_device: "
              << " status: " <<  status
              << EndLogLine;

            // aEPState->PostRecvForAllCommands( aSeqNo );
            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_establish_client_connection_op:: Execute():: registering RMR context with EP"
              << EndLogLine;

            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_establish_client_connection_op:: Execute():: created RMR context"
              << " status: " << (int)status
              << EndLogLine;

            aEPState->SetClientInfo( ClientOrdInGroup,
                                     ClientGroupId,
                                     ClientResponseRMR );

            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_server_command_state_conn_est::Execute(): "
              << EndLogLine;

            it_lmr_triplet_t local_triplet;
            local_triplet.lmr      = aEPState->mCommandBuffLMR;
            local_triplet.addr.abs = ( void* ) aEPState->mCommandBuffRMR.GetAddr();
            local_triplet.length   = aEPState->mCommandBuffRMR.GetLen();

            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_server_command_state_conn_est::Execute(): about to accept."
              << " ClientResponseRMR: " << aEPState->mClientResponseRMR
              << " CRS: " << aEPState->mCurrentCommandSlot
              << " addr: " << (void*) &aEPState->mCurrentCommandSlot
              << " lmr.lmr: [ " << (void*)local_triplet.lmr
              << "  " << (void*)local_triplet.addr.abs
              << "  " << (void*)local_triplet.length
              << " ] "
              << EndLogLine;

            status = itx_ep_accept_with_rmr( aEPState->mEPHdl,
                                             conn_est_id,
                                             &local_triplet,
                                             &aEPState->mCommandBuffRMR.GetRMRContext() );

            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << " after accept:: "
              << " EPState: " << (void*)aEPState
              << " CRS: " << aEPState->mCurrentCommandSlot
              << " addr: " << (void*) &aEPState->mCurrentCommandSlot
              << " lmr.lmr: [ " << (void*)local_triplet.lmr
              << "  " << (void*)local_triplet.addr.abs
              << "  " << (void*)local_triplet.length
              << " ] "
              << EndLogLine;

            StrongAssertLogLine( status == IT_SUCCESS )
              << "skv_establish_client_connection_op:: Execute():: "
              << "ERROR: after it_ep_accept(): "
              << " status: " <<  status
              << EndLogLine;

            BegLogLine( SKV_SERVER_CLIENT_CONN_EST_LOG )
              << "skv_establish_client_connection_sm::Execute():: "
              << " it_ep_accept() connection called."
              << " aEPState->mEPHdl: " << (void *) aEPState->mEPHdl
              << " ClientOrdInGroup: " << ClientOrdInGroup
              << " ClientGroupId: " << ClientGroupId
              << EndLogLine;

            Command->Transit( SKV_SERVER_COMMAND_STATE_CONN_EST_PENDING );

            break;
          }
          default:
          {
            StrongAssertLogLine( 0 )
              << "skv_establish_client_connection_op:: Execute():: ERROR: State not recognized"
              << " EventType: " << aEvent->mEventType
              << EndLogLine;

            break;
          }
        }

        break;
      }
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_establish_client_connection_op:: Execute():: ERROR: State not recognized"
          << " CommandState: " << Command->mState
          << EndLogLine;

        break;
      }
    }

    return SKV_SUCCESS;
  }
};
#endif
