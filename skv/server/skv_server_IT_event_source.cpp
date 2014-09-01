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

#include <FxLogger.hpp>
#include <common/skv_types.hpp>
#include <common/skv_client_server_headers.hpp>
#include <server/skv_server_types.hpp>
#include <server/skv_server_network_event_manager.hpp>

#include <server/skv_server_event_source.hpp>
#include <server/skv_server_IT_event_source.hpp>


int
skv_server_IT_event_source_t::
FetchEvents( int aMaxEventCount )
{
  // just forward the request to the event manager since the network is an "external source"
  return mEventManager->WaitForEvents( aMaxEventCount );
}

skv_status_t
skv_server_IT_event_source_t::
PrepareEvents( skv_server_event_t *aEvents, int *aEventCount )
{
  skv_status_t status = SKV_SUCCESS;

  int skvEventCount = 0;
  int itEventCount   = *aEventCount;

  it_event_t *AEVD_Events = mEventManager->GetEventStorage();   // we have to parse the list of events, so we get the list here

  int rank = mEventManager->GetRank();

  for( int i = 0; i < itEventCount; i++ )
  {
    it_event_t* itEvent = &AEVD_Events[i];

    skv_server_event_t* currentEvent = &aEvents[i];

    it_ep_handle_t EP = ((it_affiliated_event_t *) (itEvent))->cause.ep;
    skvEventCount++;

    switch( itEvent->event_number )
    {
      default:
      {
        StrongAssertLogLine( 0 )
          << "skv_server_t::GetITEvent:: ERROR:: "
          << " Event not recognized."
          << " itEvent->event_number: " << itEvent->event_number
          << EndLogLine;

        break;
      }

        // Unaffiliated events
      case IT_ASYNC_UNAFF_SPIGOT_ONLINE:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_UNAFF_SPIGOT_ONLINE,
                            (it_ep_handle_t) NULL );
        break;

      }
      case IT_ASYNC_UNAFF_SPIGOT_OFFLINE:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_UNAFF_SPIGOT_OFFLINE,
                            (it_ep_handle_t) NULL );

        break;
      }

        // Affiliated events
      case IT_ASYNC_AFF_EP_SEVD_FULL_ERROR:
      {
            currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_SEVD_FULL_ERROR,
                                EP );

        break;
      }
      case IT_ASYNC_AFF_EP_FAILURE:
      {
            currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_FAILURE,
                                EP );

        break;
      }
      case IT_ASYNC_AFF_EP_BAD_TRANSPORT_OPCODE:
      {
            currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_BAD_TRANSPORT_OPCODE,
                                EP );

        break;
      }
      case IT_ASYNC_AFF_EP_REQ_DROPPED:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_REQ_DROPPED,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_RDMAW_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_RDMAW_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_RDMAW_CORRUPT_DATA:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_RDMAW_CORRUPT_DATA,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_RDMAR_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_RDMAR_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_LOCAL_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_LOCAL_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_L_RECV_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_RECV_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_L_IRRQ_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_IRRQ_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_L_TRANSPORT_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_TRANSPORT_ERROR,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_L_LLP_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_LLP_ERROR,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_R_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_ERROR,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_R_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_R_RECV_ACCESS_VIOLATION:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_RECV_ACCESS_VIOLATION,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_R_RECV_LENGTH_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_RECV_LENGTH_ERROR,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_SOFT_HI_WATERMARK:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_SOFT_HI_WATERMARK,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_EP_SRQ_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_SRQ_ERROR,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_SRQ_LOW_WATERMARK:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_SRQ_LOW_WATERMARK,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_SRQ_CATASTROPHIC:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_SRQ_CATASTROPHIC,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_SEVD_FULL_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_SEVD_FULL_ERROR,
                            EP );

        break;
      }
      case IT_ASYNC_AFF_SEVD_OP_ERROR:
      {
        currentEvent->Init( SKV_SERVER_EVENT_IT_ASYNC_AFF_SEVD_OP_ERROR,
                            EP );

        break;
      }
      // Conn Request
      case IT_CM_REQ_CONN_REQUEST_EVENT:
      {
        it_conn_request_event_t * ConnReqEvent = (it_conn_request_event_t *) (itEvent);

        it_cn_est_identifier_t ConnEstId = ConnReqEvent->cn_est_id;

        skv_rmr_triplet_t ResponseRMR;
        ResponseRMR.Init( ( it_rmr_context_t )(-1),
                          ( char* ) (-1),
                          -1 );

        int ClientOrdInGroup = -1;
        int ServerRank = -1;
        int ClientGroupId = -1;
        if( ConnReqEvent->private_data_present )
        {
          ClientOrdInGroup = ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 0]) );
          ServerRank       = ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 1]) );
          ClientGroupId    = ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 2]) );

          StrongAssertLogLine( ServerRank == rank )
            << "skv_server_t::GetITEvent(): ERROR: "
            << " ServerRank: " << ServerRank
            << " MyRank: " << rank
            << EndLogLine;

          it_rmr_triplet_t rmr;
          rmr.length   = (it_length_t)        ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 3]) );
          rmr.rmr      = (it_rmr_handle_t)  be64toh( *(uint64_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 4]) );
          rmr.addr.abs = (void*)            be64toh( *(uint64_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 4 + sizeof(uint64_t)]) );

          ResponseRMR = rmr;

        }

        BegLogLine( SKV_GET_IT_EVENT_LOG )
          << "skv_server_t::GetITEvent(): IT_CM_REQ_CONN_REQUEST_EVENT"
          << " ClientOrdInGroup: " << ClientOrdInGroup
          << " ClientGroupId: " << ClientGroupId
          << " ServerRank: " << ServerRank
          << " MyRank: " << rank
          << " RMR: " << ResponseRMR
          << EndLogLine;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_CMR_CONN_REQUEST,
                            ConnEstId,
                            ClientOrdInGroup,
                            ClientGroupId,
                            &ResponseRMR );

        break;
      }

      // Connection management events
      case IT_CM_MSG_CONN_ACCEPT_ARRIVAL_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_ACCEPT_ARRIVAL,
                            EP );

        break;
      }
      case IT_CM_MSG_CONN_ESTABLISHED_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_ESTABLISHED,
                            EP );

        break;
      }
      case IT_CM_MSG_CONN_DISCONNECT_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_DISCONNECT,
                            EP );

        break;
      }
      case IT_CM_MSG_CONN_PEER_REJECT_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_PEER_REJECT,
                            EP );

        break;
      }
      case IT_CM_MSG_CONN_BROKEN_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_BROKEN,
                            EP );

        break;
      }

      // DTO Events
      case IT_DTO_RDMA_WRITE_CMPL_EVENT:
      {
        it_dto_cmpl_event_t* DTO_Event = (it_dto_cmpl_event_t *) itEvent;

        if( DTO_Event->dto_status == IT_DTO_ERR_FLUSHED )
        {
          BegLogLine( SKV_GET_IT_EVENT_LOG )
            << "skv_server_t::GetITEvent:: Sq DTO Flushed: "
            << " EP: " << (void *) DTO_Event->ep
            << EndLogLine;

          skvEventCount--;
          status = SKV_ERRNO_NO_EVENT;
          break;
        }

        // For these events the command id is in the cookie.
        skv_server_rdma_write_cmpl_cookie_t* CookiePtr =
              (skv_server_rdma_write_cmpl_cookie_t *) &(DTO_Event->cookie);

        BegLogLine( SKV_GET_IT_EVENT_LOG )
          << "skv_server_t::GetITEvent(): "
          << " Cookie->GetFunc(): " << (void *) CookiePtr->GetFunc()
          << " Cookie->GetContext(): " << (void *) CookiePtr->GetContext()
          << EndLogLine;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_WRITE_CMPL,
                            *CookiePtr );
        break;
      }
      case IT_DTO_RDMA_READ_CMPL_EVENT:
      {
        it_dto_cmpl_event_t* DTO_Event = (it_dto_cmpl_event_t *) itEvent;

        if( DTO_Event->dto_status == IT_DTO_ERR_FLUSHED )
        {
          BegLogLine( SKV_GET_IT_EVENT_LOG )
            << "skv_server_t::GetITEvent:: Sq DTO Flushed: "
            << " EP: " << (void *) DTO_Event->ep
            << EndLogLine;

          skvEventCount--;
          status = SKV_ERRNO_NO_EVENT;
          break;
        }

        skv_server_cookie_t* CookiePtr =
            (skv_server_cookie_t *) & (DTO_Event->cookie);

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_READ_CMPL,
                            *CookiePtr );
        break;
      }
      case IT_RMR_LINK_CMPL_EVENT:
      case IT_LMR_LINK_CMPL_EVENT:
      {
        it_dto_cmpl_event_t* DTO_Event = (it_dto_cmpl_event_t *) itEvent;

        if( DTO_Event->dto_status == IT_DTO_ERR_FLUSHED )
        {
          BegLogLine( SKV_GET_IT_EVENT_LOG )
            << "skv_server_t::GetITEvent:: Sq DTO Flushed: "
            << " EP: " << (void *) DTO_Event->ep
            << EndLogLine;

          skvEventCount--;
          status = SKV_ERRNO_NO_EVENT;
          break;
        }

        // For these events the command id is in the cookie.
        skv_server_cookie_t* CookiePtr = (skv_server_cookie_t *) & (DTO_Event->cookie);

        AssertLogLine( CookiePtr != NULL )
          << "skv_server_t::GetITEvent:: ERROR:: CookiePtr != NULL"
          << EndLogLine;

        currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_SQ_CMPL,
                            *CookiePtr );

        break;
      }
      case IT_DTO_SEND_CMPL_EVENT:
      {
        it_dto_cmpl_event_t* DTO_Event = (it_dto_cmpl_event_t *) itEvent;

        StrongAssertLogLine( 0 )
          << " SEND_EVENT: THIS SHOULD NO LONGER HAPPEN"
          << EndLogLine;

        break;
      }

      case IT_DTO_RC_RECV_CMPL_EVENT:
      {
        StrongAssertLogLine( 0 )
          << "RECV_COMPLETION EVENT: These events should no longer happen"
          << EndLogLine;


        break;
      }
    }
  }

  *aEventCount = skvEventCount;

  if( *aEventCount > 0 )
    status = SKV_SUCCESS;

  return status;
}

