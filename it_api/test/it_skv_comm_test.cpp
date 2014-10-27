/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * it_skv_comm_test.cpp
 *
 *  Created on: May 19, 2014
 *      Author: lschneid
 */

#include <unistd.h>
#include <netdb.h>      /* struct hostent */
#include <iostream>

#include <FxLogger.hpp>

#define VP_NAME "vp_softrdma"
#define ITAPI_ENABLE_V21_BINDINGS
//#define IT_API_COMM_DEVICE "roq0"
#include <it_api.h>

#define SKV_MAX_COMMANDS_PER_EP                     ( 16 )
#define SKV_SERVER_SENDQUEUE_SIZE                   ( SKV_MAX_COMMANDS_PER_EP * 2 )
#define SKV_SERVER_PENDING_EVENTS_PER_EP            ( SKV_MAX_COMMANDS_PER_EP + 4 )
#define SKV_SERVER_COMMAND_SLOTS                    ( SKV_MAX_COMMANDS_PER_EP * 2 )
#define SKV_EVD_SEVD_QUEUE_SIZE                     ( SKV_MAX_COMMANDS_PER_EP * 4 )
#define SKV_SERVER_AEVD_EVENTS_MAX_COUNT            ( 128 )

#define BASE_PORT ( 12345 )
#define SKV_MAX_SERVER_PER_NODE ( 128 )

struct skv_rmr_triplet_t
{
  it_rmr_context_t         mRMR_Context;
  uint64_t                 mRMR_Addr;
  it_length_t              mRMR_Len;

  void
  Init( it_rmr_context_t aRmr, char* aAddr, int aLen )
  {
    mRMR_Context  = aRmr;
    mRMR_Addr     = (uint64_t) ((uintptr_t) aAddr);
    mRMR_Len      = aLen;
  }

  it_rmr_context_t&
  GetRMRContext()
  {
    return mRMR_Context;
  }
  skv_rmr_triplet_t&
  operator=( const it_rmr_triplet_t &in )
  {
    mRMR_Context  = (it_rmr_context_t)(in.rmr);
    mRMR_Addr     = (uint64_t)(in.addr.abs); // we use abs address for assignment,
                                 // however, the content shouldn't
                                 // vary if relative addressing is
                                 // used (just make sure that
                                 // mRMR_Addr type matches the largest
                                 // element of it_rmr_triplet_t.addr)
    mRMR_Len      = in.length;
    return *this;
  }
};

struct skv_client_server_conn_t
{
  it_ep_handle_t mEP;
  skv_rmr_triplet_t mServerCommandMem;
  bool mServerIsLocal;
};

struct skv_server_addr_t
{
  char mName[ 256 ];
  int  mPort;
};



it_ep_handle_t
CreateServerEP( it_pz_handle_t mPZ_Hdl,
                it_evd_handle_t mEvd_Sq_Hdl,
                it_evd_handle_t mEvd_Rq_Hdl,
                it_evd_handle_t mEvd_Cmm_Hdl )
{
  it_ep_attributes_t         ep_attr;
  it_ep_rc_creation_flags_t  ep_flags;

  ep_flags = IT_EP_NO_FLAG;

  ep_attr.max_dto_payload_size = 8192;
  ep_attr.max_request_dtos     = SKV_EVD_SEVD_QUEUE_SIZE;
  ep_attr.max_recv_dtos        = SKV_EVD_SEVD_QUEUE_SIZE;
  ep_attr.max_send_segments    = 4;
  ep_attr.max_recv_segments    = 4;

  ep_attr.srv.rc.rdma_read_enable        = IT_TRUE;
  ep_attr.srv.rc.rdma_write_enable       = IT_TRUE;
  ep_attr.srv.rc.max_rdma_read_segments  = 4;
  //ep_attr.srv.rc.max_rdma_write_segments = 4 * MULT_FACTOR_2;
  //ep_attr.srv.rc.max_rdma_write_segments = 24;
  ep_attr.srv.rc.max_rdma_write_segments = 4;

  ep_attr.srv.rc.rdma_read_ird           = SKV_EVD_SEVD_QUEUE_SIZE;
  ep_attr.srv.rc.rdma_read_ord           = SKV_EVD_SEVD_QUEUE_SIZE;

  ep_attr.srv.rc.srq                     = (it_srq_handle_t) IT_NULL_HANDLE;
  ep_attr.srv.rc.soft_hi_watermark       = 0;
  ep_attr.srv.rc.hard_hi_watermark       = 0;
  ep_attr.srv.rc.atomics_enable          = IT_FALSE;

  ep_attr.priv_ops_enable = IT_FALSE;

  it_ep_handle_t ep_hdl;

  it_status_t status = it_ep_rc_create( mPZ_Hdl,
                                        mEvd_Sq_Hdl,
                                        mEvd_Rq_Hdl,
                                        mEvd_Cmm_Hdl,
                                        ep_flags,
                                        & ep_attr,
                                        & ep_hdl );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_server_t::InitNewStateForEP()::ERROR after it_ep_rc_create() "
    << " status: " << status
    << EndLogLine;

  return ep_hdl;
}

int Server_WaitForConnection( it_pz_handle_t mPZ_Hdl,
                              it_evd_handle_t mEvd_Sq_Hdl,
                              it_evd_handle_t mEvd_Rq_Hdl,
                              it_evd_handle_t mEvd_Cmm_Hdl,
                              it_evd_handle_t aAevd_Hdl,
                              it_event_t* aAevdEvents,
                              int aMaxEventCount,
                              it_lmr_triplet_t aServerLMR,
                              it_rmr_context_t *aServerRMRCtx )
{
  AssertLogLine( aMaxEventCount <= SKV_SERVER_AEVD_EVENTS_MAX_COUNT )
    << "ERROR: "
    << " aMaxEventCount: " << aMaxEventCount
    << " SKV_SERVER_AEVD_EVENTS_MAX_COUNT: " << SKV_SERVER_AEVD_EVENTS_MAX_COUNT
    << EndLogLine;

  size_t itEventCount = 0;

  it_status_t istatus;
  BegLogLine(1)
    << "Waiting for events"
    << EndLogLine ;
  while( itEventCount <= 0 )
  {
    istatus = itx_aevd_wait( aAevd_Hdl,
                             IT_TIMEOUT_INFINITE,
                             aMaxEventCount,   // SKV_SERVER_AEVD_EVENTS_MAX_COUNT,
                             aAevdEvents,
                             &itEventCount );
    std::cout << "." << itEventCount;
    std::flush( std::cout );
  }
  std::cout << std::endl;
  BegLogLine(1)
    << "Received " << itEventCount << " events"
    << EndLogLine ;

  StrongAssertLogLine( istatus == IT_SUCCESS )
    << "ERROR: "
    << " istatus: " << istatus
    << EndLogLine;

  for( int i=0; i<itEventCount; i++ )
  {
    it_event_t* itEvent = &aAevdEvents[ i ];

    it_ep_handle_t EP = ((it_affiliated_event_t *) (itEvent))->cause.ep;

    BegLogLine(1)
      << "event_number=" << itEvent->event_number
      << EndLogLine ;
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

      // Conn Request
      case IT_CM_REQ_CONN_REQUEST_EVENT:
      {
        it_conn_request_event_t * ConnReqEvent = (it_conn_request_event_t *) (itEvent);

        it_cn_est_identifier_t ConnEstId = ConnReqEvent->cn_est_id;
        BegLogLine(1)
          << "ConnReqEvent=" << ConnReqEvent
          << " ConnEstId=" << ConnEstId
          << EndLogLine ;
//        StrongAssertLogLine(ConnEstId != 0)
//          << "ConnEstId is 0"
//          << EndLogLine ;

        it_ep_handle_t ServerEP = CreateServerEP( mPZ_Hdl, mEvd_Sq_Hdl, mEvd_Rq_Hdl, mEvd_Cmm_Hdl );


        skv_rmr_triplet_t ResponseRMR;
        ResponseRMR.Init( ( it_rmr_context_t )(-1),
                          ( char* ) (-1),
                          -1 );

        int ClientOrdInGroup = -1;
        int ServerRank = -1;
        int ClientGroupId = -1;
        BegLogLine(1)
          << "private_data_present=" << ConnReqEvent->private_data_present
          << EndLogLine ;
        if( ConnReqEvent->private_data_present )
        {
          ClientOrdInGroup = ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 0]) );
          ServerRank       = ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 1]) );
          ClientGroupId    = ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 2]) );

          it_rmr_triplet_t rmr;
          rmr.length   = (it_length_t)        ntohl( *(uint32_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 3]) );
          rmr.rmr      = (it_rmr_handle_t)  be64toh( *(uint64_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 4]) );
          rmr.addr.abs = (void*)            be64toh( *(uint64_t*)&( ((const char*)ConnReqEvent->private_data) [sizeof(uint32_t) * 4 + sizeof(uint64_t)]) );

          BegLogLine(1)
            << "ClientOrdInGroup=" << ClientOrdInGroup
            << " ServerRank=" << ServerRank
            << " ClientGroupId" << ClientGroupId
            << " rmr.length=" << rmr.length
            << " rmr.rmr=" << rmr.rmr
            << " rmr.addr.abs=" << rmr.addr.abs
            << EndLogLine ;
          ResponseRMR = rmr;
        }

        /*
         * IT-API mandates that at least one Receive be posted
         * prior to calling it_ep_accept().
         */

        it_status_t status = itx_bind_ep_to_device( ServerEP, ConnEstId );
        StrongAssertLogLine( status == IT_SUCCESS )
          << "skv_establish_client_connection_op:: Execute():: "
          << "ERROR: after itx_bind_ep_to_device: "
          << " status: " <<  status
          << EndLogLine;

        status = itx_ep_accept_with_rmr( ServerEP,
                                         ConnEstId,
                                         &aServerLMR,
                                         aServerRMRCtx );

        StrongAssertLogLine( status == IT_SUCCESS )
          << "skv_establish_client_connection_op:: Execute():: "
          << "ERROR: after it_ep_accept(): "
          << " status: " <<  status
          << EndLogLine;

        break;
      }

      // Connection management events
      case IT_CM_MSG_CONN_ACCEPT_ARRIVAL_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;
        break;
      }
      case IT_CM_MSG_CONN_ESTABLISHED_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;
        break;
      }
      case IT_CM_MSG_CONN_DISCONNECT_EVENT:
      {
        it_ep_handle_t EP = ((it_connection_event_t *) (itEvent))->ep;
        break;
      }
    }
  }
  return itEventCount;
}

int
it_skv_comm_server( int aPartitionSize )
{
  // Interface Adapter
  it_ia_handle_t                mIA_Hdl;

  // Protection Zone
  it_pz_handle_t                mPZ_Hdl;

  // Event Dispatchers
  it_evd_handle_t               mEvd_Unaff_Hdl;
  it_evd_handle_t               mEvd_Aff_Hdl;
  it_evd_handle_t               mEvd_Cmr_Hdl;
  it_evd_handle_t               mEvd_Cmm_Hdl;
  it_evd_handle_t               mEvd_Rq_Hdl;
  it_evd_handle_t               mEvd_Sq_Hdl;
  it_evd_handle_t               mEvd_Seq_Hdl;
  it_evd_handle_t               mAevd_Hdl;

  // Listen hdl
  it_listen_handle_t            mLP_Hdl;

  it_event_t*                   mAevdEvents;

  /************************************************************
   * Initialize the interface adapter
   ***********************************************************/
  it_status_t itstatus = it_ia_create( VP_NAME, 2, 0, & mIA_Hdl );
  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_ia_create() "
    << " VP_NAME: " << VP_NAME
    << " itstatus: " << itstatus
    << EndLogLine;

  /************************************************************
   * Initialize the protection zone
   ***********************************************************/
  itstatus = it_pz_create( mIA_Hdl, & mPZ_Hdl);
  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_pz_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  it_evd_flags_t evd_flags = (it_evd_flags_t) 0;
  itstatus = it_evd_create( mIA_Hdl,
                            IT_AEVD_NOTIFICATION_EVENT_STREAM,
                            evd_flags,
                            SKV_EVD_SEVD_QUEUE_SIZE,
                            1,
                            NULL,
                            & mAevd_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  int itEventSize = sizeof( it_event_t ) * SKV_SERVER_AEVD_EVENTS_MAX_COUNT;
  mAevdEvents = (it_event_t *) malloc( itEventSize );
  StrongAssertLogLine( mAevdEvents != NULL )
    << "ERROR: "
    << " itEventSize: " << itEventSize
    << EndLogLine;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_ASYNC_UNAFF_EVENT_STREAM,
                            evd_flags,
                            SKV_EVD_SEVD_QUEUE_SIZE,
                            1,
                            mAevd_Hdl,
                            & mEvd_Unaff_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_ASYNC_AFF_EVENT_STREAM,
                            evd_flags,
                            SKV_EVD_SEVD_QUEUE_SIZE,
                            1,
                            mAevd_Hdl,
                            & mEvd_Aff_Hdl,
                            NULL );

  BegLogLine( 1 )
    << "Initialised evd mEvd_Aff_Hdl=" << mEvd_Aff_Hdl
    << EndLogLine ;
  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;


  // The EVD size here should depend
  int cmr_sevd_queue_size = aPartitionSize;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_CM_REQ_EVENT_STREAM,
                            evd_flags,
                            SKV_EVD_SEVD_QUEUE_SIZE,
                            1,
                            mAevd_Hdl,
                            & mEvd_Cmr_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  int cmm_sevd_queue_size = aPartitionSize;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_CM_MSG_EVENT_STREAM,
                            evd_flags,
                            SKV_EVD_SEVD_QUEUE_SIZE,
                            1,
                            mAevd_Hdl,
                            & mEvd_Cmm_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  int rq_sevd_queue_size = 2 * SKV_MAX_COMMANDS_PER_EP * aPartitionSize;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_DTO_EVENT_STREAM,
                            evd_flags,
                            rq_sevd_queue_size,
                            1,
                            mAevd_Hdl,
                            & mEvd_Rq_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  int sq_sevd_queue_size = ( SKV_SERVER_SENDQUEUE_SIZE ) * aPartitionSize;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_DTO_EVENT_STREAM,
                            evd_flags,
                            sq_sevd_queue_size,
                            1,
                            mAevd_Hdl,
                            & mEvd_Sq_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_SOFTWARE_EVENT_STREAM,
                            evd_flags,
                            SKV_EVD_SEVD_QUEUE_SIZE,
                            1,
                            mAevd_Hdl,
                            & mEvd_Seq_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()"
    << " itstatus: " << itstatus
    << EndLogLine;


  /************************************************************
   * Initialize the listener
   ***********************************************************/
  it_listen_flags_t lp_flags = IT_LISTEN_CONN_QUAL_INPUT;

  /*
   * TODO: it_listen_create() should be extended to allow binding
   * not only to a local port, but also to a local address.
   */
  it_conn_qual_t             conn_qual;
  conn_qual.type = IT_IANA_LR_PORT;

  conn_qual.conn_qual.lr_port.remote = 0; /* Irrelevant (not "any port")*/

  int listen_attempts = 0;
  do
  {
    conn_qual.conn_qual.lr_port.local = htons( BASE_PORT + listen_attempts );

    itstatus = it_listen_create( mIA_Hdl,
                                 0 /* spigot_id */,
                                 mEvd_Cmr_Hdl,
                                 lp_flags,
                                 &conn_qual,
                                 &mLP_Hdl );

    listen_attempts++;
  }
  while( (itstatus != IT_SUCCESS) && (listen_attempts < SKV_MAX_SERVER_PER_NODE) );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init(): ERROR:: Failed in it_listen_create()"
    << " itstatus: " << itstatus
    << EndLogLine;

  sleep(1);

  char *servbuf = (char*)malloc( 1024 );
  it_lmr_handle_t serverlmrhdl;
  it_rmr_context_t rmrCtx;

  it_mem_priv_t   privs = (it_mem_priv_t) (IT_PRIV_REMOTE_WRITE | IT_PRIV_LOCAL);
  it_lmr_flag_t   lmr_flags = IT_LMR_FLAG_SHARED;

  itstatus = it_lmr_create( mPZ_Hdl,
                            servbuf,
                            NULL,
                            1024,
                            IT_ADDR_MODE_ABSOLUTE,
                            privs,
                            lmr_flags,
                            0,
                            &serverlmrhdl,
                            &rmrCtx );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_ccb_t::Init():: ERROR:: from it_lmr_create "
    << " status: " << itstatus
    << EndLogLine;

  it_lmr_triplet_t serverlmr;
  serverlmr.lmr = serverlmrhdl;
  serverlmr.addr.abs = servbuf;
  serverlmr.length = 1024;

  return Server_WaitForConnection( mPZ_Hdl,
                                   mEvd_Sq_Hdl,
                                   mEvd_Rq_Hdl,
                                   mEvd_Cmm_Hdl,
                                   mAevd_Hdl,
                                   mAevdEvents,
                                   SKV_SERVER_AEVD_EVENTS_MAX_COUNT,
                                   serverlmr,
                                   &rmrCtx );
}





/***********************************************************************************************************
 ***********************************************************************************************************
 *  CLIENT SIDE CODE ....
 ***********************************************************************************************************
 ***********************************************************************************************************
 */

it_status_t ClientConnectToServer( skv_server_addr_t& aServerAddr,
                                   skv_client_server_conn_t* aServerConn,
                                   it_pz_handle_t mPZ_Hdl,
                                   it_evd_handle_t mEvd_Sq_Hdl,
                                   it_evd_handle_t mEvd_Rq_Hdl,
                                   it_evd_handle_t mEvd_Cmm_Hdl,
                                   it_evd_handle_t mEvd_Aff_Hdl,
                                   it_evd_handle_t mEvd_Unaff_Hdl )
{
  BegLogLine( 1 )
    << "ClientConnectToServer():: Entering "
    << "aServerAddr.mName: " << aServerAddr.mName
    << "aServerAddr.mPort: " << aServerAddr.mPort
    << EndLogLine;

  it_ep_rc_creation_flags_t      ep_flags = (it_ep_rc_creation_flags_t) 0;

  struct hostent *remote_host;
  it_ep_attributes_t ep_attr;
  it_path_t path;
  it_conn_qual_t conn_qual;

  ep_attr.max_dto_payload_size = 8192;
  ep_attr.max_request_dtos = (SKV_MAX_COMMANDS_PER_EP + 5) * 8;
  ep_attr.max_recv_dtos = (SKV_MAX_COMMANDS_PER_EP + 5) * 8;
  ep_attr.max_send_segments = 4;
  ep_attr.max_recv_segments = 4;

  ep_attr.srv.rc.rdma_read_enable = IT_TRUE;
  ep_attr.srv.rc.rdma_write_enable = IT_TRUE;
// ep_attr.srv.rc.max_rdma_read_segments    = 4 * MULT_FACTOR_2;
  ep_attr.srv.rc.max_rdma_read_segments = 8;
  ep_attr.srv.rc.max_rdma_write_segments = 8;
  ep_attr.srv.rc.rdma_read_ird = SKV_MAX_COMMANDS_PER_EP * 2;
  ep_attr.srv.rc.rdma_read_ord = SKV_MAX_COMMANDS_PER_EP * 2;
  ep_attr.srv.rc.srq = (it_srq_handle_t) IT_NULL_HANDLE;
  ep_attr.srv.rc.soft_hi_watermark = 0;
  ep_attr.srv.rc.hard_hi_watermark = 0;
  ep_attr.srv.rc.atomics_enable = IT_FALSE;

  ep_attr.priv_ops_enable = IT_FALSE;

  StrongAssertLogLine( aServerConn != NULL )
    << "skv_client_conn_manager_if_t::ConnectToServer():: Error:: ( aServerConn != NULL ) "
    << EndLogLine;

  it_status_t status = it_ep_rc_create( mPZ_Hdl,
                                        mEvd_Sq_Hdl,
                                        mEvd_Rq_Hdl,
                                        mEvd_Cmm_Hdl,
                                        ep_flags,
                                        &ep_attr,
                                        &aServerConn->mEP );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::ConnectToServer():: ERROR:: after it_ep_rc_create()"
    << " status: " << status
    << EndLogLine;

  /*
   * Relationship between (it_path_t, it_conn_qual_t)
   * and                        struct sockaddr_in:
   *
   * IT-API separates L3 addresses and L4 port numbers into it_path_t and
   * it_conn_qual_t, respectively.
   * For IPv4, path.u.iwarp.raddr.ipv4 (of type struct in_addr) carries
   * the remote L3 address.
   *
   * In contrast, struct sockaddr_in has members
   * sin_family, sin_addr, and sin_port
   */

  path.u.iwarp.ip_vers = IT_IP_VERS_IPV4;

  if( strncmp( aServerAddr.mName, "127.0.0.1", 9 ) == 0 )
  {
    path.u.iwarp.laddr.ipv4.s_addr = INADDR_LOOPBACK;
    aServerConn->mServerIsLocal = true;
  }
  else
  {
    path.u.iwarp.laddr.ipv4.s_addr = INADDR_ANY;
    aServerConn->mServerIsLocal = false;
  }

  remote_host = gethostbyname( aServerAddr.mName );

  StrongAssertLogLine( remote_host != NULL )
    << "skv_client_conn_manager_if_t::ConnectToServer():: ERROR:: after gethostbyname() for: "
    << " aServerAddr.mName: " << aServerAddr.mName
    << EndLogLine;

  StrongAssertLogLine( remote_host->h_addr_list[0] != NULL )
    << "skv_client_conn_manager_if_t::ConnectToServer():: ERROR:: gethostbyname() has no address for: "
    << " aServerAddr.mName: " << aServerAddr.mName
    << EndLogLine;

  char **address = remote_host->h_addr_list;
  int i = 0;
  while( address[i] != NULL )
  {
    i++;
  }

  std::cout << "found " << i << " entries for hostname." << std::endl;

  /* Option 2 - TCP: */
  path.u.iwarp.raddr.ipv4.s_addr = ((struct in_addr*) (remote_host->h_addr_list[0]))->s_addr;
  conn_qual.type = IT_IANA_LR_PORT;
  conn_qual.conn_qual.lr_port.local = 0; /* Any local port */
//conn_qual.conn_qual.lr_port.local = htons(12345);

  conn_qual.conn_qual.lr_port.remote = htons( aServerAddr.mPort );

  status = (it_status_t) -1;
  size_t private_data_length = 3 * sizeof(uint32_t);
  unsigned char private_data[private_data_length];

  *(uint32_t*) &(private_data[sizeof(uint32_t) * 0]) = htonl( (uint32_t) 0x42 );
  *(uint32_t*) &(private_data[sizeof(uint32_t) * 1]) = htonl( (uint32_t) 0x43 );
  *(uint32_t*) &(private_data[sizeof(uint32_t) * 2]) = htonl( (uint32_t) 0x44 );

  char *clientbuf = (char*)malloc( 1024 );
  it_lmr_handle_t clientlmrhdl;
  it_rmr_context_t rmrCtx;

  it_mem_priv_t   privs = (it_mem_priv_t) (IT_PRIV_REMOTE_WRITE | IT_PRIV_LOCAL);
  it_lmr_flag_t   lmr_flags = IT_LMR_FLAG_SHARED;

  status = it_lmr_create( mPZ_Hdl,
                          clientbuf,
                          NULL,
                          1024,
                          IT_ADDR_MODE_ABSOLUTE,
                          privs,
                          lmr_flags,
                          0,
                          &clientlmrhdl,
                          &rmrCtx );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_server_ccb_t::Init():: ERROR:: from it_lmr_create "
    << " status: " << status
    << EndLogLine;

  std::cout << "created LMR/RMR..." << std::endl;

  it_lmr_triplet_t clientlmr;
  clientlmr.lmr = clientlmrhdl;
  clientlmr.addr.abs = clientbuf;
  clientlmr.length = 1024;

  status = IT_ERR_FAULT;
  int cm_retry_count = 5;
  while((  --cm_retry_count >= 0 )&&( status != IT_SUCCESS ))

  {
    std::cout << "connecting..." << cm_retry_count << std::endl;

    status = itx_ep_connect_with_rmr( aServerConn->mEP,
                                      &path,
                                      NULL,
                                      &conn_qual,
                                      IT_CONNECT_FLAG_TWO_WAY,
                                      private_data,
                                      private_data_length,
                                      &clientlmr,
                                      &rmrCtx
                                      );

    if( status != IT_SUCCESS )
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer():: "
        << " About to sleep "
        << " status: " << status
        << EndLogLine;

      sleep( 1 );
    }
  }
  std::cout << "connection call completed with status " << status << std::endl;
  std::cout << "Event checking..." << std::endl;
	sleep(2) ; 

  cm_retry_count = 5;

  // Check for errors
  while( --cm_retry_count )
  {
    it_event_t event_cmm;

    it_status_t status = it_evd_dequeue( mEvd_Cmm_Hdl,
                                         &event_cmm );

    BegLogLine(1)
      << "mEvd_Cmm_Hdl dequeue status=" << status
      << EndLogLine ;
    std::cout << "skv_comm_test:Client: ... " << cm_retry_count << std::endl;

    if( status == IT_SUCCESS )
    {
      if( event_cmm.event_number == IT_CM_MSG_CONN_ESTABLISHED_EVENT )
      {
        // Run the varification protocol.
        if( event_cmm.conn.private_data_present )
        {
          std::cout << "CM_ESTABLISHED Event received with private data" << std::endl;
          skv_rmr_triplet_t mResponseRMR;

          mResponseRMR = *((it_rmr_triplet_t*) (event_cmm.conn.private_data));
          // host-endian conversions. Data gets transferred in BE
          mResponseRMR.mRMR_Addr = be64toh( mResponseRMR.mRMR_Addr );
          mResponseRMR.mRMR_Len = be64toh( mResponseRMR.mRMR_Len );
          mResponseRMR.mRMR_Context = be64toh( mResponseRMR.mRMR_Context );
          BegLogLine(1)
            << "mRMR_Addr=" << (void *) mResponseRMR.mRMR_Addr
            << " mRMR_Len=" << (void *) mResponseRMR.mRMR_Len
            << " mRMR_Context=" << (void *) mResponseRMR.mRMR_Context
            << EndLogLine ;
        }
        else
        {
          std::cout << "CM_ESTABLISHED Event received without private data" << std::endl;

          StrongAssertLogLine( 0 )
            << "skv_client_conn_manager_if_t::ConnectToServer():: No Private Data present in connection established event. Cannot proceed"
            << EndLogLine;

          return status;
        }

        return status;
      }
      else
      {
        BegLogLine( 1 )
          << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
          << " event_number: " << event_cmm.event_number
          << EndLogLine;

        return status;
      }
    }
    else if( status != IT_ERR_QUEUE_EMPTY )
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer():: ERROR:: "
        << " getting connection established event failed"
        << " status=" << status
        << EndLogLine;

      return status;
    }

    it_event_t event_aff;

    status = it_evd_dequeue( mEvd_Aff_Hdl,
                             &event_aff );
    BegLogLine(1)
      << "mEvd_Aff_Hdl dequeue status=" << status
      << EndLogLine ;

    if(( status != IT_SUCCESS )&&( status != IT_ERR_QUEUE_EMPTY ))
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
        << " event_number: " << event_aff.event_number
        << EndLogLine;

      return status;
    }

    it_event_t event_unaff;

    status = it_evd_dequeue( mEvd_Unaff_Hdl,
                             &event_unaff );

    BegLogLine(1)
      << "mEvd_Unaff_Hdl dequeue status=" << status
      << EndLogLine ;

    if(( status != IT_SUCCESS )&&( status != IT_ERR_QUEUE_EMPTY ))
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
        << " event_number: " << event_unaff.event_number
        << EndLogLine;

      return status;
    }

    if ( status == IT_ERR_QUEUE_EMPTY )
    {
      sleep(1);
      std::cout << "skv_comm_test:Client: no event in queue. Retrying... " << cm_retry_count
        << std::endl;
      status = IT_SUCCESS;
    }

  }
  return IT_SUCCESS;
}

int
it_skv_comm_client( char* aServerName, char* aPortStr )
{
  it_ia_handle_t mIA_Hdl;
  it_pz_handle_t mPZ_Hdl;
  it_evd_handle_t mEvd_Unaff_Hdl, mEvd_Aff_Hdl, mEvd_Cmr_Hdl, mEvd_Cmm_Hdl, mEvd_Rq_Hdl, mEvd_Sq_Hdl;
  it_evd_handle_t mAevd_Hdl = (it_evd_handle_t) IT_NULL_HANDLE;

  /************************************************************
   * Initialize the interface adapter
   ***********************************************************/
  it_status_t status = it_ia_create( VP_NAME,
                                     2,
                                     0,
                                     & mIA_Hdl);

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_internal_t::Init::ERROR:: after it_ia_create()"
    << " status: " <<  status
    << EndLogLine;

  std::cout << "created IA..." << std::endl;

  /************************************************************
   * Initialize the protection zone
   ***********************************************************/
  status = it_pz_create(  mIA_Hdl,
                          &mPZ_Hdl );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_internal_t::Init::ERROR:: after it_pz_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created PZ..." << std::endl;

  it_evd_flags_t evd_flags = (it_evd_flags_t) 0;

  /************************************************************
   * Initialize the Event Dispatchers (evds)
   ***********************************************************/

  status = it_evd_create( mIA_Hdl,
                          IT_ASYNC_UNAFF_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t)IT_NULL_HANDLE,
                          & mEvd_Unaff_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created EVD(Unaff)..." << std::endl;

  status = it_evd_create( mIA_Hdl,
                          IT_ASYNC_AFF_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t)IT_NULL_HANDLE,
                          & mEvd_Aff_Hdl,
                          NULL );
  BegLogLine(1)
   << "Created evd mEvd_Aff_Hdl=" << mEvd_Aff_Hdl
   << EndLogLine ;

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created EVD(Aff)..." << std::endl;

  status = it_evd_create( mIA_Hdl,
                          IT_CM_REQ_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t)IT_NULL_HANDLE,
                          & mEvd_Cmr_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created EVD(Cmr)..." << std::endl;

  status = it_evd_create( mIA_Hdl,
                          IT_CM_MSG_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t)IT_NULL_HANDLE,
                          & mEvd_Cmm_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created EVD(Cmm)..." << std::endl;

  status = it_evd_create( mIA_Hdl,
                          IT_DTO_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t)IT_NULL_HANDLE,
                          & mEvd_Rq_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created EVD(Rq)..." << std::endl;

  status = it_evd_create( mIA_Hdl,
                          IT_DTO_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t)IT_NULL_HANDLE,
                          & mEvd_Sq_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init(): ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  std::cout << "created EVD(Sq)..." << std::endl;
  /***********************************************************/





#define MY_HOSTNAME_SIZE 128
  char MyHostname[ MY_HOSTNAME_SIZE ];
  bzero( MyHostname, MY_HOSTNAME_SIZE );
  gethostname( MyHostname, MY_HOSTNAME_SIZE );

  skv_client_server_conn_t mServerConn;

  skv_server_addr_t ServerAddr;

  strcpy( ServerAddr.mName, aServerName);
  ServerAddr.mPort = atoi( aPortStr );

  std::cout << "Connecting to host:" << ServerAddr.mName
    << ":" << ServerAddr.mPort
    << " (str:" << aPortStr << ")"
    << std::endl;

  status = ClientConnectToServer( ServerAddr,
                                  &mServerConn,
                                  mPZ_Hdl,
                                  mEvd_Sq_Hdl,
                                  mEvd_Rq_Hdl,
                                  mEvd_Cmm_Hdl,
                                  mEvd_Aff_Hdl,
                                  mEvd_Unaff_Hdl );

  if( status != IT_SUCCESS )
  {
    BegLogLine ( 1 )
      << "it_skv_comm_test "
      << " ERROR returned from ConnectToServer: " << status
      << EndLogLine;
  }
  else
  {
    BegLogLine( 1 )
      << "it_skv_comm_test ERROR: " << status
      << EndLogLine;
  }

  return (int)status;
}

int
main( int argc, char **argv)
{
  int rc = 0;
  char *SERVERNAME;
  char *PORTSTR;
  bool server = false;
  int op;

  FxLogger_Init( argv[ 0 ] );

  while ((op = getopt(argc, argv, "ha:cp:s")) != -1) {
          char *endp;
          switch(op) {
          default:
                  printf(" Invalid Options \n");
                  rc = -1;
          case 'h': {
                          printf("USAGE: random_read \n");
                          printf("  Arguments:\n");
                          printf("  -a <IPaddr>   : address to listen/connect\n");
                          printf("  -c            : run client mode (default: true)\n");
                          printf("  -h            : print help\n");
                          printf("  -p            : port number\n");
                          printf("  -s            : run server mode (default: false)\n");
                          printf("\n");
                          return rc;
                  }
          case 'a':
                  SERVERNAME = optarg;
                  break;
          case 'p':
                  PORTSTR = optarg;
                  break;
          case 'c':
                  server = false;
                  break;
          case 's':
                  server = true;
                  break;
          }
  }
  if( server )
  {
    std::cout << "Running server..." << std::endl;
    rc = it_skv_comm_server( 1 );
    BegLogLine(1)
     << "Server finished, rc=" << rc
     << EndLogLine ;
  }
  else
  {
    std::cout << "Running client..." << std::endl;
    rc = it_skv_comm_client( SERVERNAME, PORTSTR );
    BegLogLine(1)
     << "Client finished, rc=" << rc
     << EndLogLine ;
  }

  return rc;
}
