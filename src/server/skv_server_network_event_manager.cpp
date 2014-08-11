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
#include <common/skv_config.hpp>
#include <common/skv_types.hpp>
#include <common/skv_client_server_headers.hpp>
#include <server/skv_server_types.hpp>
#include <server/skv_server_cursor_manager_if.hpp>
#include <server/skv_server_network_event_manager.hpp>


#define VP_NAME                     "vp_softrdma"

#define SKV_SERVER_USE_AGGREGATE_EVD
//#define SKV_SERVER_USE_SINGLE_SEND_RECV_QUEUE


skv_status_t
skv_server_network_event_manager_if_t::
Init( int aPartitionSize, 
      int aRank )
{
  mMyRank = aRank;
  mPartitionSize = aPartitionSize;

  skv_configuration_t *SKVConfig = skv_configuration_t::GetSKVConfiguration();

  /************************************************************
   * Initialize the interface adapter
   ***********************************************************/
  it_status_t itstatus = it_ia_create( VP_NAME, 2, 0, & mIA_Hdl );
  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_ia_create() "
    << " VP_NAME: " << VP_NAME    
    << " itstatus: " << itstatus
    << EndLogLine;

  itx_init_tracing( "skv_server", aRank );
  /***********************************************************/

  /************************************************************
   * Initialize the protection zone
   ***********************************************************/
  itstatus = it_pz_create( mIA_Hdl, & mPZ_Hdl);
  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_pz_create()" 
    << " itstatus: " << itstatus
    << EndLogLine;
  /***********************************************************/

  /************************************************************
   * Initialize the Event Dispatchers (evds)
   ***********************************************************/
  mAevd_Hdl = (it_evd_handle_t) IT_NULL_HANDLE;

  it_evd_flags_t evd_flags = (it_evd_flags_t) 0;

#ifdef SKV_SERVER_USE_AGGREGATE_EVD
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
#endif

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

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()" 
    << " itstatus: " << itstatus
    << EndLogLine;


  // The EVD size here should depend
  int cmr_sevd_queue_size = aPartitionSize;

  itstatus = it_evd_create( mIA_Hdl,
                            IT_CM_REQ_EVENT_STREAM,
                            evd_flags,
                            cmr_sevd_queue_size,
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
                            cmm_sevd_queue_size,
                            1,
                            mAevd_Hdl,
                            & mEvd_Cmm_Hdl,
                            NULL );

  StrongAssertLogLine( itstatus == IT_SUCCESS )
    << "skv_server_t::Init():: ERROR:: Failed in it_evd_create()" 
    << " itstatus: " << itstatus
    << EndLogLine;

#ifdef SKV_SERVER_USE_SINGLE_SEND_RECV_QUEUE
  int rq_sevd_queue_size = 3 * SKV_MAX_COMMANDS_PER_EP * aPartitionSize;
#else
  int rq_sevd_queue_size = 2 * SKV_MAX_COMMANDS_PER_EP * aPartitionSize;
#endif

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

#ifdef SKV_SERVER_USE_SINGLE_SEND_RECV_QUEUE
  int sq_sevd_queue_size = rq_sevd_queue_size;
  mEvd_Sq_Hdl = mEvd_Rq_Hdl;
#else

  int sq_sevd_queue_size = ( SKV_SERVER_SENDQUEUE_SIZE ) * aPartitionSize;

  BegLogLine( SKV_SERVER_NETWORK_EVENT_MANAGER_LOG )
    << "skv_server_t::Init():: " 
    << " PartitionSize: " << aPartitionSize
    << " sq_sevd_queue_size: " << sq_sevd_queue_size
    << EndLogLine;

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

#endif

  BegLogLine( SKV_SERVER_NETWORK_EVENT_MANAGER_LOG )
    << "skv_server_t::Init():: " 
    << " mEvd_Unaff_Hdl: " << mEvd_Unaff_Hdl
    << " mEvd_Aff_Hdl: " << mEvd_Aff_Hdl
    << " mEvd_Cmr_Hdl: " << mEvd_Cmr_Hdl
    << " mEvd_Cmm_Hdl: " << mEvd_Cmm_Hdl
    << " mEvd_Rq_Hdl: " << mEvd_Rq_Hdl
    << " mEvd_Sq_Hdl: " << mEvd_Sq_Hdl
    << EndLogLine;
  /***********************************************************/

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

#ifdef SKV_RUNNING_LOCAL
  int listen_attempts = aRank;
#else
  int listen_attempts = 0;
#endif
  do
  {
    conn_qual.conn_qual.lr_port.local = htons( (uint16_t)SKVConfig->GetSKVServerPort() + listen_attempts );


    BegLogLine( SKV_SERVER_NETWORK_EVENT_MANAGER_LOG )
      << "skv_server_t::Init(): attempting to listen:"
      << " aRank: " << aRank
      << " conn_qual.conn_qual.lr_port.local: " << conn_qual.conn_qual.lr_port.local
      << " config.port: " << SKVConfig->GetSKVServerPort()
      << EndLogLine;

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

  // have to update the skv server in the config in case it has changed during listen attempts
  SKVConfig->SetSKVServerPort( ntohs(conn_qual.conn_qual.lr_port.local) );
  /***********************************************************/

  if( itstatus == IT_SUCCESS )
    return SKV_SUCCESS;

  return SKV_ERRNO_UNSPECIFIED_ERROR;
}


/***
 * skv_server_t::InitNewStateForEP::
 * Desc: Initiates the state for a new EP
 * input: 
 * returns: SKV_SUCCESS or SKV_ERR_NO_EVENT
 ***/
skv_status_t
skv_server_network_event_manager_if_t::
FinalizeEPState(  EPStateMap_T*         aEPStateMap,
                  it_ep_handle_t        aEP,
                 skv_server_ep_state_t* aStateForEP )
{
  AssertLogLine( aStateForEP != NULL )
    << "skv_server_t::FinalizeEPState(): ERROR: "
    << " aEP: " << (void *) aEP
    << EndLogLine;

  skv_server_finalizable_associated_ep_state_list_t::iterator iter = aStateForEP->mAssociatedStateList->begin();
  skv_server_finalizable_associated_ep_state_list_t::iterator end  = aStateForEP->mAssociatedStateList->end();

  for( ; iter != end; iter++ )
  {
    switch( iter->mStateType )
    {
      case SKV_SERVER_FINALIZABLE_ASSOCIATED_EP_STATE_CREATE_CURSOR_TYPE:
      {
        skv_server_cursor_hdl_t ServCursorHdl = (skv_server_cursor_hdl_t) iter->mState;

        ServCursorHdl->Finalize();

        free( ServCursorHdl );

        break;
      }
      default:
        StrongAssertLogLine( 0 )
          << "FinalizeEPState(): ERROR:: "
          << " iter->mStateType: " << iter->mStateType
          << EndLogLine;
    }
  }

  aStateForEP->Finalize();

  free( aStateForEP );

  aEPStateMap->erase( aEP );

  it_ep_free( aEP );

  BegLogLine( SKV_SERVER_CLEANUP_LOG )
    << "skv_server::FinalizeEPState(): completed "
    << EndLogLine;

  return SKV_SUCCESS;
}

/***
 * skv_server_t::InitNewStateForEP::
 * Desc: Initiates the state for a new EP
 * input: 
 * returns: SKV_SUCCESS or SKV_ERR_NO_EVENT
 ***/
skv_status_t
skv_server_network_event_manager_if_t::
InitNewStateForEP( EPStateMap_T* aEPStateMap,
                  skv_server_ep_state_t** aStateForEP )
{
  it_ep_attributes_t         ep_attr;
  it_ep_rc_creation_flags_t  ep_flags;

  ep_flags = IT_EP_NO_FLAG;

  ep_attr.max_dto_payload_size = 8192;
  ep_attr.max_request_dtos     = (SKV_MAX_COMMANDS_PER_EP + 5 ) * MULT_FACTOR;
  ep_attr.max_recv_dtos        = (SKV_MAX_COMMANDS_PER_EP + 5 ) * MULT_FACTOR;
  ep_attr.max_send_segments    = SKV_MAX_SGE;
  ep_attr.max_recv_segments    = SKV_MAX_SGE;

  ep_attr.srv.rc.rdma_read_enable        = IT_TRUE;
  ep_attr.srv.rc.rdma_write_enable       = IT_TRUE;
  ep_attr.srv.rc.max_rdma_read_segments  = SKV_SERVER_MAX_RDMA_WRITE_SEGMENTS; // * MULT_FACTOR_2;
  //ep_attr.srv.rc.max_rdma_write_segments = 4 * MULT_FACTOR_2;
  //ep_attr.srv.rc.max_rdma_write_segments = 24;
  ep_attr.srv.rc.max_rdma_write_segments = SKV_SERVER_MAX_RDMA_WRITE_SEGMENTS; // * MULT_FACTOR_2;

  ep_attr.srv.rc.rdma_read_ird           = SKV_MAX_COMMANDS_PER_EP; // * MULT_FACTOR_2;
  ep_attr.srv.rc.rdma_read_ord           = SKV_MAX_COMMANDS_PER_EP; // * MULT_FACTOR_2;

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

  *aStateForEP = (skv_server_ep_state_t *) malloc( sizeof( skv_server_ep_state_t ) );
  StrongAssertLogLine( *aStateForEP != NULL )
    << "skv_server_t::InitNewStateForEP()::ERROR not enough memory for "
    << " sizeof( skv_server_ep_state_t ): " << sizeof( skv_server_ep_state_t )
    << EndLogLine;

  (*aStateForEP)->Init( ep_hdl,
                        mPZ_Hdl );

  int rc = aEPStateMap->insert( std::make_pair( ep_hdl, *aStateForEP ) ).second;

  StrongAssertLogLine( rc == 1 )
    << "skv_server_t::InitNewStateForEP():: ERROR on insert to aEPStateMap"
    << EndLogLine;

  BegLogLine( SKV_SERVER_NETWORK_EVENT_MANAGER_LOG )
    << "skv_server_t::InitNewStateForEP():: "    
    << " ep_hdl: " << (void *) ep_hdl
    << " *aStateForEP: " << (void *) *aStateForEP
    << EndLogLine;

  return SKV_SUCCESS;
}
