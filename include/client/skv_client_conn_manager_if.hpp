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
 *
 * Description: Handles an array like ( 1 x N ) connection with the servers
 * Function Requirements:
 * 1. Contact a server that's tasked with name->address resolution
 * 2. Establish a connection from the client to each node of the skv server group
 * 3. Provide a send/receive interface for short messages (queue)
 *    or long messages (rdma-like read/write)
 */

#ifndef __SKV_CLIENT_CONN_MANAGER_IF_HPP__
#define __SKV_CLIENT_CONN_MANAGER_IF_HPP__

#include <common/skv_client_server_protocol.hpp>

#include <queue>
#include "common/skv_array_stack.hpp"
// #include <stack>


#ifndef SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG
#define SKV_CLIENT_RDMA_CMD_PLACEMENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_RESPONSE_POLLING_LOG
#define SKV_CLIENT_RESPONSE_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif


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


class skv_client_conn_manager_if_t
{
  int                           mServerConnCount;
  skv_client_server_conn_t*    mServerConns;

  skv_client_group_id_t         mClientGroupId;
  int                            mMyRankInGroup;

  it_ia_handle_t*                mIA_Hdl;
  it_pz_handle_t* 	         mPZ_Hdl;

  // Shared-Receive Queue NOT YET IMPLEMENTED
  // it_srq_handle_t               mSrq_Hdl;

  // Event Dispatchers
  it_evd_handle_t               mEvd_Unaff_Hdl;
  it_evd_handle_t               mEvd_Aff_Hdl; 
  it_evd_handle_t		mEvd_Cmr_Hdl;
  it_evd_handle_t               mEvd_Cmm_Hdl;
  it_evd_handle_t               mEvd_Rq_Hdl;
  it_evd_handle_t		mEvd_Sq_Hdl;

  // Need for security reasons to validate the 
  // client-server connection
  skv_client_cookie_t          mCookieSeq;

  int                           mEventsToDequeueCount;
  it_event_t*                   mEvents;
  int                           mEventLoops;

  skv_client_ccb_manager_if_t* mCCBMgrIF;

  skv_status_t ConnectToServer( int                        aServerRank,
                                 skv_server_addr_t         aServerAddr,
                                 skv_client_server_conn_t* aServerConn );

  skv_status_t DisconnectFromServer( skv_client_server_conn_t* aServerConn );

  skv_status_t ProcessOverflow( skv_client_server_conn_t* aConn );


  skv_status_t ProcessCCB( skv_client_server_conn_t*    aConn,
                            skv_client_ccb_t*            aCCB );

public:
  skv_client_conn_manager_if_t() {};
  ~skv_client_conn_manager_if_t() {};

  skv_status_t Init( skv_client_group_id_t aClientGroupId, 
                      int              aMyRank,
                      it_ia_handle_t*  aIA_Hdl,
                      it_pz_handle_t*  aPZ_Hdl,
                      int              aFlags,
                      skv_client_ccb_manager_if_t* aCCBMgrIF );

  skv_status_t Finalize();

  // Makes a 1 by N connections to the server group
  skv_status_t Connect( const char* aConfigFile, int aFlags );

  skv_status_t Disconnect();

  // Kick pipes
  // skv_status_t ProcessConnections();

  // Kick pipes only on the receive queue
  skv_status_t ProcessConnectionsRqSimple();
  skv_status_t ProcessConnectionsRqSq();

  // Kick pipes only on the receive queue
  // skv_status_t ProcessRqEvent(it_event_t* event_rq);
  skv_status_t ProcessSqEvent(it_event_t* event_rq);

  skv_status_t Dispatch( int aNodeId, skv_client_ccb_t* aCCB );

  skv_status_t Dispatch( skv_client_server_conn_t*    aConn,
                         skv_client_ccb_t*            aCCB,
                         int                           aCmdOrd = -1);

  inline int GetServerConnCount() { return mServerConnCount; }
  inline skv_client_server_conn_t* GetServerConnections() { return mServerConns; }

  skv_status_t GetEPHandle( int             aNodeId, 
                             it_ep_handle_t* aEP );

};

#endif
