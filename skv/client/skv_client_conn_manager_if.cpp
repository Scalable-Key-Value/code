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

#include <skv/client/skv_client_internal.hpp>
#include <skv/client/skv_client_conn_manager_if.hpp>


#include <fstream>
#include <string>
#include <netdb.h>	/* struct hostent */

#include <arpa/inet.h>
#include <ifaddrs.h>

// Supported Operation State Machines

#include <skv/client/commands/skv_client_open_command_sm.hpp>
#include <skv/client/commands/skv_client_retrieve_dist_command_sm.hpp>
#include <skv/client/commands/skv_client_insert_command_sm.hpp>
#include <skv/client/commands/skv_client_bulk_insert_command_sm.hpp>
#include <skv/client/commands/skv_client_retrieve_command_sm.hpp>
#include <skv/client/commands/skv_client_retrieve_n_keys_command_sm.hpp>
#include <skv/client/commands/skv_client_remove_command_sm.hpp>
#include <skv/client/commands/skv_client_active_bcast_command_sm.hpp>
#include <skv/client/commands/skv_client_pdscntl_command_sm.hpp>

#ifndef SKV_CLIENT_CONN_INFO_LOG
#define SKV_CLIENT_CONN_INFO_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_PROCESS_CCB_LOG
#define SKV_CLIENT_PROCESS_CCB_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_PROCESS_CONN_LOG
#define SKV_CLIENT_PROCESS_CONN_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG
#define SKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG ( 0 )
#endif

#ifndef SKV_CLIENT_PROCESS_SEND_RECV_RACE_LOG
#define SKV_CLIENT_PROCESS_SEND_RECV_RACE_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_ENDIAN_LOG
#define SKV_CLIENT_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL )
#endif

#ifndef SKV_CTRLMSG_DATA_LOG
#define SKV_CTRLMSG_DATA_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifdef SKV_DEBUG_MSG_MARKER  // defined in client_server_protocol.hpp (or via compile flag)
#define SKV_CLIENT_TRACK_MESSGES_LOG ( 1 )
#else
#define SKV_CLIENT_TRACK_MESSGES_LOG ( 0 )
#endif

// #ifndef SKV_SERVER_MACHINE_FILE
// #define SKV_SERVER_MACHINE_FILE "/etc/machinefile"
// #endif

/** \brief maximum number of events to dequeue and process from receive queue in one chunk (it_evd_dequeue_n)
 *
 */
#define SKV_CLIENT_RQ_EVENTS_TO_DEQUEUE_COUNT ( 8 )
#define SKV_CLIENT_RESPONSE_POLL_LOOPS ( 10 )
#define SKV_CLIENT_RESPONSE_REAP_PER_EP ( 2 )   // number of responses fetched from one EP before checking the next EP

#ifndef SKV_CLIENT_PROCESS_CONN_TRACE
#define SKV_CLIENT_PROCESS_CONN_TRACE ( 1 )
#endif

TraceClient                                      gSKVClientConnDispatch[ SKV_MAX_UNRETIRED_CMDS ];
TraceClient                                      gSKVClientConnProcessRecv[ SKV_MAX_UNRETIRED_CMDS ];


/***
 * skv_client_conn_manager_if_t::Init::
 * Desc: Initializes the state of the skv_client_conn_manager_if_t
 * Gets the client ready to create connections
 * input:
 * aFlags ->
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
Init( skv_client_group_id_t         aClientGroupId,
      int                           aMyRank,
      it_ia_handle_t*               aIA_Hdl,
      it_pz_handle_t*               aPZ_Hdl,
      int                           aFlags,
      skv_client_ccb_manager_if_t*  aCCBMgrIF )
{
  BegLogLine(SKV_CLIENT_PROCESS_CONN_LOG)
    << "aClientGroupId" << aClientGroupId
    << " aMyRank=" << aMyRank
    << EndLogLine ;

  mServerConnCount = 0;
  mServerConns = NULL;

  mCCBMgrIF = aCCBMgrIF;

  mMyRankInGroup = aMyRank;
  mClientGroupId = aClientGroupId;

  mIA_Hdl = aIA_Hdl;
  mPZ_Hdl = aPZ_Hdl;

  mCookieSeq.mConn = NULL;
  mCookieSeq.mCCB = NULL;
  mCookieSeq.mSeq = 0;

  mEventLoops = 0;

  /************************************************************
   * Initialize the Event Dispatchers (evds)
   ***********************************************************/
  it_evd_flags_t evd_flags = (it_evd_flags_t) 0;

  it_status_t status = it_evd_create( *mIA_Hdl,
                                      IT_ASYNC_UNAFF_EVENT_STREAM,
                                      evd_flags,
                                      SKV_EVD_SEVD_QUEUE_SIZE,
                                      1,
                                      (it_evd_handle_t) IT_NULL_HANDLE,
                                      & mEvd_Unaff_Hdl,
                                      NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  status = it_evd_create( *mIA_Hdl,
                          IT_ASYNC_AFF_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t) IT_NULL_HANDLE,
                          & mEvd_Aff_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  status = it_evd_create( *mIA_Hdl,
                          IT_CM_REQ_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t) IT_NULL_HANDLE,
                          & mEvd_Cmr_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  status = it_evd_create( *mIA_Hdl,
                          IT_CM_MSG_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t) IT_NULL_HANDLE,
                          & mEvd_Cmm_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  status = it_evd_create( *mIA_Hdl,
                          IT_DTO_EVENT_STREAM,
                          evd_flags,
                          SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t) IT_NULL_HANDLE,
                          & mEvd_Rq_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init():: ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;

  status = it_evd_create( *mIA_Hdl,
                          IT_DTO_EVENT_STREAM,
                          evd_flags,
                          2*SKV_EVD_SEVD_QUEUE_SIZE,
                          1,
                          (it_evd_handle_t) IT_NULL_HANDLE,
                          & mEvd_Sq_Hdl,
                          NULL );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Init(): ERROR:: Failed in it_evd_create()"
    << " status: " << status
    << EndLogLine;
  /***********************************************************/

  mEventsToDequeueCount = SKV_CLIENT_RQ_EVENTS_TO_DEQUEUE_COUNT;
  int RqEventsToDequeueSize = sizeof( it_event_t ) * mEventsToDequeueCount;
  mEvents               = (it_event_t *) malloc( RqEventsToDequeueSize );
  StrongAssertLogLine( mEvents != NULL )
    << "skv_client_conn_manager_if_t::Init(): ERROR: Not enough memory for: "
    << " RqEventsToDequeueSize: " << RqEventsToDequeueSize
    << EndLogLine;

  char TraceNameBuffer[ 128 ];
  for( int i = 0; i < SKV_MAX_UNRETIRED_CMDS; i++ )
  {

    sprintf( TraceNameBuffer, "PimcClientConnDispatchOrd_%d", i );
    gSKVClientConnDispatch[i].HitOE( SKV_CLIENT_PROCESS_CONN_TRACE,
        TraceNameBuffer,
        mMyRankInGroup,
        gSKVClientConnDispatch );

    sprintf( TraceNameBuffer, "SKVClientConnProcessRecvOrd_%d", i );
    gSKVClientConnProcessRecv[i].HitOE( SKV_CLIENT_PROCESS_CONN_TRACE,
        TraceNameBuffer,
        mMyRankInGroup,
        gSKVClientConnProcessRecv );
  }
  return status == IT_SUCCESS ? SKV_SUCCESS : SKV_ERRNO_CONN_FAILED;
}

/***
 * skv_client_conn_manager_if_t::Finalize::
 * Desc: Takes down the state of the skv_client_conn_manager_if_t
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
Finalize()
{
  it_evd_free( mEvd_Unaff_Hdl );
  it_evd_free( mEvd_Aff_Hdl );
  it_evd_free( mEvd_Cmr_Hdl );
  it_evd_free( mEvd_Cmm_Hdl );
  it_evd_free( mEvd_Sq_Hdl );
  it_evd_free( mEvd_Rq_Hdl );

  if( mEvents != NULL )
  {
    free( mEvents );
    mEvents = NULL;
  }

  if( mServerConns != NULL )
  {
    free( mServerConns );
    mServerConns = NULL;
  }
  return SKV_SUCCESS;
}

/***
 * skv_client_conn_manager_if_t::Connect::
 * Desc: Creates a connection between the client and the
 * SKV server group. For now server group name denotes a path
 * to a file with server IPs. (/etc/compute.mf)
 * input:
 * IN aServerGroupName -> Takes a name of the server. The name of the
 * server is one that's recognized by a name service.
 * IN aFlags      ->
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
Connect( const char* aConfigFile,
         int   aFlags )
{
  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Connect():: Entering "
    << " aConfigFile: " << (aConfigFile!=NULL? aConfigFile : "default")
    << " aFlags: " << aFlags
    << EndLogLine;

#define MY_HOSTNAME_SIZE 128
  char MyHostname[ MY_HOSTNAME_SIZE ];
  bzero( MyHostname, MY_HOSTNAME_SIZE );
  gethostname( MyHostname, MY_HOSTNAME_SIZE );

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Connect():: "
    << " MyHostname: " << MyHostname
    << EndLogLine;

  skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration( aConfigFile );

#ifdef SKV_ROQ_LOOPBACK_WORKAROUND
  // acquire the local interface address to check if server is local or remote
  struct sockaddr_in my_addr;
  struct ifaddrs *iflist, *ifent;
  char ClientLocalAddr[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  bzero( ClientLocalAddr, SKV_MAX_SERVER_ADDR_NAME_LENGTH );

  int rc = getifaddrs(&iflist);
  StrongAssertLogLine( rc == 0 )
    << "Failed to obtain list of interfaces, errno=" << errno
    << EndLogLine;

  ifent = iflist;
  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t: Examining from ifent=" << ifent
    << EndLogLine ;
  while( ifent )
  {
    BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
        << "ifa_name=" << ifent->ifa_name
        << " sa_family=" << ifent->ifa_addr->sa_family
        << " GetCommIF()=" << config->GetCommIF()
        << " AF_INET=" << AF_INET
        << EndLogLine ;
    if( strncmp( ifent->ifa_name, config->GetCommIF(), strnlen( ifent->ifa_name, 16 ) ) == 0 )
    {
      if( ifent->ifa_addr->sa_family == AF_INET )
      {
        struct sockaddr_in *tmp = (struct sockaddr_in*) (ifent->ifa_addr);
        my_addr.sin_family = ifent->ifa_addr->sa_family;
        my_addr.sin_addr.s_addr = tmp->sin_addr.s_addr;
        snprintf( ClientLocalAddr, SKV_MAX_SERVER_ADDR_NAME_LENGTH, "%d.%d.%d.%d",
                  (int) ((unsigned char*) &(tmp->sin_addr.s_addr))[0],
                  (int) ((unsigned char*) &(tmp->sin_addr.s_addr))[1],
                  (int) ((unsigned char*) &(tmp->sin_addr.s_addr))[2],
                  (int) ((unsigned char*) &(tmp->sin_addr.s_addr))[3] );
        BegLogLine( 1 )
          << "skv_client_conn_manager_if_t: local address: " << (void*)(uintptr_t)my_addr.sin_addr.s_addr << "; fam: " << tmp->sin_family
          << " ClientLocalAddr:" << ClientLocalAddr
          << EndLogLine;
        break;
      }
    }

    ifent = ifent->ifa_next;
  }
  freeifaddrs( iflist );
#endif

  // first pass of the server machine file to get the server count
  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Connect():: "
    << " ComputeFileNamePath: " << config->GetMachineFile()
    << EndLogLine;

  ifstream fin( config->GetMachineFile() );

  StrongAssertLogLine( !fin.fail() )
    << "skv_client_conn_manager_if_t::Connect():: ERROR opening server machine file: " << config->GetMachineFile()
    << EndLogLine;


  char line[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  mServerConnCount = 0;
  while( fin.getline(line, SKV_MAX_SERVER_ADDR_NAME_LENGTH) )
  {
    mServerConnCount++;
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Connect():: "
    << " mServerConnCount: " << mServerConnCount
    << EndLogLine;

  mServerConns = (skv_client_server_conn_t *)
    malloc( sizeof( skv_client_server_conn_t ) * mServerConnCount );

  StrongAssertLogLine( mServerConns != NULL )
    << "skv_client_conn_manager_if_t::Connect():: ERROR: "
    << " Not enough memory to allocate: "
    << sizeof( skv_client_server_conn_t ) * mServerConnCount
    << " bytes"
    << EndLogLine;

  skv_server_addr_t* ServerAddrs = (skv_server_addr_t *) malloc( mServerConnCount * sizeof( skv_server_addr_t ) );
  StrongAssertLogLine( ServerAddrs != NULL )
    << "skv_client_conn_manager_if_t::Connect():: ERROR:: ServerAddrs != NULL"
    << EndLogLine;

  int RankInx = 0;


  // Open and parse compute file name
  ifstream fin1( config->GetMachineFile() );

  // get hostname to replace local server names with localhost
  char this_host_name[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  gethostname(this_host_name, SKV_MAX_SERVER_ADDR_NAME_LENGTH );

  char ServerAddr[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];

  while( fin1.getline( ServerAddr, SKV_MAX_SERVER_ADDR_NAME_LENGTH ) )
  {
    char* firstspace=index(ServerAddr, ' ');
    char* PortStr=firstspace+1;
    *firstspace=0;

#ifdef SKV_ROQ_LOOPBACK_WORKAROUND
    // compare with local address and replace with 127.0.0.1 if it matches
    if( strncmp( ServerAddr, ClientLocalAddr, SKV_MAX_SERVER_ADDR_NAME_LENGTH ) == 0 )
      sprintf( ServerAddrs[ RankInx ].mName, "127.0.0.1" );
    else
#endif
      strcpy( ServerAddrs[ RankInx ].mName, ServerAddr );

    ServerAddrs[ RankInx ].mPort = atoi( PortStr );

    BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
      << "Connect(): init server addresses::"
      << " Index: " << RankInx
      << " Addr: " << ServerAddrs[RankInx].mName
      << " Port: " << ServerAddrs[RankInx].mPort
      << EndLogLine;

    RankInx++;
  }

  StrongAssertLogLine( RankInx == mServerConnCount )
    << "skv_client_conn_manager_if_t::Connect():: ERROR:: RankInx == mServerConnCount "
    << " RankInx: " << RankInx
    << " mServerConnCount: " << mServerConnCount
    << EndLogLine;


  int IterCount = 0;

  srand( mMyRankInGroup );
  RankInx = rand() % mServerConnCount;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Connect():: "
    << " RankInx: " << RankInx
    << EndLogLine;

  while( IterCount < mServerConnCount )
  {
    int RealIdx = RankInx % mServerConnCount;
    mServerConns[ RealIdx ].Init( *mPZ_Hdl );

    skv_status_t status = ConnectToServer( RealIdx,
                                           ServerAddrs[ RealIdx ],
                                           &mServerConns[ RealIdx ] );

    if( status != SKV_SUCCESS )
    {
      BegLogLine ( SKV_CLIENT_CONN_INFO_LOG )
        << "skv_client_conn_manager_if_t::Connect():: "
        << " ERROR returned from ConnectToServer( " << RealIdx
        << " ) "
        << EndLogLine;

      return status;
    }
    else
    {
      StrongAssertLogLine( mServerConns[ RealIdx ].mState == SKV_CLIENT_CONN_CONNECTED )
        << "skv_client_conn_manager_if_t::Connect():: ERROR: "
        << EndLogLine;

      BegLogLine ( SKV_CLIENT_CONN_INFO_LOG )
        << "skv_client_conn_manager_if_t::Connect():: "
        << " Connection: " << RealIdx
        << " EP: " << ( void* )( &mServerConns[ RealIdx ] )
        << EndLogLine;
    }

    RankInx++;
    IterCount++;
  }

  free( ServerAddrs );
  ServerAddrs = NULL;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Connect():: Exiting connected to " << IterCount
    << "/" << mServerConnCount
    << " Servers"
    << EndLogLine;

#ifndef SKV_CLIENT_UNI
  MPI_Barrier(MPI_COMM_WORLD);
#endif

  return SKV_SUCCESS;
}

/***
 * skv_client_conn_manager_if_t::Disconnect::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
Disconnect()
{
    BegLogLine(SKV_CLIENT_PROCESS_CONN_LOG)
        << "mMyRankInGroup=" << mMyRankInGroup
        << EndLogLine ;
  int conn = mMyRankInGroup;
  if( conn >= mServerConnCount )
    conn = 0;

  int Counter = 0;

  while( 1 )
  {
      BegLogLine(SKV_CLIENT_PROCESS_CONN_LOG)
          << "Disconnecting from server " << conn
          << EndLogLine ;
    skv_status_t status = DisconnectFromServer( &mServerConns[conn] );

    StrongAssertLogLine( status == SKV_SUCCESS )
      << "skv_client_conn_manager_if_t::Disconnect(): ERROR:: "
      << " status: " << skv_status_to_string( status )
      << EndLogLine;

    mServerConns[conn].Finalize();

    conn++;
    if( conn == mServerConnCount )
      conn = 0;

    Counter++;
    if( Counter == mServerConnCount )
      break;
  }

  if( mServerConns != NULL )
  {
    free( mServerConns );
    mServerConns = NULL;
  }

  mServerConnCount = 0;

  return SKV_SUCCESS;
}

/***
 * skv_client_conn_manager_if_t::ConnectToServer::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
ConnectToServer( int                        aServerRank,
                 skv_server_addr_t         aServerAddr,
                 skv_client_server_conn_t* aServerConn )
{
  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: Entering "
    << "aServerRank: " << aServerRank
    << " aServerAddr.mName: " << aServerAddr.mName
    << " aServerAddr.mPort: " << aServerAddr.mPort
    << " aServerConn: " << aServerConn
    << EndLogLine;

  it_ep_rc_creation_flags_t      ep_flags = (it_ep_rc_creation_flags_t) 0;

  struct hostent		*remote_host;
  it_ep_attributes_t		 ep_attr;
  it_path_t			 path;
  it_conn_qual_t		 conn_qual;

  ep_attr.max_dto_payload_size             = 8192;
  ep_attr.max_request_dtos                 = (SKV_MAX_COMMANDS_PER_EP + 5 ) * MULT_FACTOR;
  ep_attr.max_recv_dtos                    = (SKV_MAX_COMMANDS_PER_EP + 5 ) * MULT_FACTOR;
  ep_attr.max_send_segments                = SKV_MAX_SGE;
  ep_attr.max_recv_segments                = SKV_MAX_SGE;

  ep_attr.srv.rc.rdma_read_enable          = IT_TRUE;
  ep_attr.srv.rc.rdma_write_enable         = IT_TRUE;
  // ep_attr.srv.rc.max_rdma_read_segments    = 4 * MULT_FACTOR_2;
  ep_attr.srv.rc.max_rdma_read_segments    = SKV_SERVER_MAX_RDMA_WRITE_SEGMENTS;
  ep_attr.srv.rc.max_rdma_write_segments   = SKV_SERVER_MAX_RDMA_WRITE_SEGMENTS;
  ep_attr.srv.rc.rdma_read_ird             = SKV_MAX_COMMANDS_PER_EP;// * MULT_FACTOR_2;
  ep_attr.srv.rc.rdma_read_ord             = SKV_MAX_COMMANDS_PER_EP;// * MULT_FACTOR_2;
  ep_attr.srv.rc.srq                       = (it_srq_handle_t) IT_NULL_HANDLE;
  ep_attr.srv.rc.soft_hi_watermark         = 0;
  ep_attr.srv.rc.hard_hi_watermark         = 0;
  ep_attr.srv.rc.atomics_enable            = IT_FALSE;

  ep_attr.priv_ops_enable                  = IT_FALSE;

  StrongAssertLogLine( aServerConn != NULL )
    << "skv_client_conn_manager_if_t::ConnectToServer():: Error:: ( aServerConn != NULL ) "
    << EndLogLine;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: Before it_ep_rc_create"
    << EndLogLine;

  it_status_t status = it_ep_rc_create( *mPZ_Hdl,
                                        mEvd_Sq_Hdl,
                                        mEvd_Rq_Hdl,
                                        mEvd_Cmm_Hdl,
                                        ep_flags,
                                        & ep_attr,
                                        & aServerConn->mEP );

  StrongAssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::ConnectToServer():: ERROR:: after it_ep_rc_create()"
    << " status: " << status
    << EndLogLine;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: End-point created"
    << EndLogLine;

  /*
   * Relationship between (it_path_t, it_conn_qual_t)
   * and			struct sockaddr_in:
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


  if( strncmp( aServerAddr.mName, "127.0.0.1", SKV_MAX_SERVER_ADDR_NAME_LENGTH ) == 0 )
  {
    path.u.iwarp.laddr.ipv4.s_addr = INADDR_LOOPBACK;

    BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
      << "skv_client_conn_manager_if_t::ConnectToServer():: using loopback to connect to local server"
      << " addr: " << (void*)((uintptr_t)path.u.iwarp.laddr.ipv4.s_addr)
      << EndLogLine;
      aServerConn->mServerIsLocal = true;
  }
  else
  {
    path.u.iwarp.laddr.ipv4.s_addr = INADDR_ANY;
    aServerConn->mServerIsLocal = false;
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: "
    << " about to call gethostname() for serveraddr: " << aServerAddr.mName
    << EndLogLine;

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
  int i=0;
  while( address[i] != NULL )
  {
    BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
      << "skv_client_conn_manager_if_t::ConnectToServer()::  "
      << " address#" << i
      << " length=" << remote_host->h_length
      << " (void *)address=" << inet_ntoa( *(struct in_addr*)(address[i]) )
      << EndLogLine;
    i++;
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer()::  "
    << " remote_host->h_name: " << remote_host->h_name
    << " remote_host->h_addr: " << ((struct in_addr*)(remote_host->h_addr_list[0]))->s_addr
    << EndLogLine;

  /* Option 2 - TCP: */
  path.u.iwarp.raddr.ipv4.s_addr =  ((struct in_addr*)(remote_host->h_addr_list[0]))->s_addr;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer()::  "
    << " assigned path.s_addr of " << remote_host->h_name
    << " (void*)s_addr : " << (void*)(uintptr_t)path.u.iwarp.raddr.ipv4.s_addr
    << EndLogLine;

  conn_qual.type = IT_IANA_LR_PORT;

  conn_qual.conn_qual.lr_port.local = 0; /* Any local port */
  //conn_qual.conn_qual.lr_port.local = htons(12345);

  conn_qual.conn_qual.lr_port.remote = htons( aServerAddr.mPort );

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: "
    << " About to connect to: { "
    << aServerAddr.mName
    << " , "
    << aServerAddr.mPort
    << " }"
    << EndLogLine;

  status = (it_status_t) -1;
  size_t private_data_length = 3 * sizeof( uint32_t );
  unsigned char private_data[ private_data_length ];

  *(uint32_t*)&( private_data [sizeof(uint32_t) * 0])  =   htonl( (uint32_t)mMyRankInGroup );
  *(uint32_t*)&( private_data [sizeof(uint32_t) * 1])  =   htonl( (uint32_t)aServerRank );
  *(uint32_t*)&( private_data [sizeof(uint32_t) * 2])  =   htonl( (uint32_t)mClientGroupId );

  it_lmr_triplet_t local_triplet;
  local_triplet.lmr      = aServerConn->GetResponseLMR();
  local_triplet.addr.abs = ( void* ) aServerConn->GetResponseRMRAddr();
  local_triplet.length   = aServerConn->GetResponseRMRLength();

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: "
    << " mMyRankInGroup: " << mMyRankInGroup
    << " aServerRank: " << aServerRank
    << " mClientGroupId: " << mClientGroupId
    << " addr: " << ( void* )local_triplet.addr.abs
    << " rmrlen: " << local_triplet.length
    << EndLogLine;


  while( status != IT_SUCCESS )
  {
    status = itx_ep_connect_with_rmr( aServerConn->mEP,
                                      &path,
                                      NULL,
                                      &conn_qual,
                                      IT_CONNECT_FLAG_TWO_WAY,
                                      private_data,
                                      private_data_length,
                                      &local_triplet,
                                      &aServerConn->GetResponseRMRContext()
                                      );

    if( ( status != IT_SUCCESS )&&( status != IT_ERR_QUEUE_EMPTY ) )
    {
      BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
        << "skv_client_conn_manager_if_t::ConnectToServer():: "
        << " About to sleep "
        << " status: " << status
        << EndLogLine;

      sleep( 1 );
    }
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: "
    << " About to wait on connection establishment "
    << EndLogLine;

  // Check for errors
  int ConnectTimeOut = 20;
  while( ConnectTimeOut > 0 )
  {
    it_event_t event_cmm;

    it_status_t status = it_evd_dequeue( mEvd_Cmm_Hdl,
                                         &event_cmm );

    switch( status )
    {
      case IT_SUCCESS:
        if( event_cmm.event_number == IT_CM_MSG_CONN_ESTABLISHED_EVENT )
        {
          // Run the varification protocol.
          BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
            << "skv_client_conn_manager_if_t::ConnectToServer():: "
            << " Connection established"
            << EndLogLine;

          if( event_cmm.conn.private_data_present )
          {
            BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
              << " Retrieved Private Data value: " << *(reinterpret_cast<skv_rmr_triplet_t*>(event_cmm.conn.private_data))
              << EndLogLine;

            aServerConn->mServerCommandMem = *((it_rmr_triplet_t*) (event_cmm.conn.private_data));
            // host-endian conversions. Data gets transferred in BE
            aServerConn->mServerCommandMem.mRMR_Addr = be64toh( aServerConn->mServerCommandMem.mRMR_Addr );
            aServerConn->mServerCommandMem.mRMR_Len = be64toh( aServerConn->mServerCommandMem.mRMR_Len );
            aServerConn->mServerCommandMem.mRMR_Context = be64toh( aServerConn->mServerCommandMem.mRMR_Context );
          }
          else
          {
            StrongAssertLogLine( 0 )
              << "skv_client_conn_manager_if_t::ConnectToServer():: No Private Data present in connection established event. Cannot proceed"
              << EndLogLine;

            return SKV_ERRNO_CONN_FAILED;
          }

          aServerConn->mState = SKV_CLIENT_CONN_CONNECTED;

          BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
            << "skv_client_conn_manager_if_t::ConnectToServer():: Leaving with SUCCESS"
            << "aServerAddr.mName: " << aServerAddr.mName
            << EndLogLine;

          return SKV_SUCCESS;
        }
        else
        {
          BegLogLine( 1 )
            << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
            << " event_number: " << event_cmm.event_number
            << EndLogLine;

          return SKV_ERRNO_CONN_FAILED;
        }
      case IT_ERR_QUEUE_EMPTY:
        // retry...
        break;
      default:
        BegLogLine( 1 )
          << "skv_client_conn_manager_if_t::ConnectToServer():: ERROR:: "
          << " getting connection established event failed"
          << EndLogLine;

        return SKV_ERRNO_CONN_FAILED;
    }

    it_event_t event_aff;

    status = it_evd_dequeue( mEvd_Aff_Hdl,
                             &event_aff );

    if(( status != IT_SUCCESS ) && ( status != IT_ERR_QUEUE_EMPTY ) )
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
        << " event_number: " << event_aff.event_number
        << EndLogLine;

      return SKV_ERRNO_CONN_FAILED;
    }

    it_event_t event_unaff;

    status = it_evd_dequeue( mEvd_Unaff_Hdl,
                             &event_unaff );

    if(( status != IT_SUCCESS ) && ( status != IT_ERR_QUEUE_EMPTY ) )
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
        << " event_number: " << event_unaff.event_number
        << EndLogLine;

      return SKV_ERRNO_CONN_FAILED;
    }
    // prevent extreme polling for connections and countdown for timeout
    BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
      << "skv_client_conn_manager_if_t::ConnectToServer():: No connection event, retrying: " << ConnectTimeOut
      << EndLogLine;

    sleep(1);
    ConnectTimeOut--;
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::ConnectToServer():: Leaving with " << ConnectTimeOut
    << " retries left. aServerAddr.mName: " << aServerAddr.mName
    << EndLogLine;

  if( ConnectTimeOut > 0 )
    return SKV_SUCCESS;
  else
    return SKV_ERRNO_CONN_FAILED;
}

/***
 * skv_client_conn_manager_if_t::DisconnectFromServer::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
DisconnectFromServer( skv_client_server_conn_t* aServerConn )
{
  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::DisconnectFromServer():: Entering"
    << " aServerConn: " << (void*)aServerConn
    << " mMyRankInGroup: " << mMyRankInGroup
    << EndLogLine;

  StrongAssertLogLine( aServerConn->mState == SKV_CLIENT_CONN_CONNECTED )
    << "skv_client_conn_manager_if_t::DisconnectFromServer(): ERROR:: "
    << " aServerConn->mState: " << aServerConn->mState
    << EndLogLine;

  it_ep_handle_t EP = aServerConn->mEP;
  unsigned char private_data[ 256 ];

  int SourceNodeId = mMyRankInGroup;
  sprintf( (char *) private_data, "%d", SourceNodeId );

  size_t private_data_length = strlen((char *)private_data) + 1;

  it_status_t istatus = it_ep_disconnect( EP,
                                          private_data,
                                          private_data_length );
  AssertLogLine( istatus == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::DisconnectFromServer(): ERROR:: "
    << " istatus: " << istatus
    << EndLogLine;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::DisconnectFromServer():: Finished it_ep_disconnect"
    << " istatus: " << istatus
    << EndLogLine;

  // Check for errors
  while( 1 )
  {
    it_event_t event_cmm;

    BegLogLine( 0 )
      << "skv_client_conn_manager_if_t::DisconnectFromServer():: going it_evd_dequeue"
      << EndLogLine;

    it_status_t status = it_evd_dequeue( mEvd_Cmm_Hdl,
                                         &event_cmm );

    if( status == IT_SUCCESS )
    {
      if( event_cmm.event_number == IT_CM_MSG_CONN_DISCONNECT_EVENT )
      {
        // Run the varification protocol.
        aServerConn->mState = SKV_CLIENT_CONN_DISCONNECTED;

        BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
          << "skv_client_conn_manager_if_t::DisconnectFromServer():: going it_ep_free..."
          << " istatus: " << istatus
          << " status: " << status
          << EndLogLine;

        it_ep_free( EP );

        BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
          << "skv_client_conn_manager_if_t::DisconnectFromServer():: complete"
          << " istatus: " << status
          << EndLogLine;

        return SKV_SUCCESS;
      }
      else
      {
        BegLogLine( 1 )
          << "skv_client_conn_manager_if_t::DisconnectFromServer()::ERROR:: "
          << " event_number: " << event_cmm.event_number
          << EndLogLine;

        return SKV_ERRNO_CONN_FAILED;
      }
    }
    else
    {
      BegLogLine( 0 )
        << "skv_client_conn_manager_if_t::DisconnectFromServer()::ERROR:: "
        << "it_evd_dequeue failed with " << istatus
        << EndLogLine;
    }

    it_event_t event_aff;

    status = it_evd_dequeue( mEvd_Aff_Hdl,
                             &event_aff );

    if( status == IT_SUCCESS )
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::ConnectToServer()::ERROR:: "
        << " event_number: " << event_aff.event_number
        << EndLogLine;

      return SKV_ERRNO_CONN_FAILED;
    }

    it_event_t event_unaff;

    status = it_evd_dequeue( mEvd_Unaff_Hdl,
                             &event_unaff );

    if( status == IT_SUCCESS )
    {
      BegLogLine( 1 )
        << "skv_client_conn_manager_if_t::DisconnectFromServer()::ERROR:: "
        << " event_number: " << event_unaff.event_number
        << EndLogLine;

      return SKV_ERRNO_CONN_FAILED;
    }
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::DisconnectFromServer():: Leaving"
    << " aServerConn: " << (void*)aServerConn
    << " mMyRankInGroup: " << mMyRankInGroup
    << EndLogLine;

  return SKV_SUCCESS;
}

skv_status_t
skv_client_conn_manager_if_t::
GetEPHandle( int             aNodeId,
             it_ep_handle_t* aEP )
{
  AssertLogLine( aNodeId >= 0 && aNodeId < mServerConnCount )
    << "skv_client_conn_manager_if_t::GetEPHandle():: ERROR:: "
    << " aNodeId: " << aNodeId
    << " mServerConnCount: " << mServerConnCount
    << EndLogLine;

  skv_client_server_conn_t* Conn = & mServerConns[ aNodeId ];

  *aEP = Conn->mEP;

  return SKV_SUCCESS;
}

/***
 * skv_client_conn_manager_if_t::Dispatch::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
Dispatch( skv_client_server_conn_t*    aConn,
          skv_client_ccb_t*            aCCB,
          int                          aCmdOrd )
{
  skv_status_t status = SKV_SUCCESS;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Dispatch():: Entering"
    << " aConn: " << (void *) aConn
    << " aCCB: " << (void *) aCCB
    << " aCmdOrd: " << aCmdOrd
    << EndLogLine;

  // instead of posting send/recv we might have to defer the Dispatch() and push the request to an OverflowQueue
  if(( aConn->mUnretiredRecvCount >= SKV_MAX_UNRETIRED_CMDS ) ||
     ( aConn->mOutStandingRequests + aConn->mSendSegsCount >= SKV_MAX_COMMANDS_PER_EP ))
  {
    // Need to enqueue the request into the overflow queue
    BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
      << "skv_client_conn_manager_if_t::Dispatch():: QUEUING command"
      << " CCB: " << (void*)aCCB
      << " conn: " << (void*)aConn
      << " queued: " << aConn->mOverflowCommands->size()
      << " outstanding: " << aConn->mOutStandingRequests
      << EndLogLine;

    aConn->mOverflowCommands->push( aCCB );

    return SKV_SUCCESS;
  }

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Dispatch(): Dispatching CCB: " << (void*)aCCB
    << " conn: " << (void*)aConn
    << " queued: " << aConn->mOverflowCommands->size()
    << EndLogLine;

  it_lmr_triplet_t send_seg;

  send_seg.lmr = aCCB->mBaseLMR;
  send_seg.addr.abs = aCCB->GetSendBuff();

  /// TODO: THIS NEEDS TO BE CALCULATED
  send_seg.length = SKV_CONTROL_MESSAGE_SIZE;

  AssertLogLine( aCmdOrd == -1 )
    << "ERROR: CmdOrd must not be set. Only support Request/Response model "
    << EndLogLine;

  int CmdOrd = aConn->ReserveCmdOrdinal();
  if( CmdOrd == -1 )
  {
    BegLogLine( 1 )
      << "Dispatch: WARNING: Cannot reserver Cmd Ordinal!"
      << EndLogLine;
    return SKV_ERRNO_PENDING_COMMAND_LIMIT_REACHED;
  }

  // Set the command ordinal in the CCB
  aCCB->SetCmdOrd( CmdOrd );

  skv_client_to_server_cmd_hdr_t* Hdr = (skv_client_to_server_cmd_hdr_t *) aCCB->GetSendBuff();
  Hdr->SetCmdOrd( CmdOrd );
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian-converting the header mEventType=" << Hdr->mEventType
    << " mCmdType=" << Hdr->mCmdType
    << EndLogLine ;
  Hdr->mEventType=(skv_server_event_type_t)htonl(Hdr->mEventType) ;
  Hdr->mCmdType=(skv_command_type_t)htonl(Hdr->mCmdType) ;

#ifdef SKV_DEBUG_MSG_MARKER
  Hdr->mMarker = mCookieSeq.mSeq;

  BegLogLine( SKV_CLIENT_TRACK_MESSGES_LOG )
    << "MSG_TRACKER: submit marker = " << Hdr->mMarker
    << " cmdOrd = " << CmdOrd
    << " CCB: " << (void*)aCCB
    << " rBuf: " << (void*)aCCB->GetRecvBuff()
    << EndLogLine;
#endif

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Dispatch():: "
    << " Hdr: " << *Hdr
    << EndLogLine;

  gSKVClientConnDispatch[CmdOrd].HitOE( SKV_CLIENT_PROCESS_CONN_TRACE,
      "SKVClientConnDispatch",   // this name is not used, see Init()
      mMyRankInGroup,
      gSKVClientConnDispatch );

  // Set EOM-mark or real checksum to be checked by remote memory polling
  skv_header_as_cmd_buffer_t* cmdbuf = (skv_header_as_cmd_buffer_t*)Hdr;
  cmdbuf->SetCheckSum( Hdr->CheckSum() );

  BegLogLine( SKV_CLIENT_PROCESS_CONN_LOG )
    << "skv_client_conn_manager_if_t::Dispatch(): about to write"
    << " SrvSlot: " << aConn->mCurrentServerRecvSlot
    << " seq: " << mCookieSeq
    << " cmdOrd: " << CmdOrd
    << " wr.addr: " << send_seg.addr.abs
    << " wr.len: " << send_seg.length
    << " ChSum: " << (int)(Hdr->CheckSum())
    << EndLogLine;

#define HEXLOG( x )  (  (void*) (*((uint64_t*) &(x)) ) )

#ifdef SKV_DEBUG_MSG_MARKER
  skv_cmd_RIU_req_t* Req = (skv_cmd_RIU_req_t *) send_seg.addr.abs;
  BegLogLine( 0 )
    << "Writing: "
    << " vAddr: " << Req->mRMRTriplet
    << " CCB: " << (void*)aCCB
    << " Hdr: " << Req->mHdr
    << " last bytes: " << HEXLOG( ((char*)Req)[SKV_CONTROL_MESSAGE_SIZE-sizeof(void*)] )
    << " Now dispatching..."
    << EndLogLine;
#endif

  mCookieSeq.mCCB = aCCB;
  status = aConn->PostOrStoreRdmaWrite( send_seg, aCCB, mCookieSeq.mSeq );

  AssertLogLine( status == IT_SUCCESS )
    << "skv_client_conn_manager_if_t::Dispatch:: ERROR:: "
    << " status: " << status
    << EndLogLine;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Dispatch():: "
    << " After it_post_rdma_write"
    << " on EP: " << (void *) aConn->mEP
    << EndLogLine;

  mCookieSeq.mSeq++;

  BegLogLine( 0 )
    << "SEQNO: " << mCookieSeq.mSeq-1
    << EndLogLine;

  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Dispatch():: Leaving"
    << EndLogLine;

  return status;
}

/***
 * skv_client_conn_manager_if_t::Dispatch::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
Dispatch( int                   aNodeId,
          skv_client_ccb_t*    aCCB )
{
  BegLogLine( SKV_CLIENT_CONN_INFO_LOG )
    << "skv_client_conn_manager_if_t::Dispatch():: Entering"
    << " aNodeId: " << aNodeId
    << " aCCB: " << (void *) aCCB
    << EndLogLine;

  AssertLogLine( aCCB != NULL )
    << "skv_client_conn_manager_if_t::Dispatch:: ERROR:: "
    << " aCCB != NULL"
    << EndLogLine;

  AssertLogLine( aNodeId >= 0 && aNodeId < mServerConnCount )
    << "skv_client_conn_manager_if_t::Dispatch:: ERROR:: "
    << " aNodeId: " << aNodeId
    << " mServerConnCount: " << mServerConnCount
    << EndLogLine;

  skv_client_server_conn_t* Conn = & mServerConns[ aNodeId ];

  return Dispatch( Conn, aCCB );
}

/***
 * skv_client_conn_manager_if_t::ProcessCCB::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
ProcessCCB( skv_client_server_conn_t*    aConn,
            skv_client_ccb_t*            aCCB )
{
  AssertLogLine( aConn != NULL )
    << "skv_client_conn_manager_if_t::ProcessCCB():: ERROR:: "
    << " aConn: " << (void *) aConn
    << EndLogLine;

  AssertLogLine( aCCB != NULL )
    << "skv_client_conn_manager_if_t::ProcessCCB():: ERROR:: "
    << " aCCB: " << (void *) aCCB
    << EndLogLine;

  gSKVClientConnProcessRecv[ aCCB->mCmdOrd ].HitOE( SKV_CLIENT_PROCESS_CONN_TRACE,
                                                     "SKVClientConnProcessRecv",
                                                     mMyRankInGroup,
                                                     gSKVClientConnProcessRecv );

  skv_status_t status = aConn->RequestCompletion( aCCB );

  if( status != SKV_SUCCESS )
    return status;

  skv_server_to_client_cmd_hdr_t* Hdr = (skv_server_to_client_cmd_hdr_t *) aCCB->GetRecvBuff();

  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
   << "Endian converting the header"
   << EndLogLine ;
  Hdr->EndianConvert() ;

  skv_command_type_t CommandType = aCCB->mCommand.mType;

  skv_client_command_state_t State = aCCB->mState;

  BegLogLine( SKV_CLIENT_PROCESS_CCB_LOG )
    << "skv_client_conn_manager_if_t::ProcessCCB(): "
    << " aConn: " << (void *) aConn
    << " aCCB: " << (void *) aCCB
    << " State: " << skv_client_command_state_to_string( State )
    << " RecvBuff: " << (void *) aCCB->GetRecvBuff()
    << " Hdr: " << *Hdr
    << " CommandType: " << CommandType
    << EndLogLine;

  StrongAssertLogLine( Hdr->mEvent >= 0 )
    << "skv_client_conn_manager_if_t::ProcessCCB(): ERROR:: "
    << " Hdr->mEvent: " << Hdr->mEvent
    << EndLogLine;

#ifdef SKV_DEBUG_MSG_MARKER
  BegLogLine( SKV_CLIENT_TRACK_MESSGES_LOG )
    << "MSG_TRACKER: Retrieved marker = " << Hdr->mMarker
    << " cmdOrd = " << Hdr->mCmdOrd
    << EndLogLine;

  skv_cmd_retrieve_value_rdma_write_ack_t *Resp = (skv_cmd_retrieve_value_rdma_write_ack_t*) (Hdr);
  BegLogLine( SKV_CLIENT_RETRIEVE_COMMAND_SM_LOG )
    << "skv_client_conn_manager_if_t::ProcessCCB(): about to receive data"
    << " mBuf: " << (void*)Resp->mValue.mData
    << " rBuf: " << (void*)aCCB->GetRecvBuff()
    << " mvalue: " << (void*) (*(uint64_t*)(Resp->mValue.mData))
    << " size: " << Resp->mValue.mValueSize
    << " msg: " << Resp->mHdr.mMarker
    << EndLogLine;
#endif


  switch( CommandType )
  {
    case SKV_COMMAND_OPEN:
    {
      status = skv_client_open_command_sm::Execute( aConn, aCCB );
      break;
    }
    case SKV_COMMAND_PDSCNTL:
    case SKV_COMMAND_CLOSE:
    {
      status = skv_client_pdscntl_command_sm::Execute( aConn, aCCB );
      break;
    }
    case SKV_COMMAND_RETRIEVE_DIST:
    {
      status = skv_client_retrieve_dist_command_sm::Execute( aConn, aCCB );
      break;
    }
    case SKV_COMMAND_INSERT:
    {
      status = skv_client_insert_command_sm::Execute( this, aConn, aCCB );
      break;
    }
    case SKV_COMMAND_BULK_INSERT:
    {
      status = skv_client_bulk_insert_command_sm::Execute( this, aConn, aCCB );
      break;
    }
    case SKV_COMMAND_RETRIEVE:
    {
      status = skv_client_retrieve_command_sm::Execute( this, aConn, aCCB );
      break;
    }
    case SKV_COMMAND_RETRIEVE_N_KEYS:
    {
      status = skv_client_retrieve_n_keys_command_sm::Execute( this, aConn, aCCB );
      break;
    }
    case SKV_COMMAND_REMOVE:
    {
      status = skv_client_remove_command_sm::Execute( this, aConn, aCCB );
      break;
    }
    case SKV_COMMAND_ACTIVE_BCAST:
    {
      status = skv_client_active_bcast_command_sm::Execute( this, aConn, aCCB );

      break;
    }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_client_conn_manager_if_t::ProcessCCB:: ERROR:: Invalid Command Type: "
        << " Command Type: " << CommandType
        << EndLogLine;

      break;
    }
  }
  return status;
}

/***
 * skv_client_conn_manager_if_t::ProcessOverflow::
 * Desc:
 * input:
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_conn_manager_if_t::
ProcessOverflow( skv_client_server_conn_t* aConn )
{
  // take one off the overflow and dispatch
  while( ( aConn->mOverflowCommands->size() > 0 ) &&
         ( aConn->mUnretiredRecvCount < SKV_MAX_UNRETIRED_CMDS ) )
  {
    BegLogLine( (SKV_CLIENT_PROCESS_CONN_LOG | SKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG | SKV_CLIENT_TRACK_MESSGES_LOG) )
      << "skv_client_conn_manager_if_t::ProcessOverflow(): CMD from overflow queue " << aConn->mOverflowCommands->size()
      << EndLogLine;

      skv_client_ccb_t* CCB = aConn->mOverflowCommands->front();
    aConn->mOverflowCommands->pop();

    BegLogLine( 0 )
      << "skv_client_conn_manager_if_t::ProcessOverflow(): NewQueueSz: " << aConn->mOverflowCommands->size()
      << " CCB: " << (void*)CCB
      << " Sb: " << (void*)CCB->GetSendBuff()
      << EndLogLine;

      return Dispatch( aConn, CCB );
  }

  BegLogLine( ((SKV_CLIENT_PROCESS_CONN_LOG | SKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG) & (aConn->mOverflowCommands->size() > 0)) )
    << "skv_client_conn_manager_if_t::ProcessOverflow(): exit without action.."
    << " unretired: " << aConn->mUnretiredRecvCount
    << " oustanding: " << aConn->mOutStandingRequests
    << EndLogLine;

  if( aConn->mOverflowCommands->size() > 0 )
    return SKV_ERRNO_MAX_UNRETIRED_CMDS;
  else
    return SKV_SUCCESS;
}

skv_status_t
skv_client_conn_manager_if_t::
ProcessSqEvent(it_event_t* event_rq)
{
  BegLogLine( SKV_CLIENT_PROCESS_CONN_LOG )
    << "skv_client_conn_manager_if_t::ProcessSqEvent(): SQ Event: "
    << " event_number: " << event_rq->event_number
    << " Check for: " << IT_DTO_SEND_CMPL_EVENT
    << EndLogLine;

  it_dto_cmpl_event_t* DTO_Event = (it_dto_cmpl_event_t *) event_rq;

  switch( DTO_Event->event_number )
  {
    case IT_DTO_RDMA_WRITE_CMPL_EVENT:
      BegLogLine( SKV_CLIENT_PROCESS_CONN_LOG )
        << "skv_client_conn_manager_if_t::ProcessSqEvent():: Write completion Event"
        << EndLogLine;
    case IT_DTO_SEND_CMPL_EVENT:
    {
      it_dto_status_t DTO_Status = DTO_Event->dto_status;

      if( DTO_Status != IT_DTO_SUCCESS )
      {
        // Flushing pending post_recvs on this connection
        BegLogLine( 1 )
          << "skv_client_conn_manager_if_t::ProcessSqEvent(): "
          << " DTO_Status: " << DTO_Status
          << EndLogLine;

        break;
      }

      skv_client_cookie_t* CliCookie = (skv_client_cookie_t *) &DTO_Event->cookie;

      skv_client_server_conn_t* Conn = CliCookie->mConn;
      skv_client_ccb_t* CCB = CliCookie->mCCB;

      BegLogLine( SKV_CLIENT_PROCESS_CONN_LOG )
        << "skv_client_conn_manager_if_t::ProcessSqEvent(): "
        << *CliCookie
        << EndLogLine;

      // in case the recv cmpl event was processed before the send
      // cmpl event, the recv handling has skipped processing of
      // overflow queue to check for pending commands. This has to be done here then
      if( CCB->CheckRecvIsComplete() )
      {
        ProcessCCB( Conn, CCB );

        // BegLogLine( SKV_CLIENT_PROCESS_SEND_RECV_RACE_LOG | 1)
        //   << "skv_client_conn_manager_if_t::ProcessSqEvent():: Recv already handled!"
        //   << " Conn: " << (void*)Conn
        //   << " CCB: " << (void*)CCB
        //   << EndLogLine;

        ProcessOverflow( Conn );
      }
      else
      {
        BegLogLine( SKV_CLIENT_PROCESS_SEND_RECV_RACE_LOG )
          << "skv_client_conn_manager_if_t::ProcessSqEvent():: Sendcmpl is first"
          << " Conn: " << (void*)Conn
          << " CCB: " << (void*)CCB
          << EndLogLine;
      }

      CCB->SetSendWasFirst();   // mark this CCB as send event processed

      break;
    }
    default:
    {
      StrongAssertLogLine( 0 )
          << "skv_client_conn_manager_if_t::ProcessSqEvent(): ERROR: "
          << " DTO_Event->event_number: " << DTO_Event->event_number
          << EndLogLine;

      break;
    }
  }

  return SKV_SUCCESS;
}

// \todo: this is the wrong place for this routine: this has to go to CCB class
//        However, this currently causes circular deps because of header structures

// copies the inbound reqest data into the response data area
// after this, the recv-buffer can be reset and abandoned/rewritten
inline void
InitializeFromResponseData( skv_client_ccb_t* aCCB,
                            skv_server_to_client_cmd_hdr_t *aInBoundHdr )
{
  BegLogLine( 0 )
    << "skv_client_conn_manager_if_t::InitializeFromResponseData():: Entering"
    << " CCB: " << (void*)aCCB
    << " aInBoundHdr: " << aInBoundHdr
    << " cmdLength: " << aInBoundHdr->GetCmdLength()
    << " cmd: " << skv_command_type_to_string( aInBoundHdr->mCmdType )
    << EndLogLine;

  StrongAssertLogLine( (aInBoundHdr->GetCmdLength() < SKV_CONTROL_MESSAGE_SIZE) && ( aCCB != NULL) )
    << "skv_client_conn_manager_if_t::InitializeFromResponseData():"
    << " ERROR: command length:" << aInBoundHdr->GetCmdLength()
    << " MAX: " << SKV_CONTROL_MESSAGE_SIZE
    << " aCCB: " << aCCB
    << EndLogLine;

  // copy all inbound data to store for later use (reused outbound buffer)
  // memcpy( aCCB->GetRecvBuff(), aInBoundHdr, aInBoundHdr->GetCmdLength() );
  aCCB->SetRecvBuff( (char*)aInBoundHdr );

#if (SKV_CTRLMSG_DATA_LOG != 0)
  HexDump CtrlMsgData( (void*)aInBoundHdr, SKV_CONTROL_MESSAGE_SIZE );
  BegLogLine( 1 )
    << "INBMSG:@"<< (void*)aInBoundHdr
    << " Data:" << CtrlMsgData
    << EndLogLine;
#endif
}


#define SKV_CLIENT_SKIP_EVENT_CHECK ( (SKV_CLIENT_RQ_EVENTS_TO_DEQUEUE_COUNT>>1) )

skv_status_t
skv_client_conn_manager_if_t::
ProcessConnectionsRqSq()
//ProcessConnectionsRqSimple()
{
  /* when turning toward rdma-placed responses we don't get recv events any more
   * instead we will poll for responses in response-slots and copy new response
   * data into the CCB buffer
   *
   * later in the optimization process we can try to remove this copy by
   * 1) move the response buffer out of the CCB and have a pointer (requires good book-keeping)
   * 2) add the response address to the header and let the server directly write the response
   *    + the whole CCB space is MR at the client anyway, so no extra registration required
   *    + no further memcpy
   *    - This makes polling more complicated since we need to poll in many places
   *    - this is also more different from future AMR-attempt
   */

  // nothing to process any more just dequeue and forget
  // events are now only for low level flow control

  // BegLogLine( SKV_CLIENT_PROCESS_CONN_LOG )
  //   << "skv_client_conn_manager_if_t::ProcessConnectionsRqSq(): Entering"
  //   << EndLogLine;

  bool empty_poll = true;
  int DequeuedEventCount;
  it_status_t status;
  static int last_server = 0;

  for( int pollLoops=0;
      ( empty_poll ) && (pollLoops < SKV_CLIENT_RESPONSE_POLL_LOOPS);
      pollLoops++ )
  {
    int server = last_server;

    do
    {
      skv_client_server_conn_t& Connection = mServerConns[ server ];
      skv_server_to_client_cmd_hdr_t *RdmaHdr;

      int commandsCount = 0;
      while( ((RdmaHdr = Connection.CheckForNewResponse()) != NULL) &&
             (commandsCount < SKV_CLIENT_RESPONSE_REAP_PER_EP) )   // run max one batch for one EP
      {
        skv_client_ccb_t *CCB = (skv_client_ccb_t *) RdmaHdr->mCmdCtrlBlk;

        StrongAssertLogLine( CCB != NULL )
          << "skv_client_conn_manager_if_t::ProcessConnectionsRqSq(): ERROR: CmdCtrlBlk = " << (void*)CCB
          << " in response hdr @" << (void*)RdmaHdr
          << " Cmd: " << RdmaHdr->mCmdType
          << EndLogLine

        InitializeFromResponseData( CCB, RdmaHdr );

        ProcessCCB( &Connection, CCB );

        RdmaHdr->Reset();
        Connection.ResponseSlotAdvance();

        ProcessOverflow( &Connection );

        ++commandsCount;
        empty_poll = false;
      }

      server = (server+1) % mServerConnCount;
    }
    while( ( server != last_server ) );

    last_server = server;
  }

  mEventLoops++;
  if( mEventLoops % SKV_CLIENT_SKIP_EVENT_CHECK == 0 )
  {
    mEventLoops = 0;

    // we don't have recv-events any more. so we don't need to check for recv events
    // status = it_evd_dequeue_n( mEvd_Rq_Hdl,
    //                            mEventsToDequeueCount,
    //                            mEvents,
    //                            & DequeuedEventCount );

    // BegLogLine( SKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG )
    //   << "skv_client_conn_manager_if_t::ProcessConnectionsRq(): "
    //   << " mEventsToDequeueCount: " << mEventsToDequeueCount
    //   << " DequeuedEventCount: " << DequeuedEventCount
    //   << EndLogLine;

    status = it_evd_dequeue_n( mEvd_Sq_Hdl,
                               mEventsToDequeueCount,
                               mEvents,
                               &DequeuedEventCount );

    BegLogLine( SKV_CLIENT_PROCESS_CONN_N_DEQUEUE_LOG )
      << "skv_client_conn_manager_if_t::ProcessConnectionsSq(): "
      << " mEventsToDequeueCount: " << mEventsToDequeueCount
      << " DequeuedEventCount: " << DequeuedEventCount
      << EndLogLine;
  }
  return SKV_SUCCESS;
}

skv_status_t
skv_client_conn_manager_if_t::
ProcessConnectionsRqSimple()
//ProcessConnectionsRq()
{
  it_event_t event_rq;
  it_status_t status = it_evd_dequeue( mEvd_Rq_Hdl,
                                       &event_rq );

  if( status == IT_SUCCESS )
  {
    // skv_status_t pstatus = ProcessRqEvent( & event_rq );

    // return pstatus;
  }

  return SKV_SUCCESS;
}
