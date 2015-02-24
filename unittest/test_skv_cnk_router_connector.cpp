/************************************************
 * Copyright (c) IBM Corp. 2015
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * it_api_cnk_router_connector_test.cpp
 *
 *  Created on: Jan 14, 2015
 *      Author: lschneid
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#define ITAPI_ENABLE_V21_BINDINGS

#ifndef FXLOG_IT_API_O_SOCKETS
#define FXLOG_IT_API_O_SOCKETS ( 1 )
#endif

#ifndef FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG
#define FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG ( 1 )
#endif

#include <FxLogger.hpp>
#include <Histogram.hpp>
#include <ThreadSafeQueue.hpp>

#include <it_api.h>
#include <it_api_o_sockets_thread.h>
#include <it_api_o_sockets_types.h>
#include <cnk_router/it_api_cnk_router_types.hpp>

#include <iwarpem_socket_access.hpp>
#include <iwarpem_types.hpp>

#define SIMULATED_CLIENTS ( 64 )

#define MAX_CONNECTIONS ( 512 )
#define                    SOCK_FD_TO_END_POINT_MAP_COUNT ( MAX_CONNECTIONS )
iWARPEM_Object_EndPoint_t* gSockFdToEndPointMap[ SOCK_FD_TO_END_POINT_MAP_COUNT ];

#include <cnk_router/it_api_cnk_router_ep.hpp>
typedef iWARPEM_Multiplexed_Endpoint_t<iWARPEM_Object_EndPoint_t> iWARPEM_Router_Endpoint_t;
#include <cnk_router/iwarpem_multiplex_ep_access.hpp>

#include <skv/client/skv_client_internal.hpp>
#include <skv/client/skv_client_conn_manager_if.hpp>

class Forwarder_Endpoint_t
{
  iWARPEM_Router_Endpoint_t *mRouterEP;

public:
  Forwarder_Endpoint_t( int aSocket = 0 )
  {
    mRouterEP = new iWARPEM_Router_Endpoint_t( aSocket );
  }
  ~Forwarder_Endpoint_t()
  {
    delete mRouterEP;
  }
  inline bool FlushMessages()
  {
    return ( IWARPEM_SUCCESS == mRouterEP->FlushSendBuffer() );
  }
  bool connect()
  {
    // send private data magic
    char *data = new char[ 1024 ];
    *(int*)data = htonl( IWARPEM_MULTIPLEXED_SOCKET_MAGIC );

    int transferred = 0;
    transferred += write( mRouterEP->GetRouterFd(), data, sizeof( int ) );
    if( transferred < sizeof( int ))
      BegLogLine( 1 )
        << "Giving up... socket can't even transmit an int... ;-)"
        << EndLogLine;

    // send router info data
    iWARPEM_Router_Info_t routerInfo;
    routerInfo.RouterID = 1;

    memcpy( data, &routerInfo, sizeof( iWARPEM_Router_Info_t ));
    transferred = 0;
    while( transferred < IWARPEM_ROUTER_INFO_SIZE )
    {
      char *d = data + transferred;
      transferred += write( mRouterEP->GetRouterFd(), d, IWARPEM_ROUTER_INFO_SIZE );
    }
    return true;
  }
  bool NewClientConnectionReq( uint32_t aIPAddr,
                               uint16_t aPort,
                               uint16_t aClientId,
                               uint32_t aServerRank,
                               uint32_t aClientGroup,
                               uint32_t aClientRank,
                               iWARPEM_Object_EndPoint_t *aClientEP )
  {
    bool rc = true;
    char data[ 1024 ];
    iWARPEM_Message_Hdr_t *msg = (iWARPEM_Message_Hdr_t*)data;
    msg->mMsg_Type = iWARPEM_SOCKET_CONNECT_REQ_TYPE;
    msg->mOpType.mSocketConnect.ipv4_address = htonl(aIPAddr);
    msg->mOpType.mSocketConnect.ipv4_port = htons( aPort );
    msg->mTotalDataLen = htonl( sizeof( iWARPEM_Private_Data_t ) );

    iWARPEM_Private_Data_t *private_data = (iWARPEM_Private_Data_t*)( &data[ sizeof( iWARPEM_Message_Hdr_t ) ] );
    private_data->mLen = htonl( IT_MAX_PRIV_DATA );
    *(uint32_t*)&( private_data->mData [sizeof(uint32_t) * 0])  =   htonl( aClientRank );
    *(uint32_t*)&( private_data->mData [sizeof(uint32_t) * 1])  =   htonl( aServerRank );
    *(uint32_t*)&( private_data->mData [sizeof(uint32_t) * 2])  =   htonl( aClientGroup );

    rc &= ( NULL != mRouterEP->AddClient( aClientId, aClientEP ) );
    rc &= ( IWARPEM_SUCCESS == mRouterEP->InsertConnectRequest( aClientId,
                                                                msg, private_data,
                                                                aClientEP ) );
    BegLogLine( 1 )
      << "Inserted ConnReq."
      << " client: " << aClientId
      << " size: " << sizeof( iWARPEM_Message_Hdr_t ) + sizeof( iWARPEM_Private_Data_t )
      << EndLogLine;
    return rc;
  }
  bool PostWriteReq( uint16_t aClientId, it_rdma_addr_t aRMRAddr, it_rmr_context_t aRMRCtx, void* aBuffer, int aLen )
  {
    char data[ 1024 ];
    iWARPEM_Message_Hdr_t *msg = (iWARPEM_Message_Hdr_t*)data;
    msg->mMsg_Type = iWARPEM_DTO_RDMA_WRITE_TYPE;
    msg->mTotalDataLen = aLen;
    msg->mOpType.mRdmaWrite.mRMRAddr = aRMRAddr;
    msg->mOpType.mRdmaWrite.mRMRContext = aRMRCtx;

    int wlen;
    struct iovec IOV[2];
    IOV[0].iov_base = msg;
    IOV[0].iov_len = sizeof( iWARPEM_Message_Hdr_t );
    IOV[1].iov_base = aBuffer;
    IOV[1].iov_len = aLen;

    return ( IWARPEM_SUCCESS == mRouterEP->InsertMessageVector( aClientId, IOV, 2, &wlen ) );
  }

  bool ProcessReceiveData()
  {
    bool ret = false;
    iWARPEM_Message_Hdr_t *Hdr;
    char *Data;
    uint16_t Client;
    do
    {
      if( mRouterEP->ExtractNextMessage( &Hdr, &Data, &Client ) != IWARPEM_SUCCESS )
        return false;

      switch( Hdr->mMsg_Type )
      {
        case iWARPEM_DTO_SEND_TYPE:
        case iWARPEM_DTO_RECV_TYPE:
        case iWARPEM_DTO_RDMA_WRITE_TYPE:
        case iWARPEM_DTO_RDMA_READ_REQ_TYPE:
        case iWARPEM_DTO_RDMA_READ_RESP_TYPE:
        case iWARPEM_DTO_RDMA_READ_CMPL_TYPE:
        case iWARPEM_DISCONNECT_REQ_TYPE:
        case iWARPEM_DISCONNECT_RESP_TYPE:

        case iWARPEM_SOCKET_CLOSE_REQ_TYPE:
          BegLogLine( 1 )
            << "This message type handling not yet implemented: " << Hdr->mMsg_Type
            << EndLogLine;
          ret = false;
          break;
        case iWARPEM_SOCKET_CONNECT_RESP_TYPE:
        {
          iWARPEM_Object_EndPoint_t *cEP = mRouterEP->GetClientEP( Client );
          cEP->ConnectedFlag = IWARPEM_CONNECTION_FLAG_CONNECTED;
          iWARPEM_Private_Data_t *priv_data = (iWARPEM_Private_Data_t*)Data;

          BegLogLine( 1 )
            << "Received private data size: " << priv_data->mLen
            << EndLogLine;

          // todo: trigger forwarding of message to client
          break;
        }

        case iWARPEM_SOCKET_CONNECT_REQ_TYPE:
          BegLogLine( 1 )
            << "This message type should never appear at the router: " << Hdr->mMsg_Type
            << EndLogLine;
          ret = false;
          break;

        default:
          BegLogLine( 1 )
            << "Unknown message type: " << Hdr->mMsg_Type
            << EndLogLine;
          ret = false;
      }
    } while ( mRouterEP->RecvDataAvailable() );
    return ret;
  }
};

struct test_connection_t
{
  int socket;
  int port;
  char addr_string[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  Forwarder_Endpoint_t *forwarder;
};



int main( int argc, char **argv )
{
  int rc = 0;

  FxLogger_Init( argv[ 0 ] );
  // get configuration to find servers
  skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

  ifstream fin( config->GetServerLocalInfoFile() );

  StrongAssertLogLine( !fin.fail() )
    << "skv_client_conn_manager_if_t::Connect():: ERROR opening server machine file: " << config->GetServerLocalInfoFile()
    << EndLogLine;

  // open sockets and connect to servers

  test_connection_t *connections = new test_connection_t[ MAX_CONNECTIONS ];
  int conn_count = 0;
  char ServerAddr[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  while( fin.getline( ServerAddr, SKV_MAX_SERVER_ADDR_NAME_LENGTH) )
  {
    char* firstspace=index(ServerAddr, ' ');
    char* PortStr=firstspace+1;
    *firstspace=0;

    strncpy( connections[ conn_count ].addr_string, ServerAddr, SKV_MAX_SERVER_ADDR_NAME_LENGTH );
    connections[ conn_count ].port = atoi( PortStr );

    struct addrinfo *entries;
    struct addrinfo hints;

    bzero( &hints, sizeof( addrinfo ));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    getaddrinfo( ServerAddr, PortStr, &hints, &entries );
    struct addrinfo *srv = entries;

    bool connected = false;
    while ( srv != NULL )
    {
      connections[conn_count].socket = socket( AF_INET, SOCK_STREAM, 0 );
      if( connections[conn_count].socket <= 0 )
      {
        BegLogLine( 1 )
          << "Error creating socket# " << conn_count << " errno=" << errno
          << EndLogLine;
        rc = errno;
      }

      connected = ( connect( connections[ conn_count ].socket, srv->ai_addr, srv->ai_addrlen ) == 0 );
      if ( connected )
        break;

      close( connections[ conn_count ].socket );
      srv = srv->ai_next;
    }

    freeaddrinfo( entries );

    if( connected )
    {
      connections[ conn_count ].forwarder = new Forwarder_Endpoint_t( connections[ conn_count ].socket );
      conn_count++;
    }
    else
    {
      BegLogLine( 1 )
        << "Cannot connect to server: " << ServerAddr << ":" << PortStr
        << EndLogLine;
      rc = -1;
    }
  }

  Forwarder_Endpoint_t *fwd = connections[ 0 ].forwarder;
  fwd->connect();

  // create a virtual client
  for( int n=0; n<SIMULATED_CLIENTS; n++ )
  {
    iWARPEM_Object_EndPoint_t *clientEP = new iWARPEM_Object_EndPoint_t();
    clientEP->ClientId = n;
    fwd->NewClientConnectionReq( 0x7f000001,
                                 0x1234,
                                 n,
                                 0, 0, n*64,
                                 clientEP );

  }

  // just here to mimic progress (we need a "real" mechanism to flush for good coalescing)
  fwd->FlushMessages();

  // recv/process responses on the socket
  fwd->ProcessReceiveData();

  // exchange some data?... tough without a real client...
  char data[ 1024 ];
  uint64_t rmraddr = 0;
  uint64_t rmrctx = 0;
  sprintf( data, "Hello World." );
  fwd->PostWriteReq( 43, rmraddr, rmrctx, data, 128 );

  // just here to mimic progress (we need a "real" mechanism to flush for good coalescing)
  fwd->FlushMessages();

  // disconnect the virtual client

  // close sockets
  sleep( 3 );
  for( int i=0; i< conn_count; i++ )
  {
    close( connections[ i ].socket );
  }

  return rc;
}
