/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

#ifndef WITH_CNK_ROUTER
#define WITH_CNK_ROUTER
#endif

#include <mpi.h>
#include <FxLogger.hpp>
#include <Histogram.hpp>
#include <ThreadSafeQueue.hpp>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <list>
#include <queue>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <it_api_o_sockets_router.h>
extern "C"
{
#define ITAPI_ENABLE_V21_BINDINGS
#include <it_api.h>
//#include "ordma_debug.h"
};
#include <it_api_o_sockets_thread.h>
#include <it_api_o_sockets_types.h>

#include <iwarpem_socket_access.hpp>
#include <iwarpem_types.hpp>

#define IT_API_MAX_ROUTER_SOCKETS ( 128 )

class Crossbar_Entry_t;
#include <cnk_router/it_api_cnk_router_ep.hpp>
typedef iWARPEM_Multiplexed_Endpoint_t<Crossbar_Entry_t> iWARPEM_Router_Endpoint_t;

typedef std::list<iWARPEM_Router_Endpoint_t*> Uplink_list_t;
Uplink_list_t gUplinkList;

iWARPEM_Router_Endpoint_t *gEPfdMap[ IT_API_MAX_ROUTER_SOCKETS ];
static uint16_t gClientIdMap[ IT_API_MAX_ROUTER_SOCKETS ];

#undef offsetof
#ifdef __compiler_offsetof
#define offsetof(TYPE,MEMBER) __compiler_offsetof(TYPE,MEMBER)
#else
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

//struct allCallBuffer
//  {
//    struct callBuffer callBuffer[k_CallBufferCount] ;
//  };
//
//static struct allCallBuffer allCallBuffer __attribute__((aligned(32)));

enum optype {
  k_wc_recv ,
  k_wc_uplink ,
  k_wc_downlink ,
  k_wc_ack ,
  k_wc_downlink_complete ,
  k_wc_downlink_complete_return
};

struct oprec_recv {

};

struct oprec_send {

} ;

struct oprec_downlink {

};

struct oprec_ack {

};

struct oprec_downlink_complete {

};

struct oprec_downlink_complete_return {

};
union oprec {
  struct oprec_recv ;
  struct oprec_send ;
  struct oprec_downlink ;
  struct oprec_ack ;
  struct oprec_downlink_complete ;
  struct oprec_downlink_complete_return ;
};

struct connection ;

struct endiorec {
  struct connection * conn ;
  enum optype optype ;
  union oprec oprec ;
};

class Crossbar_Entry_t
{
  uint16_t mClientId;
  int mServerId;  // LocalEndpointIndex
  iWARPEM_Router_Endpoint_t *mUpLink;
  struct connection *mDownLink;

public:
  Crossbar_Entry_t( uint16_t aClientId = IWARPEM_INVALID_CLIENT_ID,
                    int aServerId = IWARPEM_INVALID_SERVER_ID,
                    iWARPEM_Router_Endpoint_t *aUpLink = NULL,
                    struct connection *aDownLink = NULL )
  : mClientId( aClientId ), mServerId( aServerId )
  {
    mUpLink = aUpLink;
    mDownLink = aDownLink;
    BegLogLine( 1 )
      << "Creating new Crossbar_Entry. "
      << " client: " << aClientId
      << " server: " << aServerId
      << EndLogLine;
  }
  ~Crossbar_Entry_t() {}

  inline
  uint16_t getClientId() const
  {
    return mClientId;
  }

  inline
  void setClientId( uint16_t clientId )
  {
    mClientId = clientId;
  }

  inline
  const struct connection* getDownLink() const
  {
    return mDownLink;
  }

  inline
  void setDownLink( const struct connection* downLink )
  {
    mDownLink = (struct connection*)downLink;
  }

  inline
  int getServerId() const
  {
    return mServerId;
  }

  inline
  void setServerId( int serverId )
  {
    mServerId = serverId;
  }

  inline
  const iWARPEM_Router_Endpoint_t* getUpLink() const
  {
    return mUpLink;
  }

  inline
  void setUpLink( const iWARPEM_Router_Endpoint_t* upLink )
  {
    mUpLink = (iWARPEM_Router_Endpoint_t*)upLink;
  }
};

struct iWARPEM_MultiplexedSocketControl_Hdr_t
{
  iWARPEM_SocketControl_Type_t mOpType;
  iWARPEM_Router_Endpoint_t *mServerEP;
};

#define WORKREQNUM 40000

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#ifndef FXLOG_ITAPI_ROUTER
#define FXLOG_ITAPI_ROUTER ( 0 )
#endif
#ifndef FXLOG_ITAPI_ROUTER_LW
#define FXLOG_ITAPI_ROUTER_LW ( 0 )
#endif
#ifndef FXLOG_ITAPI_ROUTER_SPIN
#define FXLOG_ITAPI_ROUTER_SPIN ( 0 )
#endif
#ifndef FXLOG_ITAPI_ROUTER_EPOLL_SPIN
#define FXLOG_ITAPI_ROUTER_EPOLL_SPIN ( 0 )
#endif

//const int BUFFER_SIZE = SENDSIZE;
static int received = 0;

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;
  pthread_t cq_poller_thread;
};

enum {
  k_LocalEndpointCount = 64
};
//static pthread_mutex_t allConnectionMutex ;
struct connection {
  unsigned long ibv_post_send_count ;
  unsigned long ibv_post_recv_count ;
  unsigned long ibv_poll_cq_send_count ;
  unsigned long ibv_poll_cq_recv_count ;
  unsigned int ibv_send_last_optype ;
  struct ibv_qp *qp;
//  pthread_mutex_t qp_write_mutex ;
//  struct ibv_mr *recv_mr;
//  struct ibv_mr *send_mr;
//  struct ibv_mr *call_mr ;
  struct ibv_mr *mr ;
  struct endiorec endio_call ;
  struct endiorec endio_uplink ;
  struct endiorec endio_downlink ;
  struct endiorec endio_ack ;
  struct endiorec endio_downlink_complete ;
  struct endiorec endio_downlink_complete_return ;
  size_t upstream_length ;
  size_t downstream_length ;
  unsigned long localDownstreamSequence ;
  unsigned long downstream_sequence ;
  unsigned long downstreamSequenceAtCall ;
  unsigned long sequence_in ;
//  unsigned long sequence_out ;
  unsigned long upstreamSequence ;
//  int downstreamBufferFree ;
  int rpcBufferPosted ;
  uint64_t routerBuffer_raddr ;
  uint32_t routerBuffer_rkey ;
  unsigned int clientRank ;
//  unsigned long stuck_epoll_count ;
  int will_flush_this_connection ;
  int flushed_downstream ;
  struct connection * next_conn_to_flush ;
  int issuedDownstreamFetch ;
//  unsigned int downlink_buffer_busy ;


  struct routerBuffer routerBuffer ;
  uint16_t clientId;

  Crossbar_Entry_t *socket_fds[k_LocalEndpointCount];
  // The following 2 items for debugging which SKVserver sends us a bad header
  unsigned int upstream_ip_address[k_LocalEndpointCount] ;
  unsigned short upstream_ip_port[k_LocalEndpointCount] ;
};

static struct connection * first_connection_to_flush ;

//static int stuck(const struct connection *conn)
//  {
//    return conn->stuck_epoll_count > 10000 ;
//  }

static void die(const char *reason);
static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn, int rcount);
static void post_all_call_buffers(struct connection *conn);
static void post_call_buffer(struct connection *conn, volatile struct callBuffer *callBuffer) ;
static void register_memory(struct connection *conn);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void wc_stat_echo(struct ibv_wc *wc);
static struct context *s_ctx = NULL;

static void setup_polling_thread(void) ;
static int ConnectToServers( int aMyRank );

enum {
  k_ListenQueueLength=64
};
int main(int argc, char **argv)
{
  struct sockaddr_in addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = k_IONPort;
  int rc = 0;
  MPI_Init( &argc, &argv );
  int Rank;
  int NodeCount;
  MPI_Comm_rank( MPI_COMM_WORLD, &Rank );
  MPI_Comm_size( MPI_COMM_WORLD, &NodeCount );
  FxLogger_Init( argv[ 0 ], Rank );

  if (argc==2) {
      port = atoi(argv[1]);
      if (port==0) {
          printf("Argument Error, setting port to default\n");
          fflush(stdout) ;
          port = k_IONPort;
      }
  }

//  pthread_mutex_init(&allConnectionMutex, NULL) ;

  setup_polling_thread() ;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  // initialize the clientid map
  for( int n=0; n<IT_API_MAX_ROUTER_SOCKETS; n++ )
    gClientIdMap[ n ] = IWARPEM_INVALID_CLIENT_ID;

  // connect the forwarder to all servers
  rc = ConnectToServers( Rank );
  if( rc != 0 )
  {
    printf("Error: Could not create connections to SKV Servers. rc = %d\n", rc);
    exit( rc );
  }

  ec = rdma_create_event_channel();
  rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP);
  rc = rdma_bind_addr(listener, (struct sockaddr *)&addr);
  if (rc != 0) {
      printf("Error: Could not bind to port %i\n", port);
      fflush(stdout) ;
      exit(rc);
  }

  rdma_listen(listener, k_ListenQueueLength);
  printf("RDMA server on port %d\n", port);
  fflush(stdout) ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << " RDMA server on port " << port
    << EndLogLine ;

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  MPI_Finalize() ;

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void wc_stat_echo(struct ibv_wc *wc)
{
    printf("Status: %u Opcode: %u\n", wc->status, wc->opcode);
    fflush(stdout) ;
}

enum {
  k_CompletionQueueSize = 128
};

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, k_CompletionQueueSize, NULL, s_ctx->comp_channel, 0));
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 128;
  qp_attr->cap.max_recv_wr = 128;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

static void issue_ack(struct connection *conn)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *)conn
        << " conn->upstreamSequence=" << conn->upstreamSequence
        << EndLogLine ;
    ((struct rpcAckBuffer *)(&conn->routerBuffer.ackBuffer)) -> upstreamSequence = conn->upstreamSequence ;

    struct ibv_send_wr wr2;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge2;
    memset(&wr2, 0, sizeof(wr2));

    wr2.wr_id = (uintptr_t) &(conn->endio_ack ) ;
    wr2.opcode = IBV_WR_RDMA_WRITE;
    wr2.sg_list = &sge2;
    wr2.num_sge = 1;
    wr2.send_flags = IBV_SEND_SIGNALED;

    sge2.addr = (uintptr_t)&(conn->routerBuffer.ackBuffer);
//              sge.addr = 0 ; // tjcw as suggested by Bernard
    sge2.length = sizeof(struct ackBuffer) ;
    sge2.lkey = conn->mr->lkey ;

    wr2.wr.rdma.remote_addr = conn->routerBuffer_raddr ;
//              wr.wr.rdma.remote_addr = 0 ; // tjcw as suggested by Bernard
    wr2.wr.rdma.rkey = conn->routerBuffer_rkey ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " wr2.wr_id=0x" << (void *) wr2.wr_id
      << " qp=0x" << (void *) conn->qp
      << " sge2.addr=0x" << (void *) sge2.addr
      << " RDMA-ing to wr2.wr.rdma.remote_addr=0x" << (void *) wr2.wr.rdma.remote_addr
      << " wr2.wr.rdma.rkey=0x" << (void *) wr2.wr.rdma.rkey
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << " conn->upstreamSequence=" << conn->upstreamSequence
      << EndLogLine ;

//    conn->sequence_out += 1 ;
    StrongAssertLogLine(conn->rpcBufferPosted != 0 )
      << "RPC buffer is not posted"
      << EndLogLine ;

//     pthread_mutex_lock((pthread_mutex_t *) &conn->qp_write_mutex) ;
//     pthread_mutex_lock(&allConnectionMutex) ;
     conn->ibv_post_send_count += 1 ;
     conn->ibv_send_last_optype = k_wc_ack ;
     int rc=ibv_post_send(conn->qp, &wr2, &bad_wr);
//     pthread_mutex_unlock(&allConnectionMutex) ;
//     pthread_mutex_unlock((pthread_mutex_t *) &conn->qp_write_mutex) ;
     StrongAssertLogLine(rc == 0)
       << "ibv_post_send fails, rc=" << rc
       << EndLogLine ;
//    TEST_NZ(ibv_post_send(conn->qp, &wr2, &bad_wr));

  }
static void process_call(struct connection *conn, size_t byte_len)
  {
    struct rpcBuffer *rpcBuffer = (struct rpcBuffer *)&(conn->routerBuffer.callBuffer) ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "byte_len=" << byte_len
        << " conn=0x" << (void *) conn
        << " conn->sequence_in=" << conn->sequence_in
        << " rpcBuffer->routerBuffer_addr=0x" << (void *) rpcBuffer->routerBuffer_addr
        << " rpcBuffer->routermemreg_lkey=0x" << (void *) rpcBuffer->routermemreg_lkey
        << " rpcBuffer->upstreamBufferLength=" << rpcBuffer->upstreamBufferLength
        << " rpcBuffer->downstreamSequence=" << rpcBuffer->downstreamSequence
        << " rpcBuffer->clientRank=" << rpcBuffer->clientRank
        << EndLogLine ;
    conn->downstreamSequenceAtCall = rpcBuffer->downstreamSequence ;

    if ( conn->sequence_in == 1)
      {
        conn->routerBuffer_raddr = rpcBuffer->routerBuffer_addr ;
        conn->routerBuffer_rkey = rpcBuffer->routermemreg_lkey ;
        conn->clientRank = rpcBuffer->clientRank ;
      }
    size_t upstreamBufferLength = rpcBuffer->upstreamBufferLength ;
    conn->upstream_length = upstreamBufferLength ;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t) &(conn->endio_uplink) ;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = NULL ;

    sge.addr = (uintptr_t)&(conn->routerBuffer.upstreamBuffer);
    sge.length = upstreamBufferLength ;
    sge.lkey = conn->mr->lkey ;

    wr.wr.rdma.remote_addr = conn->routerBuffer_raddr + offsetof(struct routerBuffer, upstreamBuffer) ;
//    wr.wr.rdma.remote_addr = conn->routerBuffer_raddr + sizeof(struct ackBuffer) + sizeof(struct callBuffer);
//              wr.wr.rdma.remote_addr = 0 ; // tjcw as suggested by Bernard
    wr.wr.rdma.rkey = conn->routerBuffer_rkey ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " wr.wr_id=0x" << (void *) wr.wr_id
      << " qp=0x" << (void *) conn->qp
      << " sge.addr=0x" << (void *) sge.addr
      << " RDMA-ing from wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
      << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << EndLogLine ;

//    pthread_mutex_lock((pthread_mutex_t *) &conn->qp_write_mutex) ;
//    pthread_mutex_lock(&allConnectionMutex) ;
    conn->ibv_post_send_count += 1 ;
    conn->ibv_send_last_optype = k_wc_uplink ;
    int rc=ibv_post_send(conn->qp, &wr, &bad_wr);
//    pthread_mutex_unlock(&allConnectionMutex) ;
//    pthread_mutex_unlock((pthread_mutex_t *) &conn->qp_write_mutex) ;
    StrongAssertLogLine(rc == 0)
      << "ibv_post_send fails, rc=" << rc
      << EndLogLine ;
//    conn->sequence_out += 1 ;

  }

unsigned int
socket_nodelay_on( int fd )
  {
    int one = 1 ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Setting NODELAY for socket " << fd
      << EndLogLine ;
    int rc=setsockopt(fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) ;
    if ( rc != 0 )
      {
        BegLogLine(1)
            << "Bad return from setsockopt fd=" << fd
            << " errno=" << errno
            << EndLogLine ;
      }
    return 0 ;
  }
#if 0
static
inline
unsigned int
write_to_socket( int sock, char * buff, int len, int* wlen )
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "Writing to FD=" << sock
        << " buff=" << (void *) buff
        << " length=" << len
        << EndLogLine ;
    int BytesWritten = 0;
    for( ; BytesWritten < len; )
    {
    int write_rc = write(   sock,
                            (((char *) buff) + BytesWritten ),
                            len - BytesWritten );
    if( write_rc < 0 )
      {
  // printf( "errno: %d\n", errno );
  if( errno == EAGAIN )
    continue;
  else if ( errno == ECONNRESET )
    {
      return 1;
    }
  else
    StrongAssertLogLine( 0 )
      << "write_to_socket:: ERROR:: "
      << "failed to write to file: " << sock
            << " buff: " << (void *) buff
            << " len: " << len
      << " errno: " << errno
      << EndLogLine;
      }

    BytesWritten += write_rc;
    }

  *wlen = BytesWritten;

#if IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL
  gBandOutStat.AddBytes( BytesWritten );
#endif

  return 0;
  }
static
inline
unsigned int
write_to_socket_writev( int sock, struct iovec *iov, int iov_count, int* wlen )
{
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Writing to FD=" << sock
    << " iovec=" << (void *) iov
    << " iov_count=" << iov_count
    << EndLogLine ;
writev_retry:
  int write_rc = writev(sock,iov,iov_count) ;
  if( write_rc < 0 )
  {
    switch( errno )
    {
      case EAGAIN:
        goto writev_retry;
      case ECONNRESET:
      case EPIPE: // This is likely to be that upstream has already closed the socket
        *wlen = write_rc ;
        return 1;
      default:
        StrongAssertLogLine( 0 )
          << "write_to_socket:: ERROR:: "
          << "failed to write to file: " << sock
          << " iovec: " << (void *) iov
          << " iov_count: " << iov_count
          << " errno: " << errno
          << EndLogLine;
    }
  }
  *wlen = write_rc ;

#if IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL
  gBandOutStat.AddBytes( write_rc );
#endif

  return 0;
}
static
inline
unsigned int
read_from_socket( int sock, char * buff, int len, int* rlen )
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "Reading from FD=" << sock
        << " buff=" << (void *) buff
        << " length=" << len
        << EndLogLine ;
    int BytesRead = 0;
    int ReadCount = 0;

    for(; BytesRead < len; )
      {
  int read_rc = read(   sock,
            (((char *) buff) + BytesRead ),
            len - BytesRead );
  if( read_rc < 0 )
    {
      // printf( "errno: %d\n", errno );
      if( errno == EAGAIN || errno == EINTR )
        continue;
      else if ( errno == ECONNRESET )
        {
          BegLogLine(FXLOG_ITAPI_ROUTER)
              << "ECONNRESET, BytesRead=" << BytesRead
              << EndLogLine ;
          *rlen = BytesRead;
          return 1;
        }
      else
        StrongAssertLogLine( 0 )
          << "ERROR:: failed to read from file: " << sock
          << " errno: " << errno
          << " buff=" << (void *) buff
          << " " << (long) buff
          << " length=" << len
          << EndLogLine;
    }
  else if( read_rc == 0 )
    {
      BegLogLine(FXLOG_ITAPI_ROUTER)
          << "Connection closed, BytesRead=" << BytesRead
          << EndLogLine ;
      *rlen = BytesRead;
      return 1;
    }

  ReadCount++;

  BytesRead += read_rc;
  }

  *rlen = BytesRead;


  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Read completes, BytesRead=" << BytesRead
    << EndLogLine ;
  return 0;
}
#endif
struct epoll_record
  {
  iWARPEM_Router_Endpoint_t *conn ;
  int fd ;
  };
int epoll_fd ;
int drc_serv_socket ;
int new_drc_serv_socket ;
int drc_cli_socket;

enum {
  k_max_epoll = 4097
};

//struct iWARPEM_SocketControl_Hdr_t
//{
//  iWARPEM_SocketControl_Type_t mOpType;
//  int                          mSockFd;
//  struct connection *mconn ;
//  unsigned int mLocalEndpointIndex ;
//};

static void add_socket_to_poll(iWARPEM_Router_Endpoint_t *conn) ;
static void remove_socket_from_poll(struct connection *conn, unsigned int LocalEndpointIndex, int fd) ;

static void process_control_message(const struct iWARPEM_MultiplexedSocketControl_Hdr_t & SocketControl_Hdr)
  {
    iWARPEM_SocketControl_Type_t OpType = SocketControl_Hdr.mOpType ;
    iWARPEM_Router_Endpoint_t *ServerEP = SocketControl_Hdr.mServerEP;
    int                          SockFd = ServerEP->GetRouterFd() ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
     << "OpType=" << OpType
     << " SockFd=" << SockFd
     << " EP=" << (void*)ServerEP
     << EndLogLine ;
    struct epoll_event EP_Event;
    EP_Event.events = EPOLLIN;
    if ( OpType == IWARPEM_SOCKETCONTROL_TYPE_ADD)
      {
        struct epoll_record * epoll_record = (struct epoll_record *) malloc(sizeof(struct epoll_record)) ;
        epoll_record->conn = ServerEP ;
        epoll_record->fd = SockFd ;
        EP_Event.data.ptr = ( void *) epoll_record ;
        BegLogLine(FXLOG_ITAPI_ROUTER)
            << "Adding socket " << SockFd
            << " to poll"
            << EndLogLine ;

        int epoll_ctl_rc = epoll_ctl( epoll_fd,
              EPOLL_CTL_ADD,
              SockFd,
              & EP_Event );

        StrongAssertLogLine( epoll_ctl_rc == 0 )
          << "epoll_ctl() failed"
          << " errno: " << errno
          << EndLogLine;

      }
    else if ( OpType == IWARPEM_SOCKETCONTROL_TYPE_REMOVE)
      {
        BegLogLine(FXLOG_ITAPI_ROUTER)
            << "Removing socket " << SockFd
            << " from poll"
            << EndLogLine ;

        int epoll_ctl_rc = epoll_ctl( epoll_fd,
              EPOLL_CTL_DEL,
              SockFd,
              & EP_Event );
        // tjcw: This leaks the store associated with the struct epoll_record

        // The EPOLL_CTL_DEL can come back with ENOENT if the file descriptor has already been taken out of
        // the poll by a close from upstream
        StrongAssertLogLine( epoll_ctl_rc == 0 || errno==ENOENT )
          << "epoll_ctl() failed"
          << " errno: " << errno
          << EndLogLine;

        int rc=close(SockFd) ;
        StrongAssertLogLine(rc == 0)
          << "EP=0x" << (void *) ServerEP
          << " close(" << SockFd
          << ") fails, errno=" << errno
          << EndLogLine ;

      }
    else StrongAssertLogLine(0)
        << "Unknown OpType=" << OpType
        << EndLogLine ;

  }

static unsigned int wait_for_downstream_buffer_count ;
static unsigned int wait_for_downstream_buffer_spin_count ;
static void wait_for_downstream_buffer(struct connection * conn)
  {
    unsigned long localDownstreamSequence=conn->localDownstreamSequence ;
    unsigned long downstream_sequence = conn->downstream_sequence ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " localDownstreamSequence=" << localDownstreamSequence
        << " downstream_sequence=" << downstream_sequence
        << EndLogLine ;
    if ( localDownstreamSequence == downstream_sequence)
      {
        wait_for_downstream_buffer_spin_count += 1 ;
        // Spin here until the verbs completion handler increments the localDownstreamSequence
        while ( localDownstreamSequence == downstream_sequence)
          {
            localDownstreamSequence=*(volatile unsigned long *)&(conn->localDownstreamSequence) ;
          }
      }
    wait_for_downstream_buffer_count += 1 ;
    if ( wait_for_downstream_buffer_count >= 4096)
      {
        BegLogLine(1)
            << "Buffer spin " << wait_for_downstream_buffer_spin_count
            << "/4096"
            << EndLogLine ;
        wait_for_downstream_buffer_spin_count = 0 ;
        wait_for_downstream_buffer_count = 0 ;
      }
    StrongAssertLogLine( localDownstreamSequence == downstream_sequence+1 )
      << "conn=0x" << (void *) conn
      << " localDownstreamSequence=" << localDownstreamSequence
      << " downstream_sequence=" << downstream_sequence
      << EndLogLine ;
    conn->downstream_sequence=downstream_sequence+1 ;
  }

static void fetch_downstreamCompleteBuffer(struct connection *conn)
  {
//    size_t downstreaml=((struct downstreamLength *)(&((conn->routerBuffer).downstreamCompleteBuffer)))->downstreaml ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
//        << " downstreaml=" << downstreaml
        << EndLogLine ;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t) & (conn->endio_downlink_complete_return) ;
    wr.opcode = IBV_WR_RDMA_READ ;
    wr.sg_list = &sge ;
    wr.num_sge = 1 ;
    wr.send_flags = IBV_SEND_SIGNALED ;
    wr.next = NULL ;

    struct downstreamSequence* downstreamSequence=(struct downstreamSequence*)(conn->routerBuffer.downstreamCompleteBuffer.downstreamCompleteBufferElement) ;
//    unsigned long old_sequence=downstreamSequence->sequence ;

//    downstreamSequence->sequence=0xffffffffffffffffUL ;
//    BegLogLine(FXLOG_ITAPI_ROUTER)
//      << "Trampling sequence from 0x" << (void *) old_sequence
//      << " to 0x" << (void *) downstreamSequence->sequence
//      << EndLogLine ;

//    ((struct downstreamSequence*)(conn->routerBuffer.downstreamCompleteBuffer.downstreamCompleteBufferElement))->sequence = 0xffffffffffffffffULL ;
    sge.addr = (uintptr_t) downstreamSequence ;
    sge.length = sizeof(struct downstreamCompleteBuffer) ;
    sge.lkey = conn->mr->lkey ;

    wr.wr.rdma.remote_addr = conn->routerBuffer_raddr
        + offsetof(struct routerBuffer, downstreamCompleteBuffer) ;
//        + sizeof(struct ackBuffer) + sizeof(struct callBuffer) + sizeof(struct upstreamBuffer) + sizeof(struct downstreamBuffer);
    wr.wr.rdma.rkey = conn->routerBuffer_rkey ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " wr.wr_id=0x" << (void *) wr.wr_id
      << " qp=0x" << (void *) conn->qp
      << " sge.addr=0x" << (void *) sge.addr
      << " RDMA-ing from wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
      << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
      << " sge.length=" << sge.length
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << EndLogLine ;

//    conn->sequence_out += 1 ;
    conn->issuedDownstreamFetch=1 ;
//    pthread_mutex_lock((pthread_mutex_t *) &conn->qp_write_mutex) ;
//    pthread_mutex_lock(&allConnectionMutex) ;
    conn->ibv_send_last_optype = k_wc_downlink_complete_return ;
    conn->ibv_post_send_count += 1 ;
    int rc=ibv_post_send(conn->qp, &wr, &bad_wr);
//    pthread_mutex_unlock(&allConnectionMutex) ;
//    pthread_mutex_unlock((pthread_mutex_t *) &conn->qp_write_mutex) ;
    StrongAssertLogLine(rc == 0)
      << "ibv_post_send fails, rc=" << rc
      << EndLogLine ;
  }
static void send_downstream_given_buffer_free(struct connection *conn) ;
static void send_downstream(struct connection *conn, size_t length)
  {
    static int send_downstream_count ;
    static int send_downstream_without_fetching_count ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " length=" << length
        << EndLogLine ;
    conn->downstream_length = length ;
    unsigned long conn_downstream_sequence = conn->downstream_sequence ;
    BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
        << "conn=0x" << (void *) conn
        << " conn_downstream_sequence=" << conn_downstream_sequence
        << EndLogLine ;
    StrongAssertLogLine(conn->downstreamSequenceAtCall <= conn->downstream_sequence)
      << "Downstream has run ahead, conn->downstreamSequenceAtCall=" << conn->downstreamSequenceAtCall
      << " conn_downstream_sequence=" << conn_downstream_sequence
      << EndLogLine ;
    if ( conn->downstreamSequenceAtCall == conn_downstream_sequence )
        {
        BegLogLine(FXLOG_ITAPI_ROUTER)
          << "conn=0x" << (void *) conn
          << " clientRank=" << conn->clientRank
          << " conn_downstream_sequence=" << conn_downstream_sequence
          << " buffer free on remote, sending downstream, length=" << conn->downstream_length
          << EndLogLine ;
//          *downstreamlP = conn->downstream_length ;
          struct downstreamLength * downstreamLengthP = (struct downstreamLength *)&((conn->routerBuffer).downstreamLengthBuffer) ;
          downstreamLengthP->sequence=conn->downstream_sequence ;
          downstreamLengthP->length=conn->downstream_length ;
          send_downstream_without_fetching_count += 1 ;
          send_downstream_given_buffer_free(conn) ;
      }
    else
      {
        fetch_downstreamCompleteBuffer(conn) ;
      }
    send_downstream_count += 1 ;
    if ( send_downstream_count >= 4096 )
      {
        BegLogLine(1)
            << "Sends downstream without fetching downstream sequence " << send_downstream_without_fetching_count
            << "/4096"
            << EndLogLine  ;
        send_downstream_count=0 ;
        send_downstream_without_fetching_count=0 ;
      }
  }
static void send_downstream_given_buffer_free(struct connection *conn) ;

static void send_downstream_if_buffer_free(struct connection *conn)
  {
    static unsigned int send_downstream_if_buffer_free_count ;
    static unsigned int send_downstream_if_buffer_free_retry_count ;
    struct downstreamSequence* downstreamSequence=(struct downstreamSequence*)(conn->routerBuffer.downstreamCompleteBuffer.downstreamCompleteBufferElement) ;
    volatile unsigned long *downstreamSequenceP = (volatile unsigned long *)&(downstreamSequence->sequence) ;
    unsigned long downstreamSequenceNumber =*downstreamSequenceP ;
    unsigned long conn_downstream_sequence = conn->downstream_sequence ;
    BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
        << "conn=0x" << (void *) conn
        << " downstreamlSequenceP=0x" << (void *) downstreamSequenceP
        << " downstreamSequenceNumber=" << downstreamSequenceNumber
        << " conn_downstream_sequence=" << conn_downstream_sequence
        << EndLogLine ;
    StrongAssertLogLine(downstreamSequenceNumber <= conn->downstream_sequence)
      << "Downstream has run ahead, downstreamSequenceNumber=" << downstreamSequenceNumber
      << " conn_downstream_sequence=" << conn_downstream_sequence
      << EndLogLine ;
    if ( downstreamSequenceNumber == conn_downstream_sequence )
        {
        BegLogLine(FXLOG_ITAPI_ROUTER)
          << "conn=0x" << (void *) conn
          << " clientRank=" << conn->clientRank
          << " downstreamlSequenceP=0x" << (void *) downstreamSequenceP
          << " downstreamSequenceNumber=" << downstreamSequenceNumber
          << " conn_downstream_sequence=" << conn_downstream_sequence
          << " buffer free on remote, sending downstream, length=" << conn->downstream_length
          << " downstreamSequence=0x" << (void *)downstreamSequenceNumber
          << EndLogLine ;
//          *downstreamlP = conn->downstream_length ;
          struct downstreamLength * downstreamLengthP = (struct downstreamLength *)&((conn->routerBuffer).downstreamLengthBuffer) ;
          downstreamLengthP->sequence=conn->downstream_sequence ;
          downstreamLengthP->length=conn->downstream_length ;
          send_downstream_given_buffer_free(conn) ;
//          BegLogLine(FXLOG_ITAPI_ROUTER_LW)
//            << "conn=" << conn
//            << " clientRank=" << conn->clientRank
//            << " downstreamlSequenceP=0x" << (void *) downstreamSequenceP
//            << " downstreamSequenceNumber=" << downstreamSequenceNumber
//            << " conn_downstream_sequence=" << conn_downstream_sequence
//            << " buffer free on remote, sending downstream, length=" << conn->downstream_length
//            << " downstreamSequence=0x" << (void *)downstreamSequenceNumber
//            << EndLogLine ;
        }
    else
      {
        BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
            << "Buffer not free on remote, retrying, conn=0x" << (void *) conn
            << " downstreamSequenceNumber=" << downstreamSequenceNumber
            << " conn_downstream_sequence=" << conn_downstream_sequence
            << EndLogLine ;
        StrongAssertLogLine(conn_downstream_sequence == downstreamSequenceNumber+1)
          << "Downstream sequencing error, conn_downstream_sequence=" << conn_downstream_sequence
          << " downstreamSequenceNumber=" << downstreamSequenceNumber
          << EndLogLine ;
        send_downstream_if_buffer_free_retry_count += 1 ;
        fetch_downstreamCompleteBuffer(conn) ;
      }
    send_downstream_if_buffer_free_count += 1 ;
    if ( send_downstream_if_buffer_free_count >= 4096 )
      {
        BegLogLine(1)
            << "Downstream buffer retries " << send_downstream_if_buffer_free_retry_count
            << "/4096"
            << EndLogLine ;
        send_downstream_if_buffer_free_count = 0 ;
        send_downstream_if_buffer_free_retry_count = 0 ;
      }
  }
static void indicate_downlink_complete(struct connection *conn)
  {
    struct ibv_send_wr wr2;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge2;
    memset(&wr2, 0, sizeof(wr2));
    wr2.wr_id = (uintptr_t) & (conn->endio_downlink_complete) ;
    wr2.opcode = IBV_WR_RDMA_WRITE ;
    wr2.sg_list = &sge2 ;
    wr2.num_sge = 1 ;
    wr2.send_flags = IBV_SEND_SIGNALED ;
    wr2.next = NULL ;

    sge2.addr = (uintptr_t) &(conn->routerBuffer.downstreamLengthBuffer) ;
    sge2.length = sizeof(struct downstreamLengthBuffer) ;
    sge2.lkey = conn->mr->lkey ;

    wr2.wr.rdma.remote_addr = conn->routerBuffer_raddr + offsetof(struct routerBuffer, downstreamLengthBuffer) ;
//        + sizeof(struct ackBuffer) + sizeof(struct callBuffer) + sizeof(struct upstreamBuffer) + sizeof(struct downstreamBuffer);
    wr2.wr.rdma.rkey = conn->routerBuffer_rkey ;

    struct downstreamLength * downstreamLength=(struct downstreamLength *)&(conn->routerBuffer.downstreamLengthBuffer) ;
    BegLogLine(FXLOG_ITAPI_ROUTER_LW)
      << "conn=0x" << (void *) conn
      << " clientRank=" << conn->clientRank
      << " length=" << downstreamLength->length
      << " sequence=" << downstreamLength->sequence
      << EndLogLine ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " qp=0x" << (void *) conn->qp
      << " wr2.wr_id=0x" << (void *) wr2.wr_id
      << " sge2.addr=0x" << (void *) sge2.addr
      << " sge2.length=" << sge2.length
      << " RDMA-ing to wr2.wr.rdma.remote_addr=0x" << (void *) wr2.wr.rdma.remote_addr
      << " wr2.wr.rdma.rkey=0x" << (void *) wr2.wr.rdma.rkey
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << EndLogLine ;

//    conn->sequence_out += 2 ;
//    pthread_mutex_lock((pthread_mutex_t *) &conn->qp_write_mutex) ;
//    pthread_mutex_lock(&allConnectionMutex) ;
    conn->ibv_send_last_optype = k_wc_downlink_complete ;
    conn->ibv_post_send_count += 1 ;
    int rc=ibv_post_send(conn->qp, &wr2, &bad_wr);
//    pthread_mutex_unlock(&allConnectionMutex) ;
//    pthread_mutex_unlock((pthread_mutex_t *) &conn->qp_write_mutex) ;
    StrongAssertLogLine(rc == 0)
      << "ibv_post_send fails, rc=" << rc
      << EndLogLine ;

  }
static void send_downstream_given_buffer_free(struct connection *conn)
  {
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge;
    size_t length = conn->downstream_length ;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t) &(conn->endio_downlink ) ;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = NULL ;

    sge.addr = (uintptr_t)&(conn->routerBuffer.downstreamBuffer);
    sge.length = sizeof(struct downstreamBuffer) ;
    sge.lkey = conn->mr->lkey ;

    wr.wr.rdma.remote_addr = conn->routerBuffer_raddr + offsetof(struct routerBuffer, downstreamBuffer) ;
//        + sizeof(struct ackBuffer) + sizeof(struct callBuffer) + sizeof(struct upstreamBuffer);
    wr.wr.rdma.rkey = conn->routerBuffer_rkey ;



    BegLogLine(FXLOG_ITAPI_ROUTER_LW)
      << "conn=0x" << (void *) conn
      << " clientRank=" << conn->clientRank
      << " wr.wr_id=0x" << (void *) wr.wr_id
      << " qp=0x" << (void *) conn->qp
      << " sge.addr=0x" << (void *) sge.addr
      << " RDMA-ing to wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
      << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << EndLogLine ;

//    conn->sequence_out += 2 ;
//    pthread_mutex_lock((pthread_mutex_t *) &conn->qp_write_mutex) ;
//    pthread_mutex_lock(&allConnectionMutex) ;
    conn->ibv_send_last_optype = k_wc_downlink ;
    conn->ibv_post_send_count += 1 ;
    int rc=ibv_post_send(conn->qp, &wr, &bad_wr);
//    pthread_mutex_unlock(&allConnectionMutex) ;
//    pthread_mutex_unlock((pthread_mutex_t *) &conn->qp_write_mutex) ;
    StrongAssertLogLine(rc == 0)
      << "ibv_post_send fails, rc=" << rc
      << EndLogLine ;

  }

static
inline
iWARPEM_Status_t
RecvRaw( iWARPEM_Router_Endpoint_t *rEP, char *buff, int len, int* wlen )
{
  iWARPEM_Status_t status = IWARPEM_SUCCESS;
  uint16_t client;
  status = rEP->ExtractRawData( buff, len, wlen, &client );
  return status;
}


// Point the connection at the start of its downlink buffer
static void rewind_downstream_buffer(struct connection *conn)
  {
    char * downstreamBufferElementPtr = (char *)(conn->routerBuffer.downstreamBuffer.downstreamBufferElement) ;
    unsigned long * downstream_sequence_ptr = (unsigned long *)downstreamBufferElementPtr ;
    size_t * downstream_length_ptr = (size_t *) downstream_sequence_ptr+1 ;
//    conn->downstream_sequence += 1 ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Downstream sequence number=" << conn->downstream_sequence
      << " conn=0x" << (void *) conn
      << EndLogLine ;
    *downstream_sequence_ptr = conn->downstream_sequence ;
//    size_t * downstream_length_ptr = (size_t *) downstream_sequence_ptr+1 ;
//    conn->downstream_sequence += 1 ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Downstream sequence number=" << conn->downstream_sequence
      << " conn=0x" << (void *) conn
      << EndLogLine ;
    conn->downstream_length = 2*sizeof(unsigned long) ;

  }
static void add_conn_to_flush_queue(struct connection *conn)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " will_flush_this_connection=" <<  conn->will_flush_this_connection
      << EndLogLine ;
    if ( 0 == conn->will_flush_this_connection)
      {
        conn->will_flush_this_connection=1 ;
        conn->next_conn_to_flush = first_connection_to_flush ;
        first_connection_to_flush = conn ;
      }
  }
static void flush_downstream(struct connection *conn)
  {
    unsigned long * downstream_sequence_ptr = (unsigned long *)(conn->routerBuffer.downstreamBuffer.downstreamBufferElement) ;
    size_t * downstream_length_ptr = (size_t *) downstream_sequence_ptr+1 ;
    *downstream_length_ptr = conn->downstream_length ;
    conn->flushed_downstream = 1 ;
    send_downstream(conn,  conn->downstream_length ) ;
  }

static void flush_all_downstream(void)
  {
    struct connection *conn=first_connection_to_flush ;
    first_connection_to_flush=NULL ;
    while (conn != NULL )
      {
        flush_downstream(conn) ;
        conn->will_flush_this_connection = 0 ;
        struct connection *next_conn = conn->next_conn_to_flush ;
        conn->next_conn_to_flush=NULL ;
        conn=next_conn ;
      }
  }


static void queue_downstream(struct connection *conn,unsigned int LocalEndpointIndex, const struct iWARPEM_Message_Hdr_t &rHdr, const char *rData)
  {
    size_t TotalDataLen = rHdr.mTotalDataLen ;
    if(conn->flushed_downstream)
      {
        BegLogLine(FXLOG_ITAPI_ROUTER)
            << "conn=0x" << (void *) conn
            << " has been flushed"
            << EndLogLine ;
        conn->flushed_downstream=0 ;
        wait_for_downstream_buffer(conn) ;
        rewind_downstream_buffer(conn) ;
      }
    if ( conn->downstream_length + sizeof(LocalEndpointIndex) + sizeof(rHdr) + TotalDataLen > k_DownstreamBufferSize)
      {
        BegLogLine(FXLOG_ITAPI_ROUTER)
            << "conn=0x" << (void *) conn
            << " buffer fills, flushing all downstream"
            << EndLogLine ;
        flush_all_downstream() ;
      }
    if(conn->flushed_downstream)
      {
        BegLogLine(FXLOG_ITAPI_ROUTER)
            << "conn=0x" << (void *) conn
            << " has been flushed"
            << EndLogLine ;
        conn->flushed_downstream=0 ;
        wait_for_downstream_buffer(conn) ;
        rewind_downstream_buffer(conn) ;
      }
    StrongAssertLogLine(conn->downstream_length + sizeof(LocalEndpointIndex) + sizeof(rHdr) + TotalDataLen <= k_DownstreamBufferSize)
      << "conn=0x" << (void *) conn
      << " message length=" << TotalDataLen
      << " overflows buffer, downstream_length=" <<conn->downstream_length
      << EndLogLine ;

    char * downstreamBufferElementPtr = (char *)(conn->routerBuffer.downstreamBuffer.downstreamBufferElement) ;
    unsigned long * downstream_sequence_ptr = (unsigned long *)downstreamBufferElementPtr ;
    size_t * downstream_length_ptr = (size_t *) downstream_sequence_ptr+1 ;
    unsigned int * LocalEndpointIndex_ptr = (unsigned int *)(downstreamBufferElementPtr+conn->downstream_length) ;
    struct iWARPEM_Message_Hdr_t * Hdr = (struct iWARPEM_Message_Hdr_t *) (LocalEndpointIndex_ptr+1) ;
//    *(unsigned int *) conn->routerBuffer.downstreamBuffer.downstreamBufferElement = LocalEndpointIndex ;
    char * message = (char *) (Hdr+1) ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " downstream_length=" << conn->downstream_length
      << " placing LocalEndpointIndex at 0x" << (void *) LocalEndpointIndex_ptr
      << " Hdr at 0x" << (void *) Hdr
      << " message at 0x" << (void *) message
      << EndLogLine ;

    *LocalEndpointIndex_ptr = LocalEndpointIndex;
    *Hdr = rHdr ;
//    memcpy( Hdr, &rHdr, sizeof( iWARPEM_Message_Hdr_t ) );
    memcpy( message, rData, TotalDataLen );
    conn->downstream_length += sizeof(unsigned int) + sizeof(struct iWARPEM_Message_Hdr_t) + TotalDataLen ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " downstream_length=" << conn->downstream_length
      << EndLogLine ;
    add_conn_to_flush_queue(conn) ;
  }

static int process_downlink( iWARPEM_Router_Endpoint_t *aServerEP )
  {
    StrongAssertLogLine( aServerEP != NULL )
      << " invalid router endpoint"
      << EndLogLine ;
    uint16_t client;
    iWARPEM_Msg_Type_t msg_type = aServerEP->GetNextMessageType( &client );

    StrongAssertLogLine(msg_type != iWARPEM_UNKNOWN_REQ_TYPE)
      << "Indicated iWARPEM_UNKNOWN_REQ_TYPE, peer has probably gone down, no recovery."
      << EndLogLine ;

    Crossbar_Entry_t *cb = aServerEP->GetClientEP( client );
    StrongAssertLogLine( cb != NULL )
      << "client=" << client
      << " has no crossbar entry"
      << EndLogLine ;

    struct connection *conn = (struct connection*)cb->getDownLink();

    if (msg_type <= 0 || msg_type > iWARPEM_SOCKET_CLOSE_REQ_TYPE )
      {
        BegLogLine(1)
          << "LocalHdr.mMsg_Type=" << msg_type
          << " Upstream IP address=0x" << (void *) conn->upstream_ip_address[ client ]
          << " port=" << conn->upstream_ip_port[ client ]
          << ". Hanging for diagnosis"
          << EndLogLine ;
        printf("mMsg_Type is out of range, hanging for diagnosis\n") ;
        fflush(stdout) ;
        for (;;) { sleep(1) ; }
      }

// Currently we spin in wait_for_downstream_buffer()
//    if ( conn->localDownstreamSequence == conn->downstream_sequence)
//      {
//        BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
//            << "conn=0x" << (void *) conn
//            << " at the moment because downstream buffer is busy. Will try again later."
//            << EndLogLine ;
//        gDownstreamWaitqueue.push( aServerEP );
//        conn->stuck_epoll_count += 1 ;
//        if(stuck(conn))
//        {
//          struct downstreamSequence* downstreamSequence=(struct downstreamSequence*)(conn->routerBuffer.downstreamCompleteBuffer.downstreamCompleteBufferElement) ;
//          volatile unsigned long *downstreamSequenceP = (volatile unsigned long *)&(downstreamSequence->sequence) ;
//          unsigned long downstreamSequenceNumber =*downstreamSequenceP ;
//          unsigned long conn_downstream_sequence = conn->downstream_sequence ;
//          BegLogLine(FXLOG_ITAPI_ROUTER_LW)
//            << "conn=0x" << (void *) conn
//            << " clientRank=" << conn->clientRank
//            << " downstreamlSequenceP=0x" << (void *) downstreamSequenceP
//            << " downstreamSequenceNumber=" << downstreamSequenceNumber
//            << " conn_downstream_sequence=" << conn_downstream_sequence
//            << " issuedDownstreamFetch=" << conn->issuedDownstreamFetch
//            << " ibv_post_recv_count=" << conn->ibv_post_recv_count
//            << " ibv_post_send_count=" << conn->ibv_post_send_count
//            << " ibv_poll_cq_recv_count=" << conn->ibv_poll_cq_recv_count
//            << " ibv_poll_cq_send_count=" << conn->ibv_poll_cq_send_count
//            << " ibv_send_last_optype=" << conn->ibv_send_last_optype
//            << EndLogLine ;
//          StrongAssertLogLine(downstreamSequenceNumber <= conn->downstream_sequence)
//            << "Downstream has run ahead, downstreamSequenceNumber=" << downstreamSequenceNumber
//            << " conn_downstream_sequence=" << conn_downstream_sequence
//            << EndLogLine ;
//        }
//        return 1;
//      }
//    if(stuck(conn))
//    {
//      BegLogLine(FXLOG_ITAPI_ROUTER_LW)
//        << "conn=0x" << (void *) conn
//        << " clientRank=" << conn->clientRank
//        << " now progressing, stuck_epoll_count was " << conn->stuck_epoll_count
//        << EndLogLine ;
//    }
//    conn->stuck_epoll_count = 0 ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " client=" << client
        << EndLogLine ;
    struct iWARPEM_Message_Hdr_t *rHdr;
    char *rData;

    iWARPEM_Status_t status = aServerEP->ExtractNextMessage( &rHdr, &rData, &client );

    if( status == IWARPEM_SUCCESS )
    {
      BegLogLine( 0 )
        << "Forwarding downstream of client " << client
        << " type: " << iWARPEM_Msg_Type_to_string( rHdr->mMsg_Type )
        << " len: " << rHdr->mTotalDataLen
        << EndLogLine;
//      wait_for_downstream_buffer(conn) ;
//      rewind_downstream_buffer(conn) ;
      queue_downstream(conn,cb->getServerId(), *rHdr,rData) ;

//      flush_downstream(conn) ;

      // -> mostly diagnosis and aftermath
      // Set up a local header to diagnose if Hdr is beibng trampled by an RDMA read
      // tjcw: If it is, this doesn't really solve the exposure
      size_t TotalDataLen = rHdr->mTotalDataLen ;
      if (TotalDataLen > sizeof(struct downstreamBuffer)-(2*sizeof(unsigned long) + sizeof(unsigned int) + sizeof(struct iWARPEM_Message_Hdr_t)) )
      {
        BegLogLine(1)
          << "LocalHdr.mTotalDataLen=" << rHdr->mTotalDataLen
          << " too large for buffer. mMsg_Type=" << iWARPEM_Msg_Type_to_string(msg_type)
          << " Upstream IP address=0x" << (void *) conn->upstream_ip_address[ client ]
          << " port=" << conn->upstream_ip_port[ client ]
          << EndLogLine ;
        printf("mTotalDataLen is too large for buffer, hanging for diagnosis\n") ;
        fflush(stdout) ;
        for (;;) { sleep(1) ; }
      }
      StrongAssertLogLine(TotalDataLen <= sizeof(struct downstreamBuffer)-(sizeof(unsigned long) + sizeof(unsigned int) + sizeof(struct iWARPEM_Message_Hdr_t)))
        << "TotalDataLen=" << TotalDataLen
        << " too large for buffer. mMsg_Type=" << iWARPEM_Msg_Type_to_string(msg_type)
        << " Upstream IP address=0x" << (void *) conn->upstream_ip_address[ client ]
        << " port=" << conn->upstream_ip_port[ client ]
        << EndLogLine ;
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << conn
        << " mMsg_Type="<< iWARPEM_Msg_Type_to_string(msg_type)
        << " TotalDataLen=" << TotalDataLen
        << EndLogLine ;
      report_hdr(*rHdr) ;
//      AssertLogLine(msg_type==Hdr->mMsg_Type && TotalDataLen == Hdr->mTotalDataLen)
//        << "Hdr trampled, Hdr->mMsg_Type=" << Hdr->mMsg_Type
//        << " Hdr->mTotalDataLen=" << Hdr->mTotalDataLen
//        << " should be mMsg_Type=" << msg_type
//        << " and TotalDataLen=" << TotalDataLen
//        << EndLogLine ;
    }
    else
    {
      // TODO: This needs to send a close signal over virtual socket instead of removing
      // the whole socket from epoll
      aServerEP->CloseAllClients();  // needs more work...

      int fd = aServerEP->GetRouterFd();
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " LocalEndpointIndex=" << cb->getServerId()
        << " close from upstream. Removing socket " << fd
        << " from poll. This will cause the downstream client to lose activation."
        << EndLogLine ;

      struct epoll_event EP_Event;
      EP_Event.events = EPOLLIN;
      // This will result in a duplicate EPOLL_CTL_DEL when we get a close from downstream, but
      // that doesn't seem to matter
      int epoll_ctl_rc = epoll_ctl( epoll_fd,
                                    EPOLL_CTL_DEL,
                                    fd,
                                    & EP_Event );

      StrongAssertLogLine( epoll_ctl_rc == 0 )
        << "epoll_ctl() failed"
        << " errno: " << errno
        << EndLogLine;

      // Synthesise a 'close' and send downstream
      struct iWARPEM_Message_Hdr_t Hdr ;

//      wait_for_downstream_buffer(conn) ;
//      rewind_downstream_buffer(conn) ;
      Hdr.mMsg_Type = iWARPEM_SOCKET_CLOSE_TYPE ;
      Hdr.mTotalDataLen = 0 ;
      queue_downstream(conn,cb->getServerId(), Hdr,NULL) ;

//      flush_downstream(conn) ;

//      *downstream_length_ptr = 2*sizeof(unsigned long) + sizeof(unsigned int) + sizeof(struct iWARPEM_Message_Hdr_t) ;
//      send_downstream(conn, 2*sizeof(unsigned long) + sizeof(unsigned int) + sizeof(struct iWARPEM_Message_Hdr_t)) ;

    }
    return 0;
  }
static int epoll_wait_handling_eintr(int epoll_fd, struct epoll_event *events, int events_size, int timeout)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
      << "epoll_wait(" << epoll_fd
      << ",...)"
      << EndLogLine ;
    int nfds = epoll_wait(epoll_fd, events, events_size, timeout ) ;
    while ( nfds == -1 && errno == EINTR)
      {
        BegLogLine(FXLOG_ITAPI_ROUTER)
          << "epoll_wait returns, nfds=" << nfds
          << " errno=" << errno
          << ". Retrying"
          << EndLogLine ;
        nfds = epoll_wait(epoll_fd, events, events_size, timeout ) ;
      }
    return nfds ;
  }
void * polling_thread(void *arg)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "Polling starting"
        << EndLogLine ;
    struct epoll_record * drc_serv_record = (struct epoll_record *) malloc(sizeof(struct epoll_record)) ;
    drc_serv_record->conn = NULL ;
    drc_serv_record->fd=new_drc_serv_socket ;
    struct epoll_event ev ;
    ev.events = EPOLLIN ;
    ev.data.ptr = ( void *) drc_serv_record ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Adding fd=" << new_drc_serv_socket
      << " to epoll"
      << EndLogLine ;
    int rc=epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_drc_serv_socket, &ev) ;
    AssertLogLine(rc == 0 )
      << "epoll_ctl failed, errno=" << errno
      << EndLogLine ;

    for(;;)
      {
        struct epoll_event events[k_max_epoll] ;
        // Handle all the data in the sockets before we flush all downstream
        int nfds = epoll_wait_handling_eintr(epoll_fd, events, k_max_epoll, -1) ;
//        if ( 0 == nfds)
//          {
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//              << "epoll_wait returns, nfds=0, flushing all downstream"
//              << EndLogLine ;
//            flush_all_downstream() ;
//            nfds = epoll_wait_handling_eintr(epoll_fd, events, k_max_epoll, -1) ;
//          }

        AssertLogLine(nfds != -1)
          << "epoll_wait failed, errno=" << errno
          << EndLogLine ;
        BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
          << "epoll_wait returns, nfds=" << nfds
          << EndLogLine ;
        for ( int n=0;n<nfds; n+=1)
          {
            struct epoll_record * ep = (struct epoll_record *)events[n].data.ptr ;
            uint32_t epoll_events = events[n].events ;
//            AssertLogLine((! (epoll_events & EPOLLERR)) && (! (epoll_events & EPOLLHUP)))
//              << "Error or hangup, 0x" << (void *) epoll_events
//              << EndLogLine ;
            iWARPEM_Router_Endpoint_t *conn = ep->conn ;
            int fd=ep->fd ;
            BegLogLine(FXLOG_ITAPI_ROUTER_EPOLL_SPIN)
              << "conn=0x" << (void *) conn
              << " fd=" << fd
              << EndLogLine ;
            if ( conn == NULL)
              {
              StrongAssertLogLine( fd == new_drc_serv_socket )
                << "Receiving data on an unknown and/or unconnected socket. Cannot proceed."
                << " fd= " << fd
                << " expected ctrl_fd= " << new_drc_serv_socket
                << EndLogLine;

              iWARPEM_MultiplexedSocketControl_Hdr_t ControlHdr;
              int rlen;
              int istatus = read_from_socket( fd,
                                              (char *) & ControlHdr,
                                              sizeof( iWARPEM_MultiplexedSocketControl_Hdr_t ),
                                              & rlen );
              process_control_message(ControlHdr);

              }
            else
              {
                int status;
                do
                {
                  status = process_downlink( conn ) ;
                } while ( (status == 0) && conn->RecvDataAvailable() );

              }
// This logic now done in individual queue_downstreams
//            while( !gDownstreamWaitqueue.empty() )
//            {
//              conn = gDownstreamWaitqueue.front();
//              gDownstreamWaitqueue.pop();
//              int status;
//              do
//              {
//                status = process_downlink( conn ) ;
//              } while ( (status == 0) && conn->RecvDataAvailable() );
//            }
          }
        flush_all_downstream() ;
      }

    return NULL ;
  }
static void setup_polling_thread(void)
  {
    epoll_fd=epoll_create(4097) ;
    AssertLogLine(epoll_fd >= 0 )
      << "epoll_create failed, errno=" << errno
      << EndLogLine ;
    struct sockaddr_in   drc_serv_addr;
    bzero( (char *) &drc_serv_addr, sizeof( drc_serv_addr ) );
    drc_serv_addr.sin_family      = AF_INET;
    drc_serv_addr.sin_port        = htons( 0 );
    drc_serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int drc_serv_addr_len         = sizeof( drc_serv_addr );


    struct sockaddr * drc_serv_saddr = (struct sockaddr *)& drc_serv_addr;

    drc_serv_socket = socket(AF_INET, SOCK_STREAM, 0) ;
    StrongAssertLogLine( drc_serv_socket >= 0 )
      << " Failed to create Data Receiver Control socket "
      << " Errno " << errno
      << EndLogLine;

    int True = 1;
    setsockopt( drc_serv_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&True, sizeof( True ) );

    int brc= bind( drc_serv_socket, drc_serv_saddr, drc_serv_addr_len ) ;
    StrongAssertLogLine(brc == 0 )
      << "bind failed, errno=" << errno
      << EndLogLine ;

    // Get the server port to connect on
    int gsnrc;
    socklen_t drc_serv_addrlen = sizeof( drc_serv_addr );
    if( (gsnrc = getsockname(drc_serv_socket, drc_serv_saddr, &drc_serv_addrlen)) != 0)
      {
      perror("getsockname()");
      close( drc_serv_socket );

      StrongAssertLogLine( 0 ) << EndLogLine;
      }

    int drc_serv_port = drc_serv_addr.sin_port;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "after getsockname(): "
      << " drc_serv_port: " << drc_serv_port
      << EndLogLine;

    struct sockaddr_in   drc_cli_addr;

    bzero( (void *) & drc_cli_addr, sizeof( struct sockaddr_in ) );
    drc_cli_addr.sin_family      = AF_INET;
    drc_cli_addr.sin_port        = drc_serv_port;
    drc_cli_addr.sin_addr.s_addr = *(unsigned int *)(gethostbyname( "localhost" )->h_addr);
  //  drc_cli_addr.sin_addr.s_addr = *(unsigned long *)(gethostbyname( "127.0.0.1" )->h_addr);
  //  drc_cli_addr.sin_addr.s_addr = 0x7f000001;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << " drc_cli_addr.sin_family: " << drc_cli_addr.sin_family
      << " drc_cli_addr.sin_port: " << drc_cli_addr.sin_port
      << " drc_cli_addr.sin_addr.s_addr: " << HexDump(&drc_cli_addr.sin_addr.s_addr,sizeof(drc_cli_addr.sin_addr.s_addr))
      << " drc_cli_addr.sin_addr.s_addr: " << drc_cli_addr.sin_addr.s_addr
      << EndLogLine;

    int drc_cli_addr_len       = sizeof( drc_cli_addr );
    drc_cli_socket = socket(AF_INET, SOCK_STREAM, 0) ;
    StrongAssertLogLine( drc_cli_socket >= 0 )
      << "ERROR: "
      << " Failed to create Data Receiver Control socket "
      << " Errno " << errno
      << EndLogLine;

    /*************************************************/


    if( listen( drc_serv_socket, 5 ) < 0 )
      {
        perror( "listen failed" );

        StrongAssertLogLine( 0 ) << EndLogLine;

        exit( -1 );
      }
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Listening"
      << EndLogLine ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Before connect()"
      << " drc_client_socket: " << drc_cli_socket
      << EndLogLine;

    while( 1 )
      {
        int conn_rc = connect( drc_cli_socket,
             (struct sockaddr *) & drc_cli_addr,
             sizeof( drc_cli_addr ) );
        int err=errno ;
        BegLogLine(FXLOG_ITAPI_ROUTER)
          << "conn_rc=" << conn_rc
          << " errno=" << errno
          << EndLogLine ;

        if( conn_rc == 0 ) break;
        else if( conn_rc < 0 )
        {
          if( errno != EAGAIN )
            {
              perror( "connect failed" );
              StrongAssertLogLine( 0 )
                << "Error after connect(): "
                << " errno: " << err
                << " conn_rc: " << conn_rc
                << EndLogLine;
            }
        }
      }

    struct sockaddr_in   drc_serv_addr_tmp;
    socklen_t drc_serv_addr_tmp_len = sizeof( drc_serv_addr_tmp );
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Before accept()"
      << EndLogLine ;

    new_drc_serv_socket = accept( drc_serv_socket,
            (struct sockaddr *) & drc_serv_addr_tmp,
                                    & drc_serv_addr_tmp_len );

    StrongAssertLogLine( new_drc_serv_socket > 0 )
      << "after accept(): "
      << " errno: " << errno
      << EndLogLine;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "new_drc_serv_socket=" << new_drc_serv_socket
      << EndLogLine ;

    pthread_t DataReceiverTID ;
    int rc = pthread_create( & DataReceiverTID,
                             NULL,
                             polling_thread,
                             NULL );
    StrongAssertLogLine(rc == 0 )
      << "pthread_create rc=" << rc
      << EndLogLine ;

  }
static void add_socket_to_poll(iWARPEM_Router_Endpoint_t *conn )
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " fd=" << conn->GetRouterFd()
      << EndLogLine ;
    iWARPEM_MultiplexedSocketControl_Hdr_t SocketControl ;
    SocketControl.mOpType=IWARPEM_SOCKETCONTROL_TYPE_ADD ;
    SocketControl.mServerEP=conn;
    int rc=write(drc_cli_socket,(void *)&SocketControl,sizeof(SocketControl)) ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "write(" << drc_cli_socket
      << "," << (void *)&SocketControl
      << "," << sizeof(SocketControl)
      << ") returns rc=" << rc
      << EndLogLine ;
    StrongAssertLogLine(rc == sizeof(SocketControl))
      << "Wrong length write, errno=" << errno
      << EndLogLine ;

  }
static void remove_socket_from_poll_and_close( iWARPEM_Router_Endpoint_t *aServerEP, int fd)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "EP=0x" << (void *)aServerEP
      << " fd=" << aServerEP->GetRouterFd()
      << EndLogLine ;
    iWARPEM_MultiplexedSocketControl_Hdr_t SocketControl ;
    SocketControl.mOpType=IWARPEM_SOCKETCONTROL_TYPE_REMOVE ;
    SocketControl.mServerEP=aServerEP ;
    int rc=write(drc_cli_socket,(void *)&SocketControl,sizeof(SocketControl)) ;
    StrongAssertLogLine(rc == sizeof(SocketControl))
      << "Wrong length write, errno=" << errno
      << EndLogLine ;
  }
static
iWARPEM_Router_Endpoint_t*
LookUpServerEP( const struct iWARPEM_SocketConnect_t &aSocketConnect )
{
  if( gUplinkList.empty() )
    return NULL;

  Uplink_list_t::iterator ServerEP = gUplinkList.begin();
  while( ServerEP != gUplinkList.end() )
  {
    // BegLogLine( FXLOG_ITAPI_ROUTER && ( ServerEP != NULL ) )
    //   << "Comparing routerEP: " << index
    //   << " IP:Port[ " << (void*)ServerEP->GetRouterInfoPtr()->SocketInfo.ipv4_address
    //   << " : " << (void*)ServerEP->GetRouterInfoPtr()->SocketInfo.ipv4_port
    //   << " ] with request: [ " << (void*)aSocketConnect.ipv4_address
    //   << " : " << (void*)aSocketConnect.ipv4_port
    //   << " ] "
    //   << EndLogLine;

    if( ( (*ServerEP) != NULL ) &&
        ( (*ServerEP)->GetRouterInfoPtr()->SocketInfo.ipv4_address == aSocketConnect.ipv4_address ) &&
        ( (*ServerEP)->GetRouterInfoPtr()->SocketInfo.ipv4_port == aSocketConnect.ipv4_port ) )
      return (*ServerEP);
    ServerEP++;
  }
  return NULL;
}

static void open_socket_send_private_data( struct connection *conn,
                                           unsigned int LocalEndpointIndex,
                                           const struct iWARPEM_SocketConnect_t &SocketConnect,
                                           const iWARPEM_Private_Data_t & PrivateData )
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " LocalEndpointIndex=" << LocalEndpointIndex
        << " ipv4_address=0x" << (void*)SocketConnect.ipv4_address
        << " ipv4_port=" << SocketConnect.ipv4_port
        << EndLogLine ;
    StrongAssertLogLine(LocalEndpointIndex < k_LocalEndpointCount)
      << "LocalEndpointIndex=" << LocalEndpointIndex
      << " is too large"
      << EndLogLine ;
    // Record the upstream address for debug
    conn->upstream_ip_address[LocalEndpointIndex] = SocketConnect.ipv4_address ;
    conn->upstream_ip_port[LocalEndpointIndex] = SocketConnect.ipv4_port ;

    iWARPEM_Message_Hdr_t Hdr;
    Hdr.mMsg_Type = iWARPEM_SOCKET_CONNECT_REQ_TYPE;
    Hdr.mOpType.mSocketConnect.ipv4_address = SocketConnect.ipv4_address;
    Hdr.mOpType.mSocketConnect.ipv4_port = SocketConnect.ipv4_port;
    Hdr.mTotalDataLen = sizeof( iWARPEM_Private_Data_t );

    // server endpoint lookup
    iWARPEM_Router_Endpoint_t *ServerEP = LookUpServerEP( SocketConnect );

    // create the crossbar link entry to do forward and reverse lookup on events
    Crossbar_Entry_t *cb = new Crossbar_Entry_t( conn->clientId, LocalEndpointIndex, ServerEP, conn );

    ServerEP->AddClient( conn->clientId, cb );
    conn->socket_fds[ LocalEndpointIndex ] = cb ;

    if( ServerEP != NULL )
      ServerEP->InsertConnectRequest( conn->clientId, &Hdr, &PrivateData, cb );
  }
static void close_socket(struct connection *conn, unsigned int LocalEndpointIndex)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " LocalEndpointIndex=" << LocalEndpointIndex
        << EndLogLine ;
    StrongAssertLogLine(LocalEndpointIndex < k_LocalEndpointCount)
      << "LocalEndpointIndex=" << LocalEndpointIndex
      << " is too large"
      << EndLogLine ;

    Crossbar_Entry_t *cb = conn->socket_fds[LocalEndpointIndex];
    iWARPEM_Router_Endpoint_t *ServerEP = (iWARPEM_Router_Endpoint_t*)cb->getUpLink();

//    ServerEP->InsertDisconnectMessage();
    ServerEP->RemoveClient( cb->getClientId() ) ;

    conn->socket_fds[LocalEndpointIndex] = NULL ;
    delete cb;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Removed client " << LocalEndpointIndex
      << " from ServerEP and ClientEP lists."
      << EndLogLine ;
  }
static void drain_cq(void) ;

static inline
void FlushMarkedUplinks()
{
  int flushedEPs = 0;
  static uint32_t flushcounts = 0;
  static double flushavg = 1.0;

  Uplink_list_t::iterator ServerEP = gUplinkList.begin();
  while( ServerEP != gUplinkList.end() )
  {
    if( (*ServerEP)->NeedsFlush() )
    {
      (*ServerEP)->FlushSendBuffer();
      flushedEPs++;
      drain_cq() ;
    }
    ServerEP++;
  }

  flushavg = (flushavg*0.995) + (flushedEPs*0.005);
  if( ((flushcounts++) & 0xfff) == 0 )
  {
    BegLogLine( 1 )
      << "average flushed uplinks: " << flushavg
      << EndLogLine;
  }
}
static void send_upstream( const iWARPEM_Router_Endpoint_t *aServerEP,
                           const uint16_t aClientId,
                           const struct iWARPEM_Message_Hdr_t& Hdr,
                           const void * message)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "EP=0x" << (void *)aServerEP
        << " clientId=" << aClientId
        << EndLogLine ;
    StrongAssertLogLine( aServerEP->IsValidClient( aClientId ) )
      << "clientId=" << aClientId
      << " is too large"
      << EndLogLine ;

    BegLogLine( Hdr.mMsg_Type == iWARPEM_UNKNOWN_REQ_TYPE )
      << "Sending unknown message type upstream."
      << " client: " << aClientId
      << EndLogLine;

    struct iovec iov[2] ;
    iov[0].iov_base = ( void *) &Hdr ;
    iov[0].iov_len =  sizeof(iWARPEM_Message_Hdr_t) ;
    iov[1].iov_base = ( void *) message ;
    iov[1].iov_len = Hdr.mTotalDataLen ;
    int wlen ;
    iWARPEM_Status_t status = ((iWARPEM_Router_Endpoint_t*)aServerEP)->InsertMessageVector( aClientId, iov, 2, &wlen );

    AssertLogLine( (status == IWARPEM_SUCCESS) && (wlen == sizeof(iWARPEM_Message_Hdr_t) + Hdr.mTotalDataLen ))
      << "Message insertion error on EP: " << aServerEP->GetRouterFd()
      << " rc=" << status
      << " wlen=" << wlen
      << " expected=" << sizeof(iWARPEM_Message_Hdr_t) + Hdr.mTotalDataLen
      << EndLogLine ;
  }
static void process_uplink_element(struct connection *conn, unsigned int LocalEndpointIndex, const struct iWARPEM_Message_Hdr_t& Hdr, const void * message)
  {
    iWARPEM_Msg_Type_t Msg_Type=Hdr.mMsg_Type ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "LocalEndpointIndex=" << LocalEndpointIndex
      << " Msg_Type=" << iWARPEM_Msg_Type_to_string(Msg_Type)
      << " Hdr.mTotalDataLen=" << Hdr.mTotalDataLen
      << " message=0x" << (void *) message
      << EndLogLine ;
    if ( Msg_Type == iWARPEM_SOCKET_CONNECT_REQ_TYPE)
      {
        open_socket_send_private_data(conn,LocalEndpointIndex,Hdr.mOpType.mSocketConnect,*(const iWARPEM_Private_Data_t *)message);
      }
    else if ( Msg_Type == iWARPEM_SOCKET_CLOSE_REQ_TYPE)
      {
        close_socket(conn,LocalEndpointIndex) ;
      }
    else
      {
        StrongAssertLogLine(Msg_Type >= iWARPEM_DTO_SEND_TYPE && Msg_Type <= iWARPEM_DISCONNECT_RESP_TYPE)
            << "Hdr.mMsg_Type=" << Msg_Type
            << " is out of range"
            << EndLogLine ;
        Crossbar_Entry_t *cb = conn->socket_fds[ LocalEndpointIndex ];
        send_upstream( cb->getUpLink(), cb->getClientId(), Hdr, message) ;
      }
  }
enum {
  k_LazyFlushUplinks = 1
};
static void process_uplink(struct connection *conn)
  {
//    BegLogLine(FXLOG_ITAPI_ROUTER)
//        << "byte_len=" << byte_len
//        << EndLogLine ;
//    struct rpcBuffer *rpcBuffer = (struct rpcBuffer *)&(conn->routerBuffer.callBuffer) ;
    struct upstreamBuffer * upstreamBuffer = (struct upstreamBuffer *)&(conn->routerBuffer.upstreamBuffer) ;
    unsigned long * upstreamSequenceP = (unsigned long *) upstreamBuffer ;
    unsigned long upstreamSequence = * upstreamSequenceP ;
    unsigned long conn_upstreamSequence=conn->upstreamSequence ;
    size_t byte_len=conn->upstream_length ;
//    StrongAssertLogLine(conn->upstream_length == byte_len)
//      << "conn=0x" << (void *) conn
//      << " conn->upstream_length=" << conn->upstream_length
//      << " disagrees with byte_len=" << byte_len
//      << " (conn->upstreamSequence=" << conn_upstreamSequence
//      << " upstreamSequence=" << upstreamSequence
//      << ")"
//      << EndLogLine ;
    StrongAssertLogLine(conn->upstreamSequence == upstreamSequence)
      << "conn=0x" << (void *) conn
      << " conn->upstreamSequence=" << conn_upstreamSequence
      << " disagrees with upstreamSequence=" << upstreamSequence
      << EndLogLine ;
    conn->upstreamSequence=conn_upstreamSequence+1 ;
    size_t bufferIndex = sizeof(unsigned long) ;

    while ( bufferIndex < byte_len )
      {
        unsigned int LocalEndpointIndex = * (unsigned int *) (upstreamBuffer->upstreamBufferElement+bufferIndex) ;
        bufferIndex += sizeof(unsigned int) ;
        struct iWARPEM_Message_Hdr_t * Hdr = (struct iWARPEM_Message_Hdr_t *) (upstreamBuffer->upstreamBufferElement+bufferIndex);
        size_t nextBufferIndex = bufferIndex + sizeof(struct iWARPEM_Message_Hdr_t) + Hdr->mTotalDataLen ;
        StrongAssertLogLine(nextBufferIndex <= byte_len )
          << "Message ovflows buffer, Hdr->mTotalDataLen=" << Hdr->mTotalDataLen
          << " nextBufferIndex=" << nextBufferIndex
          << " byte_len=" << byte_len
          << EndLogLine ;
        process_uplink_element(conn,LocalEndpointIndex,*Hdr,(void *)(upstreamBuffer->upstreamBufferElement+bufferIndex+sizeof(struct iWARPEM_Message_Hdr_t))) ;
        bufferIndex = nextBufferIndex ;
      }
    StrongAssertLogLine(bufferIndex == byte_len)
      << " bufferIndex=" << bufferIndex
      << " disagrees with byte_len=" << byte_len
      << EndLogLine ;

    if ( 0 == k_LazyFlushUplinks)
      {
        FlushMarkedUplinks();
      }
  }

#include <skv/common/skv_config.hpp>
#include <skv/common/skv_types.hpp>

struct Server_Connection_t
{
  int socket;
  int port;
  char addr_string[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  iWARPEM_Router_Endpoint_t *ServerEP;
};


int ConnectToServers( int aMyRank )
{
  int rc = 0;

  // get configuration to find servers
  skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

  ifstream fin( config->GetServerLocalInfoFile() );

  StrongAssertLogLine( !fin.fail() )
    << "skv_client_conn_manager_if_t::Connect():: ERROR opening server machine file: " << config->GetServerLocalInfoFile()
    << EndLogLine;

  // open sockets and connect to servers

  Server_Connection_t *connections = new Server_Connection_t[ IT_API_MAX_ROUTER_SOCKETS ];
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

    if( connected )
    {
      connections[ conn_count ].ServerEP = new iWARPEM_Router_Endpoint_t( connections[ conn_count ].socket );

      // set up the router info
      iWARPEM_Router_Info_t *routerInfo = connections[ conn_count ].ServerEP->GetRouterInfoPtr();
      routerInfo->RouterID = aMyRank;
      routerInfo->SocketInfo.ipv4_address = (unsigned int)( ((struct sockaddr_in*)srv->ai_addr)->sin_addr.s_addr );
      routerInfo->SocketInfo.ipv4_port = (unsigned short)( ((struct sockaddr_in*)srv->ai_addr)->sin_port );
    }
    else
    {
      BegLogLine( 1 )
        << "Cannot connect to server: " << ServerAddr << ":" << PortStr
        << EndLogLine;
      AssertLogLine(0)
        << "Cannot connect to server: " << ServerAddr << ":" << PortStr
        << " (SKVServer probably needs starting)"
        << EndLogLine ;
      rc = -1;
    }

    freeaddrinfo( entries );

    // Complete the connection by sending private data and router info
    iWARPEM_Router_Endpoint_t *ServerEP = connections[ conn_count ].ServerEP;

    // send private data magic
    char data[ 1024 ];
    *(int*)data = htonl( IWARPEM_MULTIPLEXED_SOCKET_MAGIC );

    int transferred = 0;
    transferred += write( ServerEP->GetRouterFd(), data, sizeof( int ) );
    if( transferred < sizeof( int ))
        BegLogLine( 1 )
          << "Giving up... socket can't even transmit an int... ;-)"
          << EndLogLine;

    // send router info data
    iWARPEM_Router_Info_t *routerInfo = ServerEP->GetRouterInfoPtr();

    memcpy( data, &routerInfo, sizeof( iWARPEM_Router_Info_t ));
    transferred = 0;
    while( transferred < IWARPEM_ROUTER_INFO_SIZE )
    {
      char *d = data + transferred;
      transferred += write( ServerEP->GetRouterFd(), d, IWARPEM_ROUTER_INFO_SIZE );
    }

    AssertLogLine( ServerEP->GetRouterFd() < IT_API_MAX_ROUTER_SOCKETS )
      << "Problem detected: socket descriptor exceeds expected range..."
      << EndLogLine;

    gEPfdMap[ ServerEP->GetRouterFd() ] = ServerEP;
    gUplinkList.push_back( ServerEP );
    add_socket_to_poll( ServerEP );

    conn_count++;
  }

  return rc;
}


//static void post_ack(struct connection *conn)
//  {
//    ((struct rpcAckBuffer *)(&conn->routerBuffer.ackBuffer)) -> response = 0 ;
//
//    struct ibv_send_wr wr;
//    struct ibv_send_wr *bad_wr = NULL;
//    struct ibv_sge sge;
//    memset(&wr, 0, sizeof(wr));
//
//    wr.wr_id = (uintptr_t) &(conn->endio_ack ) ;
//    wr.opcode = IBV_WR_RDMA_WRITE;
//    wr.sg_list = &sge;
//    wr.num_sge = 1;
//    wr.send_flags = IBV_SEND_SIGNALED;
//
//    sge.addr = (uintptr_t)&(conn->routerBuffer.ackBuffer);
////              sge.addr = 0 ; // tjcw as suggested by Bernard
//    sge.length = sizeof(struct ackBuffer) ;
//    sge.lkey = conn->mr->lkey ;
//
//    wr.wr.rdma.remote_addr = ((struct rpcBuffer *) &(conn->routerBuffer.callBuffer))->routerBuffer_addr ;
////              wr.wr.rdma.remote_addr = 0 ; // tjcw as suggested by Bernard
//    wr.wr.rdma.rkey = ((struct rpcBuffer *) &(conn->routerBuffer.callBuffer))->routermemreg_lkey ;
//
//    BegLogLine(FXLOG_ITAPI_ROUTER)
//      << "conn=0x" << (void *) conn
//      << " wr.wr_id=0x" << (void *) wr.wr_id
//      << " qp=0x" << (void *) conn->qp
//      << " sge.addr=0x" << (void *) sge.addr
//      << " RDMA-ing to wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
//      << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
//      << " sequence_in=" << conn->sequence_in
//      << " sequence_out=" << conn->sequence_out
//      << EndLogLine ;
//
//    conn->sequence_out += 1 ;
//    StrongAssertLogLine(conn->rpcBufferPosted != 0 )
//      << "RPC buffer is not posted"
//      << EndLogLine ;
//
//    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
//
//  }
static struct ThreadSafeQueue_t<struct endiorec *, 0> cqSlihQueue ;

static void do_cq_slih_processing(struct endiorec * endiorec, int * requireFlushUplinks)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << " endiorec=0x" << (void *) endiorec
      << EndLogLine ;
    enum optype optype = endiorec->optype ;
    struct connection *conn = endiorec->conn ;
    switch(optype)
      {
    case k_wc_uplink:
      BegLogLine(FXLOG_ITAPI_ROUTER )
        << " conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " k_wc_uplink"
        << EndLogLine ;
      process_uplink(conn) ;
      * requireFlushUplinks = 1 ;
      issue_ack(conn) ;
      break ;
    default:
      StrongAssertLogLine(0)
        << "Unknown optype=" << optype
        << EndLogLine ;
      break ;
      }

  }

static void do_cq_processing(struct ibv_wc& wc)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << " ibv_poll_cq returns wc.status=" << wc.status
      << " wc.opcode=" << wc.opcode
      << EndLogLine ;

//        if (wc.status != IBV_WC_SUCCESS) {
//            wc_stat_echo(&wc);
//            die("on_completion: status is not IBV_WC_SUCCESS.");
//        }
    StrongAssertLogLine(wc.status == IBV_WC_SUCCESS)
      << "Bad wc.status=" << wc.status
      << " from ibv_poll_cq"
      << EndLogLine ;

    struct endiorec * endiorec = ( struct endiorec * )wc.wr_id ;
    enum optype optype = endiorec->optype ;
    struct connection *conn = endiorec->conn ;
    size_t byte_len = wc.byte_len ;
    if ( optype == k_wc_recv)
      {
        conn->ibv_poll_cq_recv_count += 1 ;
      }
    else
      {
        conn->ibv_poll_cq_send_count += 1 ;
      }
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "endiorec=0x" << (void *) endiorec
      << " optype=" << optype
      << " conn=0x" << (void *) conn
      << " qp=0x" << (void *) conn->qp
      << " byte_len=" << byte_len
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << " ibv_post_recv_count=" << conn->ibv_post_recv_count
      << " ibv_post_send_count=" << conn->ibv_post_send_count
      << " ibv_poll_cq_recv_count=" << conn->ibv_poll_cq_recv_count
      << " ibv_poll_cq_send_count=" << conn->ibv_poll_cq_send_count
      << " ibv_send_last_optype=" << conn->ibv_send_last_optype
      << EndLogLine ;

    conn->sequence_in += 1 ;
    switch(optype)
      {
    case k_wc_recv:
      BegLogLine(FXLOG_ITAPI_ROUTER_LW )
        << "conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " k_wc_recv"
        << EndLogLine ;
      received++ ;
      conn->rpcBufferPosted=0 ;
      // We won't actually get the call until (after) we set the ack buffer to zero. But we must have the
      // receive buffer posted early.
      post_call_buffer(conn, &conn->routerBuffer.callBuffer) ;
      process_call(conn,byte_len) ;
      break ;
    case k_wc_uplink:
      BegLogLine(FXLOG_ITAPI_ROUTER_LW )
        << " conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " k_wc_uplink"
        << EndLogLine ;
//          printf("uplink completed successfully.\n");
//          fflush(stdout) ;
      cqSlihQueue.Enqueue(endiorec) ;
//      process_uplink(conn,byte_len) ;
//      if ( k_LazyFlushUplinks )
//        {
//          *requireFlushUplinks = 1 ;
//        }
//      issue_ack(conn) ;
      break ;
    case k_wc_downlink:
      {
        unsigned long localDownstreamSequence=conn->localDownstreamSequence ;
        unsigned long downstream_sequence=conn->downstream_sequence ;
        conn->localDownstreamSequence = localDownstreamSequence+1 ;
        BegLogLine(FXLOG_ITAPI_ROUTER_LW)
          << "conn=0x" << (void *) conn
          << " clientRank=" << conn->clientRank
          << " k_wc_downlink, setting conn->localDownstreamSequence=" << localDownstreamSequence+1
          << EndLogLine ;
        AssertLogLine(localDownstreamSequence == downstream_sequence)
          << "conn=0x" << (void *) conn
          << " clientRank=" << conn->clientRank
          << " localDownstreamSequence=" << localDownstreamSequence
          << " downstream_sequence=" << downstream_sequence
          << EndLogLine ;
//          conn->downstreamBufferFree=1 ;
        indicate_downlink_complete(conn) ;
//          // DMA into the compute node's ack buffer to indicate that we are ready to receive another call
//          post_ack(conn) ;
      }
      break ;
    case k_wc_ack:
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " k_wc_ack"
        << EndLogLine ;
      break ;
    case k_wc_downlink_complete:
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "k_wc_downlink_complete"
        << EndLogLine ;
      {
        struct downstreamLength * downstreamLength=(struct downstreamLength *)&(conn->routerBuffer.downstreamLengthBuffer) ;
        BegLogLine(FXLOG_ITAPI_ROUTER_LW )
          << "conn=0x" << (void *) conn
          << " clientRank=" << conn->clientRank
          << " length=" << downstreamLength->length
          << " sequence=" << downstreamLength->sequence
          << EndLogLine ;
      }
//          conn->downlink_buffer_busy=0 ;
//          fetch_downlink_complete_flag(conn) ;
      break ;
    case k_wc_downlink_complete_return:
      {
      struct downstreamSequence* downstreamSequence=(struct downstreamSequence*)(conn->routerBuffer.downstreamCompleteBuffer.downstreamCompleteBufferElement) ;
      unsigned long *downstreamSequenceP = &(downstreamSequence->sequence) ;
      unsigned long downstreamSequenceNumber =*downstreamSequenceP ;
      unsigned long conn_downstream_sequence = conn->downstream_sequence ;
      BegLogLine(FXLOG_ITAPI_ROUTER_LW)
        << "conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " downstreamSequenceNumber=" << downstreamSequenceNumber
        << " conn_downstream_sequence=" << conn_downstream_sequence
        << " k_wc_downlink_complete_return"
        << EndLogLine ;
      conn->issuedDownstreamFetch=0 ;
      send_downstream_if_buffer_free(conn) ;
      }
      break ;
    default:
      StrongAssertLogLine(0)
        << "Unknown optype=" << optype
        << EndLogLine ;
      break ;
      }

//        if (wc.opcode & IBV_WC_RECV) {
//            received++;
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//              << " IBV_WC_RECV"
//              << EndLogLine ;
//            process_call(conn,byte_len) ;
////            struct rpcBuffer * rpcBuffer = (struct rpcBuffer *) conn->recv_region ;
////            BegLogLine(FXLOG_ITAPI_ROUTER)
////              << "rpcBuffer->sendmemreg_lkey 0x" << (void *) rpcBuffer->sendmemreg_lkey
////              << " rpcBuffer->recvmemreg_lkey 0x" << (void *) rpcBuffer->recvmemreg_lkey
////              << EndLogLine ;l
////            // printf("received message: %s\n", conn->recv_region);
//////            if ( received % 10 == 0 ) {
//////                post_receives(conn, 1);
//////            }
////            // Re-post the buffer just processed
//            post_call_buffer(conn, &conn->routerBuffer.callBuffer) ;
//
//        }
//        else if (wc.opcode == IBV_WC_SEND)
//          {
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//              << " IBV_WC_SEND"
//              << EndLogLine ;
//            printf("send completed successfully.\n");
//            fflush(stdout) ;
//          }
//        else if ( wc.opcode == IBV_WC_RDMA_READ)
//          {
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//              << " IBV_WC_RDMA_READ"
//              << EndLogLine ;
//            process_uplink(conn,byte_len) ;
//            // DMA into the compute node's ack buffer to indicate that we are ready to receive another call
//            post_ack(conn) ;
//          }
//        else if ( wc.opcode == IBV_WC_RDMA_WRITE )
//          {
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//              << " IBV_WC_RDMA_WRITE"
//              << EndLogLine ;
//          }
//        else {
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//                << "Unknown IBV opcode " << wc.opcode
//                << EndLogLine ;
//        }

  }

enum {
  k_spin_poll=1
};

enum {
  k_wc_array_size=64 ,
  k_drain_loop=0
};
static void drain_cq(void)
  {
    struct ibv_cq *cq = s_ctx->cq ;;
    struct ibv_wc wc[k_wc_array_size];
    int rv = ibv_poll_cq(cq, k_wc_array_size, wc) ;
    for ( unsigned int wc_index=0; wc_index<rv;wc_index+=1)
      {
        do_cq_processing( wc[wc_index]) ;
      }
    if ( k_drain_loop )
      {
        rv = ibv_poll_cq(cq, k_wc_array_size, wc) ;
        while ( rv > 0 )
          {
            for ( unsigned int wc_index=0; wc_index<rv;wc_index+=1)
              {
                do_cq_processing( wc[wc_index]) ;
              }
            rv = ibv_poll_cq(cq, k_wc_array_size, wc) ;
          }
      }
  }
static void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc[k_wc_array_size];

  cqSlihQueue.Init(k_CompletionQueueSize) ;
  cq=s_ctx->cq ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "cq=" << (void *) cq
    << EndLogLine ;
  int requireFlushUplinks = 0 ;
  // Follow Bernard Matzler's completion handling sequence
  while(1)
    {
      int rv ;
rearm:
      if ( 0 == k_spin_poll)
        {
          ibv_req_notify_cq(cq, 0) ;
        }
again:
      rv = ibv_poll_cq(cq, k_wc_array_size, wc) ;
      if ( rv < 0 )
        {
          StrongAssertLogLine(0)
                << "poll_cq processing ends because ibv_poll_cq returns with rv=" << rv
                << EndLogLine ;
          break ;
        }
      if ( rv > 0 )
        {
          for ( unsigned int wc_index=0; wc_index<rv;wc_index+=1)
            {
              do_cq_processing( wc[wc_index]) ;
            }
          goto again ;
        }
      else
        {
          struct endiorec * endiorec ;
          int rc=cqSlihQueue.Dequeue(&endiorec) ;
          if ( 0 == rc)
            {
              do_cq_slih_processing(endiorec, &requireFlushUplinks) ;
            }
          else if ( k_LazyFlushUplinks && 0 != requireFlushUplinks )
            {
              FlushMarkedUplinks();
              requireFlushUplinks = 0 ;
            }
        }
      if ( 0 == k_spin_poll)
        {
          struct ibv_cq *next_cq ;

          rv=ibv_get_cq_event(s_ctx->comp_channel, &next_cq, &ctx) ;
          if ( rv )
            {
              StrongAssertLogLine(0)
                    << "poll_cq processing ends because ibv_get_cq_event returns with rv=" << rv
                    << EndLogLine ;
              break ;
            }
          AssertLogLine(cq == next_cq)
            << "CQ changed from " << (void *) cq
            << " to " << next_cq
            << EndLogLine ;
          ibv_ack_cq_events(cq, 1);
          goto rearm ;
        }
    }
//  while (1) {
//    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
//    ibv_ack_cq_events(cq, 1);
//    TEST_NZ(ibv_req_notify_cq(cq, 0));
//
//    while (ibv_poll_cq(cq, 1, &wc)) {
//        BegLogLine(FXLOG_ITAPI_ROUTER)
//          << " ibv_poll_cq returns wc.status=" << wc.status
//          << " wc.opcode=" << wc.opcode
//          << EndLogLine ;
//
////        if (wc.status != IBV_WC_SUCCESS) {
////            wc_stat_echo(&wc);
////            die("on_completion: status is not IBV_WC_SUCCESS.");
////        }
//        StrongAssertLogLine(wc.status == IBV_WC_SUCCESS)
//          << "Bad wc.status=" << wc.status
//          << " from ibv_poll_cq"
//          << EndLogLine ;
//
//        struct endiorec * endiorec = ( struct endiorec * )wc.wr_id ;
//        enum optype optype = endiorec->optype ;
//        struct connection *conn = endiorec->conn ;
//        size_t byte_len = wc.byte_len ;
//        if ( optype == k_wc_recv)
//          {
//            conn->ibv_poll_cq_recv_count += 1 ;
//          }
//        else
//          {
//            conn->ibv_poll_cq_send_count += 1 ;
//          }
//        BegLogLine(FXLOG_ITAPI_ROUTER)
//          << "endiorec=0x" << (void *) endiorec
//          << " optype=" << optype
//          << " conn=0x" << (void *) conn
//          << " qp=0x" << (void *) conn->qp
//          << " byte_len=" << byte_len
//          << " sequence_in=" << conn->sequence_in
//          << " downstream_sequence=" << conn->downstream_sequence
//          << " ibv_post_recv_count=" << conn->ibv_post_recv_count
//          << " ibv_post_send_count=" << conn->ibv_post_send_count
//          << " ibv_poll_cq_recv_count=" << conn->ibv_poll_cq_recv_count
//          << " ibv_poll_cq_send_count=" << conn->ibv_poll_cq_send_count
//          << " ibv_send_last_optype=" << conn->ibv_send_last_optype
//          << EndLogLine ;
//
//        conn->sequence_in += 1 ;
//        switch(optype)
//          {
//        case k_wc_recv:
//          BegLogLine(FXLOG_ITAPI_ROUTER_LW && stuck(conn) )
//            << "conn=0x" << (void *) conn
//            << " clientRank=" << conn->clientRank
//            << " k_wc_recv"
//            << EndLogLine ;
//          received++ ;
//          conn->rpcBufferPosted=0 ;
//          // We won't actually get the call until (after) we set the ack buffer to zero. But we must have the
//          // receive buffer posted early.
//          post_call_buffer(conn, &conn->routerBuffer.callBuffer) ;
//          process_call(conn,byte_len) ;
//          break ;
//        case k_wc_uplink:
//          BegLogLine(FXLOG_ITAPI_ROUTER_LW && stuck(conn) )
//            << " conn=0x" << (void *) conn
//            << " clientRank=" << conn->clientRank
//            << " k_wc_uplink"
//            << EndLogLine ;
////          printf("uplink completed successfully.\n");
////          fflush(stdout) ;
//          process_uplink(conn,byte_len) ;
//          issue_ack(conn) ;
//          break ;
//        case k_wc_downlink:
//          {
//            unsigned long localDownstreamSequence=conn->localDownstreamSequence ;
//            unsigned long downstream_sequence=conn->downstream_sequence ;
//            conn->localDownstreamSequence = localDownstreamSequence+1 ;
//            BegLogLine(FXLOG_ITAPI_ROUTER_LW && stuck(conn) )
//              << "conn=0x" << (void *) conn
//              << " clientRank=" << conn->clientRank
//              << " k_wc_downlink, setting conn->localDownstreamSequence=" << localDownstreamSequence+1
//              << EndLogLine ;
//            AssertLogLine(localDownstreamSequence == downstream_sequence)
//              << "conn=0x" << (void *) conn
//              << " clientRank=" << conn->clientRank
//              << " localDownstreamSequence=" << localDownstreamSequence
//              << " downstream_sequence=" << downstream_sequence
//              << EndLogLine ;
//  //          conn->downstreamBufferFree=1 ;
//            indicate_downlink_complete(conn) ;
//  //          // DMA into the compute node's ack buffer to indicate that we are ready to receive another call
//  //          post_ack(conn) ;
//          }
//          break ;
//        case k_wc_ack:
//          BegLogLine(FXLOG_ITAPI_ROUTER && stuck(conn) )
//            << "conn=0x" << (void *) conn
//            << " clientRank=" << conn->clientRank
//            << " k_wc_ack"
//            << EndLogLine ;
//          break ;
//        case k_wc_downlink_complete:
//          BegLogLine(FXLOG_ITAPI_ROUTER)
//            << "k_wc_downlink_complete"
//            << EndLogLine ;
//          {
//            struct downstreamLength * downstreamLength=(struct downstreamLength *)&(conn->routerBuffer.downstreamLengthBuffer) ;
//            BegLogLine(FXLOG_ITAPI_ROUTER_LW && stuck(conn) )
//              << "conn=0x" << (void *) conn
//              << " clientRank=" << conn->clientRank
//              << " length=" << downstreamLength->length
//              << " sequence=" << downstreamLength->sequence
//              << EndLogLine ;
//          }
////          conn->downlink_buffer_busy=0 ;
////          fetch_downlink_complete_flag(conn) ;
//          break ;
//        case k_wc_downlink_complete_return:
//          {
//          struct downstreamSequence* downstreamSequence=(struct downstreamSequence*)(conn->routerBuffer.downstreamCompleteBuffer.downstreamCompleteBufferElement) ;
//          unsigned long *downstreamSequenceP = &(downstreamSequence->sequence) ;
//          unsigned long downstreamSequenceNumber =*downstreamSequenceP ;
//          unsigned long conn_downstream_sequence = conn->downstream_sequence ;
//          BegLogLine(FXLOG_ITAPI_ROUTER_LW && stuck(conn))
//            << "conn=0x" << (void *) conn
//            << " clientRank=" << conn->clientRank
//            << " downstreamSequenceNumber=" << downstreamSequenceNumber
//            << " conn_downstream_sequence=" << conn_downstream_sequence
//            << " k_wc_downlink_complete_return"
//            << EndLogLine ;
//          conn->issuedDownstreamFetch=0 ;
//          send_downstream_if_buffer_free(conn) ;
//          }
//          break ;
//        default:
//          StrongAssertLogLine(0)
//            << "Unknown optype=" << optype
//            << EndLogLine ;
//          break ;
//          }
//
////        if (wc.opcode & IBV_WC_RECV) {
////            received++;
////            BegLogLine(FXLOG_ITAPI_ROUTER)
////              << " IBV_WC_RECV"
////              << EndLogLine ;
////            process_call(conn,byte_len) ;
//////            struct rpcBuffer * rpcBuffer = (struct rpcBuffer *) conn->recv_region ;
//////            BegLogLine(FXLOG_ITAPI_ROUTER)
//////              << "rpcBuffer->sendmemreg_lkey 0x" << (void *) rpcBuffer->sendmemreg_lkey
//////              << " rpcBuffer->recvmemreg_lkey 0x" << (void *) rpcBuffer->recvmemreg_lkey
//////              << EndLogLine ;l
//////            // printf("received message: %s\n", conn->recv_region);
////////            if ( received % 10 == 0 ) {
////////                post_receives(conn, 1);
////////            }
//////            // Re-post the buffer just processed
////            post_call_buffer(conn, &conn->routerBuffer.callBuffer) ;
////
////        }
////        else if (wc.opcode == IBV_WC_SEND)
////          {
////            BegLogLine(FXLOG_ITAPI_ROUTER)
////              << " IBV_WC_SEND"
////              << EndLogLine ;
////            printf("send completed successfully.\n");
////            fflush(stdout) ;
////          }
////        else if ( wc.opcode == IBV_WC_RDMA_READ)
////          {
////            BegLogLine(FXLOG_ITAPI_ROUTER)
////              << " IBV_WC_RDMA_READ"
////              << EndLogLine ;
////            process_uplink(conn,byte_len) ;
////            // DMA into the compute node's ack buffer to indicate that we are ready to receive another call
////            post_ack(conn) ;
////          }
////        else if ( wc.opcode == IBV_WC_RDMA_WRITE )
////          {
////            BegLogLine(FXLOG_ITAPI_ROUTER)
////              << " IBV_WC_RDMA_WRITE"
////              << EndLogLine ;
////          }
////        else {
////            BegLogLine(FXLOG_ITAPI_ROUTER)
////                << "Unknown IBV opcode " << wc.opcode
////                << EndLogLine ;
////        }
//    }
//  }
  return NULL;
}

static void post_call_buffer(struct connection *conn, volatile struct callBuffer * callBuffer)
  {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    int rc;
    wr.wr_id = (uintptr_t) &(conn->endio_call) ;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)callBuffer;
    sge.length = k_CallBufferSize;
    sge.lkey = conn->mr->lkey;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " qp=0x" << (void *) conn->qp
      << " wr_id=0x" << (void *) wr.wr_id
      << " sequence_in=" << conn->sequence_in
      << " downstream_sequence=" << conn->downstream_sequence
      << " upstreamSequence=" << conn->upstreamSequence
      << EndLogLine ;

//    conn->sequence_out += 1 ;
//    pthread_mutex_lock((pthread_mutex_t *) &conn->qp_write_mutex) ;
//    pthread_mutex_lock(&allConnectionMutex) ;
    rc=ibv_post_recv(conn->qp, &wr, &bad_wr);
//    pthread_mutex_unlock(&allConnectionMutex) ;
//    pthread_mutex_unlock((pthread_mutex_t *) &conn->qp_write_mutex) ;

    if (rc!=0)
      {
        printf("ERROR: posting ibv_post_recv() failed\n");
        fflush(stdout) ;
        BegLogLine(FXLOG_ITAPI_ROUTER)
          << "ERROR: posting ibv_post_recv() failed, rc=" << rc
          << EndLogLine ;
        StrongAssertLogLine(0)
          << "ibv_post_recv failed, rc=" << rc
          << EndLogLine ;
      }
    conn->ibv_post_recv_count += 1 ;
    conn->rpcBufferPosted = 1 ;

  }
//static void post_all_call_buffers(struct connection *conn)
//  {
//    for(int x=0;x<k_CallBufferCount; x += 1)
//      {
//        struct ibv_recv_wr wr, *bad_wr = NULL;
//        struct ibv_sge sge;
//        int rc;
//        struct callBuffer * callBuffer = allCallBuffer.callBuffer+x ;
//        wr.wr_id = (uintptr_t) callBuffer ;
//        wr.next = NULL;
//        wr.sg_list = &sge;
//        wr.num_sge = 1;
//
//        sge.addr = (uintptr_t)callBuffer;
//        sge.length = k_CallBufferSize;
//        sge.lkey = conn->call_mr->lkey;
//
//        rc=ibv_post_recv(conn->qp, &wr, &bad_wr);
//        BegLogLine(FXLOG_ITAPI_ROUTER)
//          << "posting ibv_post_recv(), rc=" << rc
//          << EndLogLine ;
//       if (rc!=0)
//          {
//            printf("ERROR: posting ibv_post_recv() #%i failed\n", x);
//            fflush(stdout) ;
//            BegLogLine(FXLOG_ITAPI_ROUTER)
//              << "ERROR: posting ibv_post_recv() failed, rc=" << rc
//              << EndLogLine ;
//          }
//
//      }
//  }
//void post_receives(struct connection *conn, int rcount)
//{
//  struct ibv_recv_wr wr, *bad_wr = NULL;
//  struct ibv_sge sge;
//  int rc, i;
//
//  wr.wr_id = (uintptr_t)conn;
//  wr.next = NULL;
//  wr.sg_list = &sge;
//  wr.num_sge = 1;
//
//  sge.addr = (uintptr_t)conn->recv_region;
//  sge.length = k_CallBufferSize;
//  sge.lkey = conn->recv_mr->lkey;
//
//  for (i=0; i<rcount; i++) {
////    printf("another ibv_post_recv()\n");
//      rc=ibv_post_recv(conn->qp, &wr, &bad_wr);
//      if (rc!=0)
//        {
//          printf("ERROR: posting ibv_post_recv() #%i failed\n", i);
//          fflush(stdout) ;
//        }
//  }
//}

void register_memory(struct connection *conn)
{
//  conn->send_region = (char *) malloc(k_CallBufferSize);
//  conn->recv_region = (char *) malloc(k_CallBufferSize);

//  TEST_Z(conn->send_mr = ibv_reg_mr(
//    s_ctx->pd,
//    conn->send_region,
//    k_CallBufferSize,
//    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
//
//  TEST_Z(conn->recv_mr = ibv_reg_mr(
//    s_ctx->pd,
//    conn->recv_region,
//    k_CallBufferSize,
//    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
//
//  TEST_Z(conn->call_mr = ibv_reg_mr(
//    s_ctx->pd,
//    (char *) & allCallBuffer,
//    sizeof(allCallBuffer),
//    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

        conn->mr = ibv_reg_mr(
        s_ctx->pd,
        (char *) conn,
        sizeof(*conn),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
         ) ;
    StrongAssertLogLine(conn->mr != NULL)
      << "ibv_reg_mr fails to register memory region"
      << EndLogLine ;
}

uint16_t GetFreeClientId( void )
{
  uint16_t id = 0;
  for( int n=0; (n < IT_API_MAX_ROUTER_SOCKETS); n++ )
  {
    if( gClientIdMap[ n ] == IWARPEM_INVALID_CLIENT_ID )
    {
      gClientIdMap[ n ] = n;
      return n;
    }
  }
  return IWARPEM_INVALID_CLIENT_ID;
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

//  printf("received connection request\n");
//  fflush(stdout) ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Received connection request, id=0x" << (void *) id
    << EndLogLine ;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);
  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  conn = (struct connection *)malloc(sizeof(struct connection));
  id->context = (void *) conn ;
//  pthread_mutex_init((pthread_mutex_t *) &conn->qp_write_mutex, NULL) ;
  memset((void *) &conn->routerBuffer, 0xfd, sizeof(conn->routerBuffer)) ;
  conn->qp = id->qp;
//  conn->connection = conn ;
  conn->endio_call.conn = conn ;
  conn->endio_call.optype = k_wc_recv ;
  conn->endio_uplink.conn = conn ;
  conn->endio_uplink.optype = k_wc_uplink ;
  conn->endio_downlink.conn = conn ;
  conn->endio_downlink.optype = k_wc_downlink ;
  conn->endio_ack.conn = conn ;
  conn->endio_ack.optype = k_wc_ack ;
  conn->endio_downlink_complete.conn = conn ;
  conn->endio_downlink_complete.optype = k_wc_downlink_complete ;
  conn->endio_downlink_complete_return.conn = conn ;
  conn->endio_downlink_complete_return.optype = k_wc_downlink_complete_return ;

  conn->ibv_post_send_count = 0 ;
  conn->ibv_post_recv_count = 0;
  conn->ibv_poll_cq_send_count = 0 ;
  conn->ibv_poll_cq_recv_count = 0 ;
  conn->ibv_send_last_optype = 0xffffffff ;
  conn->sequence_in = 0 ;
//  conn->sequence_out = 0 ;
  conn->upstreamSequence = 0 ;

//  conn->downstreamBufferFree = 1 ;
  conn->downstream_sequence = 0 ;
  conn->localDownstreamSequence = 1 ;
//  conn->downlink_buffer_busy = 0 ;

  conn->routerBuffer_rkey = 0 ;
  conn->routerBuffer_raddr = 0 ;

//  conn->stuck_epoll_count = 0 ;
  conn->issuedDownstreamFetch = 0 ;
  conn->will_flush_this_connection = 0 ;
  conn->next_conn_to_flush = NULL ;
  conn->flushed_downstream = 1 ;


  conn->clientId = GetFreeClientId();

  register_memory(conn);
  post_call_buffer(conn,&conn->routerBuffer.callBuffer) ;
//  post_all_call_buffers(conn) ;
//  post_receives(conn, WORKREQNUM);

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

int on_connection(void *context)
{
//  struct connection *conn = (struct connection *)context;
//  struct ibv_send_wr wr;
//  struct ibv_send_wr *bad_wr = NULL;
//  struct ibv_sge sge;

//    printf("pid: %d pthread: %i connected\n", getpid(), (int) pthread_self());
//    fflush(stdout) ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "connected, context=0x" << context
      << EndLogLine ;
  // snprintf(conn->send_region, k_CallBufferSize, "message from passive/server side with pid %d", getpid());

 /* posting send ...\n"); */

 /*  memset(&wr, 0, sizeof(wr)); */

 /*  wr.opcode = IBV_WR_SEND; */
 /*  wr.sg_list = &sge; */
 /*  wr.num_sge = 1; */
 /*  wr.send_flags = IBV_SEND_SIGNALED; */

 /*  sge.addr = (uintptr_t)conn->send_region; */
 /*  sge.length = k_CallBufferSize; */
 /*  sge.lkey = conn->send_mr->lkey; */

  // TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

//  printf("peer disconnected. Msgs received: %i\n", received);
//  fflush(stdout) ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Peer disconnected. Msgs received=" << received
    << " conn=0x" << (void *) conn
    << EndLogLine ;

  rdma_destroy_qp(id);

//  ibv_dereg_mr(conn->send_mr);
//  ibv_dereg_mr(conn->recv_mr);
  ibv_dereg_mr(conn->mr) ;

//  free(conn->send_region);
//  free(conn->recv_region);
  free((void *)conn);
  rdma_destroy_id(id);

  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
      sleep(2);
      r = on_disconnect(event->id);
  }
  else
    die("on_event: unknown event.");

  return r;
}

