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

#ifndef FXLOG_ITAPI_ROUTER
#define FXLOG_ITAPI_ROUTER ( 0 )
#endif

#ifndef FXLOG_ITAPI_ROUTER_DETAIL
#define FXLOG_ITAPI_ROUTER_DETAIL ( 0 )
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

#ifndef FXLOG_ITAPI_ROUTER_FRAMES
#define FXLOG_ITAPI_ROUTER_FRAMES ( 0 | FXLOG_ITAPI_ROUTER )
#endif

#ifndef FXLOG_ITAPI_ROUTER_CLEANUP
#define FXLOG_ITAPI_ROUTER_CLEANUP ( 0 | FXLOG_ITAPI_ROUTER )
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

//#include <it_api_o_sockets_router.h>
extern "C"
{
#define ITAPI_ENABLE_V21_BINDINGS
#include <it_api.h>
//#include "ordma_debug.h"
};

#include <skv/common/skv_config.hpp>

#include <it_api_o_sockets_thread.h>
#include <it_api_o_sockets_types.h>
#include <it_api_o_sockets_cn_ion.hpp>

#include <iwarpem_socket_access.hpp>
#include <iwarpem_types.hpp>

#define IT_API_MAX_ROUTER_SOCKETS ( 256 )

#if 1
#include <rdma/rdma_cma.h>
#define ofed_mvapich_workaround() \
{ \
  struct rdma_event_channel *ch = rdma_create_event_channel (); \
  if( !ch )\
  { \
    printf("rdma_create_event_channel failed. returned: 0x%x\n", (void*)ch ); \
    return -1; \
  } \
  rdma_destroy_event_channel( ch );  \
}
#else
#define ofed_mvapich_workaround()
#endif

class Crossbar_Entry_t;
#include <cnk_router/it_api_cnk_router_ep.hpp>
typedef iWARPEM_WriteV_Socket_Buffer_t SendBufferType;
//typedef iWARPEM_Memory_Socket_Buffer_t SendBufferType;
typedef iWARPEM_Multiplexed_Endpoint_t<Crossbar_Entry_t, SendBufferType> iWARPEM_Router_Endpoint_t;

typedef std::list<iWARPEM_Router_Endpoint_t*> Uplink_list_t;
Uplink_list_t gUplinkList;

iWARPEM_Router_Endpoint_t *gEPfdMap[ IT_API_MAX_ROUTER_SOCKETS ];
static iWARPEM_StreamId_t gClientIdMap[ IT_API_MAX_ROUTER_SOCKETS ];

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
enum {
  k_LazyFlushUplinks = 1
};

enum optype {
  k_wc_recv ,
  k_wc_uplink ,
  k_wc_downlink ,
  k_wc_ack ,
  k_wc_downlink_complete ,
  k_wc_downlink_complete_return,
  k_wc_terminate
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
struct oprec_link_terminate {
  struct rdma_cm_id *mRdmaId;
};
union oprec {
  struct oprec_recv recv;
  struct oprec_send send;
  struct oprec_downlink downlink;
  struct oprec_ack ack;
  struct oprec_downlink_complete downlink_complete;
  struct oprec_downlink_complete_return downlink_complete_return;
  struct oprec_link_terminate link_terminate;
};

struct connection ;

#define FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR ( (struct connection*)0xEFEFEFEFEF )

struct endiorec {
  struct connection * conn ;
  enum optype optype ;
  union oprec oprec ;
};

class Crossbar_Entry_t
{
  iWARPEM_StreamId_t mClientId;
  int mServerId;  // LocalEndpointIndex
  iWARPEM_Router_Endpoint_t *mUpLink;
  struct connection *mDownLink;

public:
  Crossbar_Entry_t( iWARPEM_StreamId_t aClientId = IWARPEM_INVALID_CLIENT_ID,
                    int aServerId = IWARPEM_INVALID_SERVER_ID,
                    iWARPEM_Router_Endpoint_t *aUpLink = NULL,
                    struct connection *aDownLink = NULL )
  : mClientId( aClientId ), mServerId( aServerId )
  {
    mUpLink = aUpLink;
    mDownLink = aDownLink;
    BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
      << "Creating new Crossbar_Entry. "
      << " this=0x" << (void*)this
      << " client: " << aClientId
      << " conn: 0x" << (void*)aDownLink
      << " server: " << aServerId
      << " srvEP: 0x" << (void*)aUpLink
      << EndLogLine;
  }
  ~Crossbar_Entry_t()
  {
    BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
      << "Destroying Crossbar_Entry. "
      << " client: " << mClientId
      << " conn: 0x" << (void*)mDownLink
      << " server: " << mServerId
      << " srvEP: 0x" << (void*)mUpLink
      << EndLogLine;
  }

  inline
  iWARPEM_StreamId_t getClientId() const
  {
    return mClientId;
  }

  inline
  void setClientId( iWARPEM_StreamId_t clientId )
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
  k_LocalEndpointCount = IT_API_MAX_ROUTER_SOCKETS
};
//static pthread_mutex_t allConnectionMutex ;
struct connection {
  unsigned long ibv_post_send_count ;
  unsigned long ibv_post_recv_count ;
  unsigned long ibv_poll_cq_send_count ;
  unsigned long ibv_poll_cq_recv_count ;
  unsigned int ibv_send_last_optype ;
  struct ibv_qp *qp;
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
  unsigned long upstreamSequence ;
  int rpcBufferPosted ;
  uint64_t routerBuffer_raddr ;
  uint32_t routerBuffer_rkey ;
  unsigned int clientRank ;
  int flushed_downstream ;
  volatile bool mSendAck ;
  struct connection * mNextConnToAck[2] ;
  struct connection * mNextDeferredAck;
  volatile bool mDisconnecting ;
  volatile bool mWaitForUpstreamFlush;
  int issuedDownstreamFetch ;

  iWARPEM_StreamId_t clientId;

  Crossbar_Entry_t *socket_fds[k_LocalEndpointCount];
  // The following 2 items for debugging which SKVserver sends us a bad header
  unsigned int upstream_ip_address[k_LocalEndpointCount] ;
  unsigned short upstream_ip_port[k_LocalEndpointCount] ;
  class ion_cn_all_buffer mBuffer ;
};

struct termination_entry_t
{
  struct connection *mConn;
  struct rdma_cm_id *mRdmaId;
};

static void add_conn_to_flush_queue(struct connection *conn);
static void add_conn_to_deferred_ack( struct connection *conn, bool aDownStreamWaitFlush = false );

int ion_cn_buffer::PostReceive(struct connection *conn)
  {
    BegLogLine(FXLOG_IONCN_BUFFER)
        << " Receive buffer at 0x" << (void *) this
        << EndLogLine ;

    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    int rc;
    wr.wr_id = (uintptr_t) &(conn->endio_call) ;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)this;
    sge.length = k_LargestRDMASend ;
    sge.lkey = conn->mr->lkey;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " qp=0x" << (void *) conn->qp
      << " wr_id=0x" << (void *) wr.wr_id
      << " addr=0x" << (void *) sge.addr
      << EndLogLine ;

    // double check if the qp is still alive:
    rc = ( conn->mDisconnecting ) ? 0 : ibv_post_recv(conn->qp, &wr, &bad_wr);
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
    return rc;
  }

int ion_cn_buffer::IssueRDMARead(struct connection *conn, unsigned long Offset, unsigned long Length)
  {
//    conn->upstream_length = Length ;
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

    sge.addr = (uintptr_t)(((char *)this)+Offset);
    sge.length = Length ;
    sge.lkey = conn->mr->lkey ;

    wr.wr.rdma.remote_addr = conn->routerBuffer_raddr + offsetof(class ion_cn_all_buffer,mTransmitBuffer) + conn->mBuffer.mReceiveBuffer.CurrentBufferIndex()*sizeof(class ion_cn_buffer) + Offset ;
    wr.wr.rdma.rkey = conn->routerBuffer_rkey ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " wr.wr_id=0x" << (void *) wr.wr_id
      << " qp=0x" << (void *) conn->qp
      << " sge.addr=0x" << (void *) sge.addr
      << " sge.length=0x" << (void *) sge.length
      << " RDMA-ing from wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
      << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
      << EndLogLine ;

    conn->ibv_post_send_count += 1 ;
    conn->ibv_send_last_optype = k_wc_uplink ;
    int rc = ( conn->mDisconnecting ) ? 0 : ibv_post_send(conn->qp, &wr, &bad_wr);
    BegLogLine( rc != 0 )
      << "ibv_post_send fails, rc=" << rc
      << EndLogLine ;
    if( rc != 0 )
      conn->mDisconnecting = true;
    return rc;
  }

static void process_uplink_element(struct connection *conn, unsigned int LocalEndpointIndex, const struct iWARPEM_Message_Hdr_t& Hdr, const void * message) ;
static void FlushMarkedUplinks(void) ;
static bool flush_downstream(struct connection *conn) ;
static struct connection * volatile gFirstConnToAck = FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR;
static uint64_t gAckConnectionCount = 0;
static uint64_t gFlushConnectionCount = 0;
static volatile int gFlushListIndex = 0;
static pthread_spinlock_t gDownStreamLock;
static struct connection * volatile gDeferredAckList = FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR;
static void LockDownStreamFlush( void )
{
  BegLogLine( FXLOG_ITAPI_ROUTER_DETAIL )
    << "Locking DownStreamFlush."
    << EndLogLine;
  pthread_spin_lock( &gDownStreamLock );
}
static void UnlockDownStreamFlush( void )
{
  pthread_spin_unlock( &gDownStreamLock );
  BegLogLine( FXLOG_ITAPI_ROUTER_DETAIL )
    << "Unlocked DownStreamFlush."
    << EndLogLine;
}
static pthread_spinlock_t gCMEventLock;
static void LockCMEventStream( void )
{
  pthread_spin_lock( &gCMEventLock );
}
static void UnlockCMEventStream( void )
{
  pthread_spin_unlock( &gCMEventLock );
}

static bool post_ack_only( struct connection * conn )
{
  bool rc_ack = false;
  conn->mBuffer.LockTransmit();

  if((!conn->mDisconnecting) && ( conn->mBuffer.ReadyToTransmit() || conn->mSendAck ))
    rc_ack = conn->mBuffer.PostAckOnly( conn );

  conn->mSendAck =  !rc_ack;

  // if even the ACK fails, the connection has to be queued for another attempt to flush
  conn->mBuffer.UnlockTransmit();
  if(( ! rc_ack ) && ( ! conn->mDisconnecting ))
  {
    BegLogLine(FXLOG_ITAPI_ROUTER_FRAMES)
      << "Problem: Flush and ACK failed or skipped! conn=0x" << (void*)conn
      << " clientId=" << conn->clientId
      << " mBufferAckStates( " << conn->mBuffer.mBufferAllowsAck << ":" << conn->mBuffer.mBufferRequiresAck << " )"
      << EndLogLine;
    add_conn_to_flush_queue( conn );
  }
  return rc_ack;
}

// to be executed while downstreamlock is held
static void AckConnection(struct connection *conn)
{
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "conn=0x" << (void *) conn
    << " mDisconnecting=" << conn->mDisconnecting
    << " mSendAck=" << conn->mSendAck
    << " needsTransmit=" << conn->mBuffer.NeedsAnyTransmit()
    << " readtoTransmit=" << conn->mBuffer.ReadyToTransmit()
    << EndLogLine ;

  if ( (! conn->mDisconnecting) && (conn->mSendAck || conn->mBuffer.NeedsAnyTransmit()) )
  {
    bool rc_flush = false;
    // attempt to do a general TX only if there's actually data otherwise use the ACK-only method and don't mess with the buffer
    if( conn->mBuffer.mTransmitBufferIndex > 0 )
      rc_flush = flush_downstream(conn) ;

    // if regular flush fails, we need to check if we have to send at least an ACK to prevent deadlocking
    if ( ! rc_flush)
      post_ack_only( conn );
    BegLogLine( FXLOG_ITAPI_ROUTER_DETAIL )
      << "Flushed conn=0x" << (void*)conn
      << " rc_flush=" << rc_flush
      << EndLogLine
  }
  else
    {
    BegLogLine( (FXLOG_ITAPI_ROUTER || ( FXLOG_ITAPI_ROUTER_CLEANUP && conn->mDisconnecting )) )
      << "NOT FLUSHING because: "
      << " mSendAck=" << conn->mSendAck
      << " needtr=" << conn->mBuffer.NeedsAnyTransmit()
      << " disconnecting=" << conn->mDisconnecting
      << EndLogLine;
    }
}
static void AckAllConnections(void)
{
  LockDownStreamFlush();
  BegLogLine( (FXLOG_ITAPI_ROUTER_DETAIL | 0) && (gFirstConnToAck != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR))
    << "Flushing all, first_connection_to_flush=0x" << (void *)gFirstConnToAck
    << " Index: " << gFlushListIndex
    << EndLogLine ;
  struct connection * ConnToAck=gFirstConnToAck ;
  gFirstConnToAck= FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR ;

  int ThisFlushListIndex = gFlushListIndex;
  gFlushListIndex = 1 - gFlushListIndex;
  UnlockDownStreamFlush();

  gAckConnectionCount++;
  while( (ConnToAck != NULL) && (ConnToAck != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR) )
  {
    struct connection *NextConnection = ConnToAck->mNextConnToAck[ ThisFlushListIndex ];
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Acknowledging connection.  conn=0x" << ConnToAck
      << " next=0x" << NextConnection
      << EndLogLine;

    AckConnection(ConnToAck);
    ConnToAck->mNextConnToAck[ ThisFlushListIndex ] = NULL;
    ConnToAck = NextConnection;
  }
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "All connections acknowledged"
    << EndLogLine ;
}
void ion_cn_buffer::ProcessReceiveBuffer(struct connection *conn, bool contained_ack )
  {
    class ion_cn_all_buffer *buffer=&conn->mBuffer ;
    unsigned long BytesInThisCall=mSentBytes - buffer->mReceivedBytes ;
    bool RequiresFlush = ((BytesInThisCall > 0) || ( contained_ack ));
    BegLogLine(FXLOG_ITAPI_ROUTER)
        << "this=0x" << (void *) this
        << " conn=0x" << (void *) conn
        << " mSentBytes=" << mSentBytes
        << " mReceivedBytes=" << mReceivedBytes
        << " buffer->mSentBytes=" << buffer->mSentBytes
        << " buffer->mReceivedBytes=" << buffer->mReceivedBytes
        << " BytesInThisCall=" << BytesInThisCall
        << " Processing receive buffer"
        << EndLogLine ;
    BegLogLine(FXLOG_ITAPI_ROUTER | FXLOG_ITAPI_ROUTER_FRAMES )
      << "conn->clientId=" << conn->clientId
      << " this=0x" << (void *) this
      << " RX-FRAME {" << mSentBytes
      << "," << mReceivedBytes
      << "," << BytesInThisCall
      << "}"
      << EndLogLine ;
    BegLogLine(FXLOG_ITAPI_ROUTER_DETAIL)
      << "*this=" << HexDump(this,BytesInThisCall+(sizeof(class ion_cn_buffer)-k_ApplicationBufferSize) )
      << EndLogLine ;
    char * bufferPtr = mApplicationBuffer ;
    while ( bufferPtr < &(mApplicationBuffer[ BytesInThisCall ]))
    {
      unsigned int LocalEndpointIndex = * (unsigned int *) bufferPtr ;
      bufferPtr +=  sizeof(unsigned int) ;
      struct iWARPEM_Message_Hdr_t * Hdr = (struct iWARPEM_Message_Hdr_t *) bufferPtr;
      char *nextBufferPtr = bufferPtr + sizeof(struct iWARPEM_Message_Hdr_t) + Hdr->mTotalDataLen ;
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << " LocalEndpointIndex=" << LocalEndpointIndex
        << " bufferPtr=0x" << (void *) bufferPtr
        << " nextBufferPtr=0x" << (void*)nextBufferPtr
        << EndLogLine ;
      report_hdr(*Hdr) ;
      StrongAssertLogLine(nextBufferPtr <= &mApplicationBuffer[ k_ApplicationBufferSize ] )
        << "Message overflows buffer, Hdr->mTotalDataLen=" << Hdr->mTotalDataLen
        << " nextBufferPtr=" << nextBufferPtr
        << " BytesInThisCall=" << BytesInThisCall
        << EndLogLine ;
      process_uplink_element(conn,LocalEndpointIndex,*Hdr,(void *)(bufferPtr+sizeof(struct iWARPEM_Message_Hdr_t))) ;
      bufferPtr = nextBufferPtr ;
    }
    StrongAssertLogLine(bufferPtr == &mApplicationBuffer[ BytesInThisCall ])
      << "bufferPtr=0x" << (void*)bufferPtr
      << " disagrees with BytesInThisCall=" << BytesInThisCall
      << " last byte= 0x" << (void*)&mApplicationBuffer[ BytesInThisCall ]
      << EndLogLine ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " buff=0x" << (void*) buffer
      << " Advancing mReceivedBytes from " << buffer->mReceivedBytes
      << " to " << mSentBytes
      << EndLogLine ;

    buffer->mReceivedBytes = mSentBytes ;
    buffer->mSentBytesPrevious=buffer->mSentBytes ;
    buffer->mAckedSentBytes=mReceivedBytes ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " buffer->mReceivedBytes=" << buffer->mReceivedBytes
      << " buffer->mSentBytesPrevious=" << buffer->mSentBytesPrevious
      << " buffer->mAckedSentBytes=" << buffer->mAckedSentBytes
      << " BytesInThisCall=" << BytesInThisCall
      << " RequiresFlush=" << RequiresFlush
      << " AckRequest=" << contained_ack
      << EndLogLine ;

    if( RequiresFlush )
    {
      add_conn_to_deferred_ack( conn, true );
      if(( 0 == k_LazyFlushUplinks) && ( BytesInThisCall > 0 ))
      {
        FlushMarkedUplinks();
      }
    }
  }

// returns >0 if upstream flush required
// return == 0 if no extra action neede
// return <0 on error
int ion_cn_buffer::ProcessCall(struct connection *conn)
  {
     class ion_cn_all_buffer *buffer=&conn->mBuffer ;
     // check if client has any new sent bytes to ACK
     bool requires_ack = buffer->AdvanceAcked(mReceivedBytes) && buffer->NeedsAnyTransmit();

     unsigned long BytesInThisCall=mSentBytes - buffer->mReceivedBytes ;
     if ( BytesInThisCall > k_LargestRDMASend-(sizeof(class ion_cn_buffer)-k_ApplicationBufferSize))
       {
         BegLogLine(FXLOG_ITAPI_ROUTER | FXLOG_ITAPI_ROUTER_FRAMES )
           << "conn->clientId=" << conn->clientId
           << " this=0x" << (void *) this
           << " RX-FRAME-WILL-BE {" << mSentBytes
           << "," << mReceivedBytes
           << "," << BytesInThisCall
           << "}"
           << EndLogLine ;
         BegLogLine(FXLOG_ITAPI_ROUTER)
             << "this=0x" << (void *) this
             << " conn=0x" << (void *) conn
             << " BytesInThisCall=" << BytesInThisCall
             << "Issuing RDMA read to pick up rest of receive buffer"
             << EndLogLine ;
         return IssueRDMARead(conn,k_InitialRDMASend,BytesInThisCall) ;;
       }
     else
       {
         BegLogLine(FXLOG_ITAPI_ROUTER)
             << "this=0x" << (void *) this
             << " conn=0x" << (void *) conn
             << " BytesInThisCall=" << BytesInThisCall
             << " RDMA receive is complete, processing ..."
             << EndLogLine ;
         ProcessReceiveBuffer(conn, requires_ack ) ;
         if( ! conn->mDisconnecting )
           buffer->PostReceive(conn) ;
         BegLogLine(FXLOG_ITAPI_ROUTER)
           << "Completed RDMA_Recv processing. this=0x" << (void *) this
           << " conn=0x" << (void *) conn
           << " BytesInThisCall=" << BytesInThisCall
           << EndLogLine ;
         return 1;
       }
  }
void ion_cn_buffer::ProcessRead( struct connection *conn)
{
  class ion_cn_all_buffer *buffer=&conn->mBuffer ;
  // check if client has any new sent bytes to ACK
  bool requires_ack = buffer->AdvanceAcked(mReceivedBytes) && buffer->NeedsAnyTransmit();
  unsigned long BytesInThisCall=mSentBytes - buffer->mReceivedBytes ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "this=0x" << (void *) this
    << " conn=0x" << (void *) conn
    << " BytesInThisCall=" << BytesInThisCall
    << " RDMA receive is complete, processing ..."
    << EndLogLine ;
  ProcessReceiveBuffer(conn, requires_ack ) ;
  if( ! conn->mDisconnecting )
    buffer->PostReceive(conn) ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Completed RDMA_Read processing. this=0x" << (void *) this
    << " conn=0x" << (void *) conn
    << " BytesInThisCall=" << BytesInThisCall
    << EndLogLine ;
}
bool ion_cn_buffer::pushAckOnly( struct connection *conn, unsigned long aSentBytes, unsigned long aReceivedBytes )
{
  mReceivedBytes = aReceivedBytes;

  struct ibv_send_wr wr;
  struct ibv_send_wr *bad_wr = NULL;
  struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));

  unsigned long Sentinel=SentinelIndex( 0 ) ;
  unsigned long RDMACount=Sentinel+1+(sizeof(class ion_cn_buffer) - k_ApplicationBufferSize) ;

  wr.wr_id = (uintptr_t) &(conn->endio_ack ) ;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.next = NULL ;

  sge.addr = (uintptr_t)this;
  sge.length = RDMACount;
  sge.lkey = conn->mr->lkey ;

  wr.wr.rdma.remote_addr = conn->routerBuffer_raddr + offsetof(class ion_cn_all_buffer,mRemoteWrittenSendAckBytes);;
  wr.wr.rdma.rkey = conn->routerBuffer_rkey ;

  BegLogLine(FXLOG_ITAPI_ROUTER_LW)
    << "conn=0x" << (void *) conn
    << " clientRank=" << conn->clientRank
    << " wr.wr_id=0x" << (void *) wr.wr_id
    << " qp=0x" << (void *) conn->qp
    << " sge.addr=0x" << (void *) sge.addr
    << " sge.length=" << sge.length
    << " RDMA-ing to wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
    << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
    << " mSentBytes=" << mSentBytes
    << " mReceivedBytes=" << mReceivedBytes
    << " "
    << EndLogLine ;

  BegLogLine(FXLOG_ITAPI_ROUTER | FXLOG_ITAPI_ROUTER_FRAMES )
      << "conn->clientId=" << conn->clientId
      << " this=0x" << (void *) this
      << " ACK-TX-FRAME {" << mSentBytes
      << "," << mReceivedBytes
      << "," << 0
      << "}"
      << " DestBuf=0x" << (void*)wr.wr.rdma.remote_addr
      << EndLogLine ;

  conn->ibv_send_last_optype = k_wc_ack;
  conn->ibv_post_send_count += 1 ;
  int rc = ( conn->mDisconnecting ) ? 0 : ibv_post_send(conn->qp, &wr, &bad_wr);
  BegLogLine( rc != 0 )
    << "ibv_post_send fails, rc=" << rc
    << EndLogLine ;

  if( rc != 0 )
    conn->mDisconnecting = true;
  return (rc == 0);
}

bool ion_cn_buffer::rawTransmit(struct connection *conn, unsigned long aTransmitCount)
  {
    unsigned long Sentinel=SentinelIndex(aTransmitCount) ;
    AssertLogLine(Sentinel < k_ApplicationBufferSize)
      << "Sentinel=" << Sentinel
      << " is outside application buffer"
      << EndLogLine ;
    // This marker is how the CN receiver tells that the RDMA is complete
    mApplicationBuffer[Sentinel] = 0xff ;

    unsigned long RDMACount=Sentinel+1+(sizeof(class ion_cn_buffer) - k_ApplicationBufferSize) ;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t) &(conn->endio_downlink ) ;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = NULL ;

    sge.addr = (uintptr_t)this;
    sge.length = RDMACount ;
    sge.lkey = conn->mr->lkey ;

    wr.wr.rdma.remote_addr = conn->routerBuffer_raddr + offsetof(class ion_cn_all_buffer,mReceiveBuffer) + conn->mBuffer.mTransmitBuffer.CurrentBufferIndex()*sizeof(class ion_cn_buffer) ;
    wr.wr.rdma.rkey = conn->routerBuffer_rkey ;

    BegLogLine(FXLOG_ITAPI_ROUTER_LW)
      << "conn=0x" << (void *) conn
      << " clientRank=" << conn->clientRank
      << " wr.wr_id=0x" << (void *) wr.wr_id
      << " qp=0x" << (void *) conn->qp
      << " sge.addr=0x" << (void *) sge.addr
      << " sge.length=" << sge.length
      << " RDMA-ing to wr.wr.rdma.remote_addr=0x" << (void *) wr.wr.rdma.remote_addr
      << " wr.wr.rdma.rkey=0x" << (void *) wr.wr.rdma.rkey
      << " mSentBytes=" << mSentBytes
      << " mReceivedBytes=" << mReceivedBytes
      << " "
      << EndLogLine ;

    BegLogLine(FXLOG_ITAPI_ROUTER | FXLOG_ITAPI_ROUTER_FRAMES )
        << "conn->clientId=" << conn->clientId
        << " this=0x" << (void *) this
        << " TX-FRAME {" << mSentBytes
        << "," << mReceivedBytes
        << "," << aTransmitCount
        << "}"
        << " DestBuf=0x" << (void*)wr.wr.rdma.remote_addr
        << " actual_size: " << RDMACount
        << EndLogLine ;

    conn->ibv_send_last_optype = k_wc_downlink ;
    conn->ibv_post_send_count += 1 ;
    int rc = ( conn->mDisconnecting ) ? 0 : ibv_post_send(conn->qp, &wr, &bad_wr);
    BegLogLine( rc != 0 )
      << "ibv_post_send fails, rc=" << rc
      << EndLogLine ;

    if( rc != 0 )
      conn->mDisconnecting = true;
    return ( rc == 0 );
  }

static void die(const char *reason);
static int drain_cm_queue( rdma_event_channel *aEC );
static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void register_memory(struct connection *conn);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void wc_stat_echo(struct ibv_wc *wc);
static struct context *s_ctx = NULL;

static pthread_t setup_polling_thread(void) ;
static void FreeClientId( const iWARPEM_StreamId_t aClient );
static int ConnectToServers( int aMyRank, const skv_configuration_t *config );

enum {
  k_ListenQueueLength=64
};

int main(int argc, char **argv)
{
  struct sockaddr_in addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;

  // get configuration to find servers
  skv_configuration_t *config;
  if( argc > 1 )
    config = skv_configuration_t::GetSKVConfiguration( argv[ 1 ] );
  else
    config = skv_configuration_t::GetSKVConfiguration();

  ofed_mvapich_workaround();

  int rc = 0;
  MPI_Init( &argc, &argv );
  int Rank;
  int NodeCount;
  MPI_Comm_rank( MPI_COMM_WORLD, &Rank );
  MPI_Comm_size( MPI_COMM_WORLD, &NodeCount );
  FxLogger_Init( argv[ 0 ], Rank );

  pthread_t socket_poll_thread = setup_polling_thread() ;
  if( socket_poll_thread == 0 )
  {
    die( "Error: Could not create socket polling thread.");
  }
  uint16_t port = config->GetSKVForwarderPort();

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  // initialize the clientid map
  for( int n=0; n<IT_API_MAX_ROUTER_SOCKETS; n++ )
    gClientIdMap[ n ] = IWARPEM_INVALID_CLIENT_ID;

  // connect the forwarder to all servers
  rc = ConnectToServers( Rank, config );
  if( rc != 0 )
  {
    die("Error: Could not create connections to SKV Servers.");
  }

  ec = rdma_create_event_channel();
  rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP);
  rc = rdma_bind_addr(listener, (struct sockaddr *)&addr);
  if (rc != 0) {
    die("Error: Could not bind to port");
  }

  rdma_listen(listener, k_ListenQueueLength);
  printf("RDMA server on port %d\n", port);
  fflush(stdout) ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << " RDMA server on port " << port
    << EndLogLine ;

  // make sure the rdma_get_cm_event is non-blocking to allow multi-fetch within a critical section
  socket_nonblock_on( ec->fd );

  int status = 0;
  while( status == 0 )
  {
    status = rdma_get_cm_event(ec, &event);
    if(( status != 0 ) && ( errno == EAGAIN ))
    {
      status = 0;
      continue;
    }

    if( status == 0 )
    {
      struct rdma_cm_event event_copy;

      memcpy(&event_copy, event, sizeof(struct rdma_cm_event));
      rdma_ack_cm_event(event);

      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << "New CM event: " << event->event
        << " Copy: " << event_copy.event
        << EndLogLine;

      status = on_event(&event_copy);

      if( event_copy.event == RDMA_CM_EVENT_DISCONNECTED )
        {
        status = drain_cm_queue( ec );
        BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
          << "Completed CM queue drain: " << status
          << EndLogLine;
        }

    }
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  MPI_Barrier( MPI_COMM_WORLD );
  MPI_Finalize() ;

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  int yesno = 0;
  MPI_Initialized( &yesno );
  if( yesno != 0 )
    MPI_Abort( MPI_COMM_WORLD, EXIT_FAILURE );
  else
    exit(EXIT_FAILURE);
}

void wc_stat_echo(struct ibv_wc *wc)
{
    printf("Status: %u Opcode: %u\n", wc->status, wc->opcode);
    fflush(stdout) ;
}

/* in case of a disconnect, we need to first check
 * for further disconnects to mark dead connections as dead
 * before drain_cq() tries to operate on those dead connections
 */
int drain_cm_queue( rdma_event_channel *aEC )
{
  LockCMEventStream();
  struct rdma_cm_event *event = NULL;
  int status = 0;

  while( (rdma_get_cm_event( aEC, &event ) == 0 ) && ( status == 0) )
  {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(struct rdma_cm_event));
    rdma_ack_cm_event(event);

    BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
      << "New CM event: " << event->event
      << " Copy: " << event_copy.event
      << EndLogLine;

    status = on_event( &event_copy );
  }
  UnlockCMEventStream();
  return status;
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

static void add_socket_to_poll(iWARPEM_Router_Endpoint_t *conn) ;

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

// Point the connection at the start of its downlink buffer
static void rewind_downstream_buffer(struct connection *conn)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Downstream sequence number=" << conn->downstream_sequence
      << " conn=0x" << (void *) conn
      << EndLogLine ;
    conn->downstream_length = 2*sizeof(unsigned long) ;
  }

static inline void add_conn_to_flush_queue_locked( struct connection *conn )
{
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "conn=0x" << (void *) conn
    << " clientId=" << conn->clientId
    << " gFirstConnToAck=0x" << (void*)gFirstConnToAck
    << " mNextConnToAck=0x" << (void*)conn->mNextConnToAck[ gFlushListIndex ]
    << EndLogLine ;

  int ThisFlushListIndex = gFlushListIndex;

  // never try to add a connection that's already in the list
  // it's part of the list if the next ptr is != NULL
  if((conn != gFirstConnToAck) &&
     (conn->mNextConnToAck[ ThisFlushListIndex ] == NULL) &&
     (conn->mWaitForUpstreamFlush == false ) &&
     (conn->mDisconnecting == false))
  {
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void *) conn
      << " clientId=" << conn->clientId
      << " gFirstConnToAck=0x" << (void*)gFirstConnToAck
      << " mNextConnToAck=0x" << (void*)conn->mNextConnToAck[ ThisFlushListIndex ]
      << " localIndex=" << ThisFlushListIndex
      << " gIndex=" << gFlushListIndex
      << EndLogLine ;

    conn->mNextConnToAck[ ThisFlushListIndex ] = gFirstConnToAck;
    gFirstConnToAck = conn ;
  }
}
static void add_conn_to_flush_queue(struct connection *conn)
{
  LockDownStreamFlush();
  add_conn_to_flush_queue_locked( conn );
  UnlockDownStreamFlush();
}
static bool flush_downstream(struct connection *conn)
{
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Flushing conn=0x" << (void *) conn
    << EndLogLine ;
  BegLogLine( FXLOG_ITAPI_ROUTER && (conn->mWaitForUpstreamFlush == true) )
    << " Flushing while we have to wait for upstream flush"
    << " conn=0x" << (void*)conn
    << " mWait=" << conn->mWaitForUpstreamFlush
    << EndLogLine;

  class ion_cn_all_buffer *buffer=&conn->mBuffer ;
  bool rc_transmit=buffer->Transmit(conn) ;

  conn->mSendAck = !rc_transmit;

  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "rc_transmit=" << rc_transmit
    << EndLogLine ;
  return rc_transmit ;
}

static void flush_all_downstream(void)
{
  LockDownStreamFlush();

  BegLogLine(0 | FXLOG_ITAPI_ROUTER_DETAIL)
    << "Flushing all, first_connection_to_flush=0x" << (void *)gFirstConnToAck
    << " Index: " << gFlushListIndex
    << EndLogLine ;
  gFlushConnectionCount++;
  struct connection *conn=gFirstConnToAck ;
  gFirstConnToAck = FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR;
  int ThisFlushListIndex = gFlushListIndex;
  gFlushListIndex = 1 - gFlushListIndex;

  UnlockDownStreamFlush();

  while ( (conn != NULL) && (conn != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR) )
  {
    BegLogLine(FXLOG_ITAPI_ROUTER_DETAIL)
      << "Flushing conn=0x" << (void *)conn
      << " via flush_all..."
      << " Index: " << gFlushListIndex
      << EndLogLine ;

    bool rc_flush=true;
    if( (! conn->mDisconnecting) && conn->mBuffer.ReadyToTransmit())
      rc_flush = flush_downstream(conn);
    struct connection *next_conn = conn->mNextConnToAck[ ThisFlushListIndex ] ;
    conn->mNextConnToAck[ ThisFlushListIndex ]=NULL ;

    if( ! rc_flush  &&  ! conn->mDisconnecting )
    {
      // Re-queue this connection because the attempted transmit didn't go anywhere
      add_conn_to_flush_queue(conn) ;
    }
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
  class ion_cn_all_buffer *buffer=&conn->mBuffer ;

  // There must be room for a sentinel byte at the end of the buffer, because that is how the ION will
  // determine that the RDMA is complete
  if ( sizeof(LocalEndpointIndex) + sizeof(rHdr) + TotalDataLen >= buffer->SpaceInBuffer())
  {
    BegLogLine(FXLOG_ITAPI_ROUTER | FXLOG_ITAPI_ROUTER_FRAMES)
      << "conn=0x" << (void *) conn
      << " buffer fills, flushing all downstream"
      << EndLogLine ;
    bool rc_flushed = true;
    do
    {
      LockDownStreamFlush();
      rc_flushed = flush_downstream( conn ) ;
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " flushed=" << rc_flushed
        << EndLogLine ;
      UnlockDownStreamFlush();
      // no need to check rc_flushed since flush_downstream resets will_flush_this_connection
    } while( ! rc_flushed );
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
  StrongAssertLogLine(sizeof(LocalEndpointIndex) + sizeof(rHdr) + TotalDataLen < buffer->SpaceInBuffer())
    << "conn=0x" << (void *) conn
    << " message length=" << TotalDataLen
    << " overflows buffer, SpaceInBuffer=" << buffer->SpaceInBuffer()
    << EndLogLine ;

  // We have to hold the transmit mutex over this, otherwise we get deadlocks if the receive thread sends an
  // ack when this is part-done
  buffer->LockTransmit() ;
  buffer->AppendToBuffer(&LocalEndpointIndex,sizeof(LocalEndpointIndex)) ;
  buffer->AppendToBuffer(&rHdr,sizeof(rHdr)) ;
  buffer->AppendToBuffer(rData,TotalDataLen) ;
  buffer->UnlockTransmit() ;

  add_conn_to_deferred_ack(conn) ;
}

static void close_crossbar_link(struct connection *conn, unsigned int LocalEndpointIndex)
  {
    BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
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

    BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
      << "Removed serverId/LocalEndpointIndex " << LocalEndpointIndex
      << " from uplink and downlink lists."
      << EndLogLine ;
  }
static int process_downlink( iWARPEM_Router_Endpoint_t *aServerEP )
{
  StrongAssertLogLine( aServerEP != NULL )
    << " invalid router endpoint"
    << EndLogLine ;
  iWARPEM_StreamId_t client;
  iWARPEM_Msg_Type_t msg_type;
  struct iWARPEM_Message_Hdr_t *rHdr;
  char *rData;

  Crossbar_Entry_t *cb = NULL;
  iWARPEM_Status_t status;

  // read until the next message of valid client gets extracted
  // in general, we'll only loop if there are messages for clients that went down
  do
  {
    status = aServerEP->GetNextMessageType( &msg_type, &client );

    if( (status == IWARPEM_SUCCESS) && (msg_type <= 0 || msg_type > iWARPEM_SOCKET_CLOSE_REQ_TYPE ) )
    {
      BegLogLine( aServerEP->IsValidClient( client ) )
        << "LocalHdr.mMsg_Type=" << msg_type
        << " Upstream IP address=0x" << (void *) conn->upstream_ip_address[ client ]
        << " port=" << conn->upstream_ip_port[ client ]
        << ". Hanging for diagnosis"
        << EndLogLine ;
      printf("client (%d) or mMsg_Type (%d) is out of range, hanging for diagnosis\n", client, msg_type) ;
      fflush(stdout) ;
      for (;;) { sleep(10) ; }
    }

    StrongAssertLogLine( status == IWARPEM_SUCCESS )
      << "Message type retrieval failed: status=" << (int)status
      << " Peer has probably gone down, no recovery."
      << " remaining data:" << aServerEP->RecvDataAvailable()
      << EndLogLine ;

    // read the data (if client is invalid, we read to skip the message)
    status = aServerEP->ExtractNextMessage( &rHdr, &rData, &client );

    if( aServerEP->IsValidClient( client ) )
    {
      cb = aServerEP->GetClientEP( client );
    }
    else
    {
      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << "client=" << client
        << " has no crossbar entry"
        << " message will be skipped/ignored"
        << EndLogLine ;
    }
  } while ( (cb == NULL) && (aServerEP->RecvDataAvailable()) );

  if( cb == NULL )
  {
    BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
      << "No data for valid client found. Exiting EP processing for this event..."
      << EndLogLine;
    return -1;
  }

  struct connection *conn = (struct connection*)cb->getDownLink();

  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "CONNECTION RESPONSE conn=0x" << (void *) conn
    << " client=" << client
    << " msg_type=" << iWARPEM_Msg_Type_to_string( msg_type )
    << EndLogLine ;

  if( status == IWARPEM_SUCCESS )
  {
    BegLogLine( 0 )
      << "Forwarding downstream of client " << client
      << " type: " << iWARPEM_Msg_Type_to_string( rHdr->mMsg_Type )
      << " len: " << rHdr->mTotalDataLen
      << EndLogLine;
    queue_downstream(conn,cb->getServerId(), *rHdr,rData) ;

    // -> mostly diagnosis and aftermath
    // Set up a local header to diagnose if Hdr is beibng trampled by an RDMA read
    // tjcw: If it is, this doesn't really solve the exposure
    size_t TotalDataLen = rHdr->mTotalDataLen ;
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "conn=0x" << conn
      << " mMsg_Type="<< iWARPEM_Msg_Type_to_string(msg_type)
      << " TotalDataLen=" << TotalDataLen
      << EndLogLine ;
    report_hdr(*rHdr) ;
    if (msg_type == iWARPEM_DISCONNECT_RESP_TYPE)
    {
      BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
        << "Forwarded a DISCONNECT RESPONSE message"
        << " Removing Crossbar entry for client=" << client
        << " server=" << cb->getServerId()
        << EndLogLine ;
      close_crossbar_link( conn, cb->getServerId() );
    }
    if (msg_type == iWARPEM_SOCKET_CONNECT_RESP_TYPE)
    {
      BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
        << "Forwarded a CONNECT RESPONSE"
        << " client=" << client
        << " server=" << cb->getServerId()
        << EndLogLine ;
    }
  }
  else
  {
    aServerEP->CloseAllClients();  // needs more work... (send downstream close for all clients of the broken serverEP)

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

    Hdr.mMsg_Type = iWARPEM_SOCKET_CLOSE_TYPE ;
    Hdr.mTotalDataLen = 0 ;
    queue_downstream(conn,cb->getServerId(), Hdr,NULL) ;
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
    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "Blocking on epoll wait"
      << EndLogLine ;
    int nfds = epoll_wait_handling_eintr(epoll_fd, events, k_max_epoll, -1) ;
    AssertLogLine(nfds != -1)
      << "epoll_wait failed, errno=" << errno
      << EndLogLine ;

    BegLogLine(FXLOG_ITAPI_ROUTER)
      << "epoll_wait returns, nfds=" << nfds
      << EndLogLine ;
    for ( int n=0;n<nfds; n+=1)
    {
      struct epoll_record * ep = (struct epoll_record *)events[n].data.ptr ;
      uint32_t epoll_events = events[n].events ;
      iWARPEM_Router_Endpoint_t *rEP = ep->conn ;
      int fd=ep->fd ;
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) rEP
        << " fd=" << fd
        << EndLogLine ;
      if ( rEP == NULL)
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
          status = process_downlink( rEP ) ;
        } while ( (status == 0) && rEP->RecvDataAvailable() );
      }
    }

    if( nfds == 0 )
      flush_all_downstream();
  }
  return NULL ;
}
static pthread_t setup_polling_thread(void)
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
    True = 1;
    setsockopt( drc_serv_socket, SOL_TCP, TCP_NODELAY, (char*)&True, sizeof(True));

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

      return 0;
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

    True = 1;
    setsockopt( drc_cli_socket, SOL_TCP, TCP_NODELAY, (char*)&True, sizeof(True));

    if( listen( drc_serv_socket, 5 ) < 0 )
      {
        perror( "listen failed" );
        die( "Listen Failed" );
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
    if( ! rc )
      return DataReceiverTID;
    else
      return 0;
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
    StrongAssertLogLine(ServerEP != NULL)
      << "No server configured on IP address " << (void *) SocketConnect.ipv4_address
      << " port " << SocketConnect.ipv4_port
      << EndLogLine ;

    // create the crossbar link entry to do forward and reverse lookup on events
    Crossbar_Entry_t *cb = new Crossbar_Entry_t( conn->clientId, LocalEndpointIndex, ServerEP, conn );

    ServerEP->AddClient( conn->clientId, cb );
    conn->socket_fds[ LocalEndpointIndex ] = cb ;

    BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
        << "CONNECT TO UPLINK conn=0x" << (void *) conn
        << " client=" << cb->getClientId()
        << " server=" << cb->getServerId()
        << " LocalEndpointIndex=" << LocalEndpointIndex
        << " ipv4_address=0x" << (void*)SocketConnect.ipv4_address
        << " ipv4_port=" << SocketConnect.ipv4_port
        << " socket_fds[]=" << (void*) conn->socket_fds[ LocalEndpointIndex ]
        << EndLogLine ;

// We have an assertion to protect against the ServerEP being NULL
//    if( ServerEP != NULL )
      ServerEP->InsertConnectRequest( conn->clientId, &Hdr, &PrivateData, cb );
  }
static void drain_cq(void) ;

static inline
void FlushMarkedUplinks()
{
  int flushedEPs = 0;
  static uint32_t flushcounts = 0;
  static double flushavg = 1.0;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << EndLogLine ;

  Uplink_list_t::iterator ServerEP = gUplinkList.begin();
  while( ServerEP != gUplinkList.end() )
  {
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "Checking *ServerEP=" << (void *)(*ServerEP)
        << " ( socket=" << (*ServerEP)->GetRouterFd()
        << " ) NeedsFlush()=" << (*ServerEP)->NeedsFlush()
        << EndLogLine ;
    if( (*ServerEP)->NeedsFlush() )
    {
      (*ServerEP)->FlushSendBuffer();
      flushedEPs++;
    }
    ServerEP++;
  }

  flushavg = (flushavg*0.995) + (flushedEPs*0.005);

  BegLogLine( 0 && ( ((flushcounts++) & 0xfff) == 0 ) )
    << "average flushed uplinks: " << flushavg
    << EndLogLine;
}
static void send_upstream( const iWARPEM_Router_Endpoint_t *aServerEP,
                           const iWARPEM_StreamId_t aClientId,
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

    BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP && (Hdr.mMsg_Type == iWARPEM_DISCONNECT_REQ_TYPE))
      << "DISCONNECT TYPE Message"
      << EndLogLine;

    iWARPEM_Status_t status = ((iWARPEM_Router_Endpoint_t*)aServerEP)->InsertMessage( aClientId, &Hdr, (const char*)message, Hdr.mTotalDataLen );

    AssertLogLine( (status == IWARPEM_SUCCESS) )
      << "Message insertion error on EP: " << aServerEP->GetRouterFd()
      << " rc=" << status
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
    report_hdr(Hdr) ;
    switch(Msg_Type)
      {
    case iWARPEM_KERNEL_CONNECT_TYPE:
      conn->clientRank=Hdr.mOpType.mKernelConnect.mClientRank;
      conn->routerBuffer_raddr=Hdr.mOpType.mKernelConnect.mRouterBuffer_addr;
      conn->routerBuffer_rkey=Hdr.mOpType.mKernelConnect.mRoutermemreg_lkey ;
      BegLogLine( FXLOG_ITAPI_ROUTER )
        << "NEW CONNECTION: "
        << " conn=0x" << (void*)conn
        << " clientRank=" << conn->clientRank
        << " clientId=" << conn->clientId
        << EndLogLine;

      break ;
    case iWARPEM_SOCKET_CONNECT_REQ_TYPE:
      open_socket_send_private_data(conn,LocalEndpointIndex,Hdr.mOpType.mSocketConnect,*(const iWARPEM_Private_Data_t *)message);
      break ;
    case iWARPEM_SOCKET_CLOSE_REQ_TYPE:
      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << "CLOSING CONNECTION: "
        << " conn=0x" << (void*)conn
        << " clientRank=" << conn->clientRank
        << " clientId=" << conn->clientId
        << EndLogLine;
      close_crossbar_link(conn,LocalEndpointIndex) ;
      break ;
    default:
      StrongAssertLogLine(Msg_Type >= iWARPEM_DTO_SEND_TYPE && Msg_Type <= iWARPEM_DISCONNECT_RESP_TYPE)
          << "Hdr.mMsg_Type=" << Msg_Type
          << " is out of range"
          << EndLogLine ;
      Crossbar_Entry_t *cb = conn->socket_fds[ LocalEndpointIndex ];

      BegLogLine( 0 )
          << "Hdr.mMsg_Type=" << Msg_Type
          << " cb: 0x" << (void*)cb
          << EndLogLine ;

      if( cb != NULL )
        send_upstream( cb->getUpLink(), cb->getClientId(), Hdr, message) ;
      break ;
    }
  }
#include <skv/common/skv_types.hpp>

struct Server_Connection_t
{
  int socket;
  int port;
  char addr_string[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  iWARPEM_Router_Endpoint_t *ServerEP;
};


int ConnectToServers( int aMyRank, const skv_configuration_t *config )
{
  int rc = 0;

  ifstream fin( config->GetMachineFile() );

  StrongAssertLogLine( !fin.fail() )
    << "skv_client_conn_manager_if_t::Connect():: ERROR opening server machine file: " << config->GetMachineFile()
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

      int True = 1;
      setsockopt( connections[conn_count].socket, SOL_TCP, TCP_NODELAY, (char*)&True, sizeof(True));

      connected = ( connect( connections[ conn_count ].socket, srv->ai_addr, srv->ai_addrlen ) == 0 );
      if ( connected )
        break;

      close( connections[ conn_count ].socket );
      srv = srv->ai_next;
    }

    if( connected )
    {
      socket_nonblock_on( connections[ conn_count ].socket );
      socket_nodelay_on( connections[ conn_count ].socket );

// #define IT_API_SOCKET_BUFF_SIZE ( 16 * 1024 )
//       socklen_t ArgSize = sizeof( int );
//       int SockSendBuffSize = IT_API_SOCKET_BUFF_SIZE;
//       int SockRecvBuffSize = 16 * IT_API_SOCKET_BUFF_SIZE;
//       setsockopt( connections[ conn_count ].socket, SOL_SOCKET, SO_SNDBUF, (const char *) & SockSendBuffSize, ArgSize );
//       setsockopt( connections[ conn_count ].socket, SOL_SOCKET, SO_RCVBUF, (const char *) & SockRecvBuffSize, ArgSize );

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

static struct ThreadSafeQueue_t<struct endiorec *, 0> cqSlihQueue ;
static struct ThreadSafeQueue_t<struct termination_entry_t *, 0> gTerminationQueue;

static void do_cq_slih_processing(struct endiorec * endiorec, int * requireFlushUplinks)
{
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << " endiorec=0x" << (void *) endiorec
    << EndLogLine ;
  if( !endiorec )
    return;

  enum optype optype = endiorec->optype ;
  struct connection *conn = endiorec->conn ;
  switch(optype)
  {
    case k_wc_recv:
      BegLogLine(FXLOG_ITAPI_ROUTER )
        << "conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " k_wc_recv"
        << EndLogLine ;
      *requireFlushUplinks = conn->mBuffer.ProcessCall(conn) ;

      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "Setting requireFlushUplinks=" << *requireFlushUplinks
        << EndLogLine ;
      break ;

    case k_wc_uplink:
      BegLogLine(FXLOG_ITAPI_ROUTER )
        << " conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " k_wc_uplink"
        << EndLogLine ;
      conn->mBuffer.ProcessRead( conn );

      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "Setting requireFlushUplinks=1"
        << EndLogLine ;
      *requireFlushUplinks = 1 ;
      break;

    case k_wc_terminate:
    {
      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << " conn=0x" << (void*)conn
        << " deferred memory disposal after termination."
        << EndLogLine;

      struct rdma_cm_id *id = endiorec->oprec.link_terminate.mRdmaId;

      rdma_destroy_qp(id);
      FreeClientId( conn->clientId );
      ibv_dereg_mr(conn->mr) ;
      rdma_destroy_id(id);

      bzero( conn, sizeof( struct connection ) );
      free( conn );
      free( endiorec );
      *requireFlushUplinks = 1;
      break;
    }
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

  StrongAssertLogLine(wc.status == IBV_WC_SUCCESS)
    << "Bad wc.status=" << wc.status
    << " from ibv_poll_cq"
    << EndLogLine ;

  struct endiorec * endiorec = ( struct endiorec * )wc.wr_id;
  enum optype optype = endiorec->optype ;
  struct connection *conn = endiorec->conn ;

  if( !conn || conn->mDisconnecting )
    return;

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
        << " clientId=" << conn->clientId
        << " k_wc_recv"
        << EndLogLine ;
      received++ ;
      cqSlihQueue.Enqueue(endiorec) ;
      break ;
    case k_wc_uplink:
      BegLogLine(FXLOG_ITAPI_ROUTER_LW )
        << " conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " clientId=" << conn->clientId
        << " k_wc_uplink"
        << EndLogLine ;
      cqSlihQueue.Enqueue(endiorec) ;
      break ;
    case k_wc_downlink:
      BegLogLine(FXLOG_ITAPI_ROUTER_LW)
        << "conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " clientId=" << conn->clientId
        << " k_wc_downlink TX complete. Nothing to do."
        << EndLogLine ;
      break ;
    case k_wc_ack:
      BegLogLine(FXLOG_ITAPI_ROUTER)
        << "conn=0x" << (void *) conn
        << " clientRank=" << conn->clientRank
        << " clientId=" << conn->clientId
        << " k_wc_ack. TX complete. Nothing to do."
        << EndLogLine ;
      break ;
    case k_wc_terminate:
      BegLogLine( FXLOG_ITAPI_ROUTER | FXLOG_ITAPI_ROUTER_CLEANUP )
        << "conn=0x" << (void*) conn
        << " Termination event/processing"
        << EndLogLine;
    default:
      StrongAssertLogLine(0)
        << "Unknown optype=" << optype
        << EndLogLine ;
      break ;
  }
}

enum {
  k_spin_poll=1
};

enum {
  k_wc_array_size=64 ,
  k_drain_loop=1
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
static void add_conn_to_deferred_ack( struct connection *conn, bool aDownStreamWaitFlush )
{
  LockDownStreamFlush();
  bool skipped = true;
  conn->mWaitForUpstreamFlush |= aDownStreamWaitFlush;
  if( ( conn != gDeferredAckList ) && ( conn->mNextDeferredAck == NULL ) && ( !conn->mDisconnecting ))
  {
    skipped = false;
    conn->mNextDeferredAck = gDeferredAckList;
    gDeferredAckList = conn;
  }
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "conn=0x" << (void*) conn
    << " gDeferredAckList=0x" << (void*)gDeferredAckList
    << " mNextDeferredAck=0x" << (void*)conn->mNextDeferredAck
    << " disconnecting?" << conn->mDisconnecting
    << " mWait=" << conn->mWaitForUpstreamFlush
    << " skipped= " << skipped
    << EndLogLine;
  UnlockDownStreamFlush();
}
static void AddAckCandidatesToQueue()
{
  LockDownStreamFlush();
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << " adding ACK-candidates list=0x" << (void*)gDeferredAckList
    << EndLogLine;
  struct connection *conn = gDeferredAckList;
  gDeferredAckList = FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR;
  while(( conn != NULL )&&( conn != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR ))
  {
    conn->mSendAck=true;
    conn->mWaitForUpstreamFlush = false;
    BegLogLine(0|FXLOG_ITAPI_ROUTER)
      << "conn=0x" << (void*)conn
      << " moving into flush queue=0x" << (void*)gFirstConnToAck
      << " setting mSendAck=" << conn->mSendAck
      << " gFlushListIndex=" << gFlushListIndex
      << " mWait=" << conn->mWaitForUpstreamFlush
      << EndLogLine;
    add_conn_to_flush_queue_locked( conn );
    struct connection *nextConn = conn->mNextDeferredAck;
    conn->mNextDeferredAck = NULL;
    conn = nextConn;
  }
  UnlockDownStreamFlush();
}
static inline
void TerminateConnection( struct termination_entry_t *aTerm )
{
  struct connection *conn = aTerm->mConn;
  struct rdma_cm_id *id = aTerm->mRdmaId;

  BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
    << "Starting deferred cleanup of conn=0x" << (void*)conn
    << " aTerm=0x" << (void*)aTerm
    << EndLogLine;

  // disconnect all Crossbar entries of conn
  for( int n=0; n < IT_API_MAX_ROUTER_SOCKETS; n++ )
  {
    Crossbar_Entry_t *cb = conn->socket_fds[ n ];
    BegLogLine( 0 )
      << "CrossBar of conn=0x" << (void*)conn
      << " at index=" << n
      << " is 0x" << (void*)cb
      << EndLogLine;
    if( cb != NULL )
    {
      iWARPEM_StreamId_t client = cb->getClientId();
      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << "Cleaning up Crossbar between client=" << client
        << " and server=" << cb->getServerId()
        << EndLogLine;

      // send a disconnect via multiplexed endpoint
      iWARPEM_Router_Endpoint_t *srv = (iWARPEM_Router_Endpoint_t*)cb->getUpLink();
      srv->InsertDisconnectRequest( client );

      close_crossbar_link(conn, cb->getServerId() );
    }
  }

  conn->mBuffer.Term() ;

  // Inject marker into sliq to prevent currently queued requests hitting freed memory
  struct endiorec *term_marker = new struct endiorec;
  term_marker->conn = conn;
  term_marker->optype = k_wc_terminate;
  term_marker->oprec.link_terminate.mRdmaId = id;
  cqSlihQueue.Enqueue( term_marker );
}

typedef enum
{
  DOWNLINK_STATUS_IDLE,
  DOWNLINK_STATUS_QUEUING,
  DOWNLINK_STATUS_PROCESSING,
  DOWNLINK_STATUS_FLUSH,
  DOWNLINK_STATUS_DISCONNECTS,
  DOWNLINK_STATUS_ERROR
}  downlink_status_t;

#define UPSTREAM_BURSTLEN ( 32 )

static inline
downlink_status_t downlink_sm( const downlink_status_t aState,
                               struct ibv_cq *aCQ )
{
  static int RequestIndex = 0;
  static int RequestCount = 0;
  static downlink_status_t saved_status = DOWNLINK_STATUS_IDLE;

  static struct ibv_wc wc[k_wc_array_size];
  downlink_status_t entry_status = aState;
  downlink_status_t return_status = aState;

  if( gTerminationQueue.GetCount() )
    {
    entry_status = DOWNLINK_STATUS_DISCONNECTS;
    if( aState != DOWNLINK_STATUS_DISCONNECTS )
      saved_status = aState;
    }

  switch( entry_status )
  {
    case DOWNLINK_STATUS_IDLE:
    {
      int rv = ibv_poll_cq(aCQ, k_wc_array_size, wc);
      if( rv > 0 )
      {
      BegLogLine( 0 )
        << "IDLE STATE found new requests: " << rv
        << EndLogLine;

        RequestCount = rv;
        RequestIndex = 0;
        return_status = DOWNLINK_STATUS_QUEUING;
      }
      else if ( rv == 0 )
      {
        saved_status = aState;
        return_status = DOWNLINK_STATUS_FLUSH;

        ::sched_yield();
      }
      else
      {
        BegLogLine( 1 )
          << "poll_cq processing ends because ibv_poll_cq returns with rv=" << rv
          << " exiting state machine."
          << EndLogLine ;
        return_status == DOWNLINK_STATUS_ERROR;
      }
      break;
    }

    case DOWNLINK_STATUS_QUEUING:
      // if there are events: process them - i.e. Queue them for slih-processing
      BegLogLine( 0 )
        << "QUEUEING STATE: preprocessing " << RequestCount << " new entries"
        << EndLogLine;
      for( int i = 0; i < RequestCount; i++ )
        do_cq_processing( wc[ i ]);

      return_status = DOWNLINK_STATUS_PROCESSING;
      break;

    case DOWNLINK_STATUS_PROCESSING:
    {
      BegLogLine( 0 )
        << "PROCESSING STATE: Index=" << RequestIndex
        << EndLogLine;

      struct endiorec * endiorec ;
      static int requireFlushUplinks = 0 ;
      int flushUplinks = 0;
      int rc = cqSlihQueue.Dequeue(&endiorec);
      if ( 0 == rc)
      {
        do_cq_slih_processing(endiorec, &flushUplinks);
        requireFlushUplinks |= flushUplinks;
        RequestIndex++;
      }
      else
        return_status = DOWNLINK_STATUS_IDLE;

      if ( k_LazyFlushUplinks &&
          ( 0 != requireFlushUplinks ) &&
          (( RequestIndex % UPSTREAM_BURSTLEN == 0 ) || ( return_status == DOWNLINK_STATUS_IDLE ))
      )
      {
        BegLogLine( 0 )
          << " UpLinkFlush -----------------------------------------------------------------"
          << EndLogLine;
        FlushMarkedUplinks();
        requireFlushUplinks = 0 ;
        saved_status = return_status;
        return_status = DOWNLINK_STATUS_FLUSH;
      }
      break;
    }

    case DOWNLINK_STATUS_DISCONNECTS:
      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << "SM-STATE:" << entry_status
        << " entering"
        << EndLogLine;

      LockCMEventStream();
      while( gTerminationQueue.GetCount() )
      {
        struct termination_entry_t *ConnToTerm = NULL;
        int rv = gTerminationQueue.Dequeue( &ConnToTerm );
        if(( rv == 0 ) && ( ConnToTerm ))
        {
          AckAllConnections();

          BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
            << "Retrieving TermEntry=0x" << (void*)ConnToTerm
            << " conn=0x" << (void*)ConnToTerm->mConn
            << " id=0x" << (void*)ConnToTerm->mRdmaId
            << EndLogLine;
          TerminateConnection( ConnToTerm );
        }
      }
      FlushMarkedUplinks();
      return_status = saved_status; // continue at the previous state
      saved_status = DOWNLINK_STATUS_IDLE; // and reset the saved_status to idle
      UnlockCMEventStream();

      BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
        << "SM-STATE:" << entry_status
        << " finished. Next state: " << return_status
        << EndLogLine ;
      break;

    case DOWNLINK_STATUS_FLUSH:
      // in case there's any ACK to be send downstream
      // first get any waiting connections into the main ACK list...
      if( gDeferredAckList != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR )
        AddAckCandidatesToQueue();

      if( gFirstConnToAck != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR )
        AckAllConnections();

      return_status = saved_status; // continue at the previous state
      break;

    case DOWNLINK_STATUS_ERROR:
    default:
      printf("downlink state machine error! unknown state = %d", entry_status );
      return_status = DOWNLINK_STATUS_ERROR;
      break;
  }
  return return_status;
}

static void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;

  cqSlihQueue.Init(k_CompletionQueueSize) ;
  gTerminationQueue.Init(k_CompletionQueueSize);
  cq=s_ctx->cq ;
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "cq=" << (void *) cq
    << EndLogLine ;

  downlink_status_t sm_state = DOWNLINK_STATUS_IDLE;
  while( sm_state != DOWNLINK_STATUS_ERROR )
  {
    sm_state = downlink_sm( sm_state,
                            cq );
  }
  return NULL;
}

void register_memory(struct connection *conn)
{

    conn->mr = ibv_reg_mr(
        s_ctx->pd,
        (char *) &conn->mBuffer,
        sizeof(conn->mBuffer),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
         ) ;
    StrongAssertLogLine(conn->mr != NULL)
      << "ibv_reg_mr fails to register memory region"
      << EndLogLine ;
}

iWARPEM_StreamId_t GetFreeClientId( void )
{
  iWARPEM_StreamId_t id = 0;
  for( int n=0; (n < IT_API_MAX_ROUTER_SOCKETS); n++ )
  {
    if( gClientIdMap[ n ] == IWARPEM_INVALID_CLIENT_ID )
    {
      gClientIdMap[ n ] = n;
      BegLogLine( FXLOG_ITAPI_ROUTER )
        << "Handing out clientId=" << n
        << EndLogLine;
      return n;
    }
  }
  return IWARPEM_INVALID_CLIENT_ID;
}
static void FreeClientId( const iWARPEM_StreamId_t aClient )
{
  if(( aClient>=0 ) && ( aClient<IT_API_MAX_ROUTER_SOCKETS ))
  {
    BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
      << "Returning clientId=" << aClient
      << EndLogLine;
    gClientIdMap[ aClient ] = IWARPEM_INVALID_CLIENT_ID;
  }
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "Received connection request, id=0x" << (void *) id
    << EndLogLine ;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);
  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  conn = (struct connection *)malloc(sizeof(struct connection));
  bzero( conn, sizeof( struct connection ) );

  id->context = (void *) conn ;
//  pthread_mutex_init((pthread_mutex_t *) &conn->qp_write_mutex, NULL) ;
#if 0
  memset((void *) &conn->routerBuffer, 0xfd, sizeof(conn->routerBuffer)) ;
#endif
  conn->qp = id->qp;
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
  conn->upstreamSequence = 0 ;

  conn->downstream_sequence = 0 ;
  conn->localDownstreamSequence = 1 ;

  conn->routerBuffer_rkey = 0 ;
  conn->routerBuffer_raddr = 0 ;

  conn->issuedDownstreamFetch = 0 ;
  conn->flushed_downstream = 1 ;
  conn->mSendAck=false ;
  conn->mDisconnecting=false ;
  conn->mNextConnToAck[ 0 ] = NULL;
  conn->mNextConnToAck[ 1 ] = NULL;

  conn->clientId = GetFreeClientId();
  conn->mWaitForUpstreamFlush = false;
  BegLogLine( FXLOG_ITAPI_ROUTER )
    << " Initialized connection: conn=0x" << (void*) conn
    << EndLogLine;

  register_memory(conn);
#if 0
  post_call_buffer(conn,&conn->routerBuffer.callBuffer) ;
#endif
  conn->mBuffer.Init() ;
  conn->mBuffer.PostAllReceives(conn) ;
//  post_all_call_buffers(conn) ;
//  post_receives(conn, WORKREQNUM);

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

int on_connection(void *context)
{
  BegLogLine(FXLOG_ITAPI_ROUTER)
    << "connected, context=0x" << context
    << " doing nothing... connection completed."
    << EndLogLine ;
  return 0;
}

// assumes external locking...
static inline
void RemoveFromAckQueue( struct connection *conn )
{
  struct connection *conn_ptr = gDeferredAckList;

  BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
    << "Removing conn=0x" << (void *)conn
    << " from gDeferredAckList=0x" << (void *) gDeferredAckList
    << EndLogLine ;

  // first entry?
  if(( conn == conn_ptr ) && ( conn != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR ))
  {
    gDeferredAckList = conn->mNextDeferredAck;
    return;
  }
  // search entry
  while( (conn_ptr != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR) &&
      ( conn_ptr->mNextDeferredAck != NULL ) &&
      ( conn_ptr->mNextDeferredAck != conn ) )
  {
    conn_ptr = conn_ptr->mNextDeferredAck;
  }
  // remove from list
  if( conn_ptr->mNextDeferredAck == conn )
  {
    conn_ptr->mNextDeferredAck = conn->mNextDeferredAck;
    conn->mNextDeferredAck = NULL;
  }
}
static inline
void RemoveFromFlushList( struct connection * conn, const int aListIndex )
{
  BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
    << "Removing conn=0x" << (void *)conn
    << " from gFirstConnToAck=0x" << (void *) gFirstConnToAck
    << " aListIndex=" << aListIndex
    << EndLogLine ;

  struct connection *conn_ptr = gFirstConnToAck;
  // first entry?
  if( conn == conn_ptr )
  {
    gFirstConnToAck = conn->mNextConnToAck[ aListIndex ];
    return;
  }
  // search entry
  while( (conn_ptr != FORWARDER_MAGIC_FLUSH_QUEUE_TERMINATOR) &&
      ( conn_ptr->mNextConnToAck[ aListIndex ] != NULL ) &&
      ( conn_ptr->mNextConnToAck[ aListIndex ] != conn ) )
  {
    conn_ptr = conn_ptr->mNextConnToAck[ aListIndex ];
  }
  // remove from list
  if( conn_ptr->mNextConnToAck[ aListIndex ] == conn )
  {
    conn_ptr->mNextConnToAck[ aListIndex ] = conn->mNextConnToAck[ aListIndex ];
    conn->mNextConnToAck[ aListIndex ] = NULL;
  }
}
int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  BegLogLine(FXLOG_ITAPI_ROUTER_CLEANUP)
    << "Peer disconnected. Msgs received=" << received
    << " conn=0x" << (void *) conn
    << EndLogLine ;

  // on a disconnect, we can't send any more data over the IO-link
  conn->mDisconnecting = true ;
  LockDownStreamFlush();

  // remove conn from all Ack and Flush lists
  if( conn->mNextDeferredAck != NULL )
    RemoveFromAckQueue( conn );
  // remove from the inactive Flush list (the list that's currently filled)
  if(( conn->mNextConnToAck[ gFlushListIndex ] != NULL ) )
    RemoveFromFlushList( conn, gFlushListIndex );

  UnlockDownStreamFlush();

  struct termination_entry_t *ConnToTerm = new struct termination_entry_t;

  ConnToTerm->mConn = conn;
  ConnToTerm->mRdmaId = id;

  BegLogLine( FXLOG_ITAPI_ROUTER_CLEANUP )
    << "Queuing TermEntry=0x" << (void*)ConnToTerm
    << " conn=0x" << (void*)conn
    << " id=0x" << (void*)id
    << EndLogLine;

  gTerminationQueue.Enqueue( ConnToTerm );
  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  switch( event->event )
  {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
      r = on_connect_request(event->id);
      break;

    case RDMA_CM_EVENT_ESTABLISHED:
      r = on_connection(event->id->context);
      break;

    case RDMA_CM_EVENT_DISCONNECTED:
      r = on_disconnect(event->id);
      break;

    default:
      r = -1;
  }
  return r;
}

