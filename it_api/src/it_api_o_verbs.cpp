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
#include <Trace.hpp>

#include <errno.h> // for perror()
#include <sys/types.h>

#include <endian.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <ifaddrs.h>

#include <rdma/rdma_cma.h>

extern "C"
{
#define ITAPI_ENABLE_V21_BINDINGS
#include <it_api.h>
};

#ifndef FXLOG_IT_API_O_VERBS
#define FXLOG_IT_API_O_VERBS ( 0 )
#endif

#ifndef FXLOG_IT_API_O_VERBS_CONNECT
#define FXLOG_IT_API_O_VERBS_CONNECT ( 0 | FXLOG_IT_API_O_VERBS )
#endif

#ifndef FXLOG_IT_API_O_VERBS_MEMREG
#define FXLOG_IT_API_O_VERBS_MEMREG ( 0 | FXLOG_IT_API_O_VERBS )
#endif

#ifndef FXLOG_IT_API_O_VERBS_WRITE
#define FXLOG_IT_API_O_VERBS_WRITE ( 0 | FXLOG_IT_API_O_VERBS )
#endif

#ifndef FXLOG_IT_API_O_VERBS_QUEUE_LENGTHS_LOG
#define FXLOG_IT_API_O_VERBS_QUEUE_LENGTHS_LOG ( 0 | FXLOG_IT_API_O_VERBS )
#endif


#ifndef IT_API_TRACE
#define IT_API_TRACE ( 1 )
#endif

#ifndef IT_API_USE_SIW_HACK
//#define IT_API_USE_SIW_HACK
#endif

//#define IT_API_POST_OP_RETRIES 3
//#define ROQ_SGE_WORKAROUND


#define IT_API_NETWORK_TIMEOUT 20000   // set a network timeout in ms


#define CONTEXT_NAME_SIZE (128)
char gContextName[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_REG_MR_START;
char   gITAPI_REG_MR_START_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_REG_MR_FINIS;
char   gITAPI_REG_MR_FINIS_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_POST_SEND_START;
char   gITAPI_POST_SEND_START_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_POST_SEND_FINIS;
char   gITAPI_POST_SEND_FINIS_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_RDMA_READ_AT_WAIT;
char   gITAPI_RDMA_READ_AT_WAIT_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_RDMA_WRITE_AT_WAIT;
char   gITAPI_RDMA_WRITE_AT_WAIT_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_SEND_AT_WAIT;
char   gITAPI_SEND_AT_WAIT_Name[ CONTEXT_NAME_SIZE ];

static TraceClient gITAPI_RECV_AT_WAIT;
char   gITAPI_RECV_AT_WAIT_Name[ CONTEXT_NAME_SIZE ];

int gTraceRank = 0;

static unsigned short local_port = 17500;

// #include <it_api_o_verbs_types.h>
#include <it_api_o_verbs_thread.h>

/*******************************************************************
 * Global data
 *******************************************************************/
pthread_mutex_t gITAPIFunctionMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t gITAPI_INITMutex = PTHREAD_MUTEX_INITIALIZER;

struct rdma_cm_event		*current_cm_event = NULL;

int itov_aevd_defined = 0;
/*******************************************************************/


it_status_t
socket_nonblock_on( int fd )
{
  int flags = fcntl( fd, F_GETFL);
  int rc = fcntl( fd, F_SETFL, flags | O_NONBLOCK);
  if (rc < 0)
    {
      BegLogLine( 1 )
        << "socket_nonblock_on(): ERROR: "
        << " errno: " << errno
        << EndLogLine;

      return IT_ERR_ABORT;
    }

  return IT_SUCCESS;
}

it_status_t
socket_nonblock_off( int fd )
{
  int flags = fcntl( fd, F_GETFL);
  int rc = fcntl( fd, F_SETFL, flags & ~O_NONBLOCK);
  if (rc < 0)
    {
      BegLogLine( 1 )
        << "socket_nonblock_off(): ERROR: "
        << " errno: " << errno
        << EndLogLine;

      return IT_ERR_ABORT;
    }

  return IT_SUCCESS;
}


it_status_t
it_api_o_verbs_process_async_event( struct ibv_async_event* aRdmaEvent,
                                    it_event_t*             aITEvent )
{
  it_affiliated_event_t* AffEvent    = (it_affiliated_event_t *) aITEvent;

  BegLogLine( 1 )
    << "it_api_o_verbs_process_async_event(): Async event! Ohh Uhh. "
    << " event_type: " << aRdmaEvent->event_type
    << EndLogLine;

  switch( aRdmaEvent->event_type )
    {
      // QP events
    case IBV_EVENT_QP_FATAL:
    case IBV_EVENT_QP_REQ_ERR:
    case IBV_EVENT_QP_ACCESS_ERR:
    case IBV_EVENT_COMM_EST:
    case IBV_EVENT_SQ_DRAINED:
    case IBV_EVENT_PATH_MIG:
    case IBV_EVENT_PATH_MIG_ERR:
    case IBV_EVENT_QP_LAST_WQE_REACHED:
      {	
        AffEvent->event_number = IT_ASYNC_AFF_EP_FAILURE;
        AffEvent->cause.ep   = (it_ep_handle_t) aRdmaEvent->element.qp->qp_context;
        break;
      }

      // CQ events
    case IBV_EVENT_CQ_ERR:
      {
        AffEvent->event_number = IT_ASYNC_AFF_SEVD_FULL_ERROR;
        AffEvent->cause.sevd   = (it_evd_handle_t) aRdmaEvent->element.cq->cq_context;

        break;
      }

      // SRQ events
    case IBV_EVENT_PORT_ACTIVE:
    case IBV_EVENT_PORT_ERR:
    case IBV_EVENT_LID_CHANGE:
    case IBV_EVENT_PKEY_CHANGE:
    case IBV_EVENT_SM_CHANGE:
    case IBV_EVENT_SRQ_ERR:
    case IBV_EVENT_SRQ_LIMIT_REACHED:
    case IBV_EVENT_CLIENT_REREGISTER:
      {
        AffEvent->event_number = IT_ASYNC_AFF_SRQ_CATASTROPHIC;
        AffEvent->cause.srq    = (it_srq_handle_t) aRdmaEvent->element.srq->srq_context;

        break;
      }

      // CA events
    case IBV_EVENT_DEVICE_FATAL:
      {
        it_unaffiliated_event_t* UnAffEvent = (it_unaffiliated_event_t *) aITEvent;

        UnAffEvent->event_number = IT_ASYNC_UNAFF_SPIGOT_OFFLINE;

        break;
      }
    default:
      {
        StrongAssertLogLine( 0 )
          << "ERROR: "
          << " aRdmaEvent->event_type: " << aRdmaEvent->event_type
          << EndLogLine;
      }
    }

  return IT_SUCCESS;
}

#define IT_API_O_VERBS_CONTEXT_QUEUE_ELEM_COUNT 4 * 8192
it_api_o_verbs_context_queue_t context_queue;

/************************************************/
int
it_api_o_verbs_get_device_ordinal( struct ibv_context*           verbs, 
                                   it_api_o_verbs_device_mgr_t*  device )
{
  int devices_count = device->devices_count;

  char* device_name = (char *) ibv_get_device_name( verbs->device );  

  for( int i = 0; i < devices_count; i++ )
    {
      if( device->devices[ i ] != NULL )
        {
          char* query_device_name = (char *)ibv_get_device_name( device->devices[ i ]->device );

          if( strcmp( query_device_name, device_name) == 0 )
            {
              BegLogLine( FXLOG_IT_API_O_VERBS )
                << "Found device name:" << device_name
                << " among " << devices_count
                << " devices at index " << i
                << EndLogLine;

              return i;
            }
        }
    }

  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "Can't find device name:" << device_name
    << " among " << devices_count
    << " devices "
    << EndLogLine;

  return -1;
}


int
it_api_o_verbs_get_device_address( struct sockaddr_in *addr,
                                   const char* device_name)
{
  /********************************************************
   * SPECIAL NOTE: Below is logic is neccessary
   * to be able to specify a local port 
   * to the rdma_route_addr(). Specifying 
   * the local port is needed because OFED
   * has a tendency to choosing an already 
   * used port, cause rdma_connect() to 
   * return with errno=98
   ********************************************************/
  struct ifaddrs * ifAddrStruct=NULL;
  struct ifaddrs * ifAddrStructOriginal =NULL;
  void * tmpAddrPtr=NULL;
  int i=0;
  int ret = 0;

  ret = getifaddrs( & ifAddrStruct );
  if( ret != 0 )
    {
      BegLogLine( 1 )
        << "it_ep_connect(): ERROR: failed getifaddrs()"
        << " errno: " << errno
        << EndLogLine;

      return IT_ERR_ABORT;
    }

  ifAddrStructOriginal = ifAddrStruct;

  int addrFound = 0;
  while( ifAddrStruct != NULL ) 
    {
      BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT )
        << " ifa_name: " << ifAddrStruct->ifa_name
        << " ifa_family: " << ((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_family
        << " ifa_addr: " << (void *) ((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr.s_addr
        << " ifa_port: " << ((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_port
        << EndLogLine;

      if ( strcmp(ifAddrStruct->ifa_name, device_name) == 0 && 
           ( ((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_family == AF_INET ) )
        {
          // is a valid IP4 Address
          addr->sin_addr.s_addr = ((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr.s_addr;

          BegLogLine( 0 )
            << "addr.sin_addr.s_addr: " << (void *) addr->sin_addr.s_addr
            << EndLogLine;

          addrFound = 1;
          break;
        }

      ifAddrStruct = ifAddrStruct->ifa_next;
    }

  freeifaddrs( ifAddrStructOriginal );

  if( !addrFound )
    {
      BegLogLine( 1 )
        << "it_ep_connect(): ERROR: " << device_name 
        << " local if is not found"
        << EndLogLine;
      return -1;
    }  
  /********************************************************/
  return 0;
}

it_status_t
it_api_o_verbs_init_cq( int                                         aDeviceOrdinal,
                        struct ibv_context*                         aVerbs, 
                        it_api_o_verbs_cq_mgr_t*                    aCQ,
                        it_api_o_verbs_cq_processing_thread_args_t* aProcessingThreadArgs )
{
  if( aCQ->event_number == IT_DTO_EVENT_STREAM )
    {
      struct ibv_comp_channel *comp_channel = ibv_create_comp_channel( aVerbs );
      if( ! comp_channel )
        {
          BegLogLine( 1 ) 
            << "it_api_o_verbs_init_cq(): ERROR: failed to create comp channel" 
            << EndLogLine;

          return IT_ERR_ABORT;	  
        }

      struct ibv_cq* new_cq = ibv_create_cq( aVerbs, aCQ->queue_size, NULL, comp_channel, 0 );

      if( !new_cq )
        {
          BegLogLine( 1 ) 
            << "it_api_o_verbs_init_cq(): ERROR: failed to create cq" 
            << EndLogLine;

          return IT_ERR_ABORT;
        }

      new_cq->cq_context = (void *) aCQ;

      aCQ->SetCq( aDeviceOrdinal, new_cq );

      if( aProcessingThreadArgs )
        {
          aProcessingThreadArgs->mCQ          = new_cq;
          aProcessingThreadArgs->mCompChannel = comp_channel;
          aProcessingThreadArgs->mSolicitedEventsOnly = 0;

          // Signal that the CQ is ready to the dto processing thread
          pthread_mutex_unlock( aProcessingThreadArgs->mCQReadyMutex );
        }
    }
  else
    {
      StrongAssertLogLine( 0 )
        << "it_api_o_verbs_init_cq(): ERROR: Unexpected event type: " 
        << aCQ->event_number
        << EndLogLine;      
    }

  return IT_SUCCESS;
}

it_status_t
it_api_o_verbs_init_pd( int                      aDeviceOrdinal,
                        struct ibv_context*      aVerbs, 
                        it_api_o_verbs_pd_mgr_t* aPD )
{
  struct ibv_pd* new_pd = ibv_alloc_pd( aVerbs );
  if (!new_pd) 
    {
      BegLogLine( 1 ) 
        << "it_api_o_verbs_init_pd(): ERROR: failed to allocate a pd" 
        << EndLogLine;

      return IT_ERR_ABORT;
    }

  StrongAssertLogLine( aDeviceOrdinal >= 0 && aDeviceOrdinal < aPD->device->devices_count )
    << "it_api_o_verbs_init_pd(): ERROR: " 
    << " aDeviceOrdinal: " << aDeviceOrdinal
    << " pd->device->devices_count: " << aPD->device->devices_count
    << EndLogLine;  

  aPD->PDs[ aDeviceOrdinal ] = new_pd;  

  return IT_SUCCESS;
}

it_status_t
it_api_o_verbs_init_qp( struct rdma_cm_id *      cm_id, 
                        it_api_o_verbs_qp_mgr_t* qp )
{  
  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "it_api_o_verbs_init_qp(): Entering"
    << " cm_id...device: " << (char *)(cm_id->verbs->device)
    << EndLogLine;

  AssertLogLine(( cm_id != NULL ) && ( cm_id->verbs != NULL ))
    << "it_api_o_verbs_init_qp(): cm_id = " << (void*)cm_id
    << " cm_id->verbs: " << (cm_id != NULL?(void*)cm_id->verbs:NULL)
    << EndLogLine;

  int device_ordinal = it_api_o_verbs_get_device_ordinal( cm_id->verbs, 
                                                          qp->send_cq->device );

  StrongAssertLogLine( device_ordinal >= 0 && device_ordinal < qp->send_cq->device->devices_count )
    << "it_api_o_verbs_init_qp(): ERROR: "
    << " device_ordinal: " << device_ordinal
    << " qp->send_cq->device->devices_count: " << qp->send_cq->device->devices_count
    << EndLogLine;

  qp->device_ord = device_ordinal;

  it_status_t status = IT_SUCCESS;
  if( qp->pd->PDs[ device_ordinal ] == NULL )
    {
      status = it_api_o_verbs_init_pd( device_ordinal, cm_id->verbs, qp->pd );
      if( status != IT_SUCCESS )
        return status;
    }  

  if( qp->send_cq->cq.cq[ device_ordinal ] == NULL )
    {
      it_api_o_verbs_cq_processing_thread_args_t* ProcessingThreadArgsPtr = NULL;

      if( qp->send_cq->aevd != NULL )
        ProcessingThreadArgsPtr = & ( qp->send_cq->aevd->mSendThreadArgs[ device_ordinal ] );

      status = it_api_o_verbs_init_cq( device_ordinal, cm_id->verbs, qp->send_cq, ProcessingThreadArgsPtr );
      if( status != IT_SUCCESS )
        return status;
    }

#ifdef IT_API_SINGLE_SEND_RECV_QUEUE
  qp->recv_cq->cq.cq[ device_ordinal ] = qp->send_cq->cq.cq[ device_ordinal ];
  // qp->recv_cq->queue_size = 2 * qp->recv_cq->queue_size;
#endif

  if( qp->recv_cq->cq.cq[ device_ordinal ] == NULL )
    {
      it_api_o_verbs_cq_processing_thread_args_t* ProcessingThreadArgsPtr = NULL;

      if( qp->recv_cq->aevd != NULL )
        ProcessingThreadArgsPtr = & ( qp->recv_cq->aevd->mRecvThreadArgs[ device_ordinal ] );

      status = it_api_o_verbs_init_cq( device_ordinal, cm_id->verbs, qp->recv_cq, ProcessingThreadArgsPtr );
      if( status != IT_SUCCESS )
        return status;
    }

  if( qp->qp == NULL )
    {
      struct ibv_qp_init_attr qp_init_attr;
      memset(&qp_init_attr, 0, sizeof qp_init_attr);
      qp_init_attr.cap.max_recv_sge = qp->ep_attr.max_recv_segments;
      qp_init_attr.cap.max_recv_wr = qp->recv_cq->queue_size;
      qp_init_attr.cap.max_send_sge = qp->ep_attr.max_send_segments;
      qp_init_attr.cap.max_send_wr = qp->send_cq->queue_size;
      qp_init_attr.qp_type = IBV_QPT_RC;
      qp_init_attr.recv_cq = qp->recv_cq->cq.cq[ device_ordinal ];
      qp_init_attr.send_cq = qp->send_cq->cq.cq[ device_ordinal ];
      qp_init_attr.sq_sig_all = 0;

      int ret = rdma_create_qp( cm_id, qp->pd->PDs[ device_ordinal ], &qp_init_attr );
      if( ret ) 
        {
          BegLogLine( 1 ) 
            << "it_api_o_verbs_init_qp(): ERROR: failed to create qp" 
            << " errno: " << errno
            << EndLogLine;

          return IT_ERR_ABORT;
        }

      qp->qp         = cm_id->qp;

      BegLogLine( 0 )
        << "after rdma_create_qp(): "
        << " qp->qp: " << (void *) qp->qp
        << EndLogLine;

      StrongAssertLogLine( qp->qp != NULL )	
        << EndLogLine;

      qp->qp->qp_context = (void *) qp;

      // // ***********************************************
      // // output of send/recv queue lengths
      // struct ibv_qp_attr attr;
      // struct ibv_qp_init_attr init_attr;
      
      // bzero( &attr, sizeof(struct ibv_qp_attr) );
      // bzero( &init_attr, sizeof(struct ibv_qp_init_attr) );

      // BegLogLine( FXLOG_IT_API_O_VERBS )
      //   << "dump_qp_stats():: qpMgr ptr: " << (void*)qp
      //   << " qp ptr: " << (void*)qp->qp
      //   << EndLogLine;

      // ibv_query_qp( qp->qp, &attr, IBV_QP_CAP,  &init_attr );
 

      // BegLogLine( FXLOG_IT_API_O_VERBS )
      //   << "dump_qp_stats():: "	
      //   << " rq_size: " << attr.cap.max_recv_wr
      //   << " sq_size: " << attr.cap.max_send_wr
      //   << EndLogLine;
      // // ***********************************************



    }

  return IT_SUCCESS;
}

it_status_t
dump_rwr_stats( struct ibv_recv_wr       *bad_rx_wr,
                const char               *aMsgText )
{
  return IT_SUCCESS;
}

it_status_t
dump_rwr_stats( struct ibv_send_wr       *bad_tx_wr,
                const char               *aMsgText )
{
  return IT_SUCCESS;
}

it_status_t
dump_qp_stats( it_api_o_verbs_qp_mgr_t  *aQPMgr,
               const char               *aMsgText,
               int                       aOpErr,
               int                       aPrevErr )
{
  struct ibv_qp_attr attr;
  struct ibv_qp_init_attr init_attr;

  bzero( &attr, sizeof(struct ibv_qp_attr) );
  bzero( &init_attr, sizeof(struct ibv_qp_init_attr) );

  BegLogLine( 1 )
    << "dump_qp_stats():: qpMgr ptr: " << (void*)aQPMgr
    << " qp ptr: " << (void*)aQPMgr->qp
    << EndLogLine;

  if(( aQPMgr != NULL ) && (aQPMgr->qp != NULL ) )
    {
      ibv_query_qp( aQPMgr->qp, &attr, IBV_QP_CAP,  &init_attr );
    }

  BegLogLine( 1 )
    << "dump_qp_stats():: ERROR: post " << aMsgText << " failed "	
    << " ret: " << aOpErr
    << " errno: " << aPrevErr
    << " rq_size: " << attr.cap.max_recv_wr
    << " sq_size: " << attr.cap.max_send_wr
    << " query_qp errno: " << errno
    << EndLogLine;

  return IT_SUCCESS;
}

int
it_api_o_verbs_map_flags_to_ofed( IN const it_dto_flags_t dto_flags )
{
  int ret_flag = 0;
  if( dto_flags & (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG ) )
    ret_flag |= IBV_SEND_SIGNALED;

  if( dto_flags & IT_SOLICITED_WAIT_FLAG )
    ret_flag |= IBV_SEND_SOLICITED;

  return ret_flag;
}


it_status_t
it_api_o_verbs_post_op( IN it_api_o_verbs_post_opcode_t post_op,
                        IN   enum ibv_wr_opcode     opcode,
                        IN        it_ep_handle_t    ep_handle,
                        IN  const it_lmr_triplet_t *local_segments,
                        IN        size_t            num_segments,
                        IN        it_dto_cookie_t   cookie,
                        IN const  it_dto_flags_t    dto_flags,
                        IN        it_rdma_addr_t    rdma_addr,
                        IN        it_rmr_context_t  rmr_context )
{
  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "it_api_o_verbs_post_op(): Entering "
    << " post_op: "  << (int) post_op
    << " opcode: "  << (int) opcode
    << EndLogLine;

  if( qpMgr->qp == NULL )
    {
      StrongAssertLogLine( qpMgr->cm_conn_id != NULL )
        << "it_api_o_verbs_post_op(): ERROR: "
        << EndLogLine;

      it_status_t status = it_api_o_verbs_init_qp( qpMgr->cm_conn_id, qpMgr );
      if( status != IT_SUCCESS )
        return status;
    }

  StrongAssertLogLine( qpMgr->qp != NULL )
    << "it_api_o_verbs_post_op(): ERROR: "
    << EndLogLine;

  StrongAssertLogLine( qpMgr->cm_conn_id != NULL )
    << "it_api_o_verbs_post_op(): ERROR: "
    << EndLogLine;

  struct ibv_sge local_sge[ IT_API_O_VERBS_MAX_SGE_LIST_SIZE ];

  AssertLogLine( num_segments >= 0 && num_segments < IT_API_O_VERBS_MAX_SGE_LIST_SIZE )
    << "it_api_o_verbs_post_op(): ERROR: "
    << " num_segments: " << num_segments
    << " IT_API_O_VERBS_MAX_SGE_LIST_SIZE: " << IT_API_O_VERBS_MAX_SGE_LIST_SIZE
    << EndLogLine;

  int device_ord = qpMgr->device_ord;

  StrongAssertLogLine( device_ord >= 0 && device_ord < qpMgr->pd->device->devices_count )
    << "it_api_o_verbs_post_op(): ERROR: "
    << " device_ordinal: " << device_ord
    << " devices_count: " << qpMgr->pd->device->devices_count
    << EndLogLine;  

  // Convert to real MRs
  for( int i = 0; i < num_segments; i++ )
    {
      it_api_o_verbs_mr_mgr_t* mrMgr = (it_api_o_verbs_mr_mgr_t* ) local_segments[ i ].lmr;

      StrongAssertLogLine( mrMgr != NULL )
        << "ERROR: mrMgr is NULL "
        << " i: " << i
        << " local_segments: " << (void *) local_segments
        << EndLogLine;

      uint64_t addr = (uint64_t) local_segments[ i ].addr.abs;

      local_sge[ i ].addr   = addr;
      local_sge[ i ].length = local_segments[ i ].length;

      if( mrMgr->MRs[ device_ord ].mr == NULL )
        {

          if( mrMgr->pd->PDs[ device_ord ] == NULL )
            {
              it_status_t status = it_api_o_verbs_init_pd( device_ord, 
                                                           qpMgr->cm_conn_id->verbs, 
                                                           mrMgr->pd );
              if( status != IT_SUCCESS )
                return status;
            }

          BegLogLine( FXLOG_IT_API_O_VERBS_MEMREG )
            << "about to register mr: " << (void*)mrMgr->addr
            << " len: " << mrMgr->length
            << EndLogLine;
          
          gITAPI_REG_MR_START.HitOE( IT_API_TRACE, 
                                     gITAPI_REG_MR_START_Name, 
                                     gTraceRank, 
                                     gITAPI_REG_MR_START );

          struct ibv_mr *local_triplet_mr = ibv_reg_mr( mrMgr->pd->PDs[ device_ord ], 
                                                        mrMgr->addr, 
                                                        mrMgr->length,
                                                        // 8 * 1024, // mrMgr->length,
                                                        mrMgr->access );
          gITAPI_REG_MR_FINIS.HitOE( IT_API_TRACE, 
                                     gITAPI_REG_MR_FINIS_Name, 
                                     gTraceRank, 
                                     gITAPI_REG_MR_FINIS );

          if( ! local_triplet_mr )
            {
              BegLogLine( 1 )
                << "it_api_o_verbs_post_op(): ERROR: "
                << " failed to register an mr "
                << " mrMgr->pd->PDs[ device_ord ]: " << (void *) mrMgr->pd->PDs[ device_ord ]
                << " mrMgr->addr: " << (void *) mrMgr->addr
                << " mrMgr->length: " <<  mrMgr->length
                << " mrMgr->access: " << mrMgr->access
                << " errno: " << errno
                << EndLogLine;

              return IT_ERR_INVALID_LMR;
            }

          mrMgr->MRs[ device_ord ].mr = local_triplet_mr;

          BegLogLine( 0 ) 
            << "it_lmr_create(): "
            << " lmr: " << (void *)mrMgr
            << " dev" << device_ord
            << " lmr.lkey: " << (void *)mrMgr->MRs[ device_ord ].mr->lkey
            << " dev" << device_ord
            << " lmr.rkey: " << (void *)mrMgr->MRs[ device_ord ].mr->rkey
            << " size: " << mrMgr->length
            << " addr: " << mrMgr->addr
            << EndLogLine;
        }

      local_sge[ i ].lkey   = mrMgr->MRs[ device_ord ].mr->lkey;

      AssertLogLine( mrMgr->MRs[ device_ord ].mr->length == mrMgr->length )
        << "it_api_o_verbs_post_op(): ERROR: "
        << " device_ord: " << device_ord
        << " mrMgr->length: " << mrMgr->length
        << " mrMgr->MRs[ device_ord ].mr->length: " << mrMgr->MRs[ device_ord ].mr->length
        << EndLogLine;

      AssertLogLine( mrMgr->MRs[ device_ord ].mr->addr == mrMgr->addr )
        << "it_api_o_verbs_post_op(): ERROR: "
        << " device_ord: " << device_ord
        << " mrMgr->addr: " << mrMgr->addr
        << " mrMgr->MRs[ device_ord ].mr->addr: " << mrMgr->MRs[ device_ord ].mr->addr
        << EndLogLine;

#if (FXLOG_IT_API_O_VERBS != 0)
      if(post_op == POST_RECV)
        {
          BegLogLine( FXLOG_IT_API_O_VERBS )
            // << "it_api_o_verbs_post_op(): POST_RECV"
            // << " qpMgr->qp: " << (void *) qpMgr->qp
            // << " i: " << i
            // << " local_sge.lkey: " << (void *) local_sge[ i ].lkey
            << " local_sge.addr: " << (void *) local_sge[ i ].addr
            // << " local_sge.length: " << local_sge[ i ].length
            << EndLogLine;
        }
      else
        {
          BegLogLine( FXLOG_IT_API_O_VERBS )
            // << "it_api_o_verbs_post_op(): POST_SEND"
            // << " qpMgr->qp: " << (void *) qpMgr->qp
            // << " i: " << i
            // << " local_sge.lkey: " << (void *) local_sge[ i ].lkey
            << " local_sge.addr: " << (void *) local_sge[ i ].addr
            // << " local_sge.length: " << local_sge[ i ].length
            << EndLogLine;	  
        }
#endif
    }

  it_api_o_verbs_context_queue_elem_t* elem;

  if( post_op == POST_RECV )
    {
      struct ibv_recv_wr remote_triplet_rx_wr, *bad_rx_wr;

      bzero( (void *) & remote_triplet_rx_wr, sizeof( struct ibv_recv_wr ) );
      remote_triplet_rx_wr.sg_list = & ( local_sge[ 0 ] );
      remote_triplet_rx_wr.num_sge = num_segments;

      elem = context_queue.Pop();
      StrongAssertLogLine( elem != NULL )
        << "ERROR: elem is NULL, no more elements in the free pool"
        << EndLogLine;

      elem->Init( cookie, qpMgr );
      remote_triplet_rx_wr.wr_id = (uint64_t) (elem);

      unsigned short *srv_fake_cookie = &(((unsigned short*)&cookie)[1]);

      BegLogLine( FXLOG_IT_API_O_VERBS )
        << "it_api_o_verbs_post_op():: About to ibv_post_recv() "
        << " opcode: " << opcode
        // << " qp: " << (void *) qpMgr->qp
        // << " wr_id: " << (unsigned long long) remote_triplet_rx_wr.wr_id
        << " cookie: " << *srv_fake_cookie
        << EndLogLine;

      StrongAssertLogLine( qpMgr->qp != NULL )
        << "it_api_o_verbs_post_op():: ERROR: qpMgr->qp "
        << EndLogLine;

      int ret = ibv_post_recv( qpMgr->qp, & remote_triplet_rx_wr, & bad_rx_wr );

      if( ret )
        {
          dump_qp_stats( qpMgr, "recv", ret, errno );

          return IT_ERR_ABORT;
        }
    }
  else if( post_op == POST_SEND )
    {
      struct ibv_send_wr           rdmaw_wr, *bad_tx_wr;
      int                          ibv_send_flags = it_api_o_verbs_map_flags_to_ofed( dto_flags );

      bzero( (void *) & rdmaw_wr, sizeof( ibv_send_wr ) );

      rdmaw_wr.opcode              = opcode;
      rdmaw_wr.send_flags          = ibv_send_flags;
      rdmaw_wr.sg_list             = & ( local_sge[ 0 ] );
      rdmaw_wr.num_sge             = num_segments;

      elem = context_queue.Pop();
      StrongAssertLogLine( elem != NULL )
        << "ERROR: elem is NULL, no more elements in the free pool"
        << EndLogLine;

      elem->Init( cookie, qpMgr );
      rdmaw_wr.wr_id               = (uint64_t) (elem);

      rdmaw_wr.wr.rdma.remote_addr = (uint64_t) rdma_addr;
      rdmaw_wr.wr.rdma.rkey        = rmr_context;

#ifdef ROQ_SGE_WORKAROUND
      struct ibv_send_wr           rdmaw_seg_wr[ IT_API_O_VERBS_MAX_SGE_LIST_SIZE ];
      rdmaw_wr.num_sge = 1;   // reset to only one seg
      if( num_segments > 1 )
      {
        uint64_t offset = rdmaw_wr.sg_list[ 0 ].length;
        int remaining_segs = num_segments-1;
        bzero( (void *) rdmaw_seg_wr, sizeof( ibv_send_wr ) * num_segments );

        rdmaw_wr.next = &( rdmaw_seg_wr[ 0 ] );
        rdmaw_wr.send_flags = ibv_send_flags & (~IBV_SEND_SIGNALED);
        for ( int sge=0; sge<remaining_segs; sge++ )
        {
          rdmaw_seg_wr[ sge ].wr_id = (uint64_t) (elem+((sge+1)*0x10000));
          rdmaw_seg_wr[ sge ].opcode = opcode;
          rdmaw_seg_wr[ sge ].sg_list = &( local_sge[ sge+1 ] );
          rdmaw_seg_wr[ sge ].num_sge = 1;

          rdmaw_seg_wr[ sge ].wr.rdma.remote_addr = (uint64_t) rdma_addr + offset;
          rdmaw_seg_wr[ sge ].wr.rdma.rkey = rmr_context;
          if( sge < remaining_segs-1 )
          {
            rdmaw_seg_wr[ sge ].next = & rdmaw_seg_wr[ sge+1 ];
            rdmaw_seg_wr[ sge ].send_flags = ibv_send_flags & (~IBV_SEND_SIGNALED);
          }
          else
          {
            rdmaw_seg_wr[ sge ].next = NULL;
            rdmaw_seg_wr[ sge ].send_flags = ibv_send_flags;
          }
          BegLogLine( FXLOG_IT_API_O_VERBS )
            << "IT_API: assembled next SGE: " << sge+1
            << " id: " << rdmaw_seg_wr[ sge ].wr_id
            << " raddr: " <<  rdmaw_seg_wr[ sge ].wr.rdma.remote_addr
            << " len: " << rdmaw_seg_wr[ sge ].sg_list[0].length
            << EndLogLine;

          offset += rdmaw_seg_wr[ sge ].sg_list[ 0 ].length;
        }
      }
#endif


      unsigned short *srv_fake_cookie = &(((unsigned short*)&cookie)[1]);

      BegLogLine( FXLOG_IT_API_O_VERBS )
        << "it_api_o_verbs_post_op():: About to ibv_post_send() "
        << " opcode: " << opcode
        // << " qp: " << (void *) qpMgr->qp
        // << " wr_id: " << (unsigned long long) rdmaw_wr.wr_id
        << " cookie: " << *srv_fake_cookie
        << EndLogLine;

      gITAPI_POST_SEND_START.HitOE( IT_API_TRACE, 
                                    gITAPI_POST_SEND_START_Name,
                                    gTraceRank,
                                    gITAPI_POST_SEND_START );

      int ret = 0;
#ifdef IT_API_POST_OP_RETRIES
      int retry = IT_API_POST_OP_RETRIES;
      while( retry )
        {
#endif
          ret = ibv_post_send( qpMgr->qp, &rdmaw_wr, &bad_tx_wr );

#ifdef IT_API_POST_OP_RETRIES
          if( ret == 0 )
            break;

          // retrying will cause a little hickup, however this will give the polling threads a little time to reap more cqe from busy queues
          BegLogLine( IT_API_POST_OP_RETRIES )
            << "it_api_o_verbs_post_op():: RETRYING post_send after usleep()"
            << EndLogLine;

          usleep(100);
          retry--;
        }
#endif

      gITAPI_POST_SEND_FINIS.HitOE( IT_API_TRACE, 
                                    gITAPI_POST_SEND_FINIS_Name,
                                    gTraceRank,
                                    gITAPI_POST_SEND_FINIS );


      switch( ret )
        {
        case IT_SUCCESS:
          break;

        case ENOMEM:
          BegLogLine( 1 )
            << "it_api_o_verbs_post_op(): too many posts failure"
            << EndLogLine;

          return IT_ERR_TOO_MANY_POSTS;

        default:
          dump_qp_stats( qpMgr, "send", ret, errno );

          BegLogLine( 1 ) 
            << "post send: bad_wr: " << (void*)bad_tx_wr
            << " length: " << bad_tx_wr->sg_list->length
            << EndLogLine;

          return IT_ERR_ABORT;
        }

    }
  else
    {
      StrongAssertLogLine( 0 )
        << "it_api_o_verbs_post_op():: ERROR: unexpected post_op code: "	
        << post_op
        << EndLogLine;
    }

  // If we run without signaling, we need to return the context element. otherwise we run out of contexts
  // \todo: need to check if the element from the queue isn't needed later
  // \todo: also need to check if the pop-push sequence is required at all
  if( ! ( dto_flags & (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG) ) )
    {
      context_queue.Push( elem );
    }      
  return IT_SUCCESS;
}

it_dto_status_t
it_api_o_verbs_get_dto_status( enum ibv_wc_status aStatus )
{
  it_dto_status_t rc = IT_DTO_SUCCESS;

  switch( aStatus )
    {
    case IBV_WC_SUCCESS:
      {
        rc = IT_DTO_SUCCESS;
        break;
      }
    case IBV_WC_WR_FLUSH_ERR:
      {
        rc = IT_DTO_ERR_FLUSHED;
        break;		    
      }
    case IBV_WC_LOC_QP_OP_ERR:
      {
        rc = IT_DTO_ERR_LOCAL_EP;
        break;		    
      }
    case IBV_WC_LOC_PROT_ERR:
      {
        rc = IT_DTO_ERR_LOCAL_PROTECTION;
        break;		    		    
      }
    default:
      {
        StrongAssertLogLine( 0 )
          << "ERROR: status: " << aStatus
          << " not recognized. "
          << EndLogLine;
      }
    }

  return rc;
}

it_status_t
it_api_o_verbs_convert_wc_to_it_dto_event( it_api_o_verbs_cq_mgr_t* aCQ, 
                                           struct ibv_wc*           aWc,
                                           it_dto_cmpl_event_t*     aEvent )
{
  it_api_o_verbs_context_queue_elem_t* elem = (it_api_o_verbs_context_queue_elem_t*) aWc->wr_id;
  aEvent->ep                 = (it_ep_handle_t) elem->qp;
  aEvent->evd                = (it_evd_handle_t) aCQ;  
  CookieAssign( &(aEvent->cookie), &(elem->cookie) );
  // aEvent->cookie.mFirst      = elem->cookie.mFirst;
  // aEvent->cookie.mSecond     = elem->cookie.mSecond;
  aEvent->transferred_length = aWc->byte_len;

  unsigned short *srv_fake_cookie = &(((unsigned short*)&(elem->cookie))[1]);
  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "it_api_o_verbs_convert_wc_to_it_dto_event(): Got a CQ event "		
    << " opcode: " << aWc->opcode
    << " cookie: " << *srv_fake_cookie
    << EndLogLine;

  switch( aWc->opcode )
    {
    case IBV_WC_RECV:
      {
        aEvent->event_number = IT_DTO_RC_RECV_CMPL_EVENT;
        break;
      }
    case IBV_WC_SEND:
      {
        aEvent->event_number = IT_DTO_SEND_CMPL_EVENT;
        break;
      }
    case IBV_WC_RDMA_WRITE:
      {
        aEvent->event_number = IT_DTO_RDMA_WRITE_CMPL_EVENT;		    
        break;
      }
    case IBV_WC_RDMA_READ:
      {
        aEvent->event_number = IT_DTO_RDMA_READ_CMPL_EVENT;		    
        break;
      }
    default:
      {
        StrongAssertLogLine( 0 )
          << "ERROR: opcode: " << aWc->opcode
          << " not recognized. "
          << EndLogLine;
      }
    }

  aEvent->dto_status = it_api_o_verbs_get_dto_status( aWc->status );

  context_queue.Push( elem );

  return IT_SUCCESS;
}

it_status_t
it_api_o_verbs_poll_cq( it_dto_cmpl_event_t *    aEvent, 
                        it_api_o_verbs_cq_mgr_t* aCQ,
                        int*                     aRRIndex )
{
  struct ibv_wc	wc;
  bzero( &wc, sizeof( struct ibv_wc ) );

  int iter_count = 0;
  int devices_count = aCQ->device->devices_count;
  while( iter_count < devices_count )
    {      
      (*aRRIndex)++;
      if( (*aRRIndex) == devices_count )
        (*aRRIndex) = 0;      

      if( aCQ->cq.cq[ (*aRRIndex) ] != NULL )
        {	  
          BegLogLine( FXLOG_IT_API_O_VERBS )
            << "it_api_o_verbs_poll_cq(): About to ibv_poll_cq(): "
            << " CQ: " << (void *) aCQ->cq.cq[ (*aRRIndex) ]
            << EndLogLine;

          int ret = ibv_poll_cq( aCQ->cq.cq[ (*aRRIndex) ], 1, &wc );

          if( ret < 0 )
            {
              BegLogLine( FXLOG_IT_API_O_VERBS )
                << "it_api_o_verbs_poll_cq(): About to ibv_poll_cq(): "
                << " ret: " << ret
                << " errno: " << errno
                << EndLogLine;

              return IT_ERR_ABORT;
            }
          else if( ret > 0 )
            {
              it_status_t status = it_api_o_verbs_convert_wc_to_it_dto_event( aCQ, & wc, aEvent );
              StrongAssertLogLine( status == IT_SUCCESS )
                << "ERROR: "
                << " status: " << status
                << EndLogLine;

              return IT_SUCCESS;
            }
        }

      iter_count++;
    }

  return IT_ERR_QUEUE_EMPTY;
}

#define MIN( x, y ) ( (x)<(y)? (x) : (y) )

it_status_t
it_api_o_verbs_handle_cm_event( it_api_o_verbs_cq_mgr_t* aCQ,
                                it_connection_event_t*   aIT_Event, 
                                struct rdma_cm_event*    aRDMA_Event )
{
  aIT_Event->evd = (it_evd_handle_t) aCQ;
  aIT_Event->ep  = (it_ep_handle_t) aRDMA_Event->id->context;
  aIT_Event->cn_est_id = (it_cn_est_identifier_t) aRDMA_Event->id;

  switch( aRDMA_Event->event )
    {
    case RDMA_CM_EVENT_ESTABLISHED:
      {
        aIT_Event->event_number = IT_CM_MSG_CONN_ESTABLISHED_EVENT;
        break;
      }
    case RDMA_CM_EVENT_DISCONNECTED:
      {
        aIT_Event->event_number = IT_CM_MSG_CONN_DISCONNECT_EVENT;
        break;
      }
    case RDMA_CM_EVENT_REJECTED:
      {
        aIT_Event->event_number = IT_CM_MSG_CONN_PEER_REJECT_EVENT;
        break;
      }
    case RDMA_CM_EVENT_UNREACHABLE:
      {
        StrongAssertLogLine( 0 )
          << " ERROR: Server not reachable"
          << EndLogLine;
        break;
      }
    default:
      {
        StrongAssertLogLine( 0 )
          << "ERROR: Unrecognized "
          << " aRDMA_Event->event: " << aRDMA_Event->event
          << EndLogLine;
      }
    }

  if( aRDMA_Event->param.conn.private_data_len > 0 )
  {
    if( aRDMA_Event->param.conn.private_data_len > IT_MAX_PRIV_DATA )
      printf( "WARNING: it_api_o_verbs_handle_cm_event:: private data len too large. reserved: %d, actual: %d. potential data truncation\n",
              IT_MAX_PRIV_DATA, aRDMA_Event->param.conn.private_data_len );

    memcpy( aIT_Event->private_data,
            aRDMA_Event->param.conn.private_data,
            MIN( aRDMA_Event->param.conn.private_data_len, IT_MAX_PRIV_DATA ) );
    aIT_Event->private_data_present = (it_boolean_t) 1;
  }

  return IT_SUCCESS;
}

it_status_t
it_api_o_verbs_handle_cr_event( it_api_o_verbs_cq_mgr_t* aCQ,
                                it_conn_request_event_t* aIT_Event, 
                                struct rdma_cm_event*    aRDMA_Event)
{
  aIT_Event->event_number = IT_CM_REQ_CONN_REQUEST_EVENT;
  aIT_Event->evd          = (it_evd_handle_t) aCQ;

  if( aRDMA_Event->param.conn.private_data_len > 0 )
  {
    if( aRDMA_Event->param.conn.private_data_len > IT_MAX_PRIV_DATA )
      printf( "WARNING: it_api_o_verbs_handle_cr_event:: private data len too large. reserved: %d, actual: %d. potential data truncation\n",
              IT_MAX_PRIV_DATA, aRDMA_Event->param.conn.private_data_len );

    memcpy( aIT_Event->private_data,
            aRDMA_Event->param.conn.private_data,
            MIN( aRDMA_Event->param.conn.private_data_len, IT_MAX_PRIV_DATA ) );
    aIT_Event->private_data_present = (it_boolean_t) 1;
  }

  aIT_Event->cn_est_id = (it_cn_est_identifier_t) aRDMA_Event->id;

  return IT_SUCCESS;
}



/************************************************/

it_status_t it_ia_create (
                          IN  const char           *name,
                          IN        uint32_t        major_version,
                          IN        uint32_t        minor_version,
                          OUT       it_ia_handle_t *ia_handle
                          )
{
  pthread_mutex_lock( & gITAPI_INITMutex );  

  it_api_o_verbs_device_mgr_t* deviceMgr = (it_api_o_verbs_device_mgr_t *) malloc( sizeof( it_api_o_verbs_device_mgr_t ) );
  StrongAssertLogLine( deviceMgr )
    << "it_ia_create(): ERROR: "
    << EndLogLine;

  bzero( deviceMgr, sizeof( it_api_o_verbs_device_mgr_t ) );

  // CM CHANNEL & ID
  deviceMgr->cm_channel = rdma_create_event_channel();
  if (! deviceMgr->cm_channel ) 
    {
      BegLogLine( 1 )
        << "it_ia_create(): ERROR: "
        << EndLogLine;
      return IT_ERR_ABORT;
    }

#if 0
  it_status_t istatus = socket_nonblock_on( deviceMgr->cm_channel->fd );
  if( istatus != IT_SUCCESS )
    {
      return istatus;
    }
#endif

  deviceMgr->devices_count = 0;

#ifndef IT_API_BGQ_VRNIC        // potentially temporary workaround for vrnic and other setups where a special device name has to be forced

  // get all available devices for the deviceMgr
  deviceMgr->devices = rdma_get_devices( & deviceMgr->devices_count );

#else

  // just get device of special name for deviceMgr
  struct ibv_context **device_list = rdma_get_devices( & deviceMgr->devices_count );

  int found_device = 0;
  int device_index = 0;
  do 
    {
      char* device_name = (char *) ibv_get_device_name( (device_list[ device_index ])->device );  
      if( strncmp( device_name,
                   "bgvrnic",
                   7) == 0)
        {
          BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT )
            << "Found requested device: " << device_name
            << " at index: " << device_index
            << EndLogLine;

          found_device = 1;
          deviceMgr->devices = &(device_list[ device_index ]);
          deviceMgr->devices_count = 1;
        }
      device_index++;
      
    }
  while( (device_index<deviceMgr->devices_count) && (!found_device) );

#endif


  StrongAssertLogLine( deviceMgr->devices_count > 0 )
    << "it_ia_create(): ERROR: "
    << " deviceMgr->devices_count: " << deviceMgr->devices_count
    << EndLogLine;

  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "it_ia_create(): deviceMgr->devices_count: " << deviceMgr->devices_count
    << EndLogLine;

  for( int i = 0; i < deviceMgr->devices_count; i++ )
    {
      struct ibv_context* context = deviceMgr->devices[ i ];

      char* device_name = (char *) ibv_get_device_name( context->device );  

      BegLogLine( FXLOG_IT_API_O_VERBS )
        << "it_ia_create(): device_name: " << device_name
        << " async_fd: " << context->async_fd
        << " context: " << (void *) context
        << EndLogLine;


      it_status_t istatus = socket_nonblock_on( context->async_fd );
      if( istatus != IT_SUCCESS )
        {
          return istatus;
        }      
    }

  context_queue.Init( IT_API_O_VERBS_CONTEXT_QUEUE_ELEM_COUNT );

  *ia_handle = (it_ia_handle_t) deviceMgr;

  pthread_mutex_unlock( & gITAPI_INITMutex );

  return(IT_SUCCESS);
}


// it_ia_free

it_status_t it_ia_free (
                        IN  it_ia_handle_t ia_handle
                        )
{
  BegLogLine(FXLOG_IT_API_O_VERBS)
    << "it_ia_free()"
    << "IN it_ia_handle_t ia_handle " << ia_handle
    << EndLogLine;

  it_api_o_verbs_device_mgr_t* deviceMgr = (it_api_o_verbs_device_mgr_t *) ia_handle;

  rdma_free_devices( deviceMgr->devices );

  free( deviceMgr );  

  return(IT_SUCCESS);
}


// U it_pz_create

it_status_t it_pz_create (
                          IN  it_ia_handle_t  ia_handle,
                          OUT it_pz_handle_t *pz_handle
                          )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS)
    << "it_pz_create(): "
    << " IN it_ia_handle "  << ia_handle
    << " OUT pz_handle "    << *pz_handle
    << EndLogLine;

  it_api_o_verbs_pd_mgr_t* pdMgr = (it_api_o_verbs_pd_mgr_t *) malloc( sizeof( it_api_o_verbs_pd_mgr_t ) );

  StrongAssertLogLine( pdMgr ) << EndLogLine;
  bzero( pdMgr, sizeof( it_api_o_verbs_pd_mgr_t ) );

  it_api_o_verbs_device_mgr_t* deviceMgr = (it_api_o_verbs_device_mgr_t *) ia_handle;

  pdMgr->device = deviceMgr;

  pdMgr->PDs = (struct ibv_pd**) malloc( deviceMgr->devices_count * sizeof(struct ibv_pd *) );
  StrongAssertLogLine( pdMgr->PDs )
    << "it_pz_create(): "
    << EndLogLine;

  for( int i = 0; i < deviceMgr->devices_count; i++ )
    {
      pdMgr->PDs[ i ] = NULL;
    }

  *pz_handle = (it_pz_handle_t) pdMgr;

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}

// it_pz_free

it_status_t it_pz_free (
                        IN  it_pz_handle_t pz_handle
                        )
{
  BegLogLine(FXLOG_IT_API_O_VERBS)
    << "it_pz_free()"
    << "IN it_pz_handle_t pz_handle " << pz_handle
    << EndLogLine;

  it_api_o_verbs_pd_mgr_t* pdMgr = (it_api_o_verbs_pd_mgr_t *) pz_handle;

  for( int i = 0; i < pdMgr->device->devices_count; i++ )
    {
      if( pdMgr->PDs[ i ] != NULL )
        {
          ibv_dealloc_pd( pdMgr->PDs[ i ] );
        }
    }

  free( pdMgr->PDs );

  free( pdMgr );

  return(IT_SUCCESS);
}


// U it_evd_create

it_status_t it_evd_create (
                           IN  it_ia_handle_t   ia_handle,
                           IN  it_event_type_t  event_number,
                           IN  it_evd_flags_t   evd_flag,
                           IN  size_t           sevd_queue_size,
                           IN  size_t           sevd_threshold,
                           IN  it_evd_handle_t  aevd_handle,
                           OUT it_evd_handle_t *evd_handle,
                           OUT int             *fd
                           )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_evd_create(): " << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_evd_create()                      " << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  it_ia_handle_t   ia_handle       " <<  ia_handle        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  it_event_type_t  event_number    " <<  event_number     << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  it_evd_flags_t   evd_flag        " <<  evd_flag         << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  size_t           sevd_queue_size " <<  sevd_queue_size  << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  size_t           sevd_threshold  " <<  sevd_threshold   << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  it_evd_handle_t  aevd_handle     " <<  aevd_handle      << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "OUT it_evd_handle_t  evd_handle      " << *evd_handle << " @ " << (void*)evd_handle       << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "OUT int             *fd              " << (void*)fd          << EndLogLine;

  it_api_o_verbs_device_mgr_t* deviceMgr = (it_api_o_verbs_device_mgr_t *) ia_handle;

  if( event_number == IT_AEVD_NOTIFICATION_EVENT_STREAM )
    {
      if( itov_aevd_defined )
        {
          return IT_ERR_INVALID_EVD_STATE;
        }

      it_api_o_verbs_aevd_mgr_t* CQ = (it_api_o_verbs_aevd_mgr_t *) malloc( sizeof( it_api_o_verbs_aevd_mgr_t ) );
      StrongAssertLogLine( CQ )
        << "it_evd_create(): ERROR: "
        << EndLogLine;

      bzero( CQ, sizeof( it_api_o_verbs_aevd_mgr_t ) );

      CQ->Init( deviceMgr );

      // 1. Start all the pthreads
      // 2. Turn off non blocking behaviour
      CQ->mCMThreadArgs.mEventCmplQueue    = & CQ->mCMQueue;
      CQ->mCMThreadArgs.mMainCond          = & CQ->mMainCond;
      CQ->mCMThreadArgs.mEventCounterMutex = & CQ->mEventCounterMutex;
      CQ->mCMThreadArgs.mEventCounter      = & CQ->mEventCounter;
      CQ->mCMThreadArgs.mCmChannel         = CQ->mDevice->cm_channel;

      int rc = pthread_create( & CQ->mCMQueueTID,
                               NULL,
                               it_api_o_verbs_cm_processing_thread,
                               (void *) & (CQ->mCMThreadArgs) );

      StrongAssertLogLine( rc == 0 )
        << "ERROR: "
        << " rc: " << rc
        << EndLogLine;

      for( int i=0; i < CQ->mDevice->devices_count; i++ )
        {
          it_status_t istatus = socket_nonblock_off( CQ->mDevice->devices[ i ]->async_fd );
          if( istatus != IT_SUCCESS )
            {
              return istatus;
            }

          CQ->mAffThreadArgs[ i ].mEventCmplQueue = & CQ->mAffQueues[ i ];
          CQ->mAffThreadArgs[ i ].mDevice         = CQ->mDevice->devices[ i ];

          CQ->mAffThreadArgs[ i ].mMainCond          = & CQ->mMainCond;
          CQ->mAffThreadArgs[ i ].mEventCounterMutex = & CQ->mEventCounterMutex;
          CQ->mAffThreadArgs[ i ].mEventCounter      = & CQ->mEventCounter;

          rc = pthread_create( & CQ->mAffQueuesTIDs[ i ],
                               NULL,
                               it_api_o_verbs_aff_processing_thread,
                               (void *) & (CQ->mAffThreadArgs[ i ]) );

          StrongAssertLogLine( rc == 0 )
            << "ERROR: "
            << " rc: " << rc
            << EndLogLine;

          CQ->mSendThreadArgs[ i ].mEventCmplQueue    = & CQ->mSendQueues[ i ];
          CQ->mSendThreadArgs[ i ].mCQReadyMutex      = & CQ->mSendCQReadyMutexes[ i ];
          CQ->mSendThreadArgs[ i ].mMainCond          = & CQ->mMainCond;
          CQ->mSendThreadArgs[ i ].mEventCounterMutex = & CQ->mEventCounterMutex;
          CQ->mSendThreadArgs[ i ].mEventCounter      = & CQ->mEventCounter;

          rc = pthread_create( & CQ->mSendQueuesTIDs[ i ],
                               NULL,
                               it_api_o_verbs_dto_processing_thread,
                               (void *) & (CQ->mSendThreadArgs[ i ]) );

          StrongAssertLogLine( rc == 0 )
            << "ERROR: "
            << " rc: " << rc
            << EndLogLine;

          CQ->mRecvThreadArgs[ i ].mEventCmplQueue    = & CQ->mRecvQueues[ i ];
          CQ->mRecvThreadArgs[ i ].mCQReadyMutex      = & CQ->mRecvCQReadyMutexes[ i ];
          CQ->mRecvThreadArgs[ i ].mMainCond          = & CQ->mMainCond;
          CQ->mRecvThreadArgs[ i ].mEventCounterMutex = & CQ->mEventCounterMutex;
          CQ->mRecvThreadArgs[ i ].mEventCounter      = & CQ->mEventCounter;

          rc = pthread_create( & CQ->mRecvQueuesTIDs[ i ],
                               NULL,
                               it_api_o_verbs_dto_processing_thread,
                               (void *) & (CQ->mRecvThreadArgs[ i ]) );

          StrongAssertLogLine( rc == 0 )
            << "ERROR: "
            << " rc: " << rc
            << EndLogLine;
        }

      itov_aevd_defined = 1; 
      *evd_handle = (it_evd_handle_t) CQ;

    }
  else
    {
      it_api_o_verbs_cq_mgr_t* CQ = (it_api_o_verbs_cq_mgr_t *) malloc( sizeof( it_api_o_verbs_cq_mgr_t ) );
      StrongAssertLogLine( CQ )
        << "it_evd_create(): ERROR: "
        << EndLogLine;

      bzero( CQ, sizeof( it_api_o_verbs_cq_mgr_t ) );

      CQ->event_number   = event_number;
      CQ->queue_size     = sevd_queue_size;
      CQ->device         = deviceMgr;
      CQ->aevd           = (it_api_o_verbs_aevd_mgr_t *) aevd_handle;

      // This field gets set letter when the QP is created
      CQ->dto_type       = CQ_UNINITIALIZED;

      switch( event_number )
        {
        case IT_ASYNC_UNAFF_EVENT_STREAM: 
        case IT_ASYNC_AFF_EVENT_STREAM:
          {
            break;
          }
        case IT_CM_REQ_EVENT_STREAM:
        case IT_CM_MSG_EVENT_STREAM:
          {	
            CQ->cq.cm_channel = deviceMgr->cm_channel;

            break;
          }
        case IT_DTO_EVENT_STREAM:
          {	
            int cqSize = sizeof( struct ibv_cq * ) * deviceMgr->devices_count;
            CQ->cq.cq = (struct ibv_cq **) malloc( cqSize );
            StrongAssertLogLine( CQ->cq.cq )
              << "it_evd_create(): ERROR: "
              << " cqSize: " << cqSize
              << EndLogLine;

            for( int i = 0; i < deviceMgr->devices_count; i++ )
              {
                CQ->cq.cq[ i ] = NULL;
              }

            break;
          }
        case IT_SOFTWARE_EVENT_STREAM:
          {
            break;
          }
        default:
          {
            StrongAssertLogLine( 0 )
              << "it_evd_create(): ERROR: Unrecognized event number: "
              << event_number
              << EndLogLine;

            break;
          }
        }

      *evd_handle = (it_evd_handle_t) CQ;
    }

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}


// it_evd_free

it_status_t it_evd_free (
                         IN  it_evd_handle_t evd_handle
                         )
{
  BegLogLine(FXLOG_IT_API_O_VERBS)
    << "it_evd_free()"
    << "IN it_evd_handle_t evd_handle " << evd_handle
    << EndLogLine;

  it_api_o_verbs_cq_mgr_t* CQ = (it_api_o_verbs_cq_mgr_t *) evd_handle;

  switch( CQ->event_number ) 
    {
    case IT_ASYNC_UNAFF_EVENT_STREAM: 
    case IT_ASYNC_AFF_EVENT_STREAM:
    case IT_CM_REQ_EVENT_STREAM:
    case IT_CM_MSG_EVENT_STREAM:
    case IT_SOFTWARE_EVENT_STREAM:
      {	
        break;
      }
    case IT_DTO_EVENT_STREAM:
      {	
        for( int i = 0; i < CQ->device->devices_count; i++ )
          {
            if( CQ->cq.cq[ i ] != NULL )
              {
                ibv_destroy_cq( CQ->cq.cq[ i ] );
              }
          }

        free( CQ->cq.cq );

        break;
      }
    default:
      {
        StrongAssertLogLine( 0 )
          << "it_evd_create(): ERROR: Unrecognized event number: "
          << CQ->event_number
          << EndLogLine;

        break;
      }
    }

  free( CQ );  

  return(IT_SUCCESS);
}

// U it_evd_dequeue
it_status_t it_evd_dequeue (
                            IN  it_evd_handle_t evd_handle, // Handle for simple or agregate queue
                            OUT it_event_t     *event
                            )
{  
  pthread_mutex_lock( & gITAPIFunctionMutex );

  StrongAssertLogLine( evd_handle != (it_evd_handle_t)NULL )
    << "it_evd_dequeue(): Handle is NULL "
    << EndLogLine;

  it_api_o_verbs_cq_mgr_t* CQ = (it_api_o_verbs_cq_mgr_t *) evd_handle;

  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "it_evd_dequeue(): Entered "
    << " CQ->event_number: " << (void*)CQ->event_number
    << EndLogLine;

  it_status_t status = IT_ERR_QUEUE_EMPTY;

  switch( CQ->event_number )
    {
    case IT_ASYNC_UNAFF_EVENT_STREAM: 
      { break; }
    case IT_ASYNC_AFF_EVENT_STREAM:
      {
        for( int i = 0; i < CQ->device->devices_count; i++ )
          {
            struct pollfd pfd;

            pfd.fd = CQ->device->devices[ i ]->async_fd;
            pfd.events = POLLIN;
            pfd.revents = 0;

            BegLogLine( FXLOG_IT_API_O_VERBS )
              << "it_evd_dequeue(): About to poll the async event queue "
              << EndLogLine;

            int ready = poll( & pfd, 1, IT_API_NETWORK_TIMEOUT );
            if( ready < 0 )
              {
                StrongAssertLogLine( 0 )
                  << "ERROR: poll failed " 
                  << " errno: " << errno
                  << EndLogLine;	      
              }
            else if( ready )
              {
                struct ibv_async_event rdma_event;
                int ret = ibv_get_async_event( CQ->device->devices[ i ], & rdma_event );
                if( ret )
                  {
                    StrongAssertLogLine( 0 )
                      << "ERROR: ibv_get_async_event failed " 
                      << " i: " << i
                      << " ret: " << ret
                      << " async_fd: " << CQ->device->devices[ i ]->async_fd
                      << " errno: " << errno
                      << " ready: " << ready
                      << " device: " << (void *) CQ->device->devices[ i ]
                      << EndLogLine;	      
                  }

                BegLogLine( 1 )
                  << "ERROR: event_type: " << rdma_event.event_type
                  << EndLogLine;

                status = it_api_o_verbs_process_async_event( & rdma_event, event );

                ibv_ack_async_event( & rdma_event );
              }
          }

        break;
      }
    case IT_DTO_EVENT_STREAM:
      {
        it_dto_cmpl_event_t* DTOEvent = (it_dto_cmpl_event_t *) event;

        if( CQ->dto_type == CQ_SEND )
          {
            static int send_rr_index = 0;

            BegLogLine( FXLOG_IT_API_O_VERBS )
              << "it_evd_dequeue(): About to poll CQ_SEND "
              << EndLogLine;

            status = it_api_o_verbs_poll_cq( DTOEvent, CQ, & send_rr_index );
          }
        else if( CQ->dto_type == CQ_RECV )
          {
            static int recv_rr_index = 0;

            BegLogLine( FXLOG_IT_API_O_VERBS )
              << "it_evd_dequeue(): About to poll CQ_RECV "
              << EndLogLine;

            status = it_api_o_verbs_poll_cq( DTOEvent, CQ, & recv_rr_index );
          }
        else if( CQ->dto_type == CQ_UNINITIALIZED )
          {
            // Nothing to poll 
          }
        else
          {
            StrongAssertLogLine( 0 )
              << "ERROR: dto_type: " << CQ->dto_type
              << " is not recognized."
              << EndLogLine;
          }

        break;
      }
    case IT_CM_REQ_EVENT_STREAM:
    case IT_CM_MSG_EVENT_STREAM:
      {
        if( current_cm_event != NULL )
          {
            BegLogLine( FXLOG_IT_API_O_VERBS )
              << "it_evd_dequeue():  "
              << " current_cm_event->event: " << current_cm_event->event 
              << " CQ->event_number: " << CQ->event_number
              << EndLogLine;

            if( current_cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST )
              {
                if( IT_CM_REQ_EVENT_STREAM == CQ->event_number ) 
                  {
                    it_conn_request_event_t * ConnReqEvent = (it_conn_request_event_t *) event;

                    status = it_api_o_verbs_handle_cr_event( CQ, ConnReqEvent, current_cm_event );

                    int ret = rdma_ack_cm_event( current_cm_event );
                    if( ret )
                      {		  
                        StrongAssertLogLine( 0 )
                          << "ERROR: rdma_ack_cm_event failed " 
                          << " ret: " << ret
                          << EndLogLine;	      
                      }

                    current_cm_event = NULL;
                  }
              }
            else
              {
                if( IT_CM_MSG_EVENT_STREAM == CQ->event_number ) 
                  {
                    it_connection_event_t * ConnEvent = (it_connection_event_t *) event;

                    status = it_api_o_verbs_handle_cm_event( CQ, ConnEvent, current_cm_event );

                    int ret = rdma_ack_cm_event( current_cm_event );
                    if( ret )
                      {		  
                        StrongAssertLogLine( 0 )
                          << "ERROR: rdma_ack_cm_event failed " 
                          << " ret: " << ret
                          << EndLogLine;	      
                      }

                    current_cm_event = NULL;  
                  }
              }

            break;
          }
        else
          {
            struct rdma_event_channel* ch = CQ->cq.cm_channel;

            struct pollfd pfd;
            pfd.fd = ch->fd;
            pfd.events = POLLIN;
            pfd.revents = 0;

            BegLogLine( FXLOG_IT_API_O_VERBS )
              // BegLogLine( 0 )
              << "it_evd_dequeue(): About to poll on CM channel: "
              << " ch: " << (void *) ch
              << " fd: " << ch->fd
              << EndLogLine;

            int ready = poll( & pfd, 1, IT_API_NETWORK_TIMEOUT );

            BegLogLine( FXLOG_IT_API_O_VERBS )
              << "it_evd_dequeue(): CM channel polled"
              << " return: " << ready
              << EndLogLine;

            if( ready < 0 )
              {
                StrongAssertLogLine( 0 )
                  << "ERROR: poll failed " 
                  << " errno: " << errno
                  << EndLogLine;	      
              }
            else if( ready )
              {
                struct rdma_cm_event		*cm_event = NULL;
                int ret = rdma_get_cm_event(ch, &cm_event);
                if( ret )
                  {		  
                    StrongAssertLogLine( 0 )
                      << "ERROR: rdma_get_cm_event failed " 
                      << " ret: " << ret
                      << EndLogLine;	      
                  }

                BegLogLine( FXLOG_IT_API_O_VERBS )
                  << "it_evd_dequeue(): "
                  << " cm_event->event: " << cm_event->event
                  << EndLogLine;

                if( cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST )
                  {
                    if( IT_CM_REQ_EVENT_STREAM == CQ->event_number ) 
                      {
                        it_conn_request_event_t * ConnReqEvent = (it_conn_request_event_t *) event;

                        BegLogLine( FXLOG_IT_API_O_VERBS )
                          << "it_evd_dequeue(): About to handle a connection request event"
                          << EndLogLine;

                        status = it_api_o_verbs_handle_cr_event( CQ, ConnReqEvent, cm_event );

                        int ret = rdma_ack_cm_event( cm_event );
                        if( ret )
                          {		  
                            StrongAssertLogLine( 0 )
                              << "ERROR: rdma_ack_cm_event failed " 
                              << " ret: " << ret
                              << EndLogLine;	      
                          }
                      }
                    else
                      {
                        StrongAssertLogLine( current_cm_event == NULL )
                          << "ERROR: Unexpected value: "
                          << " current_cm_event: " << (void *) current_cm_event
                          << EndLogLine;

                        current_cm_event = cm_event;
                      }
                  }
                else
                  {
                    if( IT_CM_MSG_EVENT_STREAM == CQ->event_number ) 
                      {
                        it_connection_event_t * ConnEvent = (it_connection_event_t *) event;

                        status = it_api_o_verbs_handle_cm_event( CQ, ConnEvent, cm_event );

                        int ret = rdma_ack_cm_event( cm_event );
                        if( ret )
                          {		  
                            StrongAssertLogLine( 0 )
                              << "ERROR: rdma_ack_cm_event failed " 
                              << " ret: " << ret
                              << EndLogLine;	      
                          }
                      }
                    else
                      {
                        StrongAssertLogLine( current_cm_event == NULL )
                          << "ERROR: Unexpected value: "
                          << " current_cm_event: " << (void *) current_cm_event
                          << EndLogLine;

                        current_cm_event = cm_event;			  
                      }
                  }
              }
          }

        break;
      }
    case IT_SOFTWARE_EVENT_STREAM:
      {	  
        break;
      }
    default:
      {
        StrongAssertLogLine( 0 )
          << "it_evd_dequeue(): ERROR: Unrecognized event number: "
          << CQ->event_number
          << EndLogLine;
      }
    }

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  BegLogLine( FXLOG_IT_API_O_VERBS )
    << "it_evd_dequeue(): Leaving code: " << status
    << EndLogLine;

  return(status);
}


it_status_t it_evd_dequeue_n(
                             IN  it_evd_handle_t evd_handle,
                             IN  int             deque_count,
                             OUT it_event_t     *events,
                             OUT int            *dequed_count )
{

  int DequeuedCount = 0;

  for( int i = 0; i < deque_count; i++ )
    {
      it_status_t rc = it_evd_dequeue( evd_handle, & events[ i ] );
      if( rc !=  IT_ERR_QUEUE_EMPTY )
        {
          if( rc == IT_SUCCESS )
            DequeuedCount++;
          else
            return rc;
        }
      else
        break;
    }

  *dequed_count = DequeuedCount;

  return IT_SUCCESS;
}

// U it_evd_wait

it_status_t it_evd_wait (
                         IN  it_evd_handle_t evd_handle,
                         IN  uint64_t        timeout,
                         OUT it_event_t     *event,
                         OUT size_t         *nmore
                         )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  StrongAssertLogLine( 0 )
    << "it_evd_wait(): Not yet implemented "
    << EndLogLine;

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return( IT_SUCCESS );
}


// U it_listen_create


it_status_t it_listen_create (
                              IN  it_ia_handle_t      ia_handle,
                              IN  size_t              spigot_id,
                              IN  it_evd_handle_t     connect_evd,
                              IN  it_listen_flags_t   flags,
                              IN  OUT it_conn_qual_t *conn_qual,
                              OUT it_listen_handle_t *listen_handle
                              )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  it_api_o_verbs_device_mgr_t* deviceMgr = (it_api_o_verbs_device_mgr_t *) ia_handle;

  struct rdma_cm_id *cm_listen_id;

  int ret = rdma_create_id( deviceMgr->cm_channel, &cm_listen_id, NULL, RDMA_PS_TCP );

  if( ret )
    {
      BegLogLine( 1 )
        << "it_listen_create():: ERROR: "	
        << " ret: " << ret
        << EndLogLine;

      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return IT_ERR_ABORT;
    }

  struct sockaddr_in s_addr;

  memset(&s_addr, 0, sizeof s_addr);
  s_addr.sin_family = AF_INET;
  s_addr.sin_port = conn_qual->conn_qual.lr_port.local;

#ifndef IT_API_USE_SIW_HACK
  ret = it_api_o_verbs_get_device_address( &s_addr, IT_API_COMM_DEVICE );
  if( ret != 0 )
    {
      BegLogLine( 1 )
        << "Couldn't find address for device " << IT_API_COMM_DEVICE
        << EndLogLine;
      return IT_ERR_ABORT;
    }
#else
  s_addr.sin_addr.s_addr = htonl(INADDR_ANY);
#endif


  ret = rdma_bind_addr(cm_listen_id, (struct sockaddr*)&s_addr);
  if (ret) 
    {
      BegLogLine( 1 )
        << "it_listen_create(): "
        << " failed to bind server address"
        << " ret: " << ret
        << " errno: " << errno
        << " addr: " << (void*)(s_addr.sin_addr.s_addr)
        << " port: " << s_addr.sin_port
        << EndLogLine;

      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return IT_ERR_ABORT;
    }

  BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT )
    << "it_listen_create(): "
    << " ret: " << ret
    << " errno: " << errno
    << " addr: " << (void*)(s_addr.sin_addr.s_addr)
    << " port: " << s_addr.sin_port
    << EndLogLine;

  ret = rdma_listen(cm_listen_id, IT_API_O_VERBS_LISTEN_BACKLOG );
  if (ret) 
    {
      BegLogLine( 1 )
        << "it_listen_create(): "
        << " failed to listen"
        << " ret: " << ret
        << " errno: " << errno
        << EndLogLine;

      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return IT_ERR_ABORT;
    }

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT )
    << "it_listen_create(): leaving.."
    << EndLogLine;

  return(IT_SUCCESS);
}

it_status_t it_ep_disconnect (
                              IN        it_ep_handle_t ep_handle,
                              IN  const unsigned char *private_data,
                              IN        size_t         private_data_length
                              )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  // BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_disconnect()" << EndLogLine;
  BegLogLine( FXLOG_IT_API_O_VERBS | FXLOG_IT_API_O_VERBS_CONNECT ) << "it_ep_disconnect() Entering... " << EndLogLine;

  BegLogLine( FXLOG_IT_API_O_VERBS | FXLOG_IT_API_O_VERBS_CONNECT ) << "IN        it_ep_handle_t        ep_handle,                 " <<  (void *) ep_handle           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS | FXLOG_IT_API_O_VERBS_CONNECT ) << "IN  const unsigned char         private_data@              " <<  (void*)private_data        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS | FXLOG_IT_API_O_VERBS_CONNECT ) << "IN        size_t                private_data_length        " <<   private_data_length << EndLogLine;

  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  int ret = rdma_disconnect( qpMgr->cm_conn_id );

  if( ret )
    {
      BegLogLine( 1 )
        << "it_ep_disconnect(): ERROR: "
        << " ret: " << ret
        << EndLogLine;

      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return IT_ERR_ABORT;
    }

  BegLogLine( FXLOG_IT_API_O_VERBS | FXLOG_IT_API_O_VERBS_CONNECT ) << "it_ep_disconnect() Leaving... " << EndLogLine;

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return IT_SUCCESS;
}


// U it_ep_rc_create
it_status_t it_ep_rc_create (
                             IN        it_pz_handle_t            pz_handle,
                             IN        it_evd_handle_t           request_sevd_handle,
                             IN        it_evd_handle_t           recv_sevd_handle,
                             IN        it_evd_handle_t           connect_sevd_handle,
                             IN        it_ep_rc_creation_flags_t flags,
                             IN  const it_ep_attributes_t       *ep_attr,
                             OUT       it_ep_handle_t           *ep_handle
                             )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_rc_create()" << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS)
    << "it_ep_rc_create()"
    << " pz_handle           " << (void*) pz_handle
    << " request_sevd_handle " << (void*) request_sevd_handle
    << " recv_sevd_handle    " << (void*) recv_sevd_handle
    << " connect_sevd_handle " << (void*) connect_sevd_handle
    << " flags               " << (void*) flags
    << " ep_attr@            " << (void*) ep_attr
    << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->max_dto_payload_size           " << ep_attr->max_dto_payload_size           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->max_request_dtos               " << ep_attr->max_request_dtos               << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->max_recv_dtos                  " << ep_attr->max_recv_dtos                  << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->max_send_segments              " << ep_attr->max_send_segments              << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->max_recv_segments              " << ep_attr->max_recv_segments              << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.rdma_read_enable        " << ep_attr->srv.rc.rdma_read_enable        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.rdma_write_enable       " << ep_attr->srv.rc.rdma_write_enable       << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.max_rdma_read_segments  " << ep_attr->srv.rc.max_rdma_read_segments  << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.max_rdma_write_segments " << ep_attr->srv.rc.max_rdma_write_segments << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.rdma_read_ird           " << ep_attr->srv.rc.rdma_read_ird           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.rdma_read_ord           " << ep_attr->srv.rc.rdma_read_ord           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.srq                     " << ep_attr->srv.rc.srq                     << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.soft_hi_watermark       " << ep_attr->srv.rc.soft_hi_watermark       << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.hard_hi_watermark       " << ep_attr->srv.rc.hard_hi_watermark       << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->srv.rc.atomics_enable          " << ep_attr->srv.rc.atomics_enable          << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "ep_attr->priv_ops_enable                " << ep_attr->priv_ops_enable                << EndLogLine;

  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) malloc( sizeof( it_api_o_verbs_qp_mgr_t ) );
  StrongAssertLogLine( qpMgr )
    << "it_ep_rc_create(): ERROR: "
    << EndLogLine;

  bzero( qpMgr, sizeof( it_api_o_verbs_qp_mgr_t ) );

  qpMgr->pd         = (it_api_o_verbs_pd_mgr_t *) pz_handle;
  qpMgr->device_ord = -1;
  qpMgr->send_cq    = (it_api_o_verbs_cq_mgr_t *) request_sevd_handle;
  qpMgr->recv_cq    = (it_api_o_verbs_cq_mgr_t *) recv_sevd_handle;
  qpMgr->cm_cq      = (it_api_o_verbs_cq_mgr_t *) connect_sevd_handle;
  qpMgr->qp         = NULL;
  qpMgr->cm_conn_id = NULL;
  qpMgr->addr_resolved = 0;
  qpMgr->route_resolved = 0;

  memcpy( & (qpMgr->ep_attr), ep_attr, sizeof( it_ep_attributes_t ) ); // get attributes copied for later use

  qpMgr->send_cq->dto_type = CQ_SEND;
  qpMgr->recv_cq->dto_type = CQ_RECV;  

  *ep_handle = (it_ep_handle_t) qpMgr;

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}



it_status_t it_prepare_connection(
                                  IN        it_api_o_verbs_qp_mgr_t  *qpMgr,
                                  IN  const it_path_t                *path,
                                  IN  const it_conn_attributes_t     *conn_attr,
                                  IN  const it_conn_qual_t           *connect_qual,
                                  IN        it_cn_est_flags_t         cn_est_flags
                                  )
{
  int ret = -1;
  struct rdma_cm_id *cm_conn_id = NULL;


#if 0
  if( qpMgr->cm_conn_id != NULL )
    {
      BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT )
        << " Found cm_conn_id: : " << (void *) qpMgr->cm_conn_id
        << EndLogLine;

      cm_conn_id = qpMgr->cm_conn_id;

      StrongAssertLogLine( cm_conn_id->context == (void *) qpMgr )
        << "it_ep_connect(): ERROR: "
        << " cm_conn_id->context: " << (void *) cm_conn_id->context
        << " qpMgr: " << (void *) qpMgr
        << EndLogLine;
    }
#endif

  // if conn_id already exists in qpMgr, the destroy old first
  if( qpMgr->cm_conn_id != NULL )
    {
      if( qpMgr->qp != NULL )
        {
          rdma_destroy_qp( qpMgr->cm_conn_id );
          qpMgr->qp = NULL;
        }

      ret = rdma_destroy_id( qpMgr->cm_conn_id );

      if( ret ) 
        {
          BegLogLine( 1 )
            << "it_ep_connect(): ERROR: rdma_destroy id failed"
            << EndLogLine;	  
        }

      qpMgr->cm_conn_id = NULL;
      qpMgr->addr_resolved = 0;
      qpMgr->route_resolved = 0;
    }


  // then create new cm_id
  // Note: qp is created later (after resolving address, route and initial handshake)
  {           
    ret = rdma_create_id(qpMgr->cm_cq->device->cm_channel, &cm_conn_id, NULL, RDMA_PS_TCP);
    if (ret) 
      {
        BegLogLine( 1 )
          << "it_ep_connect(): ERROR: creating cm id failed"
          << EndLogLine;

        pthread_mutex_unlock( & gITAPIFunctionMutex );
        return IT_ERR_ABORT;
      }

    StrongAssertLogLine( qpMgr->cm_conn_id == NULL )
      << "it_ep_connect(): ERROR: cm_conn_id should not be set"    
      << EndLogLine;

    BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT )
      << "rdma_create_id called: "
      << " cm_channel: " << (void *) qpMgr->cm_cq->device->cm_channel
      << " cm_conn_id: " << (void *) cm_conn_id
      << EndLogLine;

    qpMgr->cm_conn_id = cm_conn_id;
    cm_conn_id->context = (void *) qpMgr;
  }

  if( ! qpMgr->addr_resolved )
    {
      /* address */
      struct sockaddr_in			 s_addr;
      memset(&s_addr, 0, sizeof s_addr);
      s_addr.sin_family = AF_INET;
      s_addr.sin_addr.s_addr = path->u.iwarp.raddr.ipv4.s_addr;
      //s_addr.sin_addr.s_addr = inet_addr( "10.201.25.16" );
      s_addr.sin_port = connect_qual->conn_qual.lr_port.remote;

#ifndef IT_API_USE_SIW_HACK

      struct sockaddr_in l_addr;

      memset( &l_addr, 0, sizeof( l_addr ) );
      l_addr.sin_family = AF_INET;
      l_addr.sin_port = local_port;
      local_port++;

      ret = it_api_o_verbs_get_device_address( &l_addr, IT_API_COMM_DEVICE );
      if( ret != 0 )
        {
          BegLogLine( 1 )
            << "Couldn't find address for device " << IT_API_COMM_DEVICE
            << EndLogLine;
          return IT_ERR_ABORT;
        }

      ret = rdma_resolve_addr( cm_conn_id, (struct sockaddr*) & l_addr, (struct sockaddr*) & s_addr, 2000 );
#else
      ret = rdma_resolve_addr( cm_conn_id, NULL, (struct sockaddr*) & s_addr, 2000 );
#endif

      if (ret) 
        {
          BegLogLine( 1 )
            << "it_ep_connect(): ERROR: failed to resolve address"
            << " errno: " << errno
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      struct rdma_cm_event		*cm_event;

      ret = rdma_get_cm_event(qpMgr->cm_cq->device->cm_channel, &cm_event);
      if (ret) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: failed to get cm event"
            << " ret: " << ret
            << " errno: " << errno
            << " qpMgr->cm_cq->device->cm_channel: " << (void *) qpMgr->cm_cq->device->cm_channel
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      if (cm_event->event != RDMA_CM_EVENT_ADDR_RESOLVED) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: wrong event received: " 
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        } 
      else if (cm_event->status != 0 ) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: event has error status: " 
            << cm_event->status
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      ret = rdma_ack_cm_event( cm_event );

      if (ret) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: failed to acknowledge cm event" 
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      BegLogLine(FXLOG_IT_API_O_VERBS) 
        << "it_ep_connect(): Address resolved" 
        << "" 
        << EndLogLine;

      qpMgr->addr_resolved = 1;
    }

  if( ! qpMgr->route_resolved )
    {
      /* route */
      ret = rdma_resolve_route(qpMgr->cm_conn_id, 2000);
      if (ret) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: failed to resolve route" 
            << " ret: " << ret
            << " errno: " << errno
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      struct rdma_cm_event		*cm_event;

      ret = rdma_get_cm_event(qpMgr->cm_cq->device->cm_channel, &cm_event);
      if (ret) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: failed to get cm event " 
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      if (cm_event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: wrong event received: " 
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        } 
      else if (cm_event->status != 0 ) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: event has error status: " 
            << cm_event->status
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      ret = rdma_ack_cm_event(cm_event);
      if (ret) 
        {
          BegLogLine( 1 ) 
            << "it_ep_connect(): ERROR: failed to acknowledge cm event" 
            << EndLogLine;

          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return IT_ERR_ABORT;
        }

      BegLogLine(FXLOG_IT_API_O_VERBS_CONNECT) 
        << "it_ep_connect(): Route resolved" 
        << EndLogLine;

      qpMgr->route_resolved = 1;
    }
  return IT_SUCCESS;
}

// U it_ep_connect

// An object is needed to hold state between
// the call to connect and the generation of
// a connection event in response to this call.
it_status_t it_ep_connect (
                           IN        it_ep_handle_t        ep_handle,
                           IN  const it_path_t            *path,
                           IN  const it_conn_attributes_t *conn_attr,
                           IN  const it_conn_qual_t       *connect_qual,
                           IN        it_cn_est_flags_t     cn_est_flags,
                           IN  const unsigned char        *private_data,
                           IN        size_t                private_data_length
                           )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  // BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_connect() entering" << EndLogLine;
  BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT ) << "it_ep_connect() entering" << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN        it_ep_handle_t        ep_handle,                 " <<  (void *)ep_handle           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_path_t             path..>raddr.ipv4.s_addr   " <<  (void*) path->u.iwarp.raddr.ipv4.s_addr        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_conn_attributes_t* conn_attr@                 " <<  (void*) conn_attr                              << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_conn_qual_t        connect_qual..>port.local  " <<  (void*) connect_qual->conn_qual.lr_port.local  << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_conn_qual_t        connect_qual..>port.remote " <<  (void*) connect_qual->conn_qual.lr_port.remote << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN        it_cn_est_flags_t     cn_est_flags,              " <<  (void*)  cn_est_flags        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const unsigned char         private_data@              " <<  (void*)private_data        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN        size_t                private_data_length        " <<   private_data_length << EndLogLine;

  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  it_status_t istatus = socket_nonblock_off( qpMgr->cm_cq->device->cm_channel->fd );
  if( istatus != IT_SUCCESS )
    {
      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return istatus;
    }

  int ret = -1;

  istatus = it_prepare_connection(
                                  qpMgr,
                                  path,
                                  conn_attr,
                                  connect_qual,
                                  cn_est_flags
                                  );
                                  

  struct rdma_cm_id *cm_conn_id = qpMgr->cm_conn_id;

  /// THIS IS WHERE WE COULD SPLIT TO AVOID DUPLICATE CODE WITH ITX_EP_CONNECT_WITH_RMR



  // create qp if not already exist
  if( qpMgr->qp == NULL )
    {
      it_status_t status = it_api_o_verbs_init_qp( cm_conn_id, qpMgr );
      if( status != IT_SUCCESS )
        {
          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return status;
        }

      BegLogLine(FXLOG_IT_API_O_VERBS_CONNECT) 
        << "it_ep_connect(): QP initiated" 
        << EndLogLine;
    }

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = qpMgr->ep_attr.srv.rc.rdma_read_ird;   //IT_RDMA_INITIATOR_DEPTH;
  conn_param.responder_resources = qpMgr->ep_attr.srv.rc.rdma_read_ord; // IT_RDMA_RESPONDER_RESOUCES;
  conn_param.retry_count = 7;
  conn_param.private_data = private_data;
  conn_param.private_data_len = private_data_length;

  StrongAssertLogLine( conn_param.private_data_len < 56 )
    << "it_ep_connect(): InfiniBand maximum private data length exceeded. Limit is 56, actual is " << conn_param.private_data_len
    << EndLogLine;

  ret = rdma_connect(cm_conn_id, &conn_param);
  if (ret) 
    {
      BegLogLine( 1 ) 
        << "it_ep_connect(): ERROR: failed to connect to remote host" 
        << " ret: " << ret
        << " errno: " << errno
        << " conn_id: " << (void*)cm_conn_id
        << " conn_param.pdlen: " << conn_param.private_data_len
        << EndLogLine;

      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return IT_ERR_ABORT;
    }  

  // BegLogLine(FXLOG_IT_API_O_VERBS) 
  BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT ) 
    << "it_ep_connect(): QP connected" 
    << EndLogLine;

  istatus = socket_nonblock_on( qpMgr->cm_cq->device->cm_channel->fd );
  if( istatus != IT_SUCCESS )
    {
      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return istatus;
    }

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}


// U it_ep_accept
it_status_t it_ep_accept (
                          IN        it_ep_handle_t         ep_handle,
                          IN        it_cn_est_identifier_t cn_est_id,
                          IN  const unsigned char         *private_data,
                          IN        size_t                 private_data_length
                          )
{
  // pthread_mutex_lock( & gITAPIFunctionMutex );

  // this is the moment when the ConReq is first associated with the passive side endpoint
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_accept()" << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_handle_t         ep_handle           " << (void *) ep_handle   << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_cn_est_identifier_t cn_est_id           " << cn_est_id           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "unsigned char         *private_data @      " << (void*)private_data << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "size_t                 private_data_length " << private_data_length << EndLogLine;

  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  struct rdma_cm_id *cm_conn_id = (struct rdma_cm_id *) cn_est_id;

  qpMgr->cm_conn_id = cm_conn_id;
  cm_conn_id->context = (void *) qpMgr;

  it_status_t status = it_api_o_verbs_init_qp( cm_conn_id, qpMgr );
  if( status != IT_SUCCESS )
    {
      return status;
    }  

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = qpMgr->ep_attr.srv.rc.rdma_read_ird;   //IT_RDMA_INITIATOR_DEPTH;
  conn_param.responder_resources = qpMgr->ep_attr.srv.rc.rdma_read_ord; // IT_RDMA_RESPONDER_RESOUCES;
  conn_param.private_data = private_data;
  conn_param.private_data_len = private_data_length;

  StrongAssertLogLine( conn_param.private_data_len < 196 )
    << "it_ep_connect(): InfiniBand maximum private data length exceeded. Limit is 196, actual is " << conn_param.private_data_len
    << EndLogLine;

  int ret = rdma_accept(cm_conn_id, &conn_param);
  if( ret ) 
    {
      BegLogLine( 1 ) 
        << "it_ep_accept(): ERROR: failed to accept connection" 
        << " ret: " << ret
        << " errno: " << errno
        << EndLogLine;

      return IT_ERR_ABORT;
    }

  //  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}


// it_ep_free

it_status_t it_ep_free (
                        IN  it_ep_handle_t ep_handle
                        )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_free()" << EndLogLine;

  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  rdma_destroy_qp( qpMgr->cm_conn_id );

  rdma_destroy_id( qpMgr->cm_conn_id );

  free( qpMgr );

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}


// U it_lmr_create21
/*
  it_lmr_create21 provides the v2.1 functionality
  and may be renamed to it_lmr_create in a future IT-API version.
*/

it_status_t it_lmr_create21 (
                             IN  it_pz_handle_t        pz_handle,
                             IN  void                 *addr,
                             IN  it_iobl_t            *iobl,
                             IN  it_length_t           length,
                             IN  it_addr_mode_t        addr_mode,
                             IN  it_mem_priv_t         privs,
                             IN  it_lmr_flag_t         flags,
                             IN  uint32_t              shared_id,
                             OUT it_lmr_handle_t      *lmr_handle,
                             IN  OUT it_rmr_context_t *rmr_context
                             )
{

  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_lmr_create():       " << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_lmr_create21():       " << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  it_pz_handle_t        pz_handle   " << pz_handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  void                 *addr        " << addr << EndLogLine;
  // following line segvs
  //  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  it_iobl_t            *iobl,       " << iobl->num_elts << " fbo " << iobl->fbo << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  it_length_t           length,     " << length << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  it_addr_mode_t        addr_mode,  " << addr_mode << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  it_mem_priv_t         privs,      " << privs << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  it_lmr_flag_t         flags,      " << flags << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  uint32_t              shared_id,  " << shared_id << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " OUT it_lmr_handle_t      *lmr_handle, " << (void*) *lmr_handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << " IN  OUT it_rmr_context_t *rmr_context " << (rmr_context ? (void*)*rmr_context : 0) << EndLogLine;

  struct ibv_mr				*local_triplet_mr;

  int access = 0;

  if( privs & IT_PRIV_LOCAL_WRITE )
    access |= IBV_ACCESS_LOCAL_WRITE;

  if( privs & IT_PRIV_REMOTE_WRITE )
    access |= IBV_ACCESS_REMOTE_WRITE;

  if( privs & IT_PRIV_REMOTE_READ )
    access |= IBV_ACCESS_REMOTE_READ;

  it_api_o_verbs_pd_mgr_t* pdMgr = (it_api_o_verbs_pd_mgr_t *) pz_handle;


  it_api_o_verbs_mr_mgr_t* mrMgr = (it_api_o_verbs_mr_mgr_t*) malloc( sizeof( it_api_o_verbs_mr_mgr_t ) );
  StrongAssertLogLine( mrMgr )
    << "it_lmr_create(): ERROR: "
    << EndLogLine;
  bzero( mrMgr, sizeof( it_api_o_verbs_mr_mgr_t ) );

  mrMgr->pd           = pdMgr;
  mrMgr->access       = (ibv_access_flags) access;
  mrMgr->addr         = addr;
  mrMgr->length       = length;

  StrongAssertLogLine( pdMgr->device != NULL )
    << "it_lmr_create(): ERROR: "
    << " pdMgr: " << (void *) pdMgr
    << EndLogLine;  

  StrongAssertLogLine( pdMgr->device->devices_count > 0 )
    << "it_lmr_create(): ERROR: "
    << " pdMgr->device->devices_count: " << pdMgr->device->devices_count
    << EndLogLine;  

  mrMgr->MRs = (it_api_o_verbs_mr_record_t *) malloc( sizeof( it_api_o_verbs_mr_record_t ) * pdMgr->device->devices_count );
  StrongAssertLogLine( mrMgr->MRs )
    << "it_lmr_create(): ERROR: "
    << EndLogLine;

  for( int i = 0; i < pdMgr->device->devices_count; i++ )
    {
      if( pdMgr->PDs[ i ] != NULL )
        {
          gITAPI_REG_MR_START.HitOE( IT_API_TRACE, 
                                     gITAPI_REG_MR_START_Name, 
                                     gTraceRank, 
                                     gITAPI_REG_MR_START );

          struct ibv_mr *local_triplet_mr = ibv_reg_mr( pdMgr->PDs[ i ],
                                                        mrMgr->addr, 
                                                        mrMgr->length,
                                                        mrMgr->access );
          gITAPI_REG_MR_FINIS.HitOE( IT_API_TRACE, 
                                     gITAPI_REG_MR_FINIS_Name, 
                                     gTraceRank, 
                                     gITAPI_REG_MR_FINIS );

          if( ! local_triplet_mr )
            {
              BegLogLine( 1 )
                << "it_api_o_verbs_post_op(): ERROR: "
                << " failed to register an mr"
                << EndLogLine;

              return IT_ERR_ABORT;
            }

          mrMgr->MRs[ i ].mr = local_triplet_mr;

          BegLogLine( 0 ) 
            << "it_lmr_create(): "
            << " lmr: " << (void *)mrMgr
            << " dev" << i
            << " lmr.lkey: " << (void *)mrMgr->MRs[ i ].mr->lkey
            << " dev" << i
            << " lmr.rkey: " << (void *)mrMgr->MRs[ i ].mr->rkey
            << EndLogLine;

        }
      else
        {
          BegLogLine( FXLOG_IT_API_O_VERBS ) 
            << "it_lmr_create(): "
            << " no pd created for dev: " << i
            << " deferring registration"
            << EndLogLine;

          mrMgr->MRs[ i ].mr = NULL;
        }
    }

  *lmr_handle = (it_lmr_handle_t) mrMgr;

  // rmr-output
  if( rmr_context != NULL )
    *rmr_context = (it_rmr_context_t) mrMgr;

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}


// it_lmr_free

it_status_t it_lmr_free (
                         IN  it_lmr_handle_t lmr_handle
                         )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS)
    << "it_lmr_free()"
    << "IN it_lmr_handle_t lmr_handle " << lmr_handle
    << EndLogLine;

  it_api_o_verbs_mr_mgr_t* mrMgr = (it_api_o_verbs_mr_mgr_t*) lmr_handle;

  for( int i = 0; i < mrMgr->pd->device->devices_count; i++ )
    {
      if( mrMgr->MRs[ i ].mr != NULL )
        {
          BegLogLine( FXLOG_IT_API_O_VERBS)
            << "it_lmr_free(): dereg for device: " << i
            << " mem@: " << mrMgr->addr
            << " len: " << mrMgr->length
            << EndLogLine;

          ibv_dereg_mr((struct ibv_mr*) mrMgr->MRs[ i ].mr );
        }
    }

  free( mrMgr->MRs );

  free( mrMgr );

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}

it_status_t it_post_rdma_read (
                               IN        it_ep_handle_t    ep_handle,
                               IN  const it_lmr_triplet_t *local_segments,
                               IN        size_t            num_segments,
                               IN        it_dto_cookie_t   cookie,
                               IN  const it_dto_flags_t    dto_flags,
                               IN        it_rdma_addr_t    rdma_addr,
                               IN        it_rmr_context_t  rmr_context
                               )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_post_rdma_read(): " << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_dto_flags_t   " << (void *) dto_flags << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_rdma_addr_t   " << (void *) rdma_addr         << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_rmr_context_t " << (void *) rmr_context       << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_handle_t ep_handle " << (void *) ep_handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_lmr_triplet_t "
                                   << " @ "           << (void*) local_segments
                                   << "->lkey (handle) " << (void*) local_segments->lmr
                                   << "->addr "  << (void*) local_segments->addr.abs
                                   << "->length " << local_segments->length
                                   << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "size_t           " << num_segments      << EndLogLine;


  StrongAssertLogLine( local_segments )
    << "it_post_rdma_read(): local_segments is NULL "
    << EndLogLine;

  it_status_t status = it_api_o_verbs_post_op( POST_SEND, 
                                               IBV_WR_RDMA_READ, 
                                               ep_handle,
                                               local_segments,
                                               num_segments,
                                               cookie,
                                               dto_flags,
                                               rdma_addr, 
                                               rmr_context );

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return status;
}

// U it_post_rdma_write
it_status_t it_post_rdma_write (
                                IN        it_ep_handle_t    ep_handle,
                                IN  const it_lmr_triplet_t *local_segments,
                                IN        size_t            num_segments,
                                IN        it_dto_cookie_t   cookie,
                                IN        it_dto_flags_t    dto_flags,
                                IN        it_rdma_addr_t    rdma_addr,
                                IN        it_rmr_context_t  rmr_context
                                )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "it_post_rdma_write(): " << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "it_handle_t ep_handle " << (void *) ep_handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "it_lmr_triplet_t "
                                         << " @ "           << (void*) local_segments
                                         << "->lkey (handle) " << (void*) local_segments->lmr
                                         << "->addr "  << (void*) local_segments->addr.abs
                                         << "->length " << local_segments->length
                                   << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "size_t           " << num_segments      << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "it_dto_flags_t   " << (void *) dto_flags << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "it_rdma_addr_t   " << (void *) rdma_addr         << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS_WRITE) << "it_rmr_context_t " << (void *) rmr_context       << EndLogLine;

  StrongAssertLogLine( local_segments )
    << "it_post_rdma_write(): local_segments is NULL "
    << EndLogLine;

  it_status_t status = it_api_o_verbs_post_op( POST_SEND,
                                               IBV_WR_RDMA_WRITE, 
                                               ep_handle,
                                               local_segments,
                                               num_segments,
                                               cookie,
                                               dto_flags,
                                               rdma_addr, 
                                               rmr_context );

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(status);
}

// U it_post_recv
it_status_t it_post_recv (
                          IN        it_handle_t       handle,
                          IN  const it_lmr_triplet_t *local_segments,
                          IN        size_t            num_segments,
                          IN        it_dto_cookie_t   cookie,
                          IN        it_dto_flags_t    dto_flags
                          )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_post_recv()" << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_handle_t (ep or srq?) " << (void*)  handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_lmr_triplet_t "
                                   << " @ "           << (void*) local_segments
                                   << "->lkey (handle) " << (void*) local_segments->lmr
                                   << "->addr "  << (void*) local_segments->addr.abs
                                   << "->length " << local_segments->length
                                   << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "size_t           " << num_segments << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_dto_cookie_t  " << cookie.mFirst << " " << cookie.mSecond << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_dto_flags_t   " << (void*)dto_flags << EndLogLine;

  it_status_t status = it_api_o_verbs_post_op( POST_RECV, 
                                               IBV_WR_SEND, 
                                               (it_ep_handle_t) handle,
                                               local_segments,
                                               num_segments,
                                               cookie,
                                               dto_flags,
                                               0, 
                                               0 );

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return( status );
}

// U it_post_send
it_status_t it_post_send (
                          IN        it_ep_handle_t    ep_handle,
                          IN  const it_lmr_triplet_t *local_segments,
                          IN        size_t            num_segments,
                          IN        it_dto_cookie_t   cookie,
                          IN        it_dto_flags_t    dto_flags
                          )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_post_send()" << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_handle_t ep_handle " << (void *) ep_handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_lmr_triplet_t "
                                   << " @ "           << (void*) local_segments
                                   << "->lkey (handle) " << (void*) local_segments->lmr
                                   << "->addr "  << (void*) local_segments->addr.abs
                                   << "->length " << local_segments->length
                                   << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "size_t           " << num_segments     << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_dto_cookie_t  " << cookie.mFirst << " " << cookie.mSecond << " " << cookie.mExtend << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_dto_flags_t   " << (void*)dto_flags << EndLogLine;

  StrongAssertLogLine( local_segments )
    << "it_post_send(): local_segments is NULL "
    << EndLogLine;

  it_status_t status = it_api_o_verbs_post_op( POST_SEND, 
                                               IBV_WR_SEND, 
                                               ep_handle,
                                               local_segments,
                                               num_segments,
                                               cookie,
                                               dto_flags,
                                               0, 
                                               0 );

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(status);
}

// #include <sys/socket.h>
// #include <netinet/in.h>
// #include <netinet/ip.h>


/********************************************************************
 * Extended IT_API 
 ********************************************************************/
// Extended Accept that allows to transmit an RMR context as private data
// - registers the lmr with the newly created qp
// - makes this info part of the private data and accepts the connection
it_status_t itx_ep_accept_with_rmr (
                                    IN        it_ep_handle_t         ep_handle,
                                    IN        it_cn_est_identifier_t cn_est_id,
                                    IN        it_lmr_triplet_t      *lmr,
                                    OUT       it_rmr_context_t      *rmr_context )
{
  // pthread_mutex_lock( & gITAPIFunctionMutex );

  // this is the moment when the ConReq is first associated with the passive side endpoint
  BegLogLine(FXLOG_IT_API_O_VERBS) << "itx_ep_accept_with_rmr()" << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_handle_t         ep_handle           " << (void *) ep_handle << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_cn_est_identifier_t cn_est_id           " << cn_est_id          << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_lmr_triplet_t       *lmr                " << lmr                << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "it_rmr_context_t*      rmr_context         " << rmr_context        << EndLogLine;


  struct sockaddr_in *local_IPaddr = ( struct sockaddr_in* ) rdma_get_local_addr ( (struct rdma_cm_id*) &cn_est_id );
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IPaddr: " << (void*)(local_IPaddr->sin_addr.s_addr) << EndLogLine;


  it_rmr_triplet_t private_data;
  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  struct rdma_cm_id *cm_conn_id = (struct rdma_cm_id *) cn_est_id;

  qpMgr->cm_conn_id = cm_conn_id;
  cm_conn_id->context = (void *) qpMgr;

  it_status_t status = it_api_o_verbs_init_qp( cm_conn_id, qpMgr );
  if( status != IT_SUCCESS )
    {
      return status;
    }  

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = qpMgr->ep_attr.srv.rc.rdma_read_ird;   //IT_RDMA_INITIATOR_DEPTH;
  conn_param.responder_resources = qpMgr->ep_attr.srv.rc.rdma_read_ord; // IT_RDMA_RESPONDER_RESOUCES;

  // if lmr and rmr_context are provided:
  // - register rmr with QP
  // - make rmr private data
  if( lmr && rmr_context )
    {
      status = itx_get_rmr_context_for_ep( ep_handle,
                                           lmr->lmr,
                                           rmr_context );

      if( status != IT_SUCCESS )
        {
          return status;
        }  

      BegLogLine( 1 )
        << "accept_with_rmr: "
        << " ep_handle: " << (void*) ep_handle
        << " lmr: [" << (void*)lmr->lmr
        << " " << lmr->addr.abs
        << " " << lmr->length
        << "]"
        << " rmr: " << (void*)*rmr_context
        << EndLogLine;

      private_data.rmr      = (it_rmr_handle_t) htobe64( (*rmr_context) );
      private_data.addr.abs = (void*)htobe64( (long unsigned int) lmr->addr.abs );
      private_data.length   = htobe64( lmr->length );
  
      conn_param.private_data = &private_data;
      conn_param.private_data_len =  sizeof(it_rmr_triplet_t);
    }
      
  else
    {
      conn_param.private_data_len = 0; // assume no data
      conn_param.private_data = NULL;
    }

  StrongAssertLogLine( conn_param.private_data_len < 196 )
    << "it_ep_connect(): InfiniBand maximum private data length exceeded. Limit is 196, actual is " << conn_param.private_data_len
    << EndLogLine;

  int ret = rdma_accept(cm_conn_id, &conn_param);
  if( ret ) 
    {
      BegLogLine( 1 ) 
        << "it_ep_accept(): ERROR: failed to accept connection" 
        << " ret: " << ret
        << " errno: " << errno
        << EndLogLine;

      return IT_ERR_ABORT;
    }

  //  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}


// U it_ep_connect

// An object is needed to hold state between
// the call to connect and the generation of
// a connection event in response to this call.
it_status_t itx_ep_connect_with_rmr (
                                     IN        it_ep_handle_t        ep_handle,
                                     IN  const it_path_t            *path,
                                     IN  const it_conn_attributes_t *conn_attr,
                                     IN  const it_conn_qual_t       *connect_qual,
                                     IN        it_cn_est_flags_t     cn_est_flags,
                                     IN  const unsigned char        *private_data,
                                     IN        size_t                private_data_length,
                                     IN        it_lmr_triplet_t     *lmr,
                                     OUT       it_rmr_context_t     *rmr_context
                                     )
{
  pthread_mutex_lock( & gITAPIFunctionMutex );

  // BegLogLine(FXLOG_IT_API_O_VERBS) << "it_ep_connect() entering" << EndLogLine;
  BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT ) << "it_ep_connect() entering" << EndLogLine;

  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN        it_ep_handle_t        ep_handle,                 " <<  (void *)ep_handle           << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_path_t             path..>raddr.ipv4.s_addr   " <<  (void*) path->u.iwarp.raddr.ipv4.s_addr        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_conn_attributes_t* conn_attr@                 " <<  (void*) conn_attr                              << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_conn_qual_t        connect_qual..>port.local  " <<  (void*) connect_qual->conn_qual.lr_port.local  << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const it_conn_qual_t        connect_qual..>port.remote " <<  (void*) connect_qual->conn_qual.lr_port.remote << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN        it_cn_est_flags_t     cn_est_flags,              " <<  (void*)  cn_est_flags        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN  const unsigned char         private_data@              " <<  (void*)private_data        << EndLogLine;
  BegLogLine(FXLOG_IT_API_O_VERBS) << "IN        size_t                private_data_length        " <<   private_data_length << EndLogLine;

  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  it_status_t istatus = socket_nonblock_off( qpMgr->cm_cq->device->cm_channel->fd );
  if( istatus != IT_SUCCESS )
    {
      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return istatus;
    }

  int ret = -1;

  istatus = it_prepare_connection(
                                  qpMgr,
                                  path,
                                  conn_attr,
                                  connect_qual,
                                  cn_est_flags
                                  );
                                  

  struct rdma_cm_id *cm_conn_id = qpMgr->cm_conn_id;

  /// THIS IS WHERE WE COULD SPLIT TO AVOID DUPLICATE CODE WITH ITX_EP_CONNECT_WITH_RMR



  // create qp if not already exist
  if( qpMgr->qp == NULL )
    {
      istatus = it_api_o_verbs_init_qp( cm_conn_id, qpMgr );
      if( istatus != IT_SUCCESS )
        {
          pthread_mutex_unlock( & gITAPIFunctionMutex );
          return istatus;
        }

      BegLogLine(FXLOG_IT_API_O_VERBS_CONNECT) 
        << "it_ep_connect(): QP initiated" 
        << EndLogLine;
    }

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = qpMgr->ep_attr.srv.rc.rdma_read_ird;   //IT_RDMA_INITIATOR_DEPTH;
  conn_param.responder_resources = qpMgr->ep_attr.srv.rc.rdma_read_ord; // IT_RDMA_RESPONDER_RESOUCES;
  conn_param.retry_count = 7;

  unsigned char* internal_private_data = NULL;

  // if lmr and rmr_context are provided:
  // - register rmr with QP
  // - make rmr private data
  if( lmr && rmr_context )
    {
      istatus = itx_get_rmr_context_for_ep( ep_handle,
                                           lmr->lmr,
                                           rmr_context );

      if( istatus != IT_SUCCESS )
        {
          return istatus;
        }  

      // we have to extend the private data buffer if user provided data already
      int   internal_private_data_length;
      unsigned char *transfer_rmr;

      internal_private_data_length = private_data_length + 2 * sizeof(uint64_t) + 1 * sizeof(uint32_t); // works also if no user priv-data is present
      internal_private_data = (unsigned char*)malloc( internal_private_data_length );
          
      if( private_data != NULL )
        {
          memcpy( internal_private_data, private_data, internal_private_data_length );
          transfer_rmr = (unsigned char*) ( &internal_private_data[ private_data_length ] );
        }
      else
        {
          transfer_rmr = internal_private_data;
        }

      *((uint32_t*)&transfer_rmr[ 0 ])                   =   htonl( (uint32_t) lmr->length );
      *((uint64_t*)&transfer_rmr[ sizeof(uint32_t) ])    = htobe64( (uint64_t) (*rmr_context) );
      *((uint64_t*)&transfer_rmr[ sizeof(uint32_t) * 3]) = htobe64( (uint64_t) (lmr->addr.abs) );

      conn_param.private_data = internal_private_data;
      conn_param.private_data_len = internal_private_data_length;
    }  
  else
    {
      conn_param.private_data_len = private_data_length; // assume no data
      conn_param.private_data = private_data;
    }

  StrongAssertLogLine( conn_param.private_data_len < 56 )
    << "it_ep_connect(): InfiniBand maximum private data length exceeded. Limit is 56, actual is " << conn_param.private_data_len
    << EndLogLine;

  ret = rdma_connect(cm_conn_id, &conn_param);
  if (ret) 
    {
      BegLogLine( 1 ) 
        << "it_ep_connect(): ERROR: failed to connect to remote host" 
        << " ret: " << ret
        << " errno: " << errno
        << " conn_id: " << (void*)cm_conn_id
        << " conn_param.pdlen: " << conn_param.private_data_len
        << EndLogLine;

      if( internal_private_data )
        free( internal_private_data );

      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return IT_ERR_ABORT;
    }  

  // BegLogLine(FXLOG_IT_API_O_VERBS) 
  BegLogLine( FXLOG_IT_API_O_VERBS_CONNECT ) 
    << "it_ep_connect(): QP connected" 
    << EndLogLine;

  /* should be save to free here:
   * [manpage of rdma_connect(): private_data
   *          References a user-controlled data buffer.  The contents
   *          of the buffer are copied and transparently passed to the
   *          remote side as part of the communication request.  May
   *          be NULL if pri- vate_data is not required.
   */
  if( internal_private_data )
    free( internal_private_data );


  istatus = socket_nonblock_on( qpMgr->cm_cq->device->cm_channel->fd );
  if( istatus != IT_SUCCESS )
    {
      pthread_mutex_unlock( & gITAPIFunctionMutex );
      return istatus;
    }

  pthread_mutex_unlock( & gITAPIFunctionMutex );

  return(IT_SUCCESS);
}




it_status_t
itx_get_rmr_context_for_ep( IN  it_ep_handle_t    ep_handle,
                            IN  it_lmr_handle_t   lmr,
                            OUT it_rmr_context_t* rmr_context )
{
  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;
  int device_ordinal = qpMgr->device_ord;

  int device_count = qpMgr->pd->device->devices_count;
  StrongAssertLogLine( device_ordinal >= 0 && device_ordinal < device_count )
    << "ERROR: "
    << " device_ordinal: " << device_ordinal
    << " device_count: " << device_count    
    << EndLogLine;

  it_api_o_verbs_mr_mgr_t* mrMgr = (it_api_o_verbs_mr_mgr_t* ) lmr;

  StrongAssertLogLine( mrMgr != NULL )
    << EndLogLine;

  if( mrMgr->MRs[ device_ordinal ].mr == NULL )
    {

      if( mrMgr->pd->PDs[ device_ordinal ] == NULL )
        {
          it_status_t status = it_api_o_verbs_init_pd( device_ordinal, 
                                                       qpMgr->cm_conn_id->verbs, 
                                                       mrMgr->pd );
          if( status != IT_SUCCESS )
            return status;
        }

      BegLogLine( FXLOG_IT_API_O_VERBS_MEMREG )
        << "about to register mr: " << (void*)mrMgr->addr
        << " len: " << mrMgr->length
        << EndLogLine;
          
          BegLogLine( 1 )
            << "it_api_o_verbs_post_op(): "
            << " about to an mr "
            << " mrMgr->pd->PDs[ device_ord ]: " << (void *) mrMgr->pd->PDs[ device_ordinal ]
            << " mrMgr->addr: " << (void *) mrMgr->addr
            << " mrMgr->length: " <<  mrMgr->length
            << " mrMgr->access: " << mrMgr->access
            << EndLogLine;

      gITAPI_REG_MR_START.HitOE( IT_API_TRACE, 
                                 gITAPI_REG_MR_START_Name, 
                                 gTraceRank, 
                                 gITAPI_REG_MR_START );

      struct ibv_mr *local_triplet_mr = ibv_reg_mr( mrMgr->pd->PDs[ device_ordinal ], 
                                                    mrMgr->addr, 
                                                    mrMgr->length,
                                                    // 8 * 1024, // mrMgr->length,
                                                    mrMgr->access );
      gITAPI_REG_MR_FINIS.HitOE( IT_API_TRACE, 
                                 gITAPI_REG_MR_FINIS_Name, 
                                 gTraceRank, 
                                 gITAPI_REG_MR_FINIS );

      if( ! local_triplet_mr )
        {
          BegLogLine( 1 )
            << "it_api_o_verbs_post_op(): ERROR: "
            << " failed to register an mr "
            << " mrMgr->pd->PDs[ device_ord ]: " << (void *) mrMgr->pd->PDs[ device_ordinal ]
            << " mrMgr->addr: " << (void *) mrMgr->addr
            << " mrMgr->length: " <<  mrMgr->length
            << " mrMgr->access: " << mrMgr->access
            << " errno: " << errno
            << EndLogLine;

          return IT_ERR_ABORT;
        }

      mrMgr->MRs[ device_ordinal ].mr = local_triplet_mr;
    }

  // BegLogLine( 1 ) << "itx_get_rmr_context_for_ep():: "                        << EndLogLine;
  // BegLogLine( 1 ) << " rmr_context: " << (void*)rmr_context                   << EndLogLine;
  // BegLogLine( 1 ) << " device: " << device_ordinal                            << EndLogLine;
  // BegLogLine( 1 ) << " mrMgr: " << (void*)mrMgr                               << EndLogLine;
  // BegLogLine( 1 ) << " MRs: "  << (void*)mrMgr->MRs                           << EndLogLine;
  // BegLogLine( 1 ) << " [dev].mr: " << (void*)mrMgr->MRs[ device_ordinal ].mr  << EndLogLine;
  // BegLogLine( 1 ) << " rkey: " << (void*)mrMgr->MRs[ device_ordinal ].mr->rkey       << EndLogLine;
    

  *rmr_context = mrMgr->MRs[ device_ordinal ].mr->rkey;

  return IT_SUCCESS;
}

it_status_t
itx_bind_ep_to_device( IN  it_ep_handle_t          ep_handle,
                       IN  it_cn_est_identifier_t  cn_id )
{  
  it_api_o_verbs_qp_mgr_t* qpMgr = (it_api_o_verbs_qp_mgr_t*) ep_handle;

  qpMgr->cm_conn_id = (struct rdma_cm_id *) cn_id;

  return IT_SUCCESS;
}

/***
 * evd_handle has to be an aevd
 ***/
it_status_t
itx_aevd_wait( IN  it_evd_handle_t evd_handle,	       
               IN  uint64_t        timeout,
               IN  size_t          max_event_count,
               OUT it_event_t     *events,
               OUT size_t         *events_count )
{
  it_api_o_verbs_aevd_mgr_t* AEVD = (it_api_o_verbs_aevd_mgr_t *) evd_handle;

  /************************************************************
   * Block on event ready notification from the processing
   * threads
   ************************************************************/
  pthread_mutex_lock( & ( AEVD->mEventCounterMutex ) );  
  if( timeout == 0 )
    {
      // early exit if there are no events yet
      if( AEVD->mEventCounter == 0 )
        {
          // pthread_mutex_unlock( & ( AEVD->mEventCounterMutex ) );
          // *events_count = 0;
          // return IT_SUCCESS;
        }
    }
  else if( (timeout == IT_TIMEOUT_INFINITE) )
    {
      while( AEVD->mEventCounter == 0 )
        pthread_cond_wait( &(AEVD->mMainCond), &(AEVD->mEventCounterMutex) );
    }
  else
    {
      // timeout is in milliseconds
      struct timespec t;

      t.tv_sec  = ( timeout / 1000 );
      t.tv_nsec = ( timeout - (t.tv_sec*1000) ) * 1000000;

      pthread_cond_timedwait( &(AEVD->mMainCond), &(AEVD->mEventCounterMutex), &t );
      if( AEVD->mEventCounter == 0 )
        {
          *events_count = 0;
          return IT_SUCCESS;
        }
    }
  pthread_mutex_unlock( & ( AEVD->mEventCounterMutex ) );
  /************************************************************/

  int gatheredEventCount = 0;  

  /***********************************************************************************
   * Dequeue AFF Events
   ***********************************************************************************/  
  int availableEventSlotsCount = max_event_count; 
  int deviceCount = AEVD->mDevice->devices_count;
  for( int deviceOrd = 0; deviceOrd < deviceCount; deviceOrd++ )
    {
      int eventCountInQueue = AEVD->mAffQueues[ deviceOrd ].GetCount();
      if( eventCountInQueue > 0 )
        {
          int eventCount = min( availableEventSlotsCount, eventCountInQueue );

          for( int i = 0; i < eventCount; i++ )
            {
              AEVD->mAffQueues[ deviceOrd ].Dequeue( & events[ gatheredEventCount ] );
              gatheredEventCount++;
              availableEventSlotsCount--;
            }
        }
    }
  /***********************************************************************************/  




  /***********************************************************************************
   * Dequeue CM Events
   ***********************************************************************************/  
  AssertLogLine( availableEventSlotsCount >= 0 )
    << "ERROR: "
    << " availableEventSlotsCount: " << availableEventSlotsCount
    << EndLogLine;

  int eventCountInCMQueue = AEVD->mCMQueue.GetCount();
  if( eventCountInCMQueue > 0 )
    {
      int eventCount = min( availableEventSlotsCount, eventCountInCMQueue );

      for( int i = 0; i < eventCount; i++ )
        {
          AEVD->mCMQueue.Dequeue( & events[ gatheredEventCount ] );
          gatheredEventCount++;
          availableEventSlotsCount--;
        }
    }
  /***********************************************************************************/  




  /***********************************************************************************
   * Dequeue Send CQ Events
   ***********************************************************************************/
  AssertLogLine( availableEventSlotsCount >= 0 )
    << "ERROR: "
    << " availableEventSlotsCount: " << availableEventSlotsCount
    << EndLogLine;

  for( int deviceOrd = 0; deviceOrd < deviceCount; deviceOrd++ )
    {
      int eventCountInQueue = AEVD->mSendQueues[ deviceOrd ].GetCount();
      if( eventCountInQueue > 0 )
        {
          int eventCount = min( availableEventSlotsCount, eventCountInQueue );

          BegLogLine( FXLOG_IT_API_O_VERBS_QUEUE_LENGTHS_LOG )
            << "itx_aevd_wait():: send events: " << eventCount
            << EndLogLine;

          for( int i = 0; i < eventCount; i++ )
            {
              it_event_t* ievent = & events[ gatheredEventCount ];

              AEVD->mSendQueues[ deviceOrd ].Dequeue( ievent );
              availableEventSlotsCount--;
              gatheredEventCount++;

              switch( ievent->event_number )
                {
                case IT_DTO_RDMA_READ_CMPL_EVENT:
                  {
                    gITAPI_RDMA_READ_AT_WAIT.HitOE( IT_API_TRACE,
                                                    gITAPI_RDMA_READ_AT_WAIT_Name,
                                                    gTraceRank,
                                                    gITAPI_RDMA_READ_AT_WAIT );
                    break;
                  }
                case IT_DTO_RDMA_WRITE_CMPL_EVENT:
                  {
                    gITAPI_RDMA_WRITE_AT_WAIT.HitOE( IT_API_TRACE,
                                                     gITAPI_RDMA_WRITE_AT_WAIT_Name,
                                                     gTraceRank,
                                                     gITAPI_RDMA_WRITE_AT_WAIT );	      
                    break;
                  }
                case IT_DTO_SEND_CMPL_EVENT:
                  {
                    gITAPI_SEND_AT_WAIT.HitOE( IT_API_TRACE,
                                               gITAPI_SEND_AT_WAIT_Name,
                                               gTraceRank,
                                               gITAPI_SEND_AT_WAIT );
                    break;
                  }
                default:
                  {
                    StrongAssertLogLine( 0 )
                      << "ERROR: "
                      << " ievent->event_number: " << ievent->event_number
                      << EndLogLine;
                  }
                }
            }
        }
    }
  /************************************************************************************/




  /***********************************************************************************
   * Dequeue Recv CQ Events
   ***********************************************************************************/
  AssertLogLine( availableEventSlotsCount >= 0 )
    << "ERROR: "
    << " availableEventSlotsCount: " << availableEventSlotsCount
    << EndLogLine;

  for( int deviceOrd = 0; deviceOrd < deviceCount; deviceOrd++ )
    {
      int eventCountInQueue = AEVD->mRecvQueues[ deviceOrd ].GetCount();
      if( eventCountInQueue > 0 )
        {
          int eventCount = min( availableEventSlotsCount, eventCountInQueue );

          BegLogLine( FXLOG_IT_API_O_VERBS_QUEUE_LENGTHS_LOG )
            << "itx_aevd_wait():: recv events: " << eventCount
            << EndLogLine;

          for( int i = 0; i < eventCount; i++ )
            {
              AEVD->mRecvQueues[ deviceOrd ].Dequeue( & events[ gatheredEventCount ] );
              gatheredEventCount++;
              availableEventSlotsCount--;

              gITAPI_RECV_AT_WAIT.HitOE( IT_API_TRACE,
                                         gITAPI_RECV_AT_WAIT_Name,
                                         gTraceRank,
                                         gITAPI_RECV_AT_WAIT );	      
            }
        }
    }
  /************************************************************************************/

  AssertLogLine( availableEventSlotsCount >= 0 )
    << "ERROR: "
    << " availableEventSlotsCount: " << availableEventSlotsCount
    << EndLogLine;

  AssertLogLine( gatheredEventCount >= 0 )
    << "ERROR: "
    << " gatheredEventCount: " << gatheredEventCount
    << EndLogLine;


  pthread_mutex_lock( & ( AEVD->mEventCounterMutex ) );
  AEVD->mEventCounter -= gatheredEventCount;
  pthread_mutex_unlock( & ( AEVD->mEventCounterMutex ) );

  *events_count = gatheredEventCount;

  return IT_SUCCESS;
}
/**************************************************************************************/

it_status_t
itx_init_tracing( const char* aContextName, 
                  int   aTraceRank )
{
  gTraceRank = aTraceRank;

  StrongAssertLogLine( strlen( aContextName ) < CONTEXT_NAME_SIZE )
    << "ERROR: "
    << " CONTEXT_NAME_SIZE: " << CONTEXT_NAME_SIZE
    << " strlen( aContextName ): " << strlen( aContextName )
    << EndLogLine;

  strcpy( gContextName, aContextName );

  sprintf( gITAPI_RDMA_READ_AT_WAIT_Name, "%s.%s",
           gContextName, "ITAPI_RDMA_READ_AT_WAIT" );

  sprintf( gITAPI_RDMA_WRITE_AT_WAIT_Name, "%s.%s",
           gContextName, "ITAPI_RDMA_WRITE_AT_WAIT" );

  sprintf( gITAPI_SEND_AT_WAIT_Name, "%s.%s",
           gContextName, "ITAPI_SEND_AT_WAIT" );

  sprintf( gITAPI_RECV_AT_WAIT_Name, "%s.%s",
           gContextName, "ITAPI_RECV_AT_WAIT" );

  sprintf( gITAPI_REG_MR_START_Name, "%s.%s",
           gContextName, "ITAPI_REG_MR_START" );

  sprintf( gITAPI_REG_MR_FINIS_Name, "%s.%s",
           gContextName, "ITAPI_REG_MR_FINIS" );

  sprintf( gITAPI_POST_SEND_START_Name, "%s.%s",
           gContextName, "ITAPI_POST_SEND_START" );

  sprintf( gITAPI_POST_SEND_FINIS_Name, "%s.%s",
           gContextName, "ITAPI_POST_SEND_FINIS" );

  return IT_SUCCESS;
}


