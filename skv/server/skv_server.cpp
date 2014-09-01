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

#include <endian.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <ifaddrs.h>

#include <common/skv_types.hpp>
#include <utils/skv_trace_clients.hpp>

#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>

#include <server/skv_server_network_event_manager.hpp>

// include the implementations of the local kv backend
#include <server/skv_local_kv_interface.hpp>

// include the various event sources
#include <server/skv_server_event_source.hpp>
#include <server/skv_server_IT_event_source.hpp>
#include <server/skv_server_internal_event_source.hpp>
#include <server/skv_server_command_event_source.hpp>
#include <server/skv_server_local_kv_event_source.hpp>

#include <server/skv_server.hpp>

/****************************************
 * Supported flows of the SKV Server
 ***************************************/
#include <server/commands/skv_server_establish_client_connection_sm.hpp>
#include <server/commands/skv_server_open_command_sm.hpp>
#include <server/commands/skv_server_retrieve_dist_command_sm.hpp>
#include <server/commands/skv_server_insert_command_sm.hpp>
#include <server/commands/skv_server_bulk_insert_command_sm.hpp>
#include <server/commands/skv_server_retrieve_command_sm.hpp>
#include <server/commands/skv_server_retrieve_n_keys_command_sm.hpp>
#include <server/commands/skv_server_remove_command_sm.hpp>
#include <server/commands/skv_server_active_bcast_command_sm.hpp>
#include <server/commands/skv_server_pdscntl_command_sm.hpp>


#ifdef SKV_SERVER_LOOP_STATISTICS
typedef struct
{
  uint64_t zeroPollCount;
  uint64_t overallRunLoops;
  uint64_t reqDeferCount;
  uint64_t overallCommands;
  uint64_t maxCommandBatch;
} server_stats_t;

server_stats_t ServerStatistics = {0,0,0,0,0};

#define SERVER_STATS_ECHO( x ) " empty loops: " << x.zeroPollCount << " of " << x.overallRunLoops \
  << " deferred: " << x.reqDeferCount << " of " << x.overallCommands    \
  << " maxBatch: " << x.maxCommandBatch


#define SERVER_STATS_RESET( x ) \
              x.overallRunLoops = 0; \
              x.zeroPollCount   = 0; \
              x.reqDeferCount   = 0; \
              x.overallCommands = 0; \
              x.maxCommandBatch = 0;
#endif

int    skv_server_heap_manager_t::mFd = 0;
mspace skv_server_heap_manager_t::mMspace;
char*  skv_server_heap_manager_t::mMspaceBase = NULL;
uint32_t skv_server_heap_manager_t::mMspaceLen = 0;
uint32_t skv_server_heap_manager_t::mTotalLen = 0;
char*  skv_server_heap_manager_t::mMemoryAllocation = NULL;

#ifndef SKV_EVD_POLL_THREAD_LOG
#define SKV_EVD_POLL_THREAD_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_PROCESS_IT_EVENT_LOG
#define SKV_PROCESS_IT_EVENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_PROCESS_IT_EVENT_WRITE_LOG
#define SKV_PROCESS_IT_EVENT_WRITE_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_PROCESS_IT_EVENT_READ_LOG
#define SKV_PROCESS_IT_EVENT_READ_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_PROCESS_MPI_EVENT_LOG
#define SKV_PROCESS_MPI_EVENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_PROCESS_LOCAL_KV_EVENT_LOG
#define SKV_PROCESS_LOCAL_KV_EVENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_PROCESS_EVENT_LOG
#define SKV_PROCESS_EVENT_LOG ( SKV_PROCESS_MPI_EVENT_LOG & SKV_PROCESS_IT_EVENT_LOG & SKV_PROCESS_LOCAL_KV_EVENT_LOG )
#endif

#ifndef SKV_GET_EVENT_LOG
#define SKV_GET_EVENT_LOG ( 0 )  /* exclude from SKV_LOGGING_ALL because of too high frequency */
#endif

#ifndef SKV_SERVER_RUN_LOG
#define SKV_SERVER_RUN_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_INIT_LOG
#define SKV_SERVER_INIT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_CONN_LOG
#define SKV_SERVER_CONN_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_PENDING_EVENTS_LOG
#define SKV_SERVER_PENDING_EVENTS_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_CLEANUP_LOG
#define SKV_SERVER_CLEANUP_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_TRACE
#define SKV_SERVER_TRACE ( 0 )
#endif

#ifndef SKV_SERVER_POLL_ONLY_TRACE
#define SKV_SERVER_POLL_ONLY_TRACE ( 0 )
#endif

//#define SKV_SERVER_FETCH_AND_PROCESS


static TraceClient gSKVServerPollCommandStart;
static TraceClient gSKVServerPollCommandFinis;
static TraceClient gSKVServerEventStart;
static TraceClient gSKVServerEventFinis;
static TraceClient gSKVServerGetEventStart;
static TraceClient gSKVServerGetEventFinis;
static TraceClient gSKVServerRunLoopStart;
static TraceClient gSKVServerRunLoopFinis;
static TraceClient gSKVServerEventOtherStart;
static TraceClient gSKVServerEventOtherFinis;


skv_server_ep_state_t*
skv_server_t::
GetEPStateForEPHdl( it_ep_handle_t aEP )
{
  EPStateMap_T::iterator iter = mEPStateMap->find( aEP );

  if( iter != mEPStateMap->end() )
  {
    return iter->second;
  }
  else
    return NULL;
}


// This is a dump wrapper to be able to count rdma-write completions via function-ptr that is picked from the cookie
void*
EPSTATE_CountSendCompletionsCallback( void* Arg )
{
  BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG )
    << "CountSendCompletionsCallback:: from RDMA-write"
    << EndLogLine;
  
  StrongAssertLogLine( Arg != NULL )
    << "CountSendCompletionsCallback: ERROR EPState == NULL"
    << EndLogLine;

  skv_server_rdma_write_cmpl_cookie_t* Cookie = (skv_server_rdma_write_cmpl_cookie_t*) Arg;

  skv_server_ep_state_t* EPState = (skv_server_ep_state_t*)(Cookie->GetContext());
  int SignaledSBOrd = Cookie->GetCmdOrd();

  BegLogLine( 0 )
    << "EPSTATE_CountSendCompletionsCallback:"
    << " completing all send buffers until: " << SignaledSBOrd
    << EndLogLine;

  EPState->AllSendsComplete( SignaledSBOrd );

  return NULL;
}


void*
EPSTATE_RetrieveWriteComplete( void* Arg )
{
  BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG )
    << " EPSTATE_RetrieveWriteComplete() Callback from retrieve RDMA-write"
    << " THIS IS DUMMY, WE SHOULD NOT GET CALLED!!" 
    << EndLogLine;

  return NULL;
}

/***
 * skv_server_t::EvdPollThread::
 * Desc: This is thread polls on one EVD as 
 * specified by the ThreadArgs
 * input: 
 * returns: SKV_SUCCESS or SKV_ERR_NO_EVENT
 ***/
void* 
skv_server_t::
EvdPollThread( void* args )
{  
  ThreadArgs* TA = (ThreadArgs *) args;

  while( 1 )
  {
    AssertLogLine( TA->mEventPtr != NULL )
      << "skv_server_t::EvdPollThread:: ERROR:: "
      << " TA->mEventPtr != NULL"
      << " TA->mEVDType: " << (int) TA->mEVDType
      << EndLogLine

    bzero( TA->mEventPtr, sizeof(it_event_t) );

    BegLogLine( SKV_EVD_POLL_THREAD_LOG )
      << "skv_server_t::EvdPollThread:: "
      << " About to call it_evd_wait( " << TA->mEVDHandle
      << " , "
      << (void *) TA->mEventPtr
      << " )"
      << EndLogLine;

    size_t nmore;
    it_status_t status = it_evd_wait( TA->mEVDHandle,
    IT_TIMEOUT_INFINITE,
                                      TA->mEventPtr,
                                      &nmore );

    if( status == IT_SUCCESS )
    {
      skv_server_cookie_t* Cookie = (skv_server_cookie_t *) &(TA->mEventPtr->dto_cmpl.cookie);

      BegLogLine( SKV_EVD_POLL_THREAD_LOG )
        << "skv_server_t::EvdPollThread:: "
        << " Event received. "
        << " SEVD Type: " << skv_poll_type_to_string( TA->mEVDType )
        << " Cookie: " << *Cookie
        << " TransferredLength: " << TA->mEventPtr->dto_cmpl.transferred_length
        << " DTO_Status: " << TA->mEventPtr->dto_cmpl.dto_status
        << " About to call pthread_mutex_unlock( " << (void *) TA->mEventPresent
        << " )"
        << EndLogLine;

      // signal the main thread that
      // an event came in
      pthread_mutex_unlock( TA->mEventPresent );

      // wait for the signal from the main thread
      // to keep waiting on the next event

      BegLogLine( SKV_EVD_POLL_THREAD_LOG )
        << "skv_server_t::EvdPollThread:: "
        << " Waiting for the signal to wait on next event"
        << EndLogLine;

      while( pthread_mutex_trylock( TA->mReadyToPollNextEvent ) != 0 )
        ;
    }
    else
    {
      if( status != IT_ERR_INTERRUPT )
      {
        StrongAssertLogLine( 0 )
          << "skv_server_t::EvdPollThread:: ERROR from it_evd_wait() "
          << " status: " << status
          << " SEVD Type: " << skv_poll_type_to_string( TA->mEVDType )
          << EndLogLine;
      }
    }
  }

  return NULL;
}

/***
 * skv_server_t::SetCurrentState::
 * Desc: Assigns the current state
 * input: 
 * returns:
 ***/
void 
skv_server_t::
SetState( skv_server_state_t aState )
{
  mState = aState; 
}

/***
 * skv_server_t::SetCurrentState::
 * Desc: Assigns the current state
 * input: 
 * returns:
 ***/
skv_server_state_t 
skv_server_t::
GetState()
{
  return mState;
}

/***
 * skv_server_t::PollOnITEventClass::
 * Desc: Poll the 1 evd (Event Dispatchers) and return
 * the event
 * input: 
 * returns: SKV_SUCCESS or SKV_ERR_NO_EVENT
 ***/
skv_status_t 
skv_server_t::
PollOnITEventClass( it_evd_handle_t  aEvdHdl, 
                    it_event_t*       aEventCopy,
                    it_event_t*       aEventInPthread,
                    pthread_mutex_t*  aEventPresentMutex,
                    pthread_mutex_t*  aReadyToWaitOnNextEventMutex )
{
  it_status_t status = it_evd_dequeue( aEvdHdl,
                                       aEventCopy );

  if( status == IT_SUCCESS )
    return SKV_SUCCESS;
  else
  {
    static int counter = 1;

    BegLogLine( counter == 10000 )
      << "POLLING TEST: "
      << " status: " << status
      << EndLogLine;

    counter++;

    return SKV_ERRNO_NO_EVENT;
  }
}

/***
 * skv_server_t::GetEvent::
 * Desc: Poll either an IT_API event or an MPI event
 * input: 
 * returns: SKV_SUCCESS or SKV_ERR_NO_EVENT
 ***/
skv_status_t
skv_server_t::
GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount )
{
  gSKVServerEventOtherStart.HitOE( SKV_SERVER_TRACE,
                                   "SKVServerNonCommand",
                                   0,
                                   gSKVServerEventOtherStart );


  // initialize global counter for priority
  static int priority_index = 0;

#ifdef PKTRACE_ON  
  static unsigned long long TraceCount = 0;
// #ifdef SKV_SERVER_USE_AGGREGATE_EVD
//   if( TraceCount % (40) )
// #else
    if( TraceCount % (1024 * 1024) )
      //#endif
    {
      pkTraceServer::FlushBuffer();
    }
  TraceCount++;
#endif

  skv_status_t status = SKV_ERRNO_NO_EVENT;

  // status = GetMPIEvent( aEvents, aEventCount, aMaxEventCount );
  // if( status == SKV_SUCCESS )
  //   return status;  

  gSKVServerEventOtherFinis.HitOE( SKV_SERVER_TRACE,
                                   "SKVServerNonCommand",
                                   0,
                                   gSKVServerEventOtherFinis );

  *aEventCount = 0;

  // for registered inbound req areas
  for( int evt_src = 0; evt_src < SKV_SERVER_EVENT_SOURCES; evt_src++ )
  {
    int evtCount = 0;

    // check if priority is met and fetch request
    if( priority_index % mEventSources[evt_src]->GetPriority() == 0 )
    {
      BegLogLine( SKV_GET_EVENT_LOG )
        << "skv_server_t::GetEvent(): "
        << " fetching evt_src: " << evt_src
        << " prio_index: " << priority_index
        << EndLogLine;

      skv_server_event_t *nextEventAddr = &(aEvents[*aEventCount]);
      status = mEventSources[evt_src]->GetEvent( nextEventAddr, &evtCount, mMaxEventCounts[evt_src] );

      BegLogLine( SKV_GET_EVENT_LOG )
        << "skv_server_t::GetEvent(): "
        << " fetching complete: " << evtCount
        << EndLogLine;

      if( status == SKV_SUCCESS )
        *aEventCount += evtCount;
    }
  }

  priority_index = (priority_index + 1) % (SKV_SERVER_EVENT_SOURCE_MAX_PRIORITY * mPriorityCDN);
  return status;
}

/***
 * skv_server_t::GetMPIEvent::
 * Desc: Poll for an MPI event
 * input: 
 * returns: SKV_SUCCESS or SKV_ERR_NO_EVENT
 ***/
skv_status_t
skv_server_t::
GetMPIEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount )
{
  return SKV_ERRNO_NO_EVENT;
}


/***
 * skv_server_t::ProcessEvent::
 * Desc: This is the entry point of an event
 * into the server state machine
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_server_t::
ProcessEvent( skv_server_state_t  aState, 
              skv_server_event_t* aEvent )
{  
  skv_status_t status = SKV_SUCCESS;
  skv_server_event_type_t EventType = aEvent->mEventType;

  BegLogLine( SKV_PROCESS_EVENT_LOG )
    << "skv_server_t::ProcessEvent():: Entering: "
    //    << " State: " << skv_server_state_to_string( aState )
    << " Event: " << skv_server_event_type_to_string( EventType )
    << EndLogLine;

  switch( aState )
  {
    case SKV_SERVER_STATE_RUN:
    case SKV_SERVER_STATE_PENDING_EVENTS:
    {
      switch( aEvent->mEventType )
      {
        case SKV_SERVER_EVENT_TYPE_IT_UNAFF_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_UNAFF_SPIGOT_ONLINE:
        case SKV_SERVER_EVENT_IT_ASYNC_UNAFF_SPIGOT_OFFLINE:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_SEVD_FULL_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_FAILURE:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_BAD_TRANSPORT_OPCODE:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_REQ_DROPPED:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_RDMAW_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_RDMAW_CORRUPT_DATA:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_RDMAR_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_LOCAL_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_RECV_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_IRRQ_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_TRANSPORT_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_L_LLP_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_RECV_ACCESS_VIOLATION:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_R_RECV_LENGTH_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_SOFT_HI_WATERMARK:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_EP_SRQ_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_SRQ_LOW_WATERMARK:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_SRQ_CATASTROPHIC:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_SEVD_FULL_ERROR:
        case SKV_SERVER_EVENT_IT_ASYNC_AFF_SEVD_OP_ERROR:
        case SKV_SERVER_EVENT_TYPE_IT_AFF_ERROR:
        {
          BegLogLine( 1 )
            << "skv_server_t::ProcessEvent::ERROR:: EXITING!!! "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << EndLogLine;

          exit( -1 );

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_BROKEN:
        {
          // BROKEN CONNECTION: WARNING BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES
          // BROKEN CONNECTION: WARNING BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES
          // BROKEN CONNECTION: WARNING BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES

          // BROKEN CONNECTION: WARNING BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES
          // BROKEN CONNECTION: WARNING BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES
          // BROKEN CONNECTION: WARNING BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES

          BegLogLine( 1 )
            << "skv_server_t::ProcessEvent(): BROKEN CONNECTION: WARNING!!! BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES"
            << " EP: " << (void *) aEvent->mEventMetadata.mEP.mIT_EP
            << EndLogLine;

          StrongAssertLogLine( 0 )
            << "skv_server_t::ProcessEvent(): BROKEN CONNECTION: WARNING!!! BROKEN CONNECTIONS DO NOT FLUSH WR QUEUES"
            << " EP: " << (void *) aEvent->mEventMetadata.mEP.mIT_EP
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_ACCEPT_ARRIVAL:
        {
          BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " EventType: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " handler not yet implemented"
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_DISCONNECT:
        {
          BegLogLine( SKV_PROCESS_IT_EVENT_LOG | SKV_SERVER_CLIENT_CONN_EST_LOG )
            << "skv_server_t::ProcessEvent(): DISCONNECTED CONNECTION: "
            << " EP: " << (void *) aEvent->mEventMetadata.mEP.mIT_EP
#ifdef SKV_SERVER_LOOP_STATISTICS
            << SERVER_STATS_ECHO( ServerStatistics )
#endif
            << EndLogLine;
        }
        case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_ESTABLISHED:
        {
          skv_server_ep_state_t* StateForEP = GetEPStateForEPHdl( aEvent->mEventMetadata.mEP.mIT_EP );

          BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
            << "skv_server_t::ProcessEvent:: SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_ESTABLISHED action block: "
            << " EP: " << (void *) aEvent->mEventMetadata.mEP.mIT_EP
            << " StateForEP: " << (void *) StateForEP
            << EndLogLine;

          StrongAssertLogLine( StateForEP != NULL )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " No state found for EP: " << (void *) aEvent->mEventMetadata.mEP.mIT_EP
            << EndLogLine;

          int ConnCommandOrdinal = StateForEP->GetConnCommandOrdinal();

          status = skv_server_establish_client_connection_sm::Execute( StateForEP,
                                                                       ConnCommandOrdinal,
                                                                       aEvent,
                                                                       &mSeqNo );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          if( aEvent->mEventType == SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_DISCONNECT )
          {
            // This should normally happen in the state machine.
            // THINK about how to get the data structures there.
            mNetworkEventManager.FinalizeEPState( mEPStateMap,
                                                  aEvent->mEventMetadata.mEP.mIT_EP,
                                                  StateForEP );
            // exit(0);
          }

#ifdef SKV_SERVER_LOOP_STATISTICS
          SERVER_STATS_RESET( ServerStatistics );
#endif

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_CMM_CONN_PEER_REJECT:
        {
          BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " EventType: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " handler not yet implemented"
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_CMR_CONN_REQUEST:
        {
          // Figure out the command
          skv_server_ep_state_t* StateForEP;
          status = mNetworkEventManager.InitNewStateForEP( mEPStateMap,
                                                           &StateForEP );

          int ConnCommandOrdinal = StateForEP->GetConnCommandOrdinal();

          skv_server_ccb_t* ConnCommand = StateForEP->GetCommandForOrdinal( ConnCommandOrdinal );

          StrongAssertLogLine( ConnCommand->GetState() == SKV_SERVER_COMMAND_STATE_INIT )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " ConnCommand->GetState(): "
            << ConnCommand->GetState()
            << EndLogLine;

          ConnCommand->SetType( SKV_COMMAND_CONN_EST );

          BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
            << "skv_server_t::ProcessEvent:: SKV_SERVER_EVENT_TYPE_IT_CMR_CONN_REQUEST action block: "
            << " StateForEP: " << (void *) StateForEP
            << " ConnCommand: " << (void *) ConnCommand
            << EndLogLine;

          status = skv_server_establish_client_connection_sm::Execute( StateForEP,
                                                                       ConnCommandOrdinal,
                                                                       aEvent,
                                                                       &mSeqNo );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent(): ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_READ_CMPL:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          skv_server_ccb_t* Command = EPStatePtr->GetCommandForOrdinal( CmdOrd );

          BegLogLine( SKV_PROCESS_IT_EVENT_READ_LOG )
            << "skv_server_t::ProcessEvent(): SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_READ_CMPL received "
            << " ord: " << CmdOrd
            << EndLogLine;

          switch( Command->GetType() )
          {
            case SKV_COMMAND_ACTIVE_BCAST:
            {
              status = skv_server_active_bcast_command_sm::Execute( &mLocalKV,
                                                                    EPStatePtr,
                                                                    CmdOrd,
                                                                    aEvent,
                                                                    &mSeqNo,
                                                                    mNetworkEventManager.GetPZ() );
              break;
            }
            case SKV_COMMAND_INSERT:
            {
              status = skv_server_insert_command_sm::Execute( & mInternalEventManager,
                                                              &mLocalKV,
                                                              EPStatePtr,
                                                              CmdOrd,
                                                              aEvent,
                                                              &mSeqNo,
                                                              mMyRank );
              break;
            }
            case SKV_COMMAND_BULK_INSERT:
            {
              BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
                << "skv_server_t::ProcessEvent(): SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_READ_CMPL : SKV_COMMAND_BULK_INSERT"
                << " EPStatePtr: " << (void *) EPStatePtr
                << " EP: " << (void *) EPStatePtr->mEPHdl
                << " ClientOrd: " << EPStatePtr->mClientGroupOrdinal
                << EndLogLine;

              status = skv_server_bulk_insert_command_sm::Execute( &mInternalEventManager,
                                                                   &mLocalKV,
                                                                   EPStatePtr,
                                                                   CmdOrd,
                                                                   aEvent,
                                                                   &mSeqNo,
                                                                   mMyRank );
              break;
            }
            default:
            {
              StrongAssertLogLine( 0 )
                << "skv_server_t::ProcessEvent(): ERROR:: Type not recognized: "
                << Command->GetType()
                << EndLogLine;
            }
          }

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_WRITE_CMPL:
        {
          skv_server_rdma_write_cmpl_func_t Func = aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetFunc();

          BegLogLine( SKV_PROCESS_IT_EVENT_WRITE_LOG )
            << "skv_server_t::ProcessEvent(): SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_WRITE_CMPL received "
            << " func: " << (void*)Func
            << " ctxt: " << (void*)(aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetContext())
            << " last: " << aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetIsLast()
            << " ord: " << aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetCmdOrd()
            << EndLogLine;

          if( aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetIsLast() )
          {
            if( Func == EPSTATE_RetrieveWriteComplete )
            {
              skv_server_ep_state_t* EPStatePtr =
                  (skv_server_ep_state_t*) (aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetContext());
              int CmdOrd = aEvent->mEventMetadata.mRdmaWriteCmplCookie.GetCmdOrd();

              BegLogLine( SKV_PROCESS_IT_EVENT_WRITE_LOG )
                << "skv_server_t::ProcessEvent(): calling execute in for receive "
                << EndLogLine;

              status = skv_server_retrieve_command_sm::Execute( &mLocalKV,
                                                                EPStatePtr,
                                                                CmdOrd,
                                                                aEvent,
                                                                &mSeqNo,
                                                                mNetworkEventManager.GetPZ(),
                                                                mMyRank );

              AssertLogLine( status == SKV_SUCCESS )
                << "skv_server_t::ProcessEvent::ERROR:: "
                << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
                << " status: " << skv_status_to_string( status )
                << EndLogLine;

            }
            else if( Func != NULL )
            {
              void* Arg = &(aEvent->mEventMetadata.mRdmaWriteCmplCookie);

              BegLogLine( SKV_PROCESS_IT_EVENT_WRITE_LOG )
                << "skv_server_t::ProcessEvent(): SKV_SERVER_EVENT_TYPE_IT_DTO_RDMA_WRITE_CMPL: "
                << " FuncPtr: " << (void *) Func
                << " Arg: " << (void *) Arg
                << EndLogLine;

              void* Return = Func( Arg );

              if( Func == EPSTATE_CountSendCompletionsCallback )
              {
                SetState( SKV_SERVER_STATE_RUN );
              }
            }
          }

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_SQ_CMPL:
        {
          // BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG | SKV_PROCESS_IT_EVENT_LOG )
          BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
            << "skv_server_t::ProcessEvent(): Entering SKV_SERVER_EVENT_TYPE_IT_DTO_SQ_CMPL "
            << " action block."
            << EndLogLine;

          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;

          // count send events to keep track of "out-of-order" events in the event completion queue
          // EPStatePtr->CountSendCompletions();

          // THIS SHOULD BE THE LOCATION TO CONTINUE PROCESSING OF Pending EVENTS ??
          // if( EPStatePtr->GetPendingEventsCount() > 0 )
          //   {
          //     BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
          //       << "A SEND HAS COMPLETED AND THERE ARE PENDING EVENTS."
          //       << EndLogLine;

          //     status = SKV_ERRNO_EVENTS_PENDING;
          //   }

          // skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;

          // status = EPStatePtr->ProcessPendingReplies( & mSeqNo );

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_INSERT_CMD:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          // status = EPStatePtr->EPResourceCheckAndQueue( aEvent );
          // if( status == SKV_SUCCESS )
          //   {
          status = skv_server_insert_command_sm::Execute( &mInternalEventManager,
                                                          &mLocalKV,
                                                          EPStatePtr,
                                                          CmdOrd,
                                                          aEvent,
                                                          &mSeqNo,
                                                          mMyRank );
          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          // }
          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_BULK_INSERT_CMD:
        {
              skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          BegLogLine( SKV_PROCESS_IT_EVENT_LOG )
            << "skv_server_t::ProcessEvent(): STARTING BULK_INSERT_CMD"
            << " EPStatePtr: " << (void *) EPStatePtr
            << " EP: " << (void *) EPStatePtr->mEPHdl
            << " ClientOrd: " << EPStatePtr->mClientGroupOrdinal
            << EndLogLine;

          status = skv_server_bulk_insert_command_sm::Execute( &mInternalEventManager,
                                                               &mLocalKV,
                                                               EPStatePtr,
                                                               CmdOrd,
                                                               aEvent,
                                                               &mSeqNo,
                                                               mMyRank );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_CMD:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_retrieve_command_sm::Execute( &mLocalKV,
                                                            EPStatePtr,
                                                            CmdOrd,
                                                            aEvent,
                                                            &mSeqNo,
                                                            mNetworkEventManager.GetPZ(),
                                                            mMyRank );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_REMOVE_CMD:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int                     CmdOrd     = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_remove_command_sm::Execute( &mInternalEventManager,
                                                          &mLocalKV,
                                                          EPStatePtr,
                                                          CmdOrd,
                                                          aEvent,
                                                          &mSeqNo,
                                                          mMyRank );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_ACTIVE_BCAST_CMD:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_active_bcast_command_sm::Execute( &mLocalKV,
                                                                EPStatePtr,
                                                                CmdOrd,
                                                                aEvent,
                                                                &mSeqNo,
                                                                mNetworkEventManager.GetPZ() );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_N_KEYS_CMD:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_retrieve_n_keys_command_sm::Execute( &mLocalKV,
                                                                   EPStatePtr,
                                                                   CmdOrd,
                                                                   aEvent,
                                                                   &mSeqNo,
                                                                   mNetworkEventManager.GetPZ() );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_DIST_CMD:
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_retrieve_dist_command_sm::Execute( &mLocalKV,
                                                                 EPStatePtr,
                                                                 CmdOrd,
                                                                 aEvent,
                                                                 &mSeqNo );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_OPEN_CMD:
        {
          // Read the State and CmdOrd from event
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_open_command_sm::Execute( &mLocalKV,
                                                        EPStatePtr,
                                                        CmdOrd,
                                                        aEvent,
                                                        &mSeqNo );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_PDSCNTL_CMD:
        {
          // Read the State and CmdOrd from event
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          status = skv_server_pdscntl_command_sm::Execute( &mLocalKV,
                                                           EPStatePtr,
                                                           CmdOrd,
                                                           aEvent,
                                                           &mSeqNo );

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: "  << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_IT_DTO_UPDATE_CMD:
        {
          break;
        }
        case SKV_SERVER_EVENT_TYPE_LOCAL_KV_ERROR:      // error during storage command (maybe not needed)
        case SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL:       // clean completion of command in storage
        {
          skv_server_ep_state_t* EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;
          int CmdOrd = aEvent->mEventMetadata.mCommandFinder.mCommandOrd;

          skv_server_ccb_t* Command = EPStatePtr->GetCommandForOrdinal( CmdOrd );

          BegLogLine( SKV_PROCESS_LOCAL_KV_EVENT_LOG )
            << "skv_server_t::ProcessEvent(): SKV_SERVER_EVENT_TYPE_LOCAL_KV_CMPL received "
            << " ord: " << CmdOrd
            << " type: " << Command->GetType()
            << EndLogLine;

          switch( Command->GetType() )
          {
            // walk through all potential commands that might come back from local kv...
            case SKV_COMMAND_INSERT:
              status = skv_server_insert_command_sm::Execute( &mInternalEventManager,
                                                              &mLocalKV,
                                                              EPStatePtr,
                                                              CmdOrd,
                                                              aEvent,
                                                              &mSeqNo,
                                                              mMyRank );
              break;
            case SKV_COMMAND_BULK_INSERT:
              status = skv_server_bulk_insert_command_sm::Execute( &mInternalEventManager,
                                                                   &mLocalKV,
                                                                   EPStatePtr,
                                                                   CmdOrd,
                                                                   aEvent,
                                                                   &mSeqNo,
                                                                   mMyRank );
              break;
            case SKV_COMMAND_RETRIEVE:
              status = skv_server_retrieve_command_sm::Execute( &mLocalKV,
                                                                EPStatePtr,
                                                                CmdOrd,
                                                                aEvent,
                                                                &mSeqNo,
                                                                mNetworkEventManager.GetPZ(),
                                                                mMyRank );
              break;
            case SKV_COMMAND_RETRIEVE_N_KEYS:
              status = skv_server_retrieve_n_keys_command_sm::Execute( &mLocalKV,
                                                                       EPStatePtr,
                                                                       CmdOrd,
                                                                       aEvent,
                                                                       &mSeqNo,
                                                                       mNetworkEventManager.GetPZ() );
              break;
            case SKV_COMMAND_RETRIEVE_DIST:
              status = skv_server_retrieve_dist_command_sm::Execute( &mLocalKV,
                                                                     EPStatePtr,
                                                                     CmdOrd,
                                                                     aEvent,
                                                                     &mSeqNo );
              break;
            case SKV_COMMAND_REMOVE:
              status = skv_server_remove_command_sm::Execute( &mInternalEventManager,
                                                              &mLocalKV,
                                                              EPStatePtr,
                                                              CmdOrd,
                                                              aEvent,
                                                              &mSeqNo,
                                                              mMyRank );
              break;
            case SKV_COMMAND_OPEN:
              status = skv_server_open_command_sm::Execute( &mLocalKV,
                                                            EPStatePtr,
                                                            CmdOrd,
                                                            aEvent,
                                                            &mSeqNo );
              break;
            case SKV_COMMAND_ACTIVE_BCAST:
              status = skv_server_active_bcast_command_sm::Execute( &mLocalKV,
                                                                    EPStatePtr,
                                                                    CmdOrd,
                                                                    aEvent,
                                                                    &mSeqNo,
                                                                    mNetworkEventManager.GetPZ() );
              break;
            case SKV_COMMAND_CLOSE:
            case SKV_COMMAND_PDSCNTL:
              status = skv_server_pdscntl_command_sm::Execute( &mLocalKV,
                                                               EPStatePtr,
                                                               CmdOrd,
                                                               aEvent,
                                                               &mSeqNo );
              break;
            case SKV_COMMAND_UPDATE:
            case SKV_COMMAND_CURSOR_PREFETCH:
            case SKV_COMMAND_CONN_EST:
            case SKV_COMMAND_NONE:
            default:
              AssertLogLine( 1 )
                << "skv_server_t::ProcessEvent(): Unexpected command type: " << Command->GetType()
                << EndLogLine;
          }

          AssertLogLine( status == SKV_SUCCESS )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: "  << skv_server_event_type_to_string( aEvent->mEventType )
            << " status: " << skv_status_to_string( status )
            << EndLogLine;

          break;
        }
        case SKV_SERVER_EVENT_TYPE_LOCAL_KV_ASYNC_ERROR:
        {
          // async error from storage
          break;
        }


        default:
        {
          StrongAssertLogLine( 0 )
            << "skv_server_t::ProcessEvent::ERROR:: "
            << " Event: " << skv_server_event_type_to_string( aEvent->mEventType )
            << " is not recognized."
            << EndLogLine;

          break;
        }
      }

      break;
    }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_server_t::ProcessEvent::ERROR:: "
        << " State: " << aState
        << " is not recognized."
        << EndLogLine;
    }
  }

  if( status == SKV_ERRNO_COMMAND_LIMIT_REACHED )
  {
    BegLogLine( 1 )
      << "IF WE GET HERE, WE ARE IN TROUBLE.... COMMAND_LIMIT_REACHED in ProcessEvent!! "
      << EndLogLine;

      SetState( SKV_SERVER_STATE_PENDING_EVENTS );
    status = SKV_SUCCESS;
  }

  return status;
}

skv_status_t
skv_server_t::
ProcessPendingEvents( skv_server_event_t * aEvent )
{
  // input event ONLY used to get the EPStatePtr to process pending commands of that EP
  skv_server_ep_state_t*  EPStatePtr = aEvent->mEventMetadata.mCommandFinder.mEPStatePtr;

  return ProcessPendingEvents( EPStatePtr );
}

skv_status_t
skv_server_t::
ProcessPendingEvents( skv_server_ep_state_t * aEPStatePtr )
{
  BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
    << "skv_server_t::ProcessPendingEvents():: Entering for EPState: " << ( void* )aEPStatePtr
    << EndLogLine;

  skv_server_event_t *pEvent;
  skv_status_t status = SKV_SUCCESS;

  while( (aEPStatePtr->GetPendingEventsCount() > 0) )
  {
    pEvent = aEPStatePtr->GetNextPendingEvent();

    skv_server_state_t State = GetState();

    BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_t::ProcessPendingEvents():: Starting to process pending event: " << ( void* )pEvent
      << EndLogLine;

    status = ProcessEvent( State, pEvent );

    BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_t::ProcessPendingEvents():: ProcessEvent return: " << skv_status_to_string( status )
      << EndLogLine;

    aEPStatePtr->FreeFirstPendingEvent();
  }

  // transition to general RUN status if more events pending
  if( aEPStatePtr->GetPendingEventsCount() == 0 )
  {
    SetState( SKV_SERVER_STATE_RUN );
  }
  return SKV_SUCCESS;
}

skv_status_t
skv_server_t::
ProgressAnyEP()
{
  skv_status_t status = SKV_SUCCESS;

  EPStateMap_T::iterator iter =  mEPStateMap->begin();

  while( (iter != mEPStateMap->end()) &&
         (status == SKV_SUCCESS) )
  {
    skv_server_ep_state_t *EPState = iter->second;

    // if( !EPState->SendPostOverflow() )
    //   {
    //     BegLogLine( 1 )
    //       << "CHECKING FOR PROGRESS ON EP. THERE WERE NO EVENTS FOR A WHILE"
    //       << EndLogLine;

    //     status = ProcessPendingEvents( EPState );
    //   }

    iter++;
  }
  return status;
}

/***
 * skv_server_t::Run::
 * Desc: Starts the state machine on the server
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_server_t::
Run()
{
  BegLogLine( SKV_SERVER_RUN_LOG )
    << "skv_server_t::Run():: Entering... "
    << EndLogLine;

  SetState( SKV_SERVER_STATE_RUN );

  int IterCount = 0;
  int PollLoops = 0;

  skv_server_event_t *Events = new skv_server_event_t[ SKV_SERVER_EVENTS_MAX_COUNT ];
  // skv_server_event_t PendingEvents[ SKV_SERVER_EVENTS_MAX_COUNT ];
  // int EventCount = 0;
  // int CommandCount = 0;
  // int LastPending = 0;

  // memset( PendingEvents, 0, SKV_SERVER_EVENTS_MAX_COUNT * sizeof( skv_server_event_t ) );

  while( (GetState() != SKV_SERVER_STATE_ERROR) &&
         (GetState() != SKV_SERVER_STATE_EXIT) )
  {
    // gSKVServerRunLoopStart.HitOE( SKV_SERVER_TRACE,
    //                                "SKVServerMainLoop",
    //                                0,
    //                                gSKVServerRunLoopStart );

// #ifdef PKTRACE_ON
// #define SKV_SERVER_TRACE_DUMP_MODULO ( 10000 )

//       if( IterCount == SKV_SERVER_TRACE_DUMP_MODULO )
//         {
//           pkTraceServer::FlushBuffer();
//           IterCount = 0;
//         }
//       IterCount++;
// #endif

#ifdef SKV_SERVER_LOOP_STATISTICS
      ServerStatistics.overallRunLoops++;
#endif

    skv_status_t status;
    int EventCount = 0;
    int CommandCount = 0;

    skv_server_state_t State = GetState();

    // might look strange, but this allows to just exchange GetEvent and GetCommand without changing the parameters
    // Get event-based activities
    status = GetEvent( &(Events[CommandCount]), &EventCount, SKV_SERVER_EVENTS_MAX_COUNT );

    if( EventCount > 0 )
    {
      for( int i = 0; i < EventCount; i++ )
      {
        BegLogLine( 0 )
          << "skv_server_t::Run() :: now processing event"
          << " #" << i
          << " of " << EventCount
          << EndLogLine;

        gSKVServerEventStart.HitOE( SKV_SERVER_TRACE,
            "SKVServerEventProcessing",
            0,
            gSKVServerEventStart );

        status = ProcessEvent( State, &Events[i] );

        gSKVServerEventFinis.HitOE( SKV_SERVER_TRACE,
            "SKVServerEventProcessing",
            0,
            gSKVServerEventFinis );

        BegLogLine( 0 )
          << "ProcessEvent returned: " << skv_status_to_string( status )
          << EndLogLine;

        // check if the processing was deferred and if so, copy it to the pending events queue
        // if( GetState() == SKV_SERVER_STATE_PENDING_EVENTS )
        //   {
        //     BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
        //       << "skv_server_t::Run(): found pending events, trying to process..."
        //       << " event@: " << & Events[ i ]
        //       << EndLogLine;

        //     ProcessPendingEvents( & Events[ i ] ); // the current event parameter is required only to pick the right EPState
        //   }
      }
    }

    // try to check if we are able to progress any EPs with pending events
    // else
    //   {
    //     if( GetState() == SKV_SERVER_STATE_PENDING_EVENTS )
    //       PollLoops++;        // count eventless loops in PENDING state
    //     else
    //       PollLoops = 0;      // if not in PENDING state, reset counter

    //     if( PollLoops > 1000000 )
    //       {
    //         ProgressAnyEP( );
    //         PollLoops = 0;
    //       }
    //   }

    // gSKVServerRunLoopFinis.HitOE( SKV_SERVER_TRACE,
    //                                "SKVServerMainLoop",
    //                                0,
    //                                gSKVServerRunLoopFinis );
  }

  delete [] Events;
  return SKV_SUCCESS;
}

/***
 * skv_server_t::Init::
 * Desc: Initializes the state of the skv_server_t
 * Gets the server ready to accept/service connections
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
int
skv_server_t::
Init( int   aRank,
      int   aNodeCount,
      int   aFlags,
      char* aCheckpointPath )
{
  SetState( SKV_SERVER_STATE_INIT );

  StrongAssertLogLine( sizeof( it_dto_cookie_t ) >=  sizeof( skv_server_cookie_t ))
    << "skv_server_t::Init::ERROR:: "
    << " sizeof( it_dto_cookie_t ): " << sizeof( it_dto_cookie_t )
    << " sizeof( skv_server_cookie_t ): " << sizeof( skv_server_cookie_t )
    << EndLogLine;

  mMyRank           = aRank;
  int Rank          = aRank;
  int PartitionSize = aNodeCount;

  mSKVConfiguration = skv_configuration_t::GetSKVConfiguration();

  /***********************************************************
   *  INITIALIZE EVENT MANAGERS,  SOURCES (AND SINKS)        
   ***********************************************************/  

  // Initialize the internal (local) event handling
  mInternalEventManager.Init();
  mEventSources[ SKV_SERVER_INTERNAL_EVENT_SRC_INDEX ] = new skv_server_internal_event_source_t();
  ((skv_server_internal_event_source_t*)(mEventSources[ SKV_SERVER_INTERNAL_EVENT_SRC_INDEX ]))->Init( &mInternalEventManager,
                                                                                                       SKV_SERVER_INTERNAL_SRC_PRIORITY );

  // Initialize IT Event handling
  mNetworkEventManager.Init( PartitionSize, mMyRank );
  mEventSources[ SKV_SERVER_NETWORK_EVENT_SRC_INDEX ] = new skv_server_IT_event_source_t();
  ((skv_server_IT_event_source_t*)(mEventSources[ SKV_SERVER_NETWORK_EVENT_SRC_INDEX ]))->Init( &mNetworkEventManager,
                                                                                                SKV_SERVER_NETWORK_SRC_PRIORITY );

  // Initialize command fetching  (the "manager" for commands is the EPStateMap because the EPs are where the commands are fetched
  mEPStateMap = new EPStateMap_T;
  StrongAssertLogLine( mEPStateMap != NULL )
    << "skv_server_t::Init():: ERROR:: "
    << "mEPStateMap != NULL"
    << EndLogLine;

  mEventSources[ SKV_SERVER_COMMAND_EVENT_SRC_INDEX ] = new skv_server_command_event_source_t();
  ((skv_server_command_event_source_t*)(mEventSources[ SKV_SERVER_COMMAND_EVENT_SRC_INDEX ]))->Init( mEPStateMap,
                                                                                                     SKV_SERVER_COMMAND_SRC_PRIORITY );
  

  skv_status_t status = mLocalKV.Init( Rank,
                                       PartitionSize,
                                       & mInternalEventManager,
                                       mNetworkEventManager.GetPZ(),
                                       aCheckpointPath );
  StrongAssertLogLine( status == SKV_SUCCESS )
    << "pimd_server_t::Init():: ERROR:: mLocalKV.Init() failed. "
    << " status: " << skv_status_to_string( status )
    << " Rank: " << Rank
    << " PartitionSize: " << PartitionSize
    << EndLogLine;

  mEventSources[ SKV_SERVER_LOCAL_KV_EVENT_SRC_INDEX ] = new skv_server_local_kv_event_source_t();
  ((skv_server_local_kv_event_source_t*)(mEventSources[ SKV_SERVER_LOCAL_KV_EVENT_SRC_INDEX ]))->Init( &mLocalKV,
                                                                                                       SKV_SERVER_LOCAL_KV_SRC_PRIORITY );

  // common denominator to calc number of event slots for event fetching, also used to assure priority when counter is wrapped
  mPriorityCDN = 1;
  for( int evt_src=0; evt_src<SKV_SERVER_EVENT_SOURCES; evt_src++ )
    mPriorityCDN *= mEventSources[ evt_src ]->GetPriority();

  int prio_sum = 0;
  for( int evt_src=0; evt_src< SKV_SERVER_EVENT_SOURCES; evt_src++ )
    prio_sum +=  mPriorityCDN / mEventSources[ evt_src ]->GetPriority();
    
  for( int evt_src = 0; evt_src < SKV_SERVER_EVENT_SOURCES; evt_src++ )
  {
    double slot_fraction = (double) (mPriorityCDN / mEventSources[evt_src]->GetPriority()) / (double) (prio_sum);
    mMaxEventCounts[evt_src] = (int) (slot_fraction * SKV_SERVER_EVENTS_MAX_COUNT);

    BegLogLine( 1 )
      << "server_init(): slots for evt_src: " << evt_src
      << " prio: " << mEventSources[ evt_src ]->GetPriority()
      << " slots: " << mMaxEventCounts[ evt_src ]
      << " all: " << SKV_SERVER_EVENTS_MAX_COUNT
      << EndLogLine;
  }
  /***********************************************************/

  mSeqNo = 0;

  /************************************************************
   * Initialize startup
   ***********************************************************/
  // create the server machine file
  // the last SKV_SERVER_PORT_LENGTH bytes of the servername are the port number !!
  char *ServerList = (char*) malloc ( SKV_MAX_SERVER_ADDR_NAME_LENGTH * aNodeCount );
  char ServerName[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  bzero( ServerName, SKV_MAX_SERVER_ADDR_NAME_LENGTH );

  struct sockaddr_in my_addr;

  struct ifaddrs *iflist, *ifent;

  AssertLogLine( getifaddrs(&iflist) == 0 )
    << "Failed to obtain list of interfaces"
    << EndLogLine;

  /* todo all potential addresses into server machine file
   *  The IT_API allows binding and connection via multiple interfaces
   *   and we loose the automatic setup of this by placing only one address into the machinefile
   */
  ifent = iflist;
  while( ifent )
  {
    if( strncmp( ifent->ifa_name, mSKVConfiguration->GetCommIF(), strlen( ifent->ifa_name ) ) == 0 )
    {
      if( ifent->ifa_addr->sa_family == AF_INET )
      {
        struct sockaddr_in *tmp = (struct sockaddr_in*) (ifent->ifa_addr);
        my_addr.sin_family = ifent->ifa_addr->sa_family;
        my_addr.sin_addr.s_addr = tmp->sin_addr.s_addr;
        snprintf( ServerName, SKV_MAX_SERVER_ADDR_NAME_LENGTH, "%d.%d.%d.%d",
                  (int) ((char*) &(tmp->sin_addr.s_addr))[0],
                  (int) ((char*) &(tmp->sin_addr.s_addr))[1],
                  (int) ((char*) &(tmp->sin_addr.s_addr))[2],
                  (int) ((char*) &(tmp->sin_addr.s_addr))[3] );
        BegLogLine( 1 )
          << "skv_server_t: got addr: " << (void*)my_addr.sin_addr.s_addr << "; fam: " << tmp->sin_family
          << " ServerName:" << ServerName
          << EndLogLine;
        break;
      }
    }

    ifent = ifent->ifa_next;
  }
  freeifaddrs( iflist );

  StrongAssertLogLine( strnlen( ServerName, SKV_MAX_SERVER_ADDR_NAME_LENGTH ) < SKV_MAX_SERVER_ADDR_NAME_LENGTH-SKV_SERVER_PORT_LENGTH )
    << "skv_server_t::Init():: ERROR: Servername " << ServerName
    << " too long"
    << EndLogLine;
    
  snprintf( ServerName+(SKV_MAX_SERVER_ADDR_NAME_LENGTH-SKV_SERVER_PORT_LENGTH),
            SKV_SERVER_PORT_LENGTH,
            "%d", mSKVConfiguration->GetSKVServerPort() );

  MPI_Allgather( ServerName, SKV_MAX_SERVER_ADDR_NAME_LENGTH, MPI_BYTE,
                 ServerList, SKV_MAX_SERVER_ADDR_NAME_LENGTH, MPI_BYTE,
                 MPI_COMM_WORLD );

  char ServerAddrInfoFilename[ 256 ];
  bzero( ServerAddrInfoFilename, 256 );

  sprintf( ServerAddrInfoFilename,
           "%s",
           mSKVConfiguration->GetServerLocalInfoFile()
           );

  BegLogLine( 1 )
    << "skv_server_t::Init():: "
    << " ServerAddrInfoFilename: " << ServerAddrInfoFilename
    << EndLogLine;

  // rank 0 creating the file (if FS is shared between nodes, all other ranks should see the file afterwards)
  int fd;
  int active_rank;
  if( aRank == 0 )
  {
    fd = open( ServerAddrInfoFilename,
               O_CREAT | O_RDWR | O_TRUNC, 
               S_IRUSR | S_IWUSR | S_IROTH );

    StrongAssertLogLine( fd > 0 )
      << "skv_server_t::Init():: ERROR:: fd: " << fd
      << " errno: " << errno
      << EndLogLine;
    close( fd );
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // check if everyone can read the file
  fd = open( ServerAddrInfoFilename,
  O_RDONLY,
             S_IRUSR | S_IWUSR | S_IROTH );
  if( fd > 0 )
  {
    active_rank = 0;
    close( fd );
  }
  else
    active_rank = aRank;

  // all who cannot read and rank 0 create a new file
  if( aRank == active_rank )
  {

    BegLogLine( 1 )
      << "Creating local server info file on rank: " << aRank
      << EndLogLine;

    fd = open( ServerAddrInfoFilename,
               O_CREAT | O_RDWR | O_TRUNC, 
               S_IRUSR | S_IWUSR | S_IROTH );

    StrongAssertLogLine( fd > 0 )
      << "skv_server_t::Init():: ERROR:: fd: " << fd
      << " errno: " << errno
      << EndLogLine;

    for( int i = 0; i < aNodeCount; i++ )
    {
      const char* ServerI = (const char*)&ServerList[ i * SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
      const char* PortI   = (const char*)&ServerI[SKV_MAX_SERVER_ADDR_NAME_LENGTH-SKV_SERVER_PORT_LENGTH];

      char buff[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
      bzero( buff, SKV_MAX_SERVER_ADDR_NAME_LENGTH );

#ifdef SKV_ROQ_LOOPBACK_WORKAROUND
      // replace local hostname by loopback address
      if(  ( strlen( ServerName ) == strlen( ServerI ) ) &&
           ( strncmp( ServerName, ServerI, strlen( ServerName ) ) == 0 ) )
      {
        snprintf( buff, SKV_MAX_SERVER_ADDR_NAME_LENGTH, "127.0.0.1 %s", PortI);
      }
      else
#endif
      {
        snprintf( buff, SKV_MAX_SERVER_ADDR_NAME_LENGTH, "%s %s", ServerI, PortI);
      }
      write( fd, buff, strlen( buff ) );
      write( fd, "\n", strlen("\n") );
    }
    close( fd );      
  }

  MPI_Barrier( MPI_COMM_WORLD );

  /***********************************************************/

  return SKV_SUCCESS;
}

/***
 * skv_server_t::Finalize::
 * Desc: Stops the server and deallocates the state
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
int
skv_server_t::
Finalize()
{
  mLocalKV.Exit();
  return SKV_SUCCESS;
}
