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
#include <server/skv_server_command_event_source.hpp>


#define SKV_SERVER_COMMAND_MEM_POLL_LOOPS 10
#if (SKV_SERVER_COMMAND_POLLING_LOG != 0)
static int state_has_changed = 1;
#endif

skv_status_t
skv_server_command_event_source_t::
GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount )
{
  skv_status_t status = SKV_SUCCESS;

  static EPStateMap_T::iterator iter;

  static bool first_entry = true;

  if( first_entry )
  {
    // in case of many clients and many events, the "begin" has to be
    // turned into a more round-robin scheme, otherwise there's a risk
    //  of EPStates to starve
    iter = mEventManager->begin();

    first_entry = false;
  }

  int currentCommand = 0;
  int dont_retry = 0;

  for( int poll_loops = 0;
      (dont_retry == 0) && (poll_loops < SKV_SERVER_COMMAND_MEM_POLL_LOOPS);
      poll_loops++ )
  {

    if( iter == mEventManager->end() )
    {
      iter = mEventManager->begin();
    }

    while( (iter != mEventManager->end()) &&
           (currentCommand < aMaxEventCount) )
    {
      skv_server_ep_state_t *EPState = iter->second;

#if (SKV_SERVER_COMMAND_POLLING_LOG != 0)
      if( ( poll_loops == 0 ) && ( state_has_changed != 0 ) )
      {
        int CCS = EPState->GetCurrentCommandSlot();
        BegLogLine( 1 )
          << "skv_server_t::GetCommand():: checking new commands of EPState " << EPState
          << " cmdSlot: " << CCS
          << " addr: " << (void*)(EPState->GetPrimaryCommandBuff( CCS )->GetAddr())
          << EndLogLine;
        state_has_changed = 0;
      }
#endif

      int currentCommandsPerEP = 0;   // count command per EP to avoid starvation of EPs

      // Check for commands arrived in memory region
      while( (EPState->CheckForNewCommands() == true) &&
             (currentCommandsPerEP < (SKV_MAX_COMMANDS_PER_EP >> 1))
             &&
             (currentCommand < aMaxEventCount) )
      {

        dont_retry = 1;   // exit the MEM_POLL_LOOP if at least one new command is found

        // BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
        //   << "skv_server_t::GetCommand():: "
        //   << " commands found for EP, now initializing from slot: " << EPState->GetCurrentCommandSlot()
        //   << EndLogLine;

        // initializes the command ( returns COMMAND_LIMIT_REACHED if no more slots available)
        status = PrepareEvent( &aEvents[currentCommand], EPState );

        if( status == SKV_ERRNO_COMMAND_LIMIT_REACHED )
        {
          BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
            << "skv_server_t::GetCommand(): "
            << " Setting State: SKV_SERVER_STATE_PENDING_EVENTS"
            << EndLogLine;

          status = SKV_SUCCESS;

          break;   // continue attempt with next EP
        }

        StrongAssertLogLine( status == SKV_SUCCESS )
          << "skv_server_t::GetCommand(): "
          << "ERROR initializing fetched command at slot: " << EPState->GetCurrentCommandSlot()
          << EndLogLine;
#if (SKV_SERVER_COMMAND_POLLING_LOG != 0)
        if( ( status == SKV_SUCCESS ) || ( status == SKV_ERRNO_COMMAND_LIMIT_REACHED ) )
        {
          state_has_changed = 1;
        }
#endif

        // set index to next slot
        EPState->CommandSlotAdvance();

        currentCommand++;
        currentCommandsPerEP++;
      }

      iter++;
    }

  }

  GetCommandCleanExit:

  *aEventCount = currentCommand;
  return status;
}



skv_status_t
skv_server_command_event_source_t::
PrepareEvent( skv_server_event_t *currentEvent, skv_server_ep_state_t *aEPState )
{
  AssertLogLine( aEPState != NULL )
    << "skv_server_t::GetServerCmdFromEP:: ERROR:: EPState != NULL"
    << EndLogLine;

  // first: check if there will be sufficient resources to start processing of command
  if( !aEPState->ResourceCheck() )
  {
    BegLogLine( 0 )
      << "out of cmd resources. deferring..."
      << EndLogLine;

    // ServerStatistics.reqDeferCount++;

    // stop fetching commands from this EP because of limited resources
    aEPState->StallEP();

    // aEPState->AddToPendingEventsList( currentEvent );
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;
  }

  int CmdBuffOrdinal = aEPState->GetNextUnusedCommandBufferOrd();
  int CmdOrd         = aEPState->GetNextFreeCommandSlotOrdinal();

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event: "
    << " CmdOrd: " << CmdOrd
    << EndLogLine;

  // retrieve the actual CCB handle from Cmd-ordinal
  skv_server_ccb_t* CCB = aEPState->GetCommandForOrdinal( CmdOrd );

  AssertLogLine( CCB != NULL )
    << "skv_server_t::GetServerCmdFromEP:: ERROR:: CCB != NULL"
    << EndLogLine;

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event: "
    << " CCB: " << CCB
    << EndLogLine;

  int RecvBuffOrdinal = aEPState->GetCurrentCommandSlot();

  StrongAssertLogLine( RecvBuffOrdinal == CmdBuffOrdinal )
    << " Command Ordinal Mismatch"
    << " current: " << RecvBuffOrdinal
    << " unposted: " << CmdBuffOrdinal
    << EndLogLine;

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event: "
    << " EPState: " << (void *) aEPState
    << " RecvBuffOrdinal: " << RecvBuffOrdinal
    << EndLogLine;

  skv_lmr_triplet_t* RecvBuffTriplet = aEPState->GetCommandTriplet( RecvBuffOrdinal );

  AssertLogLine( RecvBuffTriplet != NULL )
    << "skv_server_t::GetServerCmdFromEP:: ERROR:: RecvBuffTriplet != NULL"
    << EndLogLine;

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event: "
    << " RecvBuffTriplet: " << (void *) RecvBuffTriplet
    << " content: " << *RecvBuffTriplet
    << EndLogLine;

  // This initializes the reponse data based on the incoming request
  CCB->SetSendBuffInfo( RecvBuffTriplet, RecvBuffOrdinal);

  // acquire address of incomming request data
  skv_client_to_server_cmd_hdr_t* InBoundHdr = (skv_client_to_server_cmd_hdr_t *) RecvBuffTriplet->GetAddr();

  AssertLogLine( InBoundHdr != NULL )
    << "skv_server_t::GetServerCmdFromEP:: ERROR:: Hdr != NULL"
    << EndLogLine;

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event: "
    << " InBoundHdr: " << (void *) InBoundHdr
    << EndLogLine;

#ifdef SKV_DEBUG_MSG_MARKER
  BegLogLine( 1 )
    << "Received Marker: " << InBoundHdr->mMarker
    << " sndBuf: " << (void*)InBoundHdr
    << " " << skv_command_type_to_string( InBoundHdr->mCmdType )
    << EndLogLine;
#endif

  // This event is generated on the client
  skv_server_event_type_t CmdEventType = InBoundHdr->mEventType;

  skv_command_type_t      CmdType      = InBoundHdr->mCmdType;

  CCB->SetType( CmdType );
  CCB->SetCommandClass( SKV_COMMAND_CLASS_IMMEDIATE );  // assume immediate for now

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event"
    << " EPState: " << (void *) aEPState
    << " CmdOrd: " << CmdOrd 
    << " ServerCCB: " << (void *) CCB
    << " CmdEventType: " << skv_server_event_type_to_string ( CmdEventType )
    << " CmdType: " << skv_command_type_to_string( CmdType )
    << EndLogLine;

  switch( CmdType )
  {
    case SKV_COMMAND_CURSOR_PREFETCH:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_CURSOR_PREFETCH_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_ACTIVE_BCAST:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_ACTIVE_BCAST_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_INSERT:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_INSERT_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );
      break;
    }
    case SKV_COMMAND_BULK_INSERT:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_BULK_INSERT_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_RETRIEVE:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_RETRIEVE_N_KEYS:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_N_KEYS_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_RETRIEVE_DIST:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_RETRIEVE_DIST_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_UPDATE:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_UPDATE_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_PDSCNTL:
    case SKV_COMMAND_CLOSE:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_PDSCNTL_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_OPEN:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_OPEN_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    case SKV_COMMAND_REMOVE:
    {
      currentEvent->Init( SKV_SERVER_EVENT_TYPE_IT_DTO_REMOVE_CMD,
                          aEPState,
                          CmdOrd,
                          CmdEventType );

      break;
    }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_server_t::GetServerCmdFromEP::ERROR:: "
        << " CmdType: " << CmdType
        << " is not recognized."
        << EndLogLine;

      break;
    }
  }

  return SKV_SUCCESS;
}

