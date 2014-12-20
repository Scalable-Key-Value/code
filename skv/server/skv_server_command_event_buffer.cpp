/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_server_command_event_buffer.cpp
 *
 *  Created on: Dec 22, 2014
 *      Author: lschneid
 */

#include <FxLogger.hpp>
#include <pthread.h>
#include <skv/common/skv_errno.hpp>
#include <skv/common/skv_types.hpp>
#include <skv/common/skv_client_server_headers.hpp>
#include <skv/server/skv_server_types.hpp>

#include <skv/server/skv_server_command_event_buffer.hpp>

skv_status_t
skv_server_command_event_buffer_list_t::PrepareEvent( skv_server_event_t *currentEvent, skv_server_ep_state_t *aEPState )
{
  AssertLogLine( aEPState != NULL )
    << "skv_server_t::GetServerCmdFromEP:: ERROR:: EPState != NULL"
    << EndLogLine;

  // acquire CCB, cmdOrd, and address of incomming request data
  int CmdOrd = -1;
  skv_client_to_server_cmd_hdr_t* InBoundHdr;
  skv_server_ccb_t* CCB = aEPState->AcquireCCB( &InBoundHdr, &CmdOrd );

  BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
    << "skv_server_command_event_buffer_list_t: EP CCBs drained"
    << EndLogLine;

  if( CCB == NULL )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

#if (SKV_CTRLMSG_DATA_LOG != 0)
  HexDump CtrlMsgData( (void*)InBoundHdr, SKV_CONTROL_MESSAGE_SIZE );
  BegLogLine( 1 )
    << "INBMSG:@"<< (void*)InBoundHdr
    << " Data:" << CtrlMsgData
    << EndLogLine;
#endif

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
  skv_server_event_type_t CmdEventType = (skv_server_event_type_t)ntohl(InBoundHdr->mEventType);
  //  skv_server_event_type_t CmdEventType = InBoundHdr->mEventType ;

  skv_command_type_t      CmdType      = (skv_command_type_t)ntohl(InBoundHdr->mCmdType);
  //  skv_command_type_t      CmdType      = InBoundHdr->mCmdType;

  CCB->SetType( CmdType );
  CCB->SetCommandClass( SKV_COMMAND_CLASS_IMMEDIATE );  // assume immediate for now

  BegLogLine( SKV_SERVER_GET_COMMAND_LOG )
    << "skv_server_t::GetServerCmdFromEP:: IT Recv Event"
    << " EPState: " << (void *) aEPState
    << " CmdOrd: " << CmdOrd
    << " ServerCCB: " << (void *) CCB
    << " CmdEventType: " << skv_server_event_type_to_string ( CmdEventType )
    << " CmdType: " << skv_command_type_to_string( CmdType )
    << (void *) CmdType
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

#ifdef SKV_UNIT_TEST
int skv_server_command_event_buffer_t::test_fill()
{
  int result = 0;
  int count = 0;
  while ( GetSpace() > 0 )
  {
    Advance();
    count++;
  }
  if( count != mEventEntries )
  {
    std::cout
      << " Advance doesnt fill number of available slots: " << count
      << "  vs. " << mEventEntries
      << std::endl;
    return 1;
  }

  std::cout
    << " skv_server_command_event_buffer_t::test_fill() exiting with rc = " << result
    << std::endl;
  return result;
}
#endif // SKV_UNIT_TEST
