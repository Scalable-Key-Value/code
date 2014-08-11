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

#ifndef __SKV_CLIENT_SERVER_HEADERS_HPP__
#define __SKV_CLIENT_SERVER_HEADERS_HPP__

#include <common/skv_types.hpp>
#include <server/skv_server_event_type.hpp>
#include <client/skv_client_types.hpp>

#define SKV_CTRL_MSG_FLAG_EMPTY             0x00
#define SKV_CTRL_MSG_FLAG_REQUEST_COMPLETE  0x01
#define SKV_CTRL_MSG_FLAG_RESPONSE_COMPLETE 0x02

/***************************************************
 * Header Structures
 ***************************************************/
struct skv_client_to_server_cmd_hdr_t
{
  uint64_t                  mCmdCtrlBlk;
  skv_command_type_t        mCmdType;
  short                     mCmdOrd;
  short                     mCmdLength;  //! actual length of useful data in ctrl-msg

  // Client generated event for the server
  skv_server_event_type_t   mEventType;

#ifdef SKV_DEBUG_MSG_MARKER
  uint64_t                  mMarker; // to track messages
#endif

  void
  Init( skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_command_type_t aCmdType )
  {
    mEventType  = aEventType;
    mCmdCtrlBlk = (uint64_t) (uintptr_t) aCmdCtrlBlk;

    mCmdType   = aCmdType;
    mCmdOrd    = -1;            // initialize without valid CmdOrd
    mCmdLength = 0;             // initialize without valid CmdLength

#ifdef SKV_DEBUG_MSG_MARKER
    mMarker = 0;
#endif

  }

#ifdef SKV_HEADER_CHECKSUM
  inline char
  CheckSum()
  {
    char cs = 0;
    char *buf = (char*)this;
    for( unsigned int i=0; i<sizeof(skv_client_to_server_cmd_hdr_t); i++ )
      cs += (i*buf[i]);
    if( cs == 0 )
    cs = -1;
    return cs;
  }
#else
  inline char
  CheckSum()
  {
    return SKV_CTRL_MSG_FLAG_REQUEST_COMPLETE;
  }
#endif

  void
  SetCmdOrd( int aCmdOrd )
  {
    mCmdOrd = aCmdOrd;
  }

  void
  SetCmdLength( int aCmdLength )
  {
    StrongAssertLogLine( (aCmdLength>0) && (aCmdLength<SKV_CONTROL_MESSAGE_SIZE) ) // -1 because of the trailling msg-complete flag
      << "ERROR Invalid command length"
      << " 0 < " << aCmdLength << " < " << SKV_CONTROL_MESSAGE_SIZE
      << EndLogLine;
    mCmdLength = aCmdLength;
  }

  int
  GetCmdLength()
  {
    return mCmdLength;
  }

  void
  Reset()
  {
    mCmdCtrlBlk = 0;
    mCmdType    = SKV_COMMAND_NONE;
    mCmdLength  = 0;
    mEventType  = SKV_SERVER_EVENT_TYPE_NONE;
    mCmdOrd     = -1;
  }

  // Need this due to declaration forwarding
  // skv_client_to_server_cmd_hdr_t and skv_server_to_client_cmd_hdr_t
  // Need to define operator=() on each other.
  template<class type>
  skv_client_to_server_cmd_hdr_t&
  operator=( const type& aHdr2 )
  {
    mCmdCtrlBlk = aHdr2.mCmdCtrlBlk;
    mCmdOrd     = aHdr2.mCmdOrd;
    mCmdType    = aHdr2.mCmdType;
    mCmdLength  = aHdr2.mCmdLength;

#ifdef SKV_DEBUG_MSG_MARKER
    mMarker     = aHdr2.mMarker;
#endif

    return (*this);
  }  
};

template<class streamclass>
static  
streamclass& 
operator<<(streamclass& os, const skv_client_to_server_cmd_hdr_t& aHdr)
{
  os << "skv_client_to_server_cmd_hdr_t [ "
     << (void *) aHdr.mCmdCtrlBlk << ' '
     << aHdr.mCmdOrd << ' '
     << (int) aHdr.mCmdType << ' '
     << (int) aHdr.mCmdLength << ' '
     << (int) aHdr.mEventType
     << " ]";

  return (os);
}

struct skv_server_to_client_cmd_hdr_t
{
  // skv_client_ccb_t*     mCmdCtrlBlk;
  uint64_t              mCmdCtrlBlk;
  skv_command_type_t    mCmdType;
  short                 mCmdOrd;
  short                 mCmdLength;  //! actual length of useful data in ctrl-msg

  skv_client_event_t    mEvent;

#ifdef SKV_DEBUG_MSG_MARKER
  uint64_t              mMarker;
#endif

  skv_server_to_client_cmd_hdr_t&
  operator=( const skv_client_to_server_cmd_hdr_t & aHdr )
  {
    mCmdCtrlBlk = aHdr.mCmdCtrlBlk;
    mCmdOrd     = aHdr.mCmdOrd;
    mCmdType    = aHdr.mCmdType;
    mCmdLength  = aHdr.mCmdLength;

#ifdef SKV_DEBUG_MSG_MARKER
    mMarker     = aHdr.mMarker;
#endif

    return (*this);
  }

#ifdef SKV_HEADER_CHECKSUM
  // header checksum for server-client header differs from client-server header because of command buffer reuse for responses
  inline char
  CheckSum()
  {
    char cs = 0;
    char *buf = (char*)this;
    for( unsigned int i=0; i<sizeof(skv_server_to_client_cmd_hdr_t); i++ )
      cs += (i*buf[i]+1);
    if( cs == 0 )
      cs = 1;
    return cs;
  }
#else
  inline char
  CheckSum()
  {
    return SKV_CTRL_MSG_FLAG_RESPONSE_COMPLETE;
  }
#endif


  void
  SetCmdLength( int aCmdLength )
  {
    StrongAssertLogLine( (aCmdLength>0) && (aCmdLength<SKV_CONTROL_MESSAGE_SIZE) )
      << "ERROR Invalid command length"
      << " 0 < " << aCmdLength << " < " << SKV_CONTROL_MESSAGE_SIZE
      << EndLogLine;
    mCmdLength = aCmdLength;
  }

  int
  GetCmdLength()
  {
    return mCmdLength;
  }

  void
  Reset()
  {
    BegLogLine( 0 )
      << "skv_client_to_server_cmd_hdr_t::Reset(): RESETTING HEADER."
      << " addr: " << (void*)(&mCmdCtrlBlk)
      << EndLogLine;

    mCmdCtrlBlk = 0;
    mCmdType    = SKV_COMMAND_NONE;
    mCmdLength  = 0;
    mEvent      = SKV_CLIENT_EVENT_CMD_NONE;
    mCmdOrd     = -1;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_server_to_client_cmd_hdr_t& aHdr )
{
  os << "skv_server_to_client_cmd_hdr_t [  "
     << (void *) aHdr.mCmdCtrlBlk << ' '
     << aHdr.mCmdOrd << ' '
     << (int) aHdr.mCmdType << ' '
     << (int) aHdr.mCmdLength << ' '
     << (int) aHdr.mEvent
     << " ]";

  return(os);    

}
/***************************************************/

#endif // __SKV_CLIENT_SERVER_HEADERS_HPP__
