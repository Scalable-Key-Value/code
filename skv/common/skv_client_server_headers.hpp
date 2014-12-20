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

#include <skv/common/skv_types.hpp>
#include <skv/server/skv_server_event_type.hpp>
#include <skv/client/skv_client_types.hpp>

#ifndef SKV_CLIENT_ENDIAN_LOG
#define SKV_CLIENT_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL )
#endif

#define SKV_CTRL_MSG_FLAG_EMPTY             0x00
#define SKV_CTRL_MSG_FLAG_REQUEST_COMPLETE  0x01
#define SKV_CTRL_MSG_FLAG_RESPONSE_COMPLETE 0x02
#define SKV_CTRL_MSG_FLAG_IN_PROGRESS       0x03

#if SKV_USE_VERBS
#define SKV_MAX_COALESCED_COMMANDS ( 4 )   // some verbs providers don't allow more than 4 SGEs
#else
#define SKV_MAX_COALESCED_COMMANDS ( 64 )  // allow for more coalescing supported by the sockets impl of IT-API
#endif

#define SKV_CHECKSUM_BYTES ( 2 )
#define SKV_CHECKSUM_TYPE  short
#define SKV_CHECKSUM_CONVERSION_hton htons
#define SKV_CHECKSUM_CONVERSION_ntoh ntohs

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

  void EndianConvert(void)
    {
      BegLogLine(SKV_CLIENT_ENDIAN_LOG)
        << "mEventType " << mEventType
        << EndLogLine ;
      mEventType=(skv_server_event_type_t) ntohl(mEventType) ;
    }
  void
  Init( skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_command_type_t aCmdType )
  {
    mEventType  = aEventType;
    mCmdCtrlBlk = (uint64_t) (uintptr_t) aCmdCtrlBlk;

    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "aCmdType=" << aCmdType
      << EndLogLine ;
    mCmdType = aCmdType ; // will be endian-converted later
    mCmdOrd    = -1;            // initialize without valid CmdOrd
    mCmdLength = 0;             // initialize without valid CmdLength

#ifdef SKV_DEBUG_MSG_MARKER
    mMarker = 0;
#endif

  }

#ifdef SKV_HEADER_CHECKSUM
  inline SKV_CHECKSUM_TYPE
  CheckSum()
  {
    SKV_CHECKSUM_TYPE cs = 0;
    SKV_CHECKSUM_TYPE *buf = (SKV_CHECKSUM_TYPE*)this;
    for( unsigned int i=0; i<sizeof(skv_client_to_server_cmd_hdr_t)/SKV_CHECKSUM_BYTES; i++ )
      cs += (i * SKV_CHECKSUM_CONVERSION_ntoh( buf[i] )+1);
    if( (cs == SKV_CTRL_MSG_FLAG_EMPTY) || (cs == SKV_CTRL_MSG_FLAG_IN_PROGRESS) )
      cs = 1;
    return SKV_CHECKSUM_CONVERSION_hton(cs);
  }
#else
  inline SKV_CHECKSUM_TYPE
  CheckSum()
  {
    return SKV_CHECKSUM_CONVERSION_hton( SKV_CTRL_MSG_FLAG_REQUEST_COMPLETE );
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
    StrongAssertLogLine( (aCmdLength>0) && (aCmdLength<SKV_CONTROL_MESSAGE_SIZE-SKV_CHECKSUM_BYTES) )
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
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "assigned mCmdType=" << mCmdType
      << EndLogLine ;

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

  void EndianConvert(void)
    {
      mEvent=(skv_client_event_t) ntohl(mEvent) ;
      mCmdType=(skv_command_type_t) ntohl(mCmdType) ;
      BegLogLine(SKV_CLIENT_ENDIAN_LOG)
        << "mEvent endian converted to " << mEvent
        << " mCmdType endian converted to " << mCmdType
        << EndLogLine ;
    }
  skv_server_to_client_cmd_hdr_t&
  operator=( const skv_client_to_server_cmd_hdr_t & aHdr )
  {
    mCmdCtrlBlk = aHdr.mCmdCtrlBlk;
    mCmdOrd     = aHdr.mCmdOrd;
    mCmdType    = aHdr.mCmdType;
    mCmdLength  = aHdr.mCmdLength;
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "assigned mCmdType=" << mCmdType
      << EndLogLine ;

#ifdef SKV_DEBUG_MSG_MARKER
    mMarker     = aHdr.mMarker;
#endif

    return (*this);
  }

#ifdef SKV_HEADER_CHECKSUM
  // header checksum for server-client header differs from client-server header because of command buffer reuse for responses
  inline SKV_CHECKSUM_TYPE
  CheckSum()
  {
    SKV_CHECKSUM_TYPE cs = 0;
    SKV_CHECKSUM_TYPE *buf = (SKV_CHECKSUM_TYPE*)this;
    for( unsigned int i=0; i<sizeof(skv_server_to_client_cmd_hdr_t)/SKV_CHECKSUM_BYTES; i++ )
      cs += (i * SKV_CHECKSUM_CONVERSION_ntoh( buf[i] )+1);
    if( (cs == SKV_CTRL_MSG_FLAG_EMPTY) || (cs == SKV_CTRL_MSG_FLAG_IN_PROGRESS) )
      cs = 1;
    return SKV_CHECKSUM_CONVERSION_hton(-cs);
  }
#else
  inline SKV_CHECKSUM_TYPE
  CheckSum()
  {
    return SKV_CHECKSUM_CONVERSION_hton( SKV_CTRL_MSG_FLAG_RESPONSE_COMPLETE );
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

// to reinterpret and work on command/repsonse buffers
struct skv_header_as_cmd_buffer_t
{
  union {
    skv_client_to_server_cmd_hdr_t mCmndHdr;
    skv_server_to_client_cmd_hdr_t mRespHdr;
    unsigned char mBuffer[ SKV_CONTROL_MESSAGE_SIZE ];
  } mData;

  inline SKV_CHECKSUM_TYPE GetCheckSum() const
  {
    return ( *((SKV_CHECKSUM_TYPE*) &mData.mBuffer[ SKV_CONTROL_MESSAGE_SIZE - SKV_CHECKSUM_BYTES ]) );
  }
  inline void SetCheckSum( SKV_CHECKSUM_TYPE aChkSum )
  {
    *((SKV_CHECKSUM_TYPE*) &mData.mBuffer[ SKV_CONTROL_MESSAGE_SIZE - SKV_CHECKSUM_BYTES ]) = aChkSum;
  }
  inline size_t GetRoomForData( const size_t aContentSize ) const
  {
    return SKV_CONTROL_MESSAGE_SIZE - aContentSize - SKV_CHECKSUM_BYTES;
  }
};

#endif // __SKV_CLIENT_SERVER_HEADERS_HPP__
