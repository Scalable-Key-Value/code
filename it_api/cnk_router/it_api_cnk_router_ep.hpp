/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * it_api_cnk_router_ep.hpp
 *
 *  Created on: Jan 14, 2015
 *      Author: lschneid
 */

#ifndef IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_HPP_
#define IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_HPP_

#ifndef FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG
#define FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG ( 0 )
#endif

#include <stddef.h>
#include <cnk_router/it_api_cnk_router_types.hpp>
#include <cnk_router/it_api_cnk_router_ep_buffer.hpp>

#define MULTIPLEX_STATISTICS

static
inline
iWARPEM_Status_t
SendMsg( iWARPEM_Object_EndPoint_t *aEP, char * buff, int len, int* wlen, const bool aFlush = false );

template <class MultiplexedConnectionType, class SocketBufferType = iWARPEM_Memory_Socket_Buffer_t>
class iWARPEM_Multiplexed_Endpoint_t
{
  // todo: allow user to create and provide send/recv buffers to maybe prevent some memcpy

  iWARPEM_Router_Info_t mRouterInfo;
  int mRouterConnFd;
  uint16_t mClientCount;
  uint16_t mMaxClientCount;
  MultiplexedConnectionType **mClientEPs;
  iWARPEM_Object_Accept_t *mListenerContext;

  SocketBufferType *mSendBuffer;
  char* mReceiveBuffer;
  char* mCurrentRecvPtr;
  size_t mReceiveDataLen;

  uint16_t mPendingRequests;
  bool mNeedsBufferFlush;

#ifdef MULTIPLEX_STATISTICS
  uint32_t mMsgCount;
  uint32_t mFlushCount;
  double mMsgAvg;
#endif

  inline bool ReceiveProtocolHeader()
  {
    bool HdrCorrect = false;
    it_api_multiplexed_socket_message_header_t VirtHdr;
    int rlen_expected;
    int rlen = sizeof( it_api_multiplexed_socket_message_header_t );

    iWARPEM_Status_t istatus = read_from_socket( mRouterConnFd,
                                                 (char *) &VirtHdr,
                                                 rlen,
                                                 & rlen_expected );

    if(( istatus != IWARPEM_SUCCESS ) || ( rlen != rlen_expected ))
    {
      BegLogLine(rlen == rlen_expected)
        << "Short read, rlen=" << rlen
        << " rlen_expected=" << rlen_expected
        << " Peer has probably gone down"
        << EndLogLine ;

      return false;
    }

    StrongAssertLogLine( VirtHdr.ProtocolVersion == IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION )
      << "Protocol version mismatch. Recompile client and/or server to match."
      << " server: " << IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION
      << " client: " << VirtHdr.ProtocolVersion
      << EndLogLine;

    HdrCorrect = ( VirtHdr.ProtocolVersion == IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION );
    HdrCorrect &= ( istatus == IWARPEM_SUCCESS || ( rlen == rlen_expected ) );

    mReceiveDataLen = VirtHdr.DataLen;

    StrongAssertLogLine( mReceiveDataLen <= IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE )
      << "iWARPEM_Router_Endpoint_t: data length will exceed the buffer size"
      << " max: " << IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE
      << " actual: " << mReceiveDataLen
      << EndLogLine;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Received multiplex header: "
      << " socket: " << mRouterConnFd
      << " version: " << VirtHdr.ProtocolVersion
      << " datalen: " << VirtHdr.DataLen
      << EndLogLine;

    return HdrCorrect;
  }

  inline void ResetSendBuffer()
  {
    mSendBuffer->Reset();
    mSendBuffer->SetHeader( IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION, MULTIPLEXED_SOCKET_MSG_TYPE_DATA );
#ifdef MULTIPLEX_STATISTICS
    mMsgCount = 0;
#endif
  }

  inline size_t GetSendSpace() const
  {
    return mSendBuffer->GetRemainingSize();
  }

public:
  iWARPEM_Multiplexed_Endpoint_t( int aRouterFd = 0,
                             int aMaxClientCount = IT_API_MULTIPLEX_MAX_PER_SOCKET,
                             iWARPEM_Object_Accept_t *aListenerCtx = NULL )
  : mClientCount(0), mRouterConnFd( aRouterFd ), mMaxClientCount( aMaxClientCount ),
    mReceiveDataLen( 0 ), mPendingRequests( 0 ), mNeedsBufferFlush( false )
  {
    bzero( &mRouterInfo, sizeof( iWARPEM_Router_Info_t ));

    mClientEPs = new MultiplexedConnectionType*[ aMaxClientCount ];
    bzero( mClientEPs, sizeof( MultiplexedConnectionType*) * aMaxClientCount );

    mListenerContext = aListenerCtx;
    mSendBuffer = new SocketBufferType();
    mReceiveBuffer = (char*)new uintptr_t[ IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE / sizeof( uintptr_t ) ];

    AssertLogLine( ( (uintptr_t)mReceiveBuffer & IWARPEM_SOCKET_BUFFER_ALIGNMENT_MASK ) == 0 )
      << "Receive buffer is not aligned to " << IWARPEM_SOCKET_BUFFER_ALIGNMENT << " Bytes."
      << " @:" << (void*)mReceiveBuffer
      << EndLogLine;

    mCurrentRecvPtr = mReceiveBuffer;

    ResetSendBuffer();

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Creating multiplexed router endpoint..."
      << " socket: " << mRouterConnFd
      << " maxClients: " << aMaxClientCount
      << " BufferSizes: " << IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE
      << " Recv: @" << (void*)mReceiveBuffer
      << " Send: @" << (void*)mSendBuffer
      << EndLogLine;
#ifdef MULTIPLEX_STATISTICS
    mMsgCount = 0;
    mFlushCount = 0;
    mMsgAvg = 1.0;
#endif
  }

  ~iWARPEM_Multiplexed_Endpoint_t()
  {
    bzero( &mRouterInfo, sizeof( iWARPEM_Router_Info_t ));

    // go over all active EPs and flush/destroy them
    for( int i=0; i < mMaxClientCount; i++ )
      if( IsValidClient( i ) )
        RemoveClient( i );

    delete mClientEPs;
    delete mSendBuffer;
    delete mReceiveBuffer;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Destroyed multiplexed router endpoint."
      << " socket: " << mRouterConnFd
      << EndLogLine;

#ifdef MULTIPLEX_STATISTICS
    BegLogLine( 1 )
      << "average message count per send buffer:" << mMsgAvg
      << EndLogLine;
#endif
  }

  inline uint16_t GetClientCount() const { return mClientCount; }
  inline int GetRouterFd() const { return mRouterConnFd; }
  inline void SetRouterFd( int aRouterFd = 0 ) { mRouterConnFd = aRouterFd; }
  inline iWARPEM_Router_Info_t* GetRouterInfoPtr() const { return (iWARPEM_Router_Info_t*)&mRouterInfo; }
  inline iWARPEM_Object_Accept_t* GetListenerContext() const { return mListenerContext; }
  inline void IncreasePendingRequest() { mPendingRequests++; };
  inline void DecreasePendingRequests() { mPendingRequests--; if( !mPendingRequests ) FlushSendBuffer(); }

  inline MultiplexedConnectionType* GetClientEP( uint16_t aClientId ) const
  {
    if( aClientId > mMaxClientCount )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Requested ClientID " << aClientId << " out of range (" << mMaxClientCount << ")."
        << EndLogLine;
      return NULL;
    }
    return mClientEPs[ aClientId ];
  }

  inline bool IsValidClient( uint16_t aClientId ) const
  {
    return ((aClientId < mMaxClientCount)
        && ( mClientEPs[ aClientId ] != NULL ) );
  }

  MultiplexedConnectionType* AddClient( uint16_t aClientId, const MultiplexedConnectionType *aClientEP )
  {
    if( aClientId >= mMaxClientCount )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Requested client ID out of range [ 0 < " << aClientId << " < " << mMaxClientCount << " ]."
        << EndLogLine;
      return NULL;
    }

    if( mClientCount >= mMaxClientCount-1 )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Maximum number of client EPs exceeded."
        << " current: " << mClientCount
        << " max: " << mMaxClientCount
        << EndLogLine;
      return NULL;
    }

    if( mClientEPs[ aClientId ] != NULL )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "This client ID " << aClientId << " is already registered."
        << EndLogLine;
      return NULL;
    }
    mClientEPs[ aClientId ] = (MultiplexedConnectionType *)aClientEP;
    mClientCount++;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Added new client: " << aClientId
      << " @" << (void*) mClientEPs[ aClientId ]
      << EndLogLine;
    return mClientEPs[ aClientId ];
  }

  iWARPEM_Status_t RemoveClient( uint16_t aClientId )
  {
    if( aClientId >= mMaxClientCount )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Requested client ID out of range [ 0 < " << aClientId << " < " << mMaxClientCount << " ]."
        << EndLogLine;
      return IWARPEM_ERRNO_CONNECTION_RESET;
    }

    if( mClientCount < 1 )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No clients in ClientEP list."
        << EndLogLine;
      return IWARPEM_ERRNO_CONNECTION_RESET;
    }

    if( mClientEPs[ aClientId ] == NULL )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Client ID: " << aClientId
        << " is not a virtual connected ID for this RouterEP" << mRouterConnFd
        << EndLogLine;
      return IWARPEM_ERRNO_CONNECTION_RESET;
    }
    mClientEPs[ aClientId ] = NULL;
    mClientCount--;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Removed client: " << aClientId
      << EndLogLine;

    return IWARPEM_SUCCESS;
  }

  inline it_status_t FillReceiveBuffer()
  {
    if( ! ReceiveProtocolHeader() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "iWARPEM_Router_Endpoint_t:: Error receiving protocol header."
        << EndLogLine;
      return IT_ERR_INVALID_EP_STATE;
    }

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Refilling recv buffer for socket: " << mRouterConnFd
      << " expecting datalen: " << mReceiveDataLen
      << EndLogLine;

    int len;
    iWARPEM_Status_t istatus = read_from_socket( mRouterConnFd,
                                                 mReceiveBuffer,
                                                 mReceiveDataLen,
                                                 &len );
    StrongAssertLogLine( ( istatus == IWARPEM_SUCCESS ) && ( len == mReceiveDataLen )  )
      << "iWARPEM_Router_Endpoint_t: error reading from socket or data length mismatch:"
      << " status: " << (int)istatus
      << " expected: " << mReceiveDataLen
      << " actual: " << len
      << EndLogLine;

    mCurrentRecvPtr = mReceiveBuffer;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Refilled recv buffer for socket: " << mRouterConnFd
      << " datalen: " << mReceiveDataLen
      << EndLogLine;

    return IT_SUCCESS;
  }

  inline bool RecvDataAvailable() { return ( mCurrentRecvPtr - mReceiveBuffer < mReceiveDataLen ); }

  // if *aClient is passed as NULL, data is extracted without the multiplexed message header - pure raw retrieval
  // user has to make sure that this doesn't break the protocol!!
  iWARPEM_Status_t ExtractRawData( char *aBuffer, int aSize, int *aRecvd, uint16_t *aClient = NULL )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    if( ! RecvDataAvailable() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No data available in ReadBuffer. Reading new set of data."
        << EndLogLine;
      switch( FillReceiveBuffer() )
      {
        case IT_SUCCESS:
          break;
        default:
          status = IWARPEM_ERRNO_CONNECTION_RESET;
          BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
            << "Error while reading data from router endpoint."
            << EndLogLine;
          return status;
      }
    }
    if( aClient != NULL )
    {
      iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)mSendBuffer->AlignedPosition( mCurrentRecvPtr );
      mCurrentRecvPtr = (char*)MultHdr;

      MultHdr->ClientID = ntohs( MultHdr->ClientID );
      *aClient = MultHdr->ClientID;

      mCurrentRecvPtr += offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg );
    }
    int remaining_data = mReceiveDataLen - ( mCurrentRecvPtr - mReceiveBuffer );
    if( remaining_data < aSize )
      *aRecvd = remaining_data;
    else
      *aRecvd = aSize;

    memcpy( aBuffer, mCurrentRecvPtr, *aRecvd );
    mCurrentRecvPtr += *aRecvd;

    return IWARPEM_SUCCESS;
  }
  iWARPEM_Msg_Type_t GetNextMessageType( uint16_t *aClient )
  {
    if( ! RecvDataAvailable() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No data available in ReadBuffer. Reading new set of data."
        << EndLogLine;
      switch( FillReceiveBuffer() )
      {
        case IT_SUCCESS:
          break;
        default:
          BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
            << "Error while reading data from router endpoint."
            << EndLogLine;
          return iWARPEM_UNKNOWN_REQ_TYPE;
      }
    }
    iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)mSendBuffer->AlignedPosition( mCurrentRecvPtr );
    MultHdr->ClientID = ntohs( MultHdr->ClientID );
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Extracting new client message hdr @offset: " << (uintptr_t)(mCurrentRecvPtr - mReceiveBuffer)
      << EndLogLine;

    *aClient = MultHdr->ClientID;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Retrieving msg_type: " << MultHdr->ClientMsg.mMsg_Type
      << " from client: " << *aClient
      << " readptr@: " << (void*)mSendBuffer->AlignedPosition( mCurrentRecvPtr )
      << EndLogLine;

    return MultHdr->ClientMsg.mMsg_Type;
  }

  iWARPEM_Status_t ExtractNextMessage( iWARPEM_Message_Hdr_t **aHdr, char **aData, uint16_t *aClientId )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    if( ! RecvDataAvailable() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No data available in ReadBuffer. Reading new set of data."
        << EndLogLine;
      switch( FillReceiveBuffer() )
      {
        case IT_SUCCESS:
          break;
        default:
          status = IWARPEM_ERRNO_CONNECTION_RESET;
          BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
            << "Error while reading data from router endpoint."
            << EndLogLine;
          return status;
      }
    }

    iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)mSendBuffer->AlignedPosition( mCurrentRecvPtr );
    mCurrentRecvPtr = (char*)MultHdr;
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Extracting new client message hdr @offset: " << (uintptr_t)(mCurrentRecvPtr - mReceiveBuffer)
      << EndLogLine;

    MultHdr->ClientID = ntohs( MultHdr->ClientID );
    *aClientId = MultHdr->ClientID;
    mCurrentRecvPtr += offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg );

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Extracting new client message hdr @offset: " << (uintptr_t)(mCurrentRecvPtr - mReceiveBuffer)
      << EndLogLine;

    *aHdr = (iWARPEM_Message_Hdr_t*)mCurrentRecvPtr;
    (*aHdr)->EndianConvert();
    mCurrentRecvPtr += sizeof( iWARPEM_Message_Hdr_t );

    *aData = mCurrentRecvPtr;
    mCurrentRecvPtr += (*aHdr)->mTotalDataLen;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Extracted new client message: "
      << " socket: " << mRouterConnFd
      << " client: " << MultHdr->ClientID
      << " MsgHdrSize: " << sizeof( iWARPEM_Multiplexed_Msg_Hdr_t )
      << " MsgPldSize: " << (*aHdr)->mTotalDataLen
      << " Processed: " << (uintptr_t)(mCurrentRecvPtr - mReceiveBuffer)
      << EndLogLine;
    return status;
  }

  inline iWARPEM_Status_t FlushSendBuffer()
  {
    int wlen = 0;
    mNeedsBufferFlush = false;
    size_t DataLen = mSendBuffer->GetDataLen();
    if( DataLen == 0)
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Buffer is empty. Nothing to send."
        << EndLogLine;
      return IWARPEM_SUCCESS;
    }
    iWARPEM_Status_t status = mSendBuffer->FlushToSocket( mRouterConnFd, &wlen );
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Sent " << (int)(wlen)
      << " payload: " << mSendBuffer->GetDataLen()
      << EndLogLine;

#ifdef MULTIPLEX_STATISTICS
    mFlushCount++;
    mMsgAvg = (mMsgAvg * 0.9997) + (mMsgCount * 0.0003);
    BegLogLine( ( (mFlushCount & 0xfff) == 0 ) )
      << "average message count per send buffer:" << mMsgAvg
      << EndLogLine;

#endif
    ResetSendBuffer();

    return status;
  }

  iWARPEM_Status_t InsertMessage( uint16_t aClientID, const char* aData, int aSize, bool aForceNoFlush = false )
  {
    if( aSize > IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE - sizeof( it_api_multiplexed_socket_message_header_t ) )
    {
      BegLogLine( 1 )
        << "Requested message size exceeds send buffer size."
        << " MAX: " << IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE
        << " actual: " << aSize
        << EndLogLine;
      return IWARPEM_SUCCESS;
    }
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    if( GetSendSpace() < aSize )
    {
      status = FlushSendBuffer();
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Remaining space is too small. Sending existing data first.."
        << " req_size: " << aSize
        << " rem_space: " << GetSendSpace()
        << EndLogLine;
    }

    // create the multiplex header from the client id
    mSendBuffer->AddClientHdr( aClientID );
    mSendBuffer->AddData( aData, aSize );

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Inserted Message to send buffer: "
      << " ClientId: " << aClientID
      << " msg_size: " << aSize
      << " msg_type: " << ((iWARPEM_Message_Hdr_t*)aData)->mMsg_Type
      << " bytes in buffer: " << mSendBuffer->GetDataLen()
      << EndLogLine;

#ifdef MULTIPLEX_STATISTICS
    mMsgCount++;
#endif
    mNeedsBufferFlush = true;

    // initiate a send of data once we've filled up the buffer beyond 15/16ths
    if( (!aForceNoFlush) && (GetSendSpace() < IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE >> 4) )
      status = FlushSendBuffer();
    return status;
  }
  iWARPEM_Status_t InsertMessageVector( const uint16_t aClientId, struct iovec *aIOV, int aIOV_Count, int *aLen )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    // only create the msg header for the first vector
    int i=0;
    int send_size = aIOV[ i ].iov_len;
    status = InsertMessage( aClientId, (char*)(aIOV[ i ].iov_base), aIOV[ i ].iov_len, true );
    if( status == IWARPEM_SUCCESS )
      *aLen = send_size;

    for( int i=1; (i < aIOV_Count ) && ( status == IWARPEM_SUCCESS ); i++ )
    {
      send_size = aIOV[ i ].iov_len;
      StrongAssertLogLine( send_size < GetSendSpace() )
        << "Message vector entry " << i
        << " doesn't fit into send buffer. Space: " << GetSendSpace()
        << " requested: " << send_size
        << " already inserted: " << *aLen
        << EndLogLine;

      mSendBuffer->AddData( aIOV[ i ] );
      *aLen += send_size;
    }

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
    << "Inserted Message Vector to send buffer: "
    << " ClientId: " << aClientId
    << " entries: " << aIOV_Count
    << " msg_size: " << *aLen
    << " bytes in buffer: " << mSendBuffer->GetDataLen()
    << EndLogLine;

    // initiate a send of data once we've filled up the buffer beyond 15/16ths
    if( GetSendSpace() < IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE >> 4 )
      status = FlushSendBuffer();

    return status;
  }

  iWARPEM_Status_t InsertAcceptResponse( uint16_t aClientID, iWARPEM_Private_Data_t *aPrivData )
  {
    iWARPEM_Message_Hdr_t msg;
    int slen = 0;

    msg.mMsg_Type = iWARPEM_SOCKET_CONNECT_RESP_TYPE;
    msg.mTotalDataLen = sizeof( iWARPEM_Private_Data_t );

    struct iovec iov[2];
    iov[0].iov_base = &msg;
    iov[0].iov_len = sizeof( iWARPEM_Message_Hdr_t );

    iov[1].iov_base = aPrivData;
    iov[1].iov_len = sizeof( iWARPEM_Private_Data_t );

    return InsertMessageVector( aClientID, iov, 2, &slen );
  }

  iWARPEM_Status_t InsertConnectRequest( const uint16_t aClientId,
                                         const iWARPEM_Message_Hdr_t *aHdr,
                                         const iWARPEM_Private_Data_t *aPrivData,
                                         const MultiplexedConnectionType *aClientEP )
  {
    struct iovec iov[2];
    int slen;
    iov[0].iov_base = (void*)aHdr;
    iov[0].iov_len = sizeof( iWARPEM_Message_Hdr_t );

    iov[1].iov_base = (void*)aPrivData;
    iov[1].iov_len = sizeof( iWARPEM_Private_Data_t );

    return InsertMessageVector( aClientId, iov, 2, &slen );
  }

  iWARPEM_Status_t InsertCloseRequest( const uint16_t aClientId )
  {
    iWARPEM_Message_Hdr_t Hdr;
    Hdr.mMsg_Type = iWARPEM_SOCKET_CLOSE_REQ_TYPE;
    Hdr.mTotalDataLen = 0;
    return InsertMessage( aClientId, (char*)&Hdr, sizeof( iWARPEM_Message_Hdr_t ) );
  }

  inline bool NeedsFlush() const { return mNeedsBufferFlush; }

  void CloseAllClients()
  {
    for( int n=0; n<mMaxClientCount; n++ )
    {
      if( mClientEPs[ n ] != NULL )
      {
        InsertCloseRequest( n );
      }
    }
    FlushSendBuffer();
  }

};

#endif /* IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_HPP_ */
