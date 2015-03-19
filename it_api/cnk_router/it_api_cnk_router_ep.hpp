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

#define ALLOW_COPY ( true )
#define CREATE_SEND_BUFFER ( false )
#define CREATE_RECV_BUFFER ( true )

static
inline
iWARPEM_Status_t
SendMsg( iWARPEM_Object_EndPoint_t *aEP, char * buff, int len, int* wlen, const bool aFlush = false );

// for now, the receive buffer will always be a memory buffer
typedef iWARPEM_Memory_Socket_Buffer_t ReceiveBufferType;

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
  ReceiveBufferType *mReceiveBuffer;
  size_t mReceiveDataLen;

  uint16_t mPendingRequests;
  bool mNeedsBufferFlush;

#ifdef MULTIPLEX_STATISTICS
  uint32_t mMsgCount;
  uint32_t mFlushCount;
  double mMsgAvg;
#endif

  inline void ResetSendBuffer()
  {
    mSendBuffer->Reset();
    mSendBuffer->SetHeader( IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION, MULTIPLEXED_SOCKET_MSG_TYPE_DATA );
#ifdef MULTIPLEX_STATISTICS
    mMsgCount = 0;
#endif
  }
  inline void ResetRecvBuffer()
  {
    mReceiveBuffer->Reset();
    mReceiveBuffer->SetHeader( 0, 0);
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
    mReceiveBuffer = new ReceiveBufferType();
    mReceiveBuffer->ConfigureAsReceiveBuffer();

    ResetSendBuffer();
    ResetRecvBuffer();

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

  inline MultiplexedConnectionType* GetClientEP( iWARPEM_StreamId_t aClientId ) const
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

  inline bool IsValidClient( iWARPEM_StreamId_t aClientId ) const
  {
    return ((aClientId < mMaxClientCount)
        && ( mClientEPs[ aClientId ] != NULL ) );
  }

  MultiplexedConnectionType* AddClient( iWARPEM_StreamId_t aClientId, const MultiplexedConnectionType *aClientEP )
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

  iWARPEM_Status_t RemoveClient( iWARPEM_StreamId_t aClientId )
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

  inline bool RecvDataAvailable() { return ( mReceiveBuffer->RecvDataAvailable() ); }

  // if *aClient is passed as NULL, data is extracted without the multiplexed message header - pure raw retrieval
  // user has to make sure that this doesn't break the protocol!!
  iWARPEM_Status_t ExtractRawData( char *aBuffer, int aSize, int *aRecvd, iWARPEM_StreamId_t *aClient = NULL )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    if( ! RecvDataAvailable() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No data available in ReadBuffer. Reading new set of data."
        << EndLogLine;
      status = mReceiveBuffer->FillFromSocket( mRouterConnFd );
      switch( status )
      {
        case IWARPEM_SUCCESS:
          break;
        default:
          BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
            << "Error while reading data from router endpoint."
            << EndLogLine;
          return status;
      }
    }
    // data can only be an iWARPEM_Message_Hdr_t if the client is requested too
    if( aClient != NULL )
    {
      size_t hdrlen = sizeof( iWARPEM_Multiplexed_Msg_Hdr_t );
      iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)mReceiveBuffer->GetHdrPtr( &hdrlen );
      AssertLogLine( hdrlen == sizeof( iWARPEM_Multiplexed_Msg_Hdr_t ) )
        << "Retrieval of HdrPtr failed: len=" << hdrlen
        << " expected=" << sizeof(iWARPEM_Multiplexed_Msg_Hdr_t)
        << EndLogLine;

      MultHdr->ClientID = be64toh( MultHdr->ClientID );
      *aClient = MultHdr->ClientID;

      hdrlen = sizeof( iWARPEM_Message_Hdr_t );
      char* databuf = mReceiveBuffer->GetHdrPtr( &hdrlen );
      memcpy( aBuffer, databuf, hdrlen );
    }
    else
      mReceiveBuffer->GetData( &aBuffer, (size_t*)&aSize );

    return IWARPEM_SUCCESS;
  }
  // needs to make sure to not change the status of the receive buffer
  iWARPEM_Status_t GetNextMessageType( iWARPEM_Msg_Type_t *aMsgType, iWARPEM_StreamId_t *aClient )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    if( ! RecvDataAvailable() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No data available in ReadBuffer. Reading new set of data."
        << EndLogLine;
      status = mReceiveBuffer->FillFromSocket( mRouterConnFd );
      switch( status )
      {
        case IWARPEM_SUCCESS:
          break;
        default:
          BegLogLine( 1 )
            << "Error while reading data from router endpoint."
            << EndLogLine;
          *aClient = IWARPEM_INVALID_CLIENT_ID;
          *aMsgType = iWARPEM_UNKNOWN_REQ_TYPE;
          return status;
      }
    }
    size_t hdrlen = sizeof( iWARPEM_Multiplexed_Msg_Hdr_t );
    iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)mReceiveBuffer->GetHdrPtr( &hdrlen, 0 );
    AssertLogLine( hdrlen == sizeof( iWARPEM_Multiplexed_Msg_Hdr_t ) )
      << "Retrieval of HdrPtr failed: len=" << hdrlen
      << " expected=" << sizeof(iWARPEM_Multiplexed_Msg_Hdr_t)
      << EndLogLine;

    MultHdr->ClientID = be64toh( MultHdr->ClientID );
    *aClient = MultHdr->ClientID;

    // advance the hdr ptr ourselves to peek into MsgType without advancing the receive buffer ptr
    char *HdrData = (char*)MultHdr;
    HdrData += sizeof( iWARPEM_Multiplexed_Msg_Hdr_t );
    HdrData = (char*)mReceiveBuffer->AlignedHeaderPosition( HdrData );

    iWARPEM_Message_Hdr_t *MsgPtr = (iWARPEM_Message_Hdr_t*)HdrData;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Retrieving msg_type: " << MsgPtr->mMsg_Type
      << " from client: " << *aClient
      << " readptr@: " << (void*)(MsgPtr)
      << " len: " << MsgPtr->mTotalDataLen
      << EndLogLine;

    *aMsgType = MsgPtr->mMsg_Type;
    return status;
  }

  iWARPEM_Status_t ExtractNextMessage( iWARPEM_Message_Hdr_t **aHdr, char **aData, iWARPEM_StreamId_t *aClientId )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    if( ! RecvDataAvailable() )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "No data available in ReadBuffer. Reading new set of data."
        << EndLogLine;
      switch( mReceiveBuffer->FillFromSocket( mRouterConnFd ) )
      {
        case IWARPEM_SUCCESS:
          break;
        default:
          status = IWARPEM_ERRNO_CONNECTION_RESET;
          BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
            << "Error while reading data from router endpoint."
            << EndLogLine;
          return status;
      }
    }
    size_t rlen = sizeof( iWARPEM_Multiplexed_Msg_Hdr_t );
    iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)mReceiveBuffer->GetHdrPtr( &rlen );
    AssertLogLine( rlen == sizeof( iWARPEM_Multiplexed_Msg_Hdr_t ) )
      << "Retrieval of HdrPtr failed: len=" << rlen
      << " expected=" << sizeof(iWARPEM_Multiplexed_Msg_Hdr_t)
      << EndLogLine;

    MultHdr->ClientID = ntohs( MultHdr->ClientID );
    *aClientId = MultHdr->ClientID;

    rlen = sizeof( iWARPEM_Message_Hdr_t );
    *aHdr = (iWARPEM_Message_Hdr_t*)mReceiveBuffer->GetHdrPtr( &rlen );
    AssertLogLine( rlen == sizeof( iWARPEM_Message_Hdr_t ) )
      << "Retrieval of HdrPtr failed: len=" << rlen
      << " expected=" << sizeof(iWARPEM_Multiplexed_Msg_Hdr_t)
      << EndLogLine;

    (*aHdr)->EndianConvert();

    rlen = (*aHdr)->mTotalDataLen;
    rlen = mReceiveBuffer->GetData( aData, &rlen );
    AssertLogLine( rlen == (*aHdr)->mTotalDataLen )
      << "Retrieval of HdrPtr failed: len=" << rlen
      << " expected=" << sizeof(iWARPEM_Multiplexed_Msg_Hdr_t)
      << EndLogLine;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Extracted new client message: "
      << " socket: " << mRouterConnFd
      << " client: " << MultHdr->ClientID
      << " MsgHdrSize: " << sizeof( iWARPEM_Multiplexed_Msg_Hdr_t )
      << " MsgPldSize: " << (*aHdr)->mTotalDataLen
      << " Processed: " << mReceiveBuffer->GetDataLen()
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

  iWARPEM_Status_t InsertMessage( iWARPEM_StreamId_t aClientID,
                                  const iWARPEM_Message_Hdr_t* aHdr,
                                  const char *aData,
                                  int aSize, bool aForceNoFlush = false )
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
    iWARPEM_StreamId_t *client = (iWARPEM_StreamId_t*)&aClientID;
    mSendBuffer->AddHdr( (const char*)client, sizeof( iWARPEM_StreamId_t ) );

    if( aHdr )
      mSendBuffer->AddHdr( (const char*)aHdr, sizeof( iWARPEM_Message_Hdr_t ) );

    if( aSize > 0 )
      mSendBuffer->AddData( aData, aSize );

#if (FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG != 0)
    iWARPEM_Msg_Type_t MsgType = iWARPEM_UNKNOWN_REQ_TYPE;
    if( aHdr )
    {
      MsgType = aHdr->mMsg_Type;
    }

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Inserted Message to send buffer: "
      << " ClientId: " << aClientID
      << " msg_size: " << aSize
      << " msg_type: " << iWARPEM_Msg_Type_to_string( MsgType )
      << " bytes in buffer: " << mSendBuffer->GetDataLen()
      << EndLogLine;
#endif

#ifdef MULTIPLEX_STATISTICS
    mMsgCount++;
#endif
    mNeedsBufferFlush = true;

    // initiate a send of data once we've filled up the buffer beyond a threshold
    if( (!aForceNoFlush) && ( mSendBuffer->FlushRecommended() ) )
      status = FlushSendBuffer();
    return status;
  }
  iWARPEM_Status_t InsertMessageVector( const iWARPEM_StreamId_t aClientId,
                                        struct iovec *aIOV,
                                        int aIOV_Count,
                                        int *aLen,
                                        bool aFirstIsHeader = true )
  {
    iWARPEM_Status_t status = IWARPEM_SUCCESS;
    int i=0;
    *aLen = 0;

    // only create the msg header for the first vector
    iWARPEM_Message_Hdr_t *hdr = NULL;
    if( aFirstIsHeader )
    {
      hdr = (iWARPEM_Message_Hdr_t*)aIOV[ 0 ].iov_base;
      *aLen += sizeof( iWARPEM_Message_Hdr_t );
      i++;
    }

    int send_size = aIOV[ i ].iov_len;
    status = InsertMessage( aClientId, hdr, (char*)(aIOV[ i ].iov_base), aIOV[ i ].iov_len, true );
    if( status == IWARPEM_SUCCESS )
      *aLen += send_size;

    i++;
    for( ; (i < aIOV_Count ) && ( status == IWARPEM_SUCCESS ); i++ )
    {
      send_size = aIOV[ i ].iov_len;
      StrongAssertLogLine( send_size < GetSendSpace() )
        << "Message vector entry " << i
        << " doesn't fit into send buffer. Space: " << GetSendSpace()
        << " requested: " << send_size
        << " already inserted: " << *aLen
        << EndLogLine;

      mSendBuffer->AddDataContigous( (const char*)aIOV[ i ].iov_base, aIOV[ i ].iov_len );
      *aLen += send_size;
    }

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
    << "Inserted Message Vector to send buffer: "
    << " ClientId: " << aClientId
    << " entries: " << aIOV_Count
    << " msg_size: " << *aLen
    << " bytes in buffer: " << mSendBuffer->GetDataLen()
    << EndLogLine;

    // initiate a send of data once we've filled up the buffer beyond a threshold
    if( mSendBuffer->FlushRecommended() )
      status = FlushSendBuffer();

    return status;
  }

  iWARPEM_Status_t InsertAcceptResponse( iWARPEM_StreamId_t aClientID, iWARPEM_Private_Data_t *aPrivData )
  {
    iWARPEM_Message_Hdr_t msg;
    int slen = 0;

    msg.mMsg_Type = iWARPEM_SOCKET_CONNECT_RESP_TYPE;
    msg.mTotalDataLen = sizeof( iWARPEM_Private_Data_t );

    return InsertMessage( aClientID, &msg, (char*)aPrivData, msg.mTotalDataLen );
  }

  iWARPEM_Status_t InsertConnectRequest( const iWARPEM_StreamId_t aClientId,
                                         const iWARPEM_Message_Hdr_t *aHdr,
                                         const iWARPEM_Private_Data_t *aPrivData,
                                         const MultiplexedConnectionType *aClientEP )
  {
    return InsertMessage( aClientId, aHdr, (char*)aPrivData, sizeof( iWARPEM_Private_Data_t ));
  }

  iWARPEM_Status_t InsertCloseRequest( const iWARPEM_StreamId_t aClientId )
  {
    iWARPEM_Message_Hdr_t Hdr;
    Hdr.mMsg_Type = iWARPEM_SOCKET_CLOSE_REQ_TYPE;
    Hdr.mTotalDataLen = 0;
    char *data = (char*)&Hdr.mTotalDataLen;
    return InsertMessage( aClientId, &Hdr, data, 0 );
  }

  iWARPEM_Status_t InsertDisconnectRequest( const iWARPEM_StreamId_t aClientId )
  {
    iWARPEM_Message_Hdr_t Hdr;
    Hdr.mMsg_Type = iWARPEM_DISCONNECT_REQ_TYPE;
    Hdr.mTotalDataLen = 0;
    char *data = (char*)&Hdr.mTotalDataLen;
    return InsertMessage( aClientId, &Hdr, data, 0 );
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
