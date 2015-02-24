/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * it_api_cnk_router_ep_buffer.hpp
 *
 *  Created on: Feb 11, 2015
 *      Author: lschneid
 */

#ifndef IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_BUFFER_HPP_
#define IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_BUFFER_HPP_

#ifndef FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG
#define FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG ( 0 )
#endif

#define IWARPEM_SOCKET_BUFFER_ALIGNMENT ( 8 )
#define IWARPEM_SOCKET_BUFFER_ALIGNMENT_MASK ( 0x7 )

class iWARPEM_Socket_Buffer_t
{
protected:
  it_api_multiplexed_socket_message_header_t *mHeader;

  inline void UpdateHeaderLen( const size_t aLen )
  {
    mHeader->DataLen = aLen;
  }
public:
  iWARPEM_Socket_Buffer_t( const size_t aBufferSize = IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE,
                           const bool aAllowCopy = false ) { mHeader = NULL; }
  virtual ~iWARPEM_Socket_Buffer_t() {};

  inline size_t GetRemainingSize() const;
  inline void AddClientHdr( uint16_t aClient );
  virtual void AddData( const char* aBuffer, const size_t aSize ) = 0;
  virtual void AddData( const struct iovec &aIOV ) = 0;
  inline size_t GetDataLen() const;
  virtual iWARPEM_Status_t FlushToSocket( const int aSocket, int *wlen ) = 0;
  inline void Reset();

  inline void SetHeader( const it_api_multiplexed_socket_message_header_t &aHdr );
  inline void SetHeader( const unsigned char aProtocolVersion, const unsigned char aMsgType );
};

class iWARPEM_Memory_Socket_Buffer_t : protected iWARPEM_Socket_Buffer_t
{
  char *mBuffer;
  char *mCurrentPosition;
  char *mBufferEnd;

  size_t mBufferSize;
  bool mAllowCopy;

public:
  iWARPEM_Memory_Socket_Buffer_t( const size_t aBufferSize = IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE,
                                  const bool aAllowCopy = true )
  {
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Creating Data Buffer. size=" << aBufferSize
      << EndLogLine;

    mBufferSize = aBufferSize;
    mBuffer = (char*)new uintptr_t[ mBufferSize / sizeof(uintptr_t) ];

    AssertLogLine( ((uintptr_t)mBuffer & IWARPEM_SOCKET_BUFFER_ALIGNMENT_MASK ) == 0 )
      << "Buffer is not aligned to " << IWARPEM_SOCKET_BUFFER_ALIGNMENT << " Bytes."
      << " @: " << (void*)mBuffer
      << EndLogLine;

    mHeader = (it_api_multiplexed_socket_message_header_t*)mBuffer;
    mBufferEnd = mBuffer + mBufferSize;
    mAllowCopy = aAllowCopy;

    Reset();
  }
  ~iWARPEM_Memory_Socket_Buffer_t()
  {
    delete mBuffer;
    mBuffer = NULL;
    mCurrentPosition = NULL;
  }

  inline uintptr_t AlignedPosition( const char* aAddr ) 
  {
  uintptr_t base = (uintptr_t)aAddr;
  return ( (base - 1) & (~IWARPEM_SOCKET_BUFFER_ALIGNMENT_MASK )) + IWARPEM_SOCKET_BUFFER_ALIGNMENT;
  }
  inline size_t GetRemainingSize() const { return (size_t)(mBufferEnd - mCurrentPosition); }
  inline void SetHeader( const it_api_multiplexed_socket_message_header_t &aHdr )
  {
    mHeader->MsgType = aHdr.MsgType;
    mHeader->ProtocolVersion = aHdr.ProtocolVersion;
  }
  inline void SetHeader( const unsigned char aProtocolVersion, const unsigned char aMsgType )
  {
    mHeader->MsgType = aMsgType;
    mHeader->ProtocolVersion = aProtocolVersion;
  }
  inline void AddClientHdr( uint16_t aClient )
  {
    BegLogLine( (uintptr_t)mCurrentPosition % IWARPEM_SOCKET_BUFFER_ALIGNMENT != 0 )
      << " Buffer Pointer alignment problem: " << (void*)mCurrentPosition
      << " alignment required: " << offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg )
      << EndLogLine;

    iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)AlignedPosition( mCurrentPosition );
    mCurrentPosition = (char*)MultHdr;

    MultHdr->ClientID = htons( aClient );

    mCurrentPosition += offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg );
  }
  inline void AddData( const char* aBuffer, const size_t aSize )
  {
    StrongAssertLogLine( (mAllowCopy) || (aBuffer == mCurrentPosition ) )
      << "Socket buffer configured without memcpy. But src and dest buffer differ:"
      << "@" << (void*)aBuffer
      << " vs. @" << (void*)mCurrentPosition
      << " request size was: " << aSize
      << EndLogLine;

    if(( mAllowCopy ) && ( aBuffer != mCurrentPosition ))
      memcpy( mCurrentPosition, aBuffer, aSize );

    mCurrentPosition += aSize;
  }
  inline void AddData( const struct iovec &aIOV )
  {
    if(( mAllowCopy ) && ( aIOV.iov_base != mCurrentPosition ))
      memcpy( mCurrentPosition, aIOV.iov_base, aIOV.iov_len );

    mCurrentPosition += aIOV.iov_len;
  }
  inline size_t GetDataLen() const
  {
    return (size_t)(mCurrentPosition - mBuffer - sizeof( it_api_multiplexed_socket_message_header_t ));
  }
  inline iWARPEM_Status_t FlushToSocket( const int aSocket, int *wlen )
  {
    UpdateHeaderLen( GetDataLen() );
    return write_to_socket( aSocket, mBuffer, mCurrentPosition - mBuffer, wlen );
  }
  inline void Reset()
  {
    UpdateHeaderLen( 0 );
    mCurrentPosition = mBuffer + sizeof( it_api_multiplexed_socket_message_header_t );
  }
};

#define IWARPEM_MAX_VECTOR_COUNT ( 256 )

class iWARPEM_WriteV_Socket_Buffer_t : protected iWARPEM_Socket_Buffer_t
{
  struct iovec *mVector;
  size_t mVectorCount;
  size_t mVectorMax;

  size_t mDataSize;

  char *mIntBuffer;
  char *mIntBufferPosition;
  size_t mIntBufferSize;


  bool mAllowCopy;

public:
  iWARPEM_WriteV_Socket_Buffer_t( const size_t aBufferSize = IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE,
                                  const bool aAllowCopy = false )
  {
    mVectorMax = IWARPEM_MAX_VECTOR_COUNT;
    mVector = new struct iovec[ IWARPEM_MAX_VECTOR_COUNT ];

    // prepare internal buffer space for on-the-fly vectors by AddData() (e.g. for headers)
    mIntBufferSize = aBufferSize;
    mIntBuffer = (char*)new uintptr_t[ mIntBufferSize / sizeof(uintptr_t) ];

    AssertLogLine( ((uintptr_t)mIntBuffer & IWARPEM_SOCKET_BUFFER_ALIGNMENT_MASK ) == 0 )
      << "Buffer is not aligned to " << IWARPEM_SOCKET_BUFFER_ALIGNMENT << " Bytes."
      << " @: " << (void*)mIntBuffer
      << EndLogLine;

    mAllowCopy = aAllowCopy;
    mHeader = ( it_api_multiplexed_socket_message_header_t* )mIntBuffer;

    Reset();

    struct iovec *iov = &(mVector[ 0 ]);
    iov->iov_base = mHeader;
    iov->iov_len = sizeof( it_api_multiplexed_socket_message_header_t );
  }
  ~iWARPEM_WriteV_Socket_Buffer_t()
  {
    delete mIntBuffer;
    delete mVector;
  }

  inline uintptr_t AlignedPosition( const char* aAddr ) 
  {
    uintptr_t base = (uintptr_t)aAddr;
    return ( (base - 1) & (~IWARPEM_SOCKET_BUFFER_ALIGNMENT_MASK )) + IWARPEM_SOCKET_BUFFER_ALIGNMENT;
  }
  inline size_t GetRemainingSize() const { return mIntBufferSize - (mIntBufferPosition - mIntBuffer); }
  inline void SkipToNextAlignment()
  {
    uintptr_t fill_size = (uintptr_t)AlignedPosition( mIntBufferPosition ) - (uintptr_t)mIntBufferPosition;
    if( fill_size > 0 )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Need to skip to next aligned spot (injecting iov @" << mVectorCount
        << EndLogLine;

      struct iovec *iov = &(mVector[ mVectorCount ]);
      iov->iov_base = mIntBufferPosition;
      iov->iov_len = fill_size;
      mDataSize += fill_size;
      mIntBufferPosition += fill_size;
      mVectorCount++;
    }
  }
  inline void AddClientHdr( uint16_t aClient )
  {
    SkipToNextAlignment();
    struct iovec *iov = &(mVector[ mVectorCount ]);
    iov->iov_base = (void*)AlignedPosition( mIntBufferPosition );
    iov->iov_len = offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg );

    iWARPEM_Multiplexed_Msg_Hdr_t *MultHdr = (iWARPEM_Multiplexed_Msg_Hdr_t*)iov->iov_base;
    MultHdr->ClientID = htons( aClient );

    mIntBufferPosition += offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg );
    mDataSize += offsetof( iWARPEM_Multiplexed_Msg_Hdr_t, ClientMsg );
    mVectorCount++;
  }
  virtual void AddData( const char* aBuffer, const size_t aSize )
  {
    struct iovec *iov = &(mVector[ mVectorCount ]);
    iov->iov_base = (void*)aBuffer;
    iov->iov_len = aSize;
    mDataSize += aSize;
    mVectorCount++;
  }
  virtual void AddData( const struct iovec &aIOV )
  {
    struct iovec *iov = &(mVector[ mVectorCount ]);
    iov->iov_base = aIOV.iov_base;
    iov->iov_len = aIOV.iov_len;
    mDataSize += aIOV.iov_len;
    mVectorCount++;
  }
  inline size_t GetDataLen() const { return mDataSize; }
  virtual iWARPEM_Status_t FlushToSocket( const int aSocket, int *wlen )
  {
    UpdateHeaderLen( GetDataLen() );
    return write_to_socket( aSocket, mVector, mVectorCount, wlen );
  }
  inline void Reset()
  {
    mVectorCount = 1;
    mDataSize = 0;
    UpdateHeaderLen( 0 );
    mIntBufferPosition = mIntBuffer + sizeof( it_api_multiplexed_socket_message_header_t );
  }
  inline void SetHeader( const it_api_multiplexed_socket_message_header_t &aHdr )
  {
    mHeader->MsgType = aHdr.MsgType;
    mHeader->ProtocolVersion = aHdr.ProtocolVersion;
  }
  inline void SetHeader( const unsigned char aProtocolVersion, const unsigned char aMsgType )
  {
    mHeader->MsgType = aMsgType;
    mHeader->ProtocolVersion = aProtocolVersion;
  }
};

#endif /* IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_BUFFER_HPP_ */
