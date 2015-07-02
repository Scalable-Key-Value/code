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

#define IWARPEM_SOCKET_DATA_ALIGNMENT_MASK ( 0xFF )
#define IWARPEM_SOCKET_DATA_ALIGNMENT ( 256 )

#define IWARPEM_SOCKET_HEADER_ALIGNMENT_MASK ( 0x7 )
#define IWARPEM_SOCKET_HEADER_ALIGNMENT ( 8 )


class iWARPEM_Socket_Buffer_t
{
protected:
  it_api_multiplexed_socket_message_header_t *mHeader;
  size_t mHeadSpace;

  inline void UpdateHeaderLen( const size_t aLen )
  {
    mHeader->DataLen = aLen;
  }
public:
  iWARPEM_Socket_Buffer_t( const size_t aBufferSize = IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE,
                           const bool aAllowCopy = false )
  {
    mHeader = NULL;
    mHeadSpace = AlignedHeaderPosition( (const char*) sizeof( it_api_multiplexed_socket_message_header_t ) );
  }
  virtual ~iWARPEM_Socket_Buffer_t() { mHeader = NULL; };

  virtual void ConfigureAsReceiveBuffer() = 0;
  inline size_t GetRemainingSize() const;
  virtual void* AddHdr( const char* aHdrPtr, const size_t aSize ) = 0;
  virtual void AddData( const char* aBuffer, const size_t aSize ) = 0;
  virtual void AddDataContigous( const char *aBuffer, const size_t aSize ) = 0;

  virtual char* GetHdrPtr( size_t *aSize, uintptr_t aAdvancePos = 1 ) = 0;
  virtual size_t GetData( char ** aBuffer, size_t *aSize ) = 0;
  inline size_t GetDataLen() const;
  inline bool FlushRecommended() const;
  virtual iWARPEM_Status_t FlushToSocket( const int aSocket, int *wlen ) = 0;
  virtual iWARPEM_Status_t FillFromSocket( const int aSocket ) = 0;
  inline void Reset();

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

  inline uintptr_t AlignedDataPosition( const char* aAddr )
  {
    uintptr_t base = (uintptr_t)aAddr;
    return ( (base - 1) & (~IWARPEM_SOCKET_DATA_ALIGNMENT_MASK )) + IWARPEM_SOCKET_DATA_ALIGNMENT;
  }
  inline uintptr_t AlignedHeaderPosition( const char* aAddr )
  {
    uintptr_t base = (uintptr_t)aAddr;
    return ( (base - 1) & (~IWARPEM_SOCKET_HEADER_ALIGNMENT_MASK )) + IWARPEM_SOCKET_HEADER_ALIGNMENT;
  }
};

class iWARPEM_Memory_Socket_Buffer_t : public iWARPEM_Socket_Buffer_t
{
  char *mShadowBufferStart;   // keep track of the actually allocated buffer in case we're not aligned
  char *mBuffer;
  char *mCurrentPosition;
  char *mBufferEnd;

  size_t mBufferSize;
  bool mAllowCopy;
  bool mIsRecvBuffer;

public:
  iWARPEM_Memory_Socket_Buffer_t( const size_t aBufferSize = IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE,
                                  const bool aAllowCopy = true )
  {
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Creating Data Buffer. size=" << aBufferSize
      << EndLogLine;

    mBufferSize = aBufferSize;
    size_t AllocSize = mBufferSize
        + sizeof( it_api_multiplexed_socket_message_header_t )
        + 2*IWARPEM_SOCKET_DATA_ALIGNMENT   // room for alignment shift
        + sizeof(uintptr_t);

    mShadowBufferStart = (char*)new uintptr_t[ AllocSize / sizeof(uintptr_t) ];

    mHeader = (struct it_api_multiplexed_socket_message_header_t*)AlignedDataPosition( mShadowBufferStart );
    bzero( mHeader, IWARPEM_SOCKET_DATA_ALIGNMENT );

    mBuffer = (char*)mHeader + mHeadSpace;

    AssertLogLine( ((uintptr_t)mBuffer & IWARPEM_SOCKET_HEADER_ALIGNMENT_MASK ) == 0 )
      << "Buffer is not aligned to " << IWARPEM_SOCKET_HEADER_ALIGNMENT << " Bytes."
      << " @: " << (void*)mBuffer
      << EndLogLine;

    mIsRecvBuffer = false;
    mAllowCopy = aAllowCopy;
    mBufferEnd = mBuffer + mBufferSize;

    Reset();
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Created Data Buffer. size=" << aBufferSize
      << " Header=0x" << (void*)mHeader
      << " Buffer=0x" << (void*)mBuffer
      << " End=0x" << (void*)mBufferEnd
      << EndLogLine;
  }
  ~iWARPEM_Memory_Socket_Buffer_t()
  {
    delete mShadowBufferStart;
    mShadowBufferStart = NULL;
    mBuffer = NULL;
    mCurrentPosition = NULL;
  }

  inline void ConfigureAsReceiveBuffer()
  {
    mIsRecvBuffer = true;
    mBufferEnd = mBuffer;
    mAllowCopy = false; 
  }
  inline size_t GetRemainingSize() const { return (size_t)(mBufferEnd - mCurrentPosition); }
  // inserts the data at the next header-aligned position and returns the destination pointer
  inline void* AddHdr( const char* aHdrPtr, const size_t aSize )
  {
    mCurrentPosition = (char*)AlignedHeaderPosition( mCurrentPosition );
    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Injecting header @0x" << (void*)mCurrentPosition
      << " aSize=" << aSize
      << " BufferOffset=" << GetDataLen()
      << EndLogLine;

    if( GetRemainingSize() < aSize )
      return NULL;
    if( mAllowCopy )
    {
      memcpy( mCurrentPosition, aHdrPtr, aSize );
    }
    void* retval = (void*)mCurrentPosition;
    mCurrentPosition += aSize;
    return retval;
  }
  inline void AddData( const char* aBuffer, const size_t aSize )
  {
    StrongAssertLogLine( (mAllowCopy) || (aBuffer == mCurrentPosition ) )
      << "Socket buffer configured without memcpy. But src and dest buffer differ:"
      << "@" << (void*)aBuffer
      << " vs. @" << (void*)mCurrentPosition
      << " request size was: " << aSize
      << EndLogLine;

    mCurrentPosition = (char*)AlignedDataPosition( mCurrentPosition );

    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Injecting data @0x" << (void*)mCurrentPosition
      << " BufferOffset=" << GetDataLen()
      << " aSize=" << aSize
      << EndLogLine;

    if(( mAllowCopy ) && ( aBuffer != mCurrentPosition ))
      memcpy( mCurrentPosition, aBuffer, aSize );

    mCurrentPosition += aSize;
  }
  // inject data to the current position without watching alignment
  // required to transmit io-vectors properly
  inline void AddDataContigous( const char *aBuffer, const size_t aSize )
  {
    StrongAssertLogLine( (mAllowCopy) || (aBuffer == mCurrentPosition ) )
      << "Socket buffer configured without memcpy. But src and dest buffer differ:"
      << "@" << (void*)aBuffer
      << " vs. @" << (void*)mCurrentPosition
      << " request size was: " << aSize
      << EndLogLine;

    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Injecting data @0x" << (void*)mCurrentPosition
      << " BufferOffset=" << GetDataLen()
      << " aSize=" << aSize
      << EndLogLine;

    if(( mAllowCopy ) && ( aBuffer != mCurrentPosition ))
      memcpy( mCurrentPosition, aBuffer, aSize );

    mCurrentPosition += aSize;
  }
  // returns pointer to potential header data at next header-aligned location
  // forwards the currentposition!
  char* GetHdrPtr( size_t *aSize, uintptr_t aAdvancePos = 1 )
  {
    mCurrentPosition = (char*)AlignedHeaderPosition( mCurrentPosition );
    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Start header retrieval @0x" << (void*)mCurrentPosition
      << " aSize=" << *aSize
      << " available=" << RecvDataAvailable()
      << " advance=" << aAdvancePos
      << EndLogLine;
    if( *aSize > RecvDataAvailable() )
      *aSize = RecvDataAvailable();

    char *retval = mCurrentPosition;
    mCurrentPosition += (*aSize) * ( aAdvancePos & 1 );
    return retval;
  }
  size_t GetData( char ** aBuffer, size_t *aSize )
  {
    mCurrentPosition = (char*)AlignedDataPosition( mCurrentPosition );
    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Start data retrieval @0x" << (void*)mCurrentPosition
      << " aSize=" << *aSize
      << " available=" << RecvDataAvailable()
      << EndLogLine;
    if( *aSize > RecvDataAvailable() )
      *aSize = RecvDataAvailable();

    if( mAllowCopy )
      memcpy( *aBuffer, mCurrentPosition, *aSize );
    else
      *aBuffer = mCurrentPosition;

    mCurrentPosition += (*aSize);
    return *aSize;
  }
  inline size_t GetDataLen() const
  {
    return (size_t)(mCurrentPosition - mBuffer);
  }
  inline bool FlushRecommended() const { return GetDataLen() > ( mBufferSize - (mBufferSize >> 3) ); }
  inline size_t RecvDataAvailable() const
  {
    if( mBufferEnd < mCurrentPosition )
      return 0;
    return ( ((uintptr_t)mBufferEnd - (uintptr_t)mCurrentPosition) );
  }
  inline iWARPEM_Status_t FlushToSocket( const int aSocket, int *wlen )
  {
    UpdateHeaderLen( GetDataLen() );
    iWARPEM_Status_t rc = write_to_socket( aSocket, (char*)mHeader, mCurrentPosition - (char*)mHeader, wlen );
    if( *wlen > mHeadSpace )
      *wlen -= mHeadSpace;
    return rc;
  }

  inline bool ReceiveProtocolHeader( int aSocket )
  {
    bool HdrCorrect = false;
    int rlen_expected;
    int rlen = mHeadSpace;

    iWARPEM_Status_t istatus = read_from_socket( aSocket,
                                                 (char *) mHeader,
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

    StrongAssertLogLine( mHeader->ProtocolVersion == IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION )
      << "Protocol version mismatch. Recompile client and/or server to match."
      << " server: " << IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION
      << " client: " << mHeader->ProtocolVersion
      << EndLogLine;

    HdrCorrect = ( mHeader->ProtocolVersion == IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION );
    HdrCorrect &= ( istatus == IWARPEM_SUCCESS || ( rlen == rlen_expected ) );

    int ReceiveDataLen = mHeader->DataLen;

    StrongAssertLogLine( ReceiveDataLen <= IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE )
      << "iWARPEM_Router_Endpoint_t: data length will exceed the buffer size"
      << " max: " << IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE
      << " actual: " << ReceiveDataLen
      << EndLogLine;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Received multiplex header: "
      << " socket: " << aSocket
      << " version: " << mHeader->ProtocolVersion
      << " datalen: " << mHeader->DataLen
      << " HdrCorrect: " << (int)HdrCorrect
      << EndLogLine;

    return HdrCorrect;
  }

  inline iWARPEM_Status_t FillFromSocket( const int aSocket )
  {
    if( ! ReceiveProtocolHeader( aSocket ) )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "iWARPEM_Router_Endpoint_t:: Error receiving protocol header."
        << EndLogLine;
      return IWARPEM_ERRNO_CONNECTION_RESET;
    }

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Refilling recv buffer for socket: " << aSocket
      << " expecting datalen: " << mHeader->DataLen
      << EndLogLine;

    int len;
    iWARPEM_Status_t istatus = read_from_socket( aSocket,
                                                 mBuffer,
                                                 mHeader->DataLen,
                                                 &len );
    if( ( istatus != IWARPEM_SUCCESS ) || ( len != mHeader->DataLen )  )
    {
      BegLogLine( 1 )
        << "iWARPEM_Router_Endpoint_t: error reading from socket or data length mismatch:"
        << " status: " << (int)istatus
        << " expected: " << mHeader->DataLen
        << " actual: " << len
        << EndLogLine;

      return IWARPEM_ERRNO_CONNECTION_RESET;
    }

    mCurrentPosition = mBuffer;
    mBufferEnd = mCurrentPosition + len;

    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Refilled recv buffer for socket: " << aSocket
      << " datalen: " << mHeader->DataLen
      << EndLogLine;

    return IWARPEM_SUCCESS;
  }

  inline void Reset()
  {
    UpdateHeaderLen( 0 );
    mCurrentPosition = mBuffer;
    if( mIsRecvBuffer )
      mBufferEnd = mBuffer;
  }
};

#define IWARPEM_MAX_VECTOR_COUNT ( 128 * 1024 )

class iWARPEM_WriteV_Socket_Buffer_t : public iWARPEM_Socket_Buffer_t
{
  struct iovec *mVector;
  size_t mVectorCount;
  size_t mVectorMax;

  size_t mDataSize;

  char *mShadowBufferStart;
  char *mIntBuffer;
  char *mIntBufferPosition;
  size_t mIntBufferSize;

  bool mAllowCopy;

public:
  iWARPEM_WriteV_Socket_Buffer_t( const size_t aBufferSize = IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE,
                                  const bool aAllowCopy = true )
  {
    BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
      << "Creating Internal Data Buffer for headers and gaps. size=" << aBufferSize
      << EndLogLine;

    // prepare internal buffer space for on-the-fly vectors by AddData() (e.g. for headers)
    mIntBufferSize = aBufferSize;
    size_t AllocSize = mIntBufferSize
        + mHeadSpace
        + 2*IWARPEM_SOCKET_DATA_ALIGNMENT   // room for alignment shift
        + sizeof(uintptr_t);

    mShadowBufferStart = (char*)new uintptr_t[ AllocSize / sizeof(uintptr_t) ];

    mHeader = (struct it_api_multiplexed_socket_message_header_t*)AlignedDataPosition( mShadowBufferStart );
    bzero( mHeader, IWARPEM_SOCKET_DATA_ALIGNMENT );

    mIntBuffer = (char*)mHeader + mHeadSpace;

    AssertLogLine( ((uintptr_t)mIntBuffer & IWARPEM_SOCKET_HEADER_ALIGNMENT_MASK ) == 0 )
      << "Buffer is not aligned to " << IWARPEM_SOCKET_HEADER_ALIGNMENT << " Bytes."
      << " @: " << (void*)mIntBuffer
      << EndLogLine;

    mAllowCopy = aAllowCopy;

    mVectorMax = IWARPEM_MAX_VECTOR_COUNT;
    mVector = new struct iovec[ IWARPEM_MAX_VECTOR_COUNT ];

    Reset();
  }
  ~iWARPEM_WriteV_Socket_Buffer_t()
  {
    delete mShadowBufferStart;
    delete mVector;
  }

  void ConfigureAsReceiveBuffer()
  {
    StrongAssertLogLine( 1 )
      << "WriteV based buffer not possible to use as receive buffer!"
      << EndLogLine;
  }
  inline size_t GetRemainingSize() const { return mIntBufferSize - mDataSize; }
  inline void InjectPaddingVector( const size_t aPaddingSize )
  {
    mIntBufferPosition = (char*)AlignedDataPosition( mIntBufferPosition );
    if( aPaddingSize > 0 )
    {
      BegLogLine( FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG )
        << "Need to skip to next aligned spot. injecting "
        << " vector #" << mVectorCount
        << " padding=" << aPaddingSize
        << " offset=" << mDataSize
        << EndLogLine;

      struct iovec *iov = &(mVector[ mVectorCount ]);
      iov->iov_base = mIntBufferPosition;
      iov->iov_len = aPaddingSize;
      mDataSize += aPaddingSize;
      mVectorCount++;
      // no need to advance the IntBufferPosition for padding!
    }
  }
  inline void SkipToNextHeaderAlignment()
  {
    size_t padding = AlignedHeaderPosition( (char*)mDataSize ) - mDataSize;
    InjectPaddingVector( padding );
  }
  inline void SkipToNextDataAlignment()
  {
    size_t padding = AlignedDataPosition( (char*)mDataSize ) - mDataSize;
    InjectPaddingVector( padding );
  }

  virtual void* AddHdr( const char* aHdrPtr, const size_t aSize )
  {
    AssertLogLine( mVectorCount < mVectorMax - 2 )
      << "VectorCount exceeds limit: mVectorCount=" << mVectorCount
      << " mVectorMax=" << mVectorMax
      << " need at least 2 space"
      << EndLogLine;

    SkipToNextHeaderAlignment();

    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Injecting header @0x" << (void*)mIntBufferPosition
      << " aSize=" << aSize
      << " vector #" << mVectorCount
      << " BufferOffset=" << GetDataLen()
      << " mAllowCopy=" << mAllowCopy
      << EndLogLine;

    struct iovec *iov = &(mVector[ mVectorCount ]);
    iov->iov_len = aSize;
    if( mAllowCopy )
    {
      iov->iov_base = (void*)mIntBufferPosition;
      memcpy( mIntBufferPosition, aHdrPtr, aSize );
      mIntBufferPosition += aSize;
    }
    else
      iov->iov_base = (void*)aHdrPtr;

    mDataSize += aSize;
    mVectorCount++;
    return NULL;
  }
  virtual void AddData( const char* aBuffer, const size_t aSize )
  {
    AssertLogLine( mVectorCount < mVectorMax - 2 )
      << "VectorCount exceeds limit: mVectorCount=" << mVectorCount
      << " mVectorMax=" << mVectorMax
      << " need at least 2 space"
      << EndLogLine;

    SkipToNextDataAlignment();

    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Injecting data @0x" << (void*)aBuffer
      << " aSize=" << aSize
      << " vector #" << mVectorCount
      << " BufferOffset=" << GetDataLen()
      << EndLogLine;

    struct iovec *iov = &(mVector[ mVectorCount ]);
    iov->iov_base = (void*)aBuffer;
    iov->iov_len = aSize;
    mDataSize += aSize;
    mVectorCount++;
  }
  virtual void AddDataContigous( const char *aBuffer, const size_t aSize )
  {
    AssertLogLine( mVectorCount < mVectorMax - 1 )
      << "VectorCount exceeds limit: mVectorCount=" << mVectorCount
      << " mVectorMax=" << mVectorMax
      << " need at least 1 space"
      << EndLogLine;

    BegLogLine(FXLOG_IT_API_O_SOCKETS_MULTIPLEX_LOG)
      << " Injecting data @0x" << (void*)aBuffer
      << " aSize=" << aSize
      << " vector #" << mVectorCount
      << " BufferOffset=" << GetDataLen()
      << EndLogLine;

    struct iovec *iov = &(mVector[ mVectorCount ]);
    iov->iov_base = (void*)aBuffer;
    iov->iov_len = aSize;
    mDataSize += aSize;
    mVectorCount++;
  }
  inline size_t GetDataLen() const { return mDataSize - mHeadSpace; }
  inline bool FlushRecommended() const
  {
    return ( (mVectorCount > ( mVectorMax - (mVectorMax >> 3) ))
        || ( (mDataSize > ( mIntBufferSize - (mIntBufferSize>>3 )) )) );
  }
  virtual iWARPEM_Status_t FlushToSocket( const int aSocket, int *wlen )
  {
    AssertLogLine( GetDataLen() < IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE )
      << "Send data size will be too large for the receiver"
      << " len=" << GetDataLen()
      << " max=" << IT_API_MULTIPLEX_SOCKET_BUFFER_SIZE
      << EndLogLine;

    UpdateHeaderLen( GetDataLen() );
    AssertLogLine(( mHeader->ProtocolVersion == IWARPEM_MULTIPLEXED_SOCKET_PROTOCOL_VERSION ) &&
                  (mHeader->MsgType == MULTIPLEXED_SOCKET_MSG_TYPE_DATA))
      << "Something has modified the header before sending."
      << " Protocol=" << mHeader->ProtocolVersion
      << " MsgType=" << mHeader->MsgType
      << " DataLen=" << mHeader->DataLen
      << EndLogLine;

    iWARPEM_Status_t rc = write_to_socket( aSocket, mVector, mVectorCount, mDataSize, wlen );

    AssertLogLine( *wlen == (mHeader->DataLen + mHeadSpace) )
      << "WriteV reports wrong data length sent=" << *wlen
      << " expected=" << mHeader->DataLen + mHeadSpace
      << EndLogLine;

    if( *wlen > mHeadSpace )
      *wlen -= mHeadSpace;
    return rc;
  }
  inline void Reset()
  {
    StrongAssertLogLine( mHeadSpace == AlignedHeaderPosition( (const char *)sizeof( it_api_multiplexed_socket_message_header_t ) ) )
      << "ERROR: Something has stomped on WriteV-Buffer Memory. "
      << " mHeadSpace=" << mHeadSpace
      << " expected=" << AlignedHeaderPosition( (const char*)sizeof( it_api_multiplexed_socket_message_header_t ) )
      << EndLogLine;

    struct iovec *iov = &(mVector[ 0 ]);
    iov->iov_base = mHeader;
    iov->iov_len = mHeadSpace;

    mVectorCount = 1;
    mDataSize = mHeadSpace;
    UpdateHeaderLen( 0 );
    mIntBufferPosition = mIntBuffer;
  }
  virtual char* GetHdrPtr( size_t *aSize, uintptr_t aAdvancePos = 1 ) { return NULL; }
  virtual size_t GetData( char ** aBuffer, size_t *aSize ) { return 0; }
  virtual iWARPEM_Status_t FillFromSocket( const int aSocket ) { return IWARPEM_ERRNO_CONNECTION_RESET; }
};

#endif /* IT_API_CNK_ROUTER_IT_API_CNK_ROUTER_EP_BUFFER_HPP_ */
