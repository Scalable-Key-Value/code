#ifndef IT_API_O_SOCKETS_CN_ION_H
#define IT_API_O_SOCKETS_CN_ION_H

#ifndef FXLOG_IONCN_BUFFER
#define FXLOG_IONCN_BUFFER (0)
#endif

#ifndef FXLOG_IONCN_BUFFER_DETAIL
#define FXLOG_IONCN_BUFFER_DETAIL (0)
#endif

#ifndef FXLOG_ITAPI_ROUTER_FRAMES 
#define FXLOG_ITAPI_ROUTER_FRAMES (0)
#endif

#define CN_ION_SENTINEL_LENGTH_VALUE ( 0xffffffffffffffffUL )

#include <string.h>
#include <it_api_o_sockets_types.h>

#define CNK_ROUTER_BUFFER_SIZE ( 32 * 1024 * 1024ull )
#define CNK_ROUTER_PORT ( 10950 )

enum {
  k_ApplicationBufferSize= CNK_ROUTER_BUFFER_SIZE ,
  k_IONPort = CNK_ROUTER_PORT
};

struct connection *conn ;
class ion_cn_buffer
  {
public:
  volatile unsigned long mSentBytes ;
  volatile unsigned long mReceivedBytes ;
  char mApplicationBuffer[k_ApplicationBufferSize] ;
  void Init(void)
    {
     mSentBytes=0 ;
     mReceivedBytes=0 ;
    } ;
  bool rawTransmit(unsigned long aTransmitCount, uint32_t aLkey) ;
  bool rawTransmit(struct connection *conn, unsigned long aTransmitCount) ;
  bool pushAckOnly(struct connection *conn, unsigned long aSentBytes, unsigned long aReceivedBytes );

  bool Transmit(unsigned long aSentBytes, unsigned long aReceivedBytes, unsigned long aTransmitCount, uint32_t aLkey)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "this=0x" << (void *) this
        << " aSentBytes=" << aSentBytes
        << " aReceivedBytes=" << aReceivedBytes
        << " aTransmitCount=" << aTransmitCount
        << " aLkey=0x" << aLkey
        << EndLogLine ;
      AssertLogLine(aTransmitCount <= k_ApplicationBufferSize)
        << "aTransmitCount=" << aTransmitCount
        << " k_ApplicationBufferSize=" << k_ApplicationBufferSize
        << " Attempt to transmit more than buffer"
        << EndLogLine ;
      BegLogLine(FXLOG_IONCN_BUFFER_DETAIL)
        << "*this=" << HexDump(this,aTransmitCount)
        <<  EndLogLine ;
      mSentBytes=aSentBytes ;
      mReceivedBytes=aReceivedBytes ;
      bool rc=rawTransmit(aTransmitCount,aLkey) ;
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "rawTransmit returns " << rc
        << EndLogLine ;
      return rc ;
    } ;
  enum
  {
    k_SentinelModulo=256
  };
  unsigned long SentinelIndex(unsigned long aTransmitCount)
    {
      unsigned long blocks=(aTransmitCount+1) / k_SentinelModulo ;
      unsigned long rc=(blocks+1) * k_SentinelModulo - 1 ;
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "aTransmitCount=" << aTransmitCount
        << " blocks=" << blocks
        << " rc=" << rc
        << EndLogLine ;
      return rc ;
    }
  void SetSentinels(void)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "Setting sentinels in buffer at 0x" << (void *) this
          << " oldvalues: mSentBytes=" << mSentBytes
          << " mReceivedBytes=" << mReceivedBytes
          << EndLogLine ;
      unsigned long SentinelCount=k_ApplicationBufferSize/k_SentinelModulo ;
      for(unsigned long x=0;x<SentinelCount;x+=1)
        {
          mApplicationBuffer[x*k_SentinelModulo-1] = 0 ;
        }
      mSentBytes=CN_ION_SENTINEL_LENGTH_VALUE;
      mReceivedBytes=CN_ION_SENTINEL_LENGTH_VALUE;
    }
  bool Transmit(struct connection *conn, unsigned long aSentBytes, unsigned long aReceivedBytes, unsigned long aTransmitCount)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "conn=0x" << (void *) conn
        << " this=0x" << (void *) this
        << " aSentBytes=" << aSentBytes
        << " aReceivedBytes=" << aReceivedBytes
        << " aTransmitCount=" << aTransmitCount
        << EndLogLine ;
      AssertLogLine(aTransmitCount <= k_ApplicationBufferSize)
        << "aTransmitCount=" << aTransmitCount
        << " k_ApplicationBufferSize=" << k_ApplicationBufferSize
        << " Attempt to transmit more than buffer"
        << EndLogLine ;
      BegLogLine(FXLOG_IONCN_BUFFER_DETAIL)
        << "*this=" << HexDump(this,aTransmitCount)
        <<  EndLogLine ;
      mSentBytes=aSentBytes ;
      mReceivedBytes=aReceivedBytes ;
      bool rc=rawTransmit(conn,aTransmitCount) ;
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "rawTransmit returns " << rc
        << EndLogLine ;
      return rc ;
    } ;
  bool CheckReceive(unsigned long aReceivedBytes, unsigned long *aBytesThisTime )
    {
      unsigned long ReceivedBytes = mSentBytes ;
      unsigned long BytesThisTime = ReceivedBytes-aReceivedBytes ;
      *aBytesThisTime=BytesThisTime ;
      return ReceivedBytes > aReceivedBytes && ReceivedBytes != CN_ION_SENTINEL_LENGTH_VALUE;
    }
  bool Receive(unsigned long aReceivedBytes, unsigned long * aReceivedThisTime)
    {
      unsigned long ReceivedBytes = mSentBytes ;
      // BegLogLine( mReceivedBytes != CN_ION_SENTINEL_LENGTH_VALUE )
      //   << "Real data available in buffer. this=0x" << (void*)this
      //   << " mSentBytes=" << mSentBytes
      //   << " mReceivedBytes=" << mReceivedBytes
      //   << EndLogLine;
        
      if ( ReceivedBytes < aReceivedBytes || ReceivedBytes == CN_ION_SENTINEL_LENGTH_VALUE ) return false ;
      unsigned long BytesThisTime = ReceivedBytes-aReceivedBytes ;
      AssertLogLine(BytesThisTime <= k_ApplicationBufferSize)
        << "BytesThisTime=" << BytesThisTime
        << " overflows buffer. ReceivedBytes=" << ReceivedBytes
        << " mReceivedBytes=" << mReceivedBytes
        << " aReceivedBytes=" << aReceivedBytes
        << EndLogLine ;
      unsigned long Sentinel=SentinelIndex(BytesThisTime) ;
      unsigned int SentinelValue=mApplicationBuffer[Sentinel] ;
      bool rc=(SentinelValue != 0) ;
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "Reception started"
        << " aReceivedBytes=" << aReceivedBytes
        << " ReceivedBytes=" << ReceivedBytes
        << " mSentBytes=" << mSentBytes
        << " mReceivedBytes=" << mReceivedBytes
        << " BytesThisTime=" << BytesThisTime
        << " Sentinel=" << Sentinel
        << " SentinelValue=" << SentinelValue
        << " rc=" << rc
        << EndLogLine ;
      AssertLogLine(SentinelValue == (rc ? 0xff : 0 ) )
        << "Incorrect sentinel value, rc=" << rc
        << " SentinelValue=" << SentinelValue
        << EndLogLine ;
      *aReceivedThisTime = BytesThisTime ;
      BegLogLine( (FXLOG_IONCN_BUFFER  | FXLOG_ITAPI_ROUTER_FRAMES ) && rc ) 
        << "this=0x" << (void *) this
        << " RX-FRAME {" << mSentBytes
        << "," << mReceivedBytes
        << "," << BytesThisTime
        << "}"
        << EndLogLine ;
      return rc ;
    } ;
  void HandleBuffer(unsigned long aLength) ;
  void CopyToBuffer(unsigned long aDest, const void *aSrc, unsigned long aLength)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "this=0x" << (void *) this
        << " aDest=" << aDest
        << " aSrc=" << (void *) aSrc
        << " aLength=" << aLength
        << EndLogLine ;
      BegLogLine(FXLOG_IONCN_BUFFER_DETAIL)
        << HexDump((void *)aSrc,aLength)
        << EndLogLine ;
      AssertLogLine(aDest+aLength <= k_ApplicationBufferSize)
        << "aDest=" << aDest
        << " + aLength=" << aLength
        << " > k_ApplicationBufferSize"
        << EndLogLine ;
      memcpy(mApplicationBuffer+aDest,aSrc,aLength) ;
    } ;
  void IssueRDMARead(struct connection *conn, unsigned long Offset, unsigned long Length) ;
  int ProcessCall(struct connection *conn) ;
  void ProcessRead(struct connection *conn);
  void ProcessReceiveBuffer(struct connection *conn, bool contained_ack ) ;
  void PostReceive(struct connection *conn) ;
  };

enum {
  k_LargestRDMASend=512 ,
  k_InitialRDMASend=sizeof(class ion_cn_buffer) - k_ApplicationBufferSize
};

class ion_cn_buffer_pair
  {
public:
  class ion_cn_buffer mBuffer[2] ;
  unsigned long mBlockCount ;
  void Init(void)
    {
      mBuffer[0].Init() ;
      mBuffer[1].Init() ;
      mBlockCount = 0 ;
    } ;
  unsigned int CurrentBufferIndex(void)
    {
      return mBlockCount & 1 ;
    }
  bool Transmit(unsigned long aSentBytes, unsigned long aReceivedBytes, unsigned long aTransmitCount, uint32_t aLkey)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "aSentBytes=" << aSentBytes
          << " aReceivedBytes=" << aReceivedBytes
          << " aTransmitCount=" << aTransmitCount
          << " mBlockCount=0x" << mBlockCount
          << " CurrentBufferIndex()=" << CurrentBufferIndex()
          << EndLogLine ;
      bool rc=mBuffer[CurrentBufferIndex()].Transmit(aSentBytes,aReceivedBytes,aTransmitCount,aLkey) ;
      if ( rc )
        {
          mBlockCount += 1 ;
        }
      return rc ;
    } ;
  bool Transmit(struct connection *conn, unsigned long aSentBytes, unsigned long aReceivedBytes, unsigned long aTransmitCount)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "conn=0x" << (void *) conn
          << " aSentBytes=" << aSentBytes
          << " aReceivedBytes=" << aReceivedBytes
          << " aTransmitCount=" << aTransmitCount
          << " mBlockCount=0x" << mBlockCount
          << " CurrentBufferIndex()=" << CurrentBufferIndex()
          << EndLogLine ;
      bool rc=mBuffer[CurrentBufferIndex()].Transmit(conn,aSentBytes,aReceivedBytes,aTransmitCount) ;
      if ( rc )
        {
          mBlockCount += 1 ;
        }
      return rc ;
    } ;
  bool PostAckOnly(struct connection *conn, unsigned long aSentBytes, unsigned long aReceivedBytes )
  {
    return mBuffer[ CurrentBufferIndex() ].pushAckOnly( conn, aSentBytes, aReceivedBytes );
  }
  enum {
    k_CheckOtherBuffer=1
  };
  bool Receive(unsigned long aReceivedCount, unsigned long *aBytesThisTime)
    {
    static unsigned long idlecount = 0;
      unsigned int CurrentBuffer=CurrentBufferIndex() ;
      bool rc=mBuffer[CurrentBuffer].Receive(aReceivedCount,aBytesThisTime) ;
      BegLogLine(FXLOG_IONCN_BUFFER )
          << "aReceivedCount=" << aReceivedCount
          << " *aBytesThisTime=" << *aBytesThisTime
          << " mBlockCount=0x" << mBlockCount
          << " CurrentBuffer=" << CurrentBuffer
          << EndLogLine ;
      if ( rc )
        {
//These members now in ion_cn_all_buffer
//          mAckedSentBytes=mBuffer[CurrentBuffer].mSentBytes ;
//          mAckedReceivedBytes=mBuffer[CurrentBuffer].mReceivedBytes ;
        idlecount=0;
        }
      else if ( k_CheckOtherBuffer )
        {
          unsigned long OtherBytesThisTime ;
          bool rc_other=mBuffer[1-CurrentBuffer].CheckReceive(aReceivedCount, &OtherBytesThisTime) ;
          BegLogLine( ( FXLOG_IONCN_BUFFER | FXLOG_ITAPI_ROUTER_FRAMES ) && rc_other )
            << "New received frame in other buffer, CurrentBuffer=" << CurrentBuffer
            << " OtherBytesThisTime=" << OtherBytesThisTime
            << " BufAddr=0x" << (void*)&(mBuffer[1-CurrentBuffer])
            << EndLogLine ;
        }
      if( ! rc )
        {
        idlecount++;
        // BegLogLine( 0 == (idlecount % 0xffff) )
        //   << "Receive still idle: " << idlecount
        //   << EndLogLine;
        }
      return rc ;
    } ;
  void HandleBuffer(unsigned long aLength)
    {
      mBuffer[CurrentBufferIndex()].HandleBuffer(aLength) ;
      mBlockCount += 1 ;
    }
  void CopyToBuffer(unsigned long aDest, const void *aSrc, unsigned long aLength)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "mBlockCount=" << mBlockCount
          << " CurrentBufferIndex()=" << CurrentBufferIndex()
          << " aDest=" << aDest
          << " aSrc=" << (void *)aSrc
          << " aLength=" << aLength
          << EndLogLine ;
      BegLogLine(FXLOG_IONCN_BUFFER_DETAIL)
          << " aSrc=" << HexDump((void *)aSrc,aLength)
          << EndLogLine ;
      mBuffer[CurrentBufferIndex()].CopyToBuffer(aDest,aSrc,aLength) ;
    } ;
  int ProcessCall(struct connection *conn)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << " mBlockCount=" << mBlockCount
          << " CurrentBufferIndex()=" << CurrentBufferIndex()
          << EndLogLine ;
      return mBuffer[CurrentBufferIndex()].ProcessCall(conn) ;
    }
  void ProcessRead(struct connection *conn)
  {
    BegLogLine(FXLOG_IONCN_BUFFER)
      << " mBlockCount=" << mBlockCount
      << " CurrentBufferIndex()=" << CurrentBufferIndex()
      << EndLogLine;
    mBuffer[CurrentBufferIndex()].ProcessRead(conn);
  }
  void PostReceive(struct connection *conn)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << " mBlockCount=0x" << mBlockCount
          << " CurrentBufferIndex()=" << CurrentBufferIndex()
          << EndLogLine ;
      mBuffer[CurrentBufferIndex()].PostReceive(conn) ;
      mBlockCount += 1 ;
    }
  void PostAllReceives(struct connection *conn)
    {
      mBuffer[0].PostReceive(conn) ;
      mBuffer[1].PostReceive(conn) ;
    }
//  void AdvanceAcked(unsigned long aAckedSentBytes)
//    {
//      BegLogLine(FXLOG_IONCN_BUFFER)
//        << "Advancing acked sent bytes from mAckedSentBytes=" <<  mAckedSentBytes
//        << " to aAckedSentBytes=" << aAckedSentBytes
//        << EndLogLine ;
//      mAckedSentBytes=aAckedSentBytes ;
//    }
  };

class ion_cn_all_buffer
  {
public:
  unsigned long mSentBytes ;
  unsigned long mReceivedBytes ;
  unsigned long mAckedSentBytes ;
  unsigned long mSentBytesPrevious ;
//  unsigned long mAckedReceivedBytes ;
  unsigned long mTransmitBufferIndex ;
  unsigned long mReceiveBufferLength ;
  uint32_t mLkey ;
  pthread_spinlock_t mTransmitMutex ;
  class ion_cn_buffer_pair mTransmitBuffer ;
  class ion_cn_buffer_pair mReceiveBuffer ;
  class ion_cn_buffer mRemoteWrittenSendAckBytes;
  volatile bool mBufferRequiresAck;
  volatile bool mBufferAllowsAck;
  void Init(void)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "Initialising, this=0x" << (void *) this
          << EndLogLine ;
      mTransmitBuffer.Init() ;
      mReceiveBuffer.Init() ;
      mRemoteWrittenSendAckBytes.Init();
      mReceiveBuffer.mBuffer[0].SetSentinels() ;
      mReceiveBuffer.mBuffer[1].SetSentinels() ;
      mSentBytes=0 ;
      mReceivedBytes=0 ;
      mAckedSentBytes=0 ;
      mSentBytesPrevious=0 ;
//      mAckedReceivedBytes=0 ;
      mTransmitBufferIndex=0 ;
      mBufferRequiresAck=false;
      mBufferAllowsAck=false;
      mLkey=0 ;
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "Initialised, this=0x" << (void *) this
          << EndLogLine ;
    } ;
  void Term(void)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "Terminating, this=0x" << (void *) this
          << EndLogLine ;
      pthread_spin_destroy(&mTransmitMutex) ;
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "Terminated, this=0x" << (void *) this
          << EndLogLine ;
    }
  void LockTransmit(void)
    {
      pthread_spin_lock(&mTransmitMutex) ;
    }
  void UnlockTransmit(void)
    {
      pthread_spin_unlock(&mTransmitMutex) ;
    }
  unsigned long GetRemoteAckedBytes( void )
  {
    return mRemoteWrittenSendAckBytes.mReceivedBytes;
  }
  bool NeedsAnyTransmit( void )
  {
    return( (mTransmitBufferIndex > 0) ||
        (mBufferRequiresAck) );
  }
  bool ReadyToTransmit( void )
  {
    return( NeedsAnyTransmit() && mBufferAllowsAck );
  }
  void SetRemoteAckedBytes( unsigned long newVal )
  {
    if( newVal > mRemoteWrittenSendAckBytes.mReceivedBytes )
      mRemoteWrittenSendAckBytes.mReceivedBytes = newVal;
  }
  enum
  {
    k_DoubleBufferingUplink=0 ,
    k_DoubleBufferingDownlink=0
  };
  unsigned long RequiredSentAckUplink(void)
    {
      return k_DoubleBufferingUplink ? mSentBytesPrevious : mSentBytes ;
    }
  unsigned long RequiredSentAckDownlink(void)
    {
      return k_DoubleBufferingDownlink ? mSentBytesPrevious : mSentBytes ;
    }
  bool Transmit(void)
    {
      bool rc ;
      LockTransmit() ;
      unsigned long RemoteAckedBytes = GetRemoteAckedBytes();
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "mSentBytes=" << mSentBytes
        << " mSentBytesPrevious=" << mSentBytesPrevious
        << " mAckedSentBytes=" << mAckedSentBytes
        << " mRemoteWrittenSendAckBytes=" << RemoteAckedBytes
        << EndLogLine ;

      // update/check for any received ACKs before attempting to send
      if( RemoteAckedBytes > mAckedSentBytes )
        {
        BegLogLine( FXLOG_IONCN_BUFFER )
          << "AckedSentBytes needs update: " 
          << " mAckedSentBytes=" << mAckedSentBytes
          << " mRemoteWrittenSendAckBytes=" << RemoteAckedBytes
          << EndLogLine;
        mAckedSentBytes = RemoteAckedBytes;
        }

      // check and send:
      if ( mAckedSentBytes >= RequiredSentAckUplink() )
        {
          rc=mTransmitBuffer.Transmit(mSentBytes+mTransmitBufferIndex,mReceivedBytes,mTransmitBufferIndex,mLkey) ;
          if(rc)
            {
              mBufferRequiresAck = false;
              mSentBytes += mTransmitBufferIndex ;
              mTransmitBufferIndex=0 ;
            }
        }
      else
      {
        mBufferRequiresAck = true;
        unsigned long TSent0 = mTransmitBuffer.mBuffer[0].mSentBytes;
        unsigned long TRecv0 = mTransmitBuffer.mBuffer[0].mReceivedBytes;
        unsigned long TSent1 = mTransmitBuffer.mBuffer[1].mSentBytes;
        unsigned long TRecv1 = mTransmitBuffer.mBuffer[1].mReceivedBytes;

        unsigned long RSent0 = mReceiveBuffer.mBuffer[0].mSentBytes;
        unsigned long RRecv0 = mReceiveBuffer.mBuffer[0].mReceivedBytes;
        unsigned long RSent1 = mReceiveBuffer.mBuffer[1].mSentBytes;
        unsigned long RRecv1 = mReceiveBuffer.mBuffer[1].mReceivedBytes;
        BegLogLine( FXLOG_IONCN_BUFFER | FXLOG_ITAPI_ROUTER_FRAMES )
          << "Not Sending. Buffer Stats: "
          << " mRemoteWrittenSendAckBytes=" << RemoteAckedBytes
          << " mReceiveBuffer[0x" << (void*)(&mReceiveBuffer.mBuffer[0]) << "]=( " << RSent0 << " : " << RRecv0 << " )"
          << " mReceiveBuffer[0x" << (void*)(&mReceiveBuffer.mBuffer[1]) << "]=( " << RSent1 << " : " << RRecv1 << " )"
          << " current_rindex=" << mReceiveBuffer.CurrentBufferIndex()
          << " mTransmitBuffer[0]=( " << TSent0 << " : " << TRecv0 << " )"
          << " mTransmitBuffer[1]=( " << TSent1 << " : " << TRecv1 << " )"
          << " current_tindex=" << mTransmitBuffer.CurrentBufferIndex()
          << EndLogLine;

          BegLogLine(FXLOG_IONCN_BUFFER)
            << "Remote buffer not free yet. mAckedSentBytes=" << mAckedSentBytes
            << " mSentBytesPrevious=" << mSentBytesPrevious
            << " mSentBytes=" << mSentBytes
            << EndLogLine ;
          rc = false;
      }
      UnlockTransmit() ;
      return rc ;
    }
  bool Transmit(struct connection *conn)
    {
      bool rc ;
      LockTransmit() ;
      BegLogLine(FXLOG_IONCN_BUFFER)
        << "conn=0x" << (void *) conn
        << " mSentBytes=" << mSentBytes
        << " mSentBytesPrevious=" << mSentBytesPrevious
        << " mAckedSentBytes=" << mAckedSentBytes
        << EndLogLine ;

      if( ( mAckedSentBytes >= RequiredSentAckDownlink()) && ( mBufferAllowsAck ) )
      {
        rc=mTransmitBuffer.Transmit(conn,mSentBytes+mTransmitBufferIndex,mReceivedBytes,mTransmitBufferIndex) ;
        if( rc )
        {
          mBufferRequiresAck = false;
          mSentBytes += mTransmitBufferIndex ;
          mTransmitBufferIndex=0 ;
        }
      }
      else
      {
        unsigned long TSent0 = mTransmitBuffer.mBuffer[0].mSentBytes;
        unsigned long TRecv0 = mTransmitBuffer.mBuffer[0].mReceivedBytes;
        unsigned long TSent1 = mTransmitBuffer.mBuffer[1].mSentBytes;
        unsigned long TRecv1 = mTransmitBuffer.mBuffer[1].mReceivedBytes;

        unsigned long RSent0 = mReceiveBuffer.mBuffer[0].mSentBytes;
        unsigned long RRecv0 = mReceiveBuffer.mBuffer[0].mReceivedBytes;
        unsigned long RSent1 = mReceiveBuffer.mBuffer[1].mSentBytes;
        unsigned long RRecv1 = mReceiveBuffer.mBuffer[1].mReceivedBytes;
        BegLogLine( FXLOG_IONCN_BUFFER | FXLOG_ITAPI_ROUTER_FRAMES )
          << "Remote Buffer not free: conn=" << (void*)conn
          << " Buffer Stats: "
          << " mReceiveBuffer[0]=( " << RSent0 << " : " << RRecv0 << " )"
          << " mReceiveBuffer[1]=( " << RSent1 << " : " << RRecv1 << " )"
          << " current_rindex=" << mReceiveBuffer.CurrentBufferIndex()
          << " mTransmitBuffer[0]=( " << TSent0 << " : " << TRecv0 << " )"
          << " mTransmitBuffer[1]=( " << TSent1 << " : " << TRecv1 << " )"
          << " current_tindex=" << mTransmitBuffer.CurrentBufferIndex()
          << EndLogLine;
        BegLogLine(FXLOG_IONCN_BUFFER)
          << "conn=0x" << (void *) conn
          << " Remote buffer not free yet. Check if we should acknowledge current status. mAckedSentBytes=" << mAckedSentBytes
          << " mSentBytesPrevious=" << mSentBytesPrevious
          << " mSentBytes=" << mSentBytes
          << " mTransmitBufferIndex=" << mTransmitBufferIndex
          << EndLogLine ;

        // ack the current status, regardless of what the remote side thinks...
        rc = false;
        unsigned long newReceivedBytes = mTransmitBuffer.mBuffer[ mTransmitBuffer.CurrentBufferIndex() ].mReceivedBytes;
        if( mReceivedBytes > newReceivedBytes )
        {
          mBufferRequiresAck = true;
          BegLogLine( FXLOG_IONCN_BUFFER | FXLOG_ITAPI_ROUTER_FRAMES )
            << "ACK required.  conn=0x" << (void*) conn
            << " mReceivedBytes=" << mReceivedBytes
            << " newReceivedBytes=" << newReceivedBytes
            << " mBufferAckStates( " << mBufferAllowsAck << ":" << mBufferRequiresAck << " )"
            << " mAckedSentBytes=" << mAckedSentBytes
            << EndLogLine;
        }
      }
      UnlockTransmit() ;
      return rc ;

    }
  bool PostAckOnly( struct connection *conn )
  {
    bool rc = mTransmitBuffer.PostAckOnly( conn, mSentBytes, mReceivedBytes );
    if( rc )
      mBufferRequiresAck = false;
    return rc;
  }
  bool Receive(void)
    {
    static volatile unsigned long idlecount = 0;
      mReceiveBufferLength = 0;
        unsigned long TSent0 = mTransmitBuffer.mBuffer[0].mSentBytes;
        unsigned long TRecv0 = mTransmitBuffer.mBuffer[0].mReceivedBytes;
        unsigned long TSent1 = mTransmitBuffer.mBuffer[1].mSentBytes;
        unsigned long TRecv1 = mTransmitBuffer.mBuffer[1].mReceivedBytes;

        unsigned long RSent0 = mReceiveBuffer.mBuffer[0].mSentBytes;
        unsigned long RRecv0 = mReceiveBuffer.mBuffer[0].mReceivedBytes;
        unsigned long RSent1 = mReceiveBuffer.mBuffer[1].mSentBytes;
        unsigned long RRecv1 = mReceiveBuffer.mBuffer[1].mReceivedBytes;
        BegLogLine( (FXLOG_IONCN_BUFFER | FXLOG_ITAPI_ROUTER_FRAMES) && (idlecount % 0xfffff == 0) )
          << "Current Buffer Stats: "
          << " mRemoteWrittenSendAckBytes=" << GetRemoteAckedBytes()
          << " mReceiveBuffer[0x" << (void*)(&mReceiveBuffer.mBuffer[0]) << "]=( " << RSent0 << " : " << RRecv0 << " )"
          << " mReceiveBuffer[0x" << (void*)(&mReceiveBuffer.mBuffer[1]) << "]=( " << RSent1 << " : " << RRecv1 << " )"
          << " current_rindex=" << mReceiveBuffer.CurrentBufferIndex()
          << " mTransmitBuffer[0]=( " << TSent0 << " : " << TRecv0 << " )"
          << " mTransmitBuffer[1]=( " << TSent1 << " : " << TRecv1 << " )"
          << " current_tindex=" << mTransmitBuffer.CurrentBufferIndex()
          << EndLogLine;
      bool rc=mReceiveBuffer.Receive(mReceivedBytes,&mReceiveBufferLength) ;
      if ( rc )
        {
          idlecount = 0;
          unsigned int CurrentBuffer=mReceiveBuffer.CurrentBufferIndex() ;
          mSentBytesPrevious=mSentBytes ;
          mAckedSentBytes=mReceiveBuffer.mBuffer[CurrentBuffer].mReceivedBytes ;
          SetRemoteAckedBytes( mAckedSentBytes );
//          mAckedReceivedBytes=mReceiveBuffer.mBuffer[CurrentBuffer].mReceivedBytes ;
          mBufferRequiresAck = true;
          BegLogLine(FXLOG_IONCN_BUFFER)
            << "mSentBytesPrevious=" << mSentBytesPrevious
            << " mAckedSentBytes=" << mAckedSentBytes
//            << " mAckedReceivedBytes=" << mAckedReceivedBytes
            << EndLogLine ;
        }
      else
      {
      idlecount++;
        if( mBufferRequiresAck )
        {
          unsigned long newAckedSentBytes = mAckedSentBytes;
          unsigned long BufferRecvBytes = mReceiveBuffer.mBuffer[ 0 ].mReceivedBytes;
          if( BufferRecvBytes < CN_ION_SENTINEL_LENGTH_VALUE )
            newAckedSentBytes = std::max( newAckedSentBytes, BufferRecvBytes );

          BufferRecvBytes = mReceiveBuffer.mBuffer[ 1 ].mReceivedBytes;
          if( BufferRecvBytes < CN_ION_SENTINEL_LENGTH_VALUE )
            newAckedSentBytes = std::max( newAckedSentBytes, BufferRecvBytes );

          if( mAckedSentBytes < newAckedSentBytes )
            {
            mAckedSentBytes = newAckedSentBytes;
            }
        }
        rc = false;
      }
      return rc ;
    }
  void HandleBuffer(void)
    {
      mReceiveBuffer.HandleBuffer(mReceiveBufferLength) ;
      mReceivedBytes += mReceiveBufferLength ;
    }
  void AppendToBuffer(const void *aSrc, unsigned long aLength)
    {
      BegLogLine(FXLOG_IONCN_BUFFER)
          << "aSrc=" << (void *) aSrc
          << " aLength=" << aLength
          << " mTransmitBufferIndex=" << mTransmitBufferIndex
          << EndLogLine ;
      mTransmitBuffer.CopyToBuffer(mTransmitBufferIndex,aSrc,aLength) ;
      mTransmitBufferIndex += aLength ;
    }
  unsigned long SpaceInBuffer(void)
    {
      return k_ApplicationBufferSize-1-mTransmitBufferIndex ;
    }
  bool Push(const void *aSrc, unsigned long aLength)
    {
      if ( aLength >= SpaceInBuffer() )
        {
          bool rc=Transmit() ;
          if(rc)
            {
              AppendToBuffer(aSrc,aLength) ;
            }
          return rc ;
        }
      else
        {
          AppendToBuffer(aSrc,aLength) ;
          return true ;
        }
    }
  bool AnythingToSend(void)
    {
      return mTransmitBufferIndex>0 ;
    }
  bool AdvanceAcked(unsigned long aAckedSentBytes)
    {
//      mTransmitBuffer.AdvanceAcked(aAckedSentBytes) ;
    if( mAckedSentBytes < aAckedSentBytes )
    {
      mAckedSentBytes=aAckedSentBytes ;
      return true;
    }
    else
      return false;
    }
//  void HoldForUpstreamSpace(void)
//    {
//
//    }
  void PostReceive(struct connection *conn)
    {
      mReceiveBuffer.PostReceive(conn) ;
      mBufferAllowsAck = true;
    }
  void PostAllReceives(struct connection *conn)
    {
      mReceiveBuffer.PostAllReceives(conn) ;
    } ;
  int ProcessCall(struct connection *conn)
    {
      mBufferAllowsAck = false;
      return mReceiveBuffer.ProcessCall(conn) ;
    }
  void ProcessRead(struct connection *conn)
  {
    mBufferAllowsAck = false;
    mReceiveBuffer.ProcessRead(conn);
  }
  };

//class LocalEndpointAndHdr
//  {
//public:
//  unsigned int mLocalEndpoint ;
//  iWARPEM_Message_Hdr_t mHdr ;
//  };


#endif
