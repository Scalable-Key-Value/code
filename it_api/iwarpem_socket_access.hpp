/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * iwarpem_socket_access.hpp
 *
 *  Created on: Jan 20, 2015
 *      Author: lschneid
 */

#ifndef IT_API_IWARPEM_SOCKET_ACCESS_HPP_
#define IT_API_IWARPEM_SOCKET_ACCESS_HPP_

#ifndef FXLOG_IT_API_O_SOCKETS
#define FXLOG_IT_API_O_SOCKETS ( 0 )
#endif

typedef enum
{
  IWARPEM_SUCCESS                 = 0x0001,
  IWARPEM_ERRNO_CONNECTION_RESET  = 0x0002,
  IWARPEM_ERRNO_CONNECTION_CLOSED = 0x0003
} iWARPEM_Status_t ;


#ifndef IT_API_REPORT_BANDWIDTH_ALL
#define IT_API_REPORT_BANDWIDTH_ALL ( 0 )
#endif

#ifndef IT_API_REPORT_BANDWIDTH_RDMA_WRITE_IN
#define IT_API_REPORT_BANDWIDTH_RDMA_WRITE_IN ( 0 | IT_API_REPORT_BANDWIDTH_ALL )
#endif

#ifndef IT_API_REPORT_BANDWIDTH_RDMA_READ_IN
#define IT_API_REPORT_BANDWIDTH_RDMA_READ_IN ( 0 | IT_API_REPORT_BANDWIDTH_ALL )
#endif

#ifndef IT_API_REPORT_BANDWIDTH_RECV
#define IT_API_REPORT_BANDWIDTH_RECV ( 0 | IT_API_REPORT_BANDWIDTH_ALL)
#endif

#ifndef IT_API_REPORT_BANDWIDTH_INCOMMING_TOTAL
#define IT_API_REPORT_BANDWIDTH_INCOMMING_TOTAL ( 0 | IT_API_REPORT_BANDWIDTH_ALL )
#endif

#ifndef IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL
#define IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL ( 0 | IT_API_REPORT_BANDWIDTH_ALL)
#endif

#define IT_API_REPORT_BANDWIDTH_DEFAULT_MODULO_BYTES (100*1024*1024)
#define IT_API_REPORT_BANDWIDTH_OUTGOING_MODULO_BYTES (1*1024*1024)

struct iWARPEM_Bandwidth_Stats_t
{
  unsigned long long mTotalBytes;
  unsigned long long mBytesThisRound;
  unsigned long long mStartTime;
  unsigned long long mFirstStartTime;

  unsigned long long mReportLimit;

#define BANDWIDTH_STATS_CONTEXT_MAX_SIZE 256
  char               mContext[ BANDWIDTH_STATS_CONTEXT_MAX_SIZE ];

public:

  void
  Reset()
  {
    mBytesThisRound  = 0;
    mStartTime       = PkTimeGetNanos();
  }

  void
  Init( const char* aContext, unsigned long long aReportLimit=IT_API_REPORT_BANDWIDTH_DEFAULT_MODULO_BYTES )
  {
    mReportLimit = aReportLimit;

    int ContextLen = strlen( aContext ) + 1;
    StrongAssertLogLine( ContextLen < BANDWIDTH_STATS_CONTEXT_MAX_SIZE )
      << "ERROR: "
      << " ContextLen: " << ContextLen
      << " BANDWIDTH_STATS_CONTEXT_MAX_SIZE: " << BANDWIDTH_STATS_CONTEXT_MAX_SIZE
      << EndLogLine;

    strcpy( mContext, aContext );

    mTotalBytes = 0;
    mFirstStartTime       = PkTimeGetNanos();

    BegLogLine( FXLOG_IT_API_O_SOCKETS )
      << "iWARPEM_Bandwidth_Stats_t::Init(): "
      << " mContext: " << mContext
      << " mFirstStartTime: " << mFirstStartTime
      << EndLogLine;

    Reset();
  }

  void
  AddBytes( unsigned long long aBytes )
  {
    mBytesThisRound += aBytes;
    mTotalBytes     += aBytes;

    if( mBytesThisRound >= mReportLimit )
      ReportBandwidth();
  }

  void
  ReportBandwidth()
  {
    unsigned long long FinishTime = PkTimeGetNanos();

    double BandwidthThisRoundMB = ((mBytesThisRound * 1e9) / ( (FinishTime - mStartTime) )) / (1024.0 * 1024.0);

    double BandwidthAvgSinceStart = ((mTotalBytes * 1e9) / ( (FinishTime - mFirstStartTime) )) / (1024.0 * 1024.0);


    BegLogLine( (IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL|IT_API_REPORT_BANDWIDTH_INCOMMING_TOTAL) )
      << "iWARPEM_Bandwidth_Stats::ReportBandwidth(): "
      << " Context: " << mContext
      << " BandwidthThisRound (MB): " << BandwidthThisRoundMB
      << " BandwidthAvgSinceStart (MB): " << BandwidthAvgSinceStart
      << EndLogLine;

    Reset();
  }
};

iWARPEM_Bandwidth_Stats_t gBandInStat;
iWARPEM_Bandwidth_Stats_t gBandOutStat;

#ifndef IT_API_READ_FROM_SOCKET_HIST
#define IT_API_READ_FROM_SOCKET_HIST ( 0 )
#endif

histogram_t<IT_API_READ_FROM_SOCKET_HIST> gReadCountHistogram;
histogram_t<IT_API_READ_FROM_SOCKET_HIST> gReadTimeHistogram;

static
inline
iWARPEM_Status_t
read_from_socket( int sock, char * buff, int len, int* rlen )
{
  BegLogLine(FXLOG_IT_API_O_SOCKETS)
    << "Reading from FD=" << sock
    << " buff=" << (void *) buff
    << " length=" << len
    << EndLogLine ;
  int BytesRead = 0;
  int ReadCount = 0;

  unsigned long long StartTime = PkTimeGetNanos();

  for(; BytesRead < len; )
  {
    int read_rc = read(   sock,
                          (((char *) buff) + BytesRead ),
                          len - BytesRead );
    if( read_rc < 0 )
    {
      // printf( "errno: %d\n", errno );
      if( errno == EAGAIN || errno == EINTR )
        continue;
      else if ( errno == ECONNRESET )
      {
        BegLogLine(FXLOG_IT_API_O_SOCKETS)
          << "ECONNRESET"
          << EndLogLine ;
        return IWARPEM_ERRNO_CONNECTION_RESET;
      }
      else
        StrongAssertLogLine( 0 )
          << "read_from_socket:: ERROR:: "
          << "failed to read from file: " << sock
          << " errno: " << errno
          << " buff=" << (void *) buff
          << " " << (long) buff
          << " length=" << len
          << EndLogLine;
    }
    else if( read_rc == 0 )
    {
      BegLogLine(FXLOG_IT_API_O_SOCKETS)
        << "Connection closed, BytesRead=" << BytesRead
        << EndLogLine ;
      *rlen = BytesRead;
      return IWARPEM_ERRNO_CONNECTION_CLOSED;
    }

    ReadCount++;

    BytesRead += read_rc;
  }

  *rlen = BytesRead;

#if IT_API_REPORT_BANDWIDTH_INCOMMING_TOTAL
  gBandInStat.AddBytes( BytesRead );
#endif

  unsigned long long FinisTime = PkTimeGetNanos();

  gReadTimeHistogram.Add( FinisTime - StartTime );

  gReadCountHistogram.Add( ReadCount );

  static int Reported = 0;
  static long long ReportOnCount = 0;
  if( !Reported && ( ReportOnCount == 145489 ))
  {
    gReadTimeHistogram.Report();
    gReadCountHistogram.Report();
    Reported = 1;
  }

  ReportOnCount++;

  BegLogLine(FXLOG_IT_API_O_SOCKETS)
    << "Read completes"
    << EndLogLine ;
  return IWARPEM_SUCCESS;
}

static
inline
iWARPEM_Status_t
write_to_socket_writev( int sock, struct iovec *iov, int iov_count, int* wlen )
{
  BegLogLine(FXLOG_IT_API_O_SOCKETS)
    << "Writing to FD=" << sock
    << " iovec=" << (void *) iov
    << " iov_count=" << iov_count
    << EndLogLine ;
writev_retry:
  int write_rc = writev(sock,iov,iov_count) ;
  if( write_rc < 0 )
  {
    switch( errno )
    {
      case EAGAIN:
        goto writev_retry;
      case ECONNRESET:
      case EPIPE: // This is likely to be that upstream has already closed the socket
        *wlen = write_rc ;
        return IWARPEM_ERRNO_CONNECTION_RESET;
      default:
        StrongAssertLogLine( 0 )
          << "write_to_socket:: ERROR:: "
          << "failed to write to file: " << sock
          << " iovec: " << (void *) iov
          << " iov_count: " << iov_count
          << " errno: " << errno
          << EndLogLine;
    }
  }
  *wlen = write_rc ;

#if IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL
  gBandOutStat.AddBytes( write_rc );
#endif
  BegLogLine(FXLOG_IT_API_O_SOCKETS)
    << "Bytes written=" << write_rc
    << EndLogLine ;
  return IWARPEM_SUCCESS;
}

static
inline
iWARPEM_Status_t
write_to_socket( int sock, char * buff, int len, int* wlen )
{
  BegLogLine(FXLOG_IT_API_O_SOCKETS)
    << "Writing to FD=" << sock
    << " buff=" << (void *) buff
    << " length=" << len
    << EndLogLine ;
  int BytesWritten = 0;
  for( ; BytesWritten < len; )
  {
    int write_rc = write(   sock,
                            (((char *) buff) + BytesWritten ),
                            len - BytesWritten );
    if( write_rc < 0 )
    {
      // printf( "errno: %d\n", errno );
      if( errno == EAGAIN )
        continue;
      else if ( errno == ECONNRESET )
      {
        return IWARPEM_ERRNO_CONNECTION_RESET;
      }
      else
        StrongAssertLogLine( 0 )
          << "write_to_socket:: ERROR:: "
          << "failed to write to file: " << sock
          << " buff: " << (void *) buff
          << " len: " << len
          << " errno: " << errno
          << EndLogLine;
    }

    BytesWritten += write_rc;
  }

  *wlen = BytesWritten;

#if IT_API_REPORT_BANDWIDTH_OUTGOING_TOTAL
  gBandOutStat.AddBytes( BytesWritten );
#endif

  return IWARPEM_SUCCESS;
}

static iWARPEM_Status_t write_to_socket(int sock,struct iovec *iov, int iov_count, int* wlen)
{
#if defined(PK_CNK)
  if ( iov_count == 0 )
  {
    *wlen = 0 ;
    return IWARPEM_SUCCESS;
  }
  else if ( iov_count == 1 )
  {
    return write_to_socket(sock,(char *)iov[0].iov_base, iov[0].iov_len,wlen) ;
  }
  else {
    size_t total_len=0 ;
    for ( int a=0;a<iov_count;a+=1)
    {
      total_len += iov[a].iov_len ;
    }
    char buffer[total_len] ;
    size_t buffer_index=0 ;
    for ( int b=0;b<iov_count;b+=1)
    {
      memcpy(buffer+buffer_index,iov[b].iov_base,iov[b].iov_len) ;
      buffer_index += iov[b].iov_len ;
    }
    return write_to_socket(sock,buffer,total_len, wlen) ;
  }
#else
  return write_to_socket_writev(sock,iov,iov_count,wlen) ;
#endif
}

#endif /* IT_API_IWARPEM_SOCKET_ACCESS_HPP_ */
