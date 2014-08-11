/************************************************
 * Copyright (c) IBM Corp. 1997-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 *************************************************/

#ifndef __PKTRACE2_HPP__
#define __PKTRACE2_HPP__

#include <Pk/FxLogger.hpp>

#if 1
// THIS IS WHY YOU"RE NOT GETTING TRACING
// THIS IS WHY YOU"RE NOT GETTING TRACING
// THIS IS WHY YOU"RE NOT GETTING TRACING
// THIS IS WHY YOU"RE NOT GETTING TRACING
// THIS IS WHY YOU"RE NOT GETTING TRACING
#ifdef PKTRACE_ON
#define PKFXLOG_TRACE_ON ( 1 )
#else
#define PKFXLOG_TRACE_ON ( 0 )
#endif
#else
#define PKFXLOG_TRACE_ON ( 0 )
#endif

#ifndef PKFXLOG_TRACE_ADDHIT
#define PKFXLOG_TRACE_ADDHIT ( 0 )
#endif

#ifndef PKFXLOG_TRACE_CONNECT
#define PKFXLOG_TRACE_CONNECT ( 0 )
#endif

#ifndef PKFXLOG_TRACE_INIT
#define PKFXLOG_TRACE_INIT ( 0 )
#endif

#define PKTRACE_NUMBER_OF_TIMESTAMPS 512*512*64
#define PKTRACE_NUMBER_OF_TRACE_NAMES 1024
#define MAX_TRACE_NAME         1022

// If we are recording 'performance counters', we may be taking several per trace point
enum {
        kTraceIdsPerTracePoint =  1
} ;


struct TraceStreamName
  {
  unsigned short   mTraceStreamId;
  char             mName[MAX_TRACE_NAME];
  };

struct PkTrace_PerCoreState
  {

  double padding[ 16 ];
  unsigned long long* mHitBuffer;

  unsigned long       mHitBufferAllocatedSize;

  int                 mUnwrittenNameCount;
  TraceStreamName*    mTraceStreamNames;

  unsigned long       mHits;
  unsigned long       mMaxRegisteredTracePointCount;
  unsigned short      mNextTraceStreamId;

  double padding1[ 16 ];
  };


#define PK_NUMBER_OF_CORES ( 1 )
extern PkTrace_PerCoreState PCS[ PK_NUMBER_OF_CORES ];


static void
InitTraceMutexes()  __attribute__((unused)) ;

static void
InitTraceMutexes()
  {
  }

static
void
InitTraceClientSync();

class pkTraceServer
{
public:
  static
  void
  Init(unsigned short aStartingTraceId = 0) 
    {
      // int CoreId = PkCoreGetId();
      int CoreId = 0;

//       StrongAssertLogLine( CoreId == 0 )
// 	<< "THIS ONLY WORKS ON CORE 0 "
// 	<< " CoreId: " << CoreId
// 	<< EndLogLine;

    PCS[ CoreId ].mHitBufferAllocatedSize = sizeof(unsigned long long) * PKTRACE_NUMBER_OF_TIMESTAMPS;

    PCS[ CoreId ].mHitBuffer = (unsigned long long *) malloc( PCS[ CoreId ].mHitBufferAllocatedSize );

    PCS[ CoreId ].mTraceStreamNames = (TraceStreamName *) malloc( sizeof( TraceStreamName ) * PKTRACE_NUMBER_OF_TRACE_NAMES );

    StrongAssertLogLine( PCS[ CoreId ].mTraceStreamNames != NULL )
      << "pkTraceServer::Init:: "
      << " sizeof( TraceStreamName ) * PKTRACE_NUMBER_OF_TRACE_NAMES: " << sizeof( TraceStreamName ) * PKTRACE_NUMBER_OF_TRACE_NAMES
      << EndLogLine;

    BegLogLine( PKFXLOG_TRACE_INIT )
      << "pkTraceServer::Init():: "
      << " @PCS[ " << CoreId << " ] "
      <<   (void *) &PCS[ CoreId ]
      << " @PCS[ " << CoreId << " ].mTraceStreamNames: "
      <<   (void *) &PCS[ CoreId ].mTraceStreamNames
      << EndLogLine;

    PCS[ CoreId ].mHits = 0;
    PCS[ CoreId ].mUnwrittenNameCount = 0;
    PCS[ CoreId ].mNextTraceStreamId = aStartingTraceId;

    InitTraceClientSync();

    FlushBuffer();

    BegLogLine( PKFXLOG_TRACE_INIT )
      << "pkTraceServer::Init():: Done"
      << EndLogLine;
    }

  static
  void
  FlushNames()
    {
    // Names are stored in an array of structures.  Should probably convert this to a
    // linked list so as to not glomb more memory than is needed.  But, it works right now...
      // int CoreId = PkCoreGetId();
      int CoreId = 0;

    BegLogLine( PKFXLOG_TRACE_INIT )
      << "FlushNames:: "
      << "PCS[ CoreId ].mUnwrittenNameCount" << PCS[ CoreId ].mUnwrittenNameCount
      << EndLogLine;

    for( int i=0; i < PCS[ CoreId ].mUnwrittenNameCount; i++)
      {
      BegLogLine( PKFXLOG_TRACE_ON )
        << "PKTRACE ID "
        << FormatString ("%04x")
        << FxLoggerNodeId << " "
        << FormatString ("%04x")
        << (int) (PCS[ CoreId ].mTraceStreamNames[ i ].mTraceStreamId)
        << " "
        << PCS[ CoreId ].mTraceStreamNames[ i ].mName
        << EndLogLine;
      }

    //BGF FlushNames must clear the names buffer or, the next time it's called, duplicates will be produced!
    PCS[ CoreId ].mUnwrittenNameCount = 0;

    }

  static
  void
  Reset()
    {
      // PCS[ PkCoreGetId() ]. mHits = 0;
      PCS[ 0 ]. mHits = 0;
    }

  static
  void
  ResetAndFlushNames()
    {
    Reset();
    FlushNames();
    }

  static void
  FlushBuffer()
    {
    static const char HexToChar[17] = "0123456789abcdef" ;
    // int CoreId = PkCoreGetId();
    int CoreId = 0;

    FlushNames();

#ifndef PKTRACE_NUMBER_OF_HITS_PER_LINE
#define PKTRACE_NUMBER_OF_HITS_PER_LINE (8)
#endif

    enum{ HitDataSize = sizeof( PCS[0].mHitBuffer[0] ) };



    char HexBuf[ PKTRACE_NUMBER_OF_HITS_PER_LINE * HitDataSize * 2 + 1];

    BegLogLine( PKFXLOG_TRACE_INIT )
      << "FlushBuffer:: "
      << "PCS[ CoreId ].mHits " << PCS[ CoreId ].mHits
      << EndLogLine;

    int  HexBufInd  = 0;
    for( unsigned int h = 0; h < PCS[ CoreId ].mHits; h++ )
      {

      char* HitBytes = (char*) &(PCS[ CoreId ].mHitBuffer[ h ]);

      for( unsigned int b = 0; b < HitDataSize; b++ )
        {
        HexBuf[ HexBufInd++ ] = HexToChar[ (HitBytes[b] >> 4) & 0x0f ] ;
        HexBuf[ HexBufInd++ ] = HexToChar[  HitBytes[b]       & 0x0f ] ;
        }

      if( ((h+1) %  PKTRACE_NUMBER_OF_HITS_PER_LINE == 0)
          ||
          ( h == (PCS[ CoreId ].mHits - 1 ) ) )
        {
        HexBuf[ HexBufInd ] = 0;
        BegLogLine( PKFXLOG_TRACE_ON )
            << "PKTRACE DATA "
            << FormatString ("%04x")
            << FxLoggerNodeId
            << " "
            << HexBuf
            << EndLogLine;

        HexBufInd = 0;
        }
      }

    PCS[ CoreId ].mHits = 0;
    }

  static
  void
  AddHit(unsigned long long aTime, unsigned short aTraceStreamId, int aTracePart=0, int aCountersConfigured=0)
    {
      // int CoreId = PkCoreGetId();
      int CoreId = 0;

    if( PCS[ CoreId ].mHits >= PKTRACE_NUMBER_OF_TIMESTAMPS )
      {
      fprintf( stderr, "WARNING: Flushing trace buffer because full... \n" );
      fflush( stderr );
      FlushBuffer();
      }

    // copy the 8 byte time stamp into place
    PCS[ CoreId ].mHitBuffer[ PCS[ CoreId ].mHits ] = aTime;

    // place the trace id in the high 2 bytes.
    char* EB = (char *) & (PCS[ CoreId ].mHitBuffer[ PCS[ CoreId ].mHits ]);
    unsigned short reportedTraceStreamId = aTraceStreamId;
    char* traceId = (char *) & reportedTraceStreamId;

    EB[0] = traceId[0];
    EB[1] = traceId[1];

    PCS[ CoreId ].mHits ++;

    BegLogLine(PKFXLOG_TRACE_ADDHIT)
      << "AddHit aTraceStreamId=" << aTraceStreamId
      << " aTracePart="     << aTracePart
      << " time="           << (long long) aTime
      << EndLogLine ;
    }
};

#define PKTRACE_TRACE_ID_INVALID  (0xFFFF)

// An instance of this class is deposited in the executable where
// a trace point has been encoded.

class pkTraceStream
{
public:
  int            mFirstEntry;
  unsigned short mTraceStreamId;

  pkTraceStream()
    {
    init();
    }

  void
  init()
    {
    mFirstEntry = 1;
    mTraceStreamId    = PKTRACE_TRACE_ID_INVALID ;
    }

  // pkTraceStreamConnect called once per trace point to register that point
  // it gets a trace stream id from the server.  it involves a lock
  unsigned short
  pkTraceStreamConnect( const char     *SourceFileName,
                        int       SourceLineNo,
                        unsigned  Class,
                        const char     *TraceStreamContext,
                        int       TraceStreamContextOrdinal,
                        const char     *TraceStreamName )
    {
      // int CoreId = PkCoreGetId();
      int CoreId = 0;

    char ComposedTraceName[ MAX_TRACE_NAME ];
    char TraceName[ MAX_TRACE_NAME ];

    if( TraceStreamContextOrdinal == -1 )
      {
      sprintf( ComposedTraceName, "%s:%s", TraceStreamContext, TraceStreamName );
      }
    else
      {
      sprintf( ComposedTraceName, "%s[%d]:%s",
               TraceStreamContext,
               TraceStreamContextOrdinal,
               TraceStreamName );
      }

    const char* ShortSourceFileName  = FxLogger_GetStartPointInFilePathName( SourceFileName );

    sprintf( TraceName, "%04d:%d:%s::%s[%04u]",
             FxLoggerNodeId == -1 ? TraceStreamContextOrdinal : FxLoggerNodeId,
             0,
             ComposedTraceName,
             ShortSourceFileName,
             SourceLineNo);

    int nloop = 0;
    while(TraceName[nloop] != '\0')
      {
      if( TraceName[nloop] == '/' )
        TraceName[nloop] = '|';
      else if( TraceName[nloop] == ' ' )
        TraceName[nloop] = '_';

      nloop++;
      }

    unsigned short traceId = PCS[ CoreId ].mNextTraceStreamId;

    PCS[ CoreId ].mNextTraceStreamId++;

    int traceIndex = PCS[ CoreId ].mUnwrittenNameCount;

    AssertLogLine( traceIndex >= 0 && traceIndex < PKTRACE_NUMBER_OF_TRACE_NAMES )
      << "pkTraceStreamConnect::"
      << " traceIndex: " << traceIndex
      << " PKTRACE_NUMBER_OF_TRACE_NAMES: " << PKTRACE_NUMBER_OF_TRACE_NAMES
      << EndLogLine;

    AssertLogLine( PCS[ CoreId ].mTraceStreamNames != NULL )
      <<"pkTraceStreamConnect:: mTraceStreamNames is NULL"
      << EndLogLine ;

    BegLogLine( 0 )
      << "pkTraceStreamConnect::"
      << " traceIndex: " << traceIndex
      << " PKTRACE_NUMBER_OF_TRACE_NAMES: " << PKTRACE_NUMBER_OF_TRACE_NAMES
      << " traceId: " << traceId
      << " PCS[ CoreId ].mTraceStreamNames: " << (void *) PCS[ CoreId ].mTraceStreamNames
      << " CoreId: " << CoreId
      << " @PCS[ " << CoreId << " ].mTraceStreamNames: "
      <<   (void *) &PCS[ CoreId ].mTraceStreamNames
      << " @PCS[ " << CoreId << " ] "
      <<   (void *) &PCS[ CoreId ]
      << EndLogLine;

    PCS[ CoreId ].mTraceStreamNames[ traceIndex ].mTraceStreamId = traceId;

    strcpy( PCS[ CoreId ].mTraceStreamNames[ traceIndex ].mName, TraceName );

    PCS[ CoreId ].mUnwrittenNameCount++;

    BegLogLine( PKFXLOG_TRACE_CONNECT )
      << "pkTraceStreamConnect "
      << FormatString ("%04x")
      << FxLoggerNodeId << " "
      << FormatString ("%04x")
      << (int) traceId << " "
      << TraceName
      << " PCS[ " << CoreId << " ].mTraceStreamNames[ " << traceIndex << " ].mName: "
      << PCS[ CoreId ].mTraceStreamNames[ traceIndex ].mName
      << EndLogLine;

    return traceId;
    }

#if !PKFXLOG_TRACE_ON
  inline void
  pkTraceStreamSourceTime( int       LineNo,
                       const char     *FileName,
                       unsigned  TpFlag,
                       const char     *TpContext,
                       int       TpOrdinal,
                       const char     *TpName,
                       unsigned long long ReportedTime)
    {
      BegLogLine(0)
          << "TpFlag" << TpFlag
          << " LineNo" << LineNo
          << " FileName" << FileName
          << EndLogLine ;
    return;
    }
  inline void pkTraceStreamSource( int       LineNo,
                       const char     *FileName,
                       unsigned  TpFlag,
                       const char     *TpContext,
                       int       TpOrdinal,
                       const char     *TpName )
    {
      BegLogLine(0)
          << "TpFlag" << TpFlag
          << " LineNo" << LineNo
          << " FileName" << FileName
          << EndLogLine ;
    return;
    }
#else
  inline void
  pkTraceStreamSourceTime( int       LineNo,
                       const char     *FileName,
                       unsigned  TpFlag,
                       const char     *TpContext,
                       int       TpOrdinal,
                       const char     *TpName,
                       unsigned long long ReportedTime)
    {
      BegLogLine(0)
          << "TpFlag" << TpFlag
          << " LineNo" << LineNo
          << " FileName" << FileName
          << EndLogLine ;
    // Should convert this to a template arg so compiler can turn points off
    if( ! TpFlag )
      return;

    // First call to function, do connect
    if( mFirstEntry )
      {
      mTraceStreamId = pkTraceStreamConnect( FileName, LineNo, TpFlag,
                                       TpContext, TpOrdinal, TpName );
      mFirstEntry = 0;
      }
//    unsigned long long ReportedTime = PkTimeGetNanos();

    pkTraceServer::AddHit(ReportedTime,mTraceStreamId,0,0) ;
    }
  inline void
  pkTraceStreamSource( int       LineNo,
                       const char     *FileName,
                       unsigned  TpFlag,
                       const char     *TpContext,
                       int       TpOrdinal,
                       const char     *TpName )
  {
	  unsigned long long ReportedTime = PkTimeGetNanos();
	  pkTraceStreamSourceTime(LineNo,FileName,TpFlag,TpContext,TpOrdinal,TpName,ReportedTime) ;
  }
#endif
};

#define HitOE(Flag, ContextName, Ordinal, EventName) \
  pkTraceStreamSource(__LINE__,__FILE__,Flag, ContextName, Ordinal, #EventName)
#define HitOET(Flag, ContextName, Ordinal, EventName, ReportedTime) \
  pkTraceStreamSourceTime(__LINE__,__FILE__,Flag, ContextName, Ordinal, #EventName, ReportedTime)

#define TraceClient pkTraceStream
static TraceClient gTraceClientSync;

static
void
InitTraceClientSync()
{
  gTraceClientSync.HitOE( PKFXLOG_TRACE_ON,
                          "TraceClientSync",
                          0,
                          gTraceClientSync );

}

#endif /* __PKTRACE_HPP__ */
