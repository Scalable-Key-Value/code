/************************************************
 * Copyright (c) IBM Corp. 1997-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * History:  170297 BGF Created.
 *           240397 BGF Sacked the complex template typing.
 *           030797 TJCW Arranged wrappering so that logging can
 *                       be compiled in on demand
 *           130603 AR BGF rewrite to be based on sprintf rather than streams.
 *
 *************************************************/

#ifndef __PKFXLOG_HPP__
#define __PKFXLOG_HPP__

#include <FxLogger/Time.hpp>

#include <errno.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifdef __linux__

#ifndef NO_PK_PLATFORM
#define NO_PK_PLATFORM ( 1 )
#endif
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

#include <fstream>
#include <stdexcept>
#include <string>

extern int  FxLoggerNodeId;
extern char FxLoggerProgramName[ 1024 ];
using namespace std;
#else

#endif

#ifdef PK_CNK
#include <firmware/include/personality.h>
#include <spi/include/kernel/cnk/process_impl.h>
#endif

//NEED: this perhaps default to packet size.  Or be drawn from platform spec
#define PKLOG_MAXBUF ( 2 * 4096 )

#ifndef PKFXLOG_PATHNAME_DIRECTORIES_LIMIT
#define PKFXLOG_PATHNAME_DIRECTORIES_LIMIT ( 3 )
#endif
// Turn PKFXLOG macro into an enum - tjcw prefers enums to macros because tjcw likes writing in C++ rather than macro language

enum
  {
  PkAnyLogging =
  #if defined( PKFXLOG )
     1
  #else
     0
  #endif
  } ;

#ifndef PKFXLOG_ALL
#define PKFXLOG_ALL 0
#endif

#include <stdio.h>  // required for all due to use of sprintf as formatting engine
#include <unistd.h> // for write() system call ... should be moved

#ifndef __PK_API_HPP__  // this file is usually included by the pk/api.hpp but can be used without.
    #include <cstdlib>
  extern "C"
    {
    #include <unistd.h>
    #include <signal.h>
    #include <stdio.h>
    #include <time.h>
    #include <fcntl.h>
    #include <sys/time.h>
    #include <string.h>
    }
#endif

#define __PK_FXLOG_SERIOUS_ABORT                                    \
    {                                                               \
        fflush(stdout);                                             \
        fflush(stderr);                                             \
        throw std::runtime_error(                                   \
            "FXLogger: Serious abort requested, check log file" );  \
    }

struct fxlogger_string_t
{
  int   mSize;
  char* mData;

  void
  Init( char* aData, int aSize )
  {
    mData = aData;
    mSize = aSize;
  }
};

static
void
FxLogger_Abort( const char * str )
  {
   #ifndef NO_PK_PLATFORM
     //assert(0);

     fprintf(stderr,"errno: %d ... FxLogger going down: Cause: %s \n", errno, str);
     fflush(stderr);

     __PK_FXLOG_SERIOUS_ABORT

     // _exit(__LINE__*-1);
     //PkAssert(0,str);
     //PLATFORM_ABORT( str );
   #else
     // more or less, posix with PK_PLATFORM
     printf( "FATAL ERROR IN FXLOGGER  >%s<\n", str );
     perror( "FxLogger causing abort" );
     fflush( stderr );
     fflush( stdout );
     kill( getpid(), SIGTERM ); // give debugger half a chance catch this
     exit( -1 );
   #endif
  }

inline
const char *
FxLogger_GetStartPointInFilePathName( const char * aFN, int aPathParts=PKFXLOG_PATHNAME_DIRECTORIES_LIMIT )
  {
  int starti = 0;

  // Find the end of the pathname
  int endi = 0;
  // while( aFN[ endi ] != '\0' )
#define PKFXLOG_MAX_PATH 240

  while( endi < PKFXLOG_MAX_PATH && aFN[ endi ] != '\0' )
    {
    endi++ ;
    }

  if( endi == PKFXLOG_MAX_PATH )
    FxLogger_Abort( "Unreasonably long file pathname" );

    int slashcount = 0;
    for( int slashstart = endi; slashstart != 0; slashstart-- )
      {
      if( aFN[ slashstart ] ==  '/' )
         slashcount++;
      if( slashcount == aPathParts )
        {
        starti = slashstart+1;
        break;
        }
      }

    //  if( starti > aPathParts )
    //     starti = endi - aPathParts;

  return( & aFN[ starti ] );
  }


static
void
FxLogger_Init( const char* aProgramName,
	       int   aRank=-1 ) __attribute__((unused)) ;
static
void
FxLogger_Init( const char* aProgramName,
	       int   aRank )
{
  struct rlimit RLim;
  RLim.rlim_cur = RLIM_INFINITY;
  RLim.rlim_max = RLIM_INFINITY;

  FxLoggerNodeId = aRank;

#ifndef PK_CNK
  if ( setrlimit( RLIMIT_CORE, &RLim ) != 0 )
    {
    perror( "ERROR:: vlimit failed" );
    FxLogger_Abort( "ERROR:: vlimit failed" );
    }
#endif

  const char* ProgName = FxLogger_GetStartPointInFilePathName( aProgramName, 1 );

  strcpy( FxLoggerProgramName, ProgName );
}

void FxLogger_WriteBufferToLogPort( char * aBuffer, int aLen );


// This calss when constructed takes a snapshot of the current time
class FxLogger_TimeStamp
  {
public:
    unsigned mSeconds, mNanoseconds;

    FxLogger_TimeStamp()
      {
      #ifndef NO_PK_PLATFORM

         PkTimeGetSecsNanos( &mSeconds, &mNanoseconds );

      #else

        #ifdef PK_AIX  // for finer resolution timer

          timebasestruct_t Now;
          read_real_time( &Now, TIMEBASE_SZ );
          time_base_to_time( &Now, TIMEBASE_SZ );
          mSeconds      = Now.tb_high;
          mNanoseconds  = Now.tb_low ;

        #else // assume POSIX interfaces
#if 0
          struct timeval  tv;
          struct timezone tz;
          gettimeofday( &tv, &tz );
          mSeconds     = tv.tv_sec;
          mNanoseconds = tv.tv_usec * 1000 ;
	  struct timespec ts;

	  clock_gettime( CLOCK_REALTIME, &ts );
	  mSeconds     = ts.tv_sec;
	  mNanoseconds = ts.tv_nsec;
#endif
	  pk_time_t Time;
	  GetPkTime( &Time );
	  mSeconds = Time.mSeconds;
	  mNanoseconds = Time.mNanoseconds;

        #endif

      #endif

      }

  };

static char itox[] __attribute__((unused)) = "0123456789ABCDEF";

class FxLogger_NodeName
  {
  public:

    char * mNodeName;

    FxLogger_NodeName()
      {
      static char static_NodeName[32];
      static int entry1 ;
      if( ! entry1 )
        {
        #ifndef NO_PK_PLATFORM
          //int NodeId = Platform::Topology::GetNodeId();
          int NodeId = PkNodeGetId();
        #else

          int NodeId = -1;
	  #if ( defined(  PK_BGP ) && defined( __linux__ ) && !defined(PK_CNK))
            NodeId = FxLoggerNodeId;
	    // int NodeId = getpid() ;
	    if( NodeId == -1 )
	      {
		//Read it out from /etc/personality

		ifstream fin("/etc/personality");

		int MAX_LENGTH = 100;
		char line[ MAX_LENGTH ];

		int RankFound = 0;
		while( fin.getline(line, MAX_LENGTH) )
		  {
		    string str( line );

		    string::size_type loc = str.find( "BG_RANK=", 0 );

		    if( loc != string::npos )
		      {
			FxLoggerNodeId = (int) atoi( str.substr( 8 ).c_str() );
			NodeId = FxLoggerNodeId;
			RankFound = 1;
			break;
		      }
		  }

		if( !RankFound )
                  {
		  //FxLogger_Abort( "FxLogger::ERROR:: BG_RANK not found in /etc/personality" );
                  }
	      }
          #endif
        #endif

        //if( NodeId >= 0 && NodeId < (1024*1024) )
        //  {
        //  entry1 = 1;
        //  }
        //else
        //  {
        //  NodeId = (int) static_NodeName;
        //  }
        if( NodeId == -1 )
          {
          char hbuf[256];
          gethostname(hbuf,256);
          hbuf[20] = '\0'; // need to ensure string isn't too long
#ifdef PK_CNK
          uint32_t id = 0;
          uint32_t rc;
          Personality_t pers;
          rc = Kernel_GetPersonality(&pers, sizeof(pers));
          if (rc == 0)
          {
            Personality_Networks_t *net = &pers.Network_Config;
            id = ((((((((net->Acoord
                * net->Bnodes) + net->Bcoord)
                * net->Cnodes) + net->Ccoord)
                * net->Dnodes) + net->Dcoord)
                * net->Enodes) + net->Ecoord);
          }
          int hpid = id ;
#else
          int hpid = getpid();
#endif
          sprintf( static_NodeName, "%s.%d", hbuf, hpid );
          }
        else
          {
          sprintf(static_NodeName, "%d", NodeId );
          }
        entry1 = 1; // tjcw only go through the nodename code once
        }

      mNodeName = static_NodeName;


      }

  };

#ifdef __linux__
class FxLogger_ProcessName
{
public:

  char mProcessName[64];

  FxLogger_ProcessName()
    {
#if defined(__linux__) && !defined(PK_CNK)
      pid_t pid = getpid();
      sprintf( mProcessName, "%d", pid);
#else
      sprintf( mProcessName, "NoPid");
#endif
    }
};

class FxLogger_ThreadName
{
public:

  char mThreadName[64];

  FxLogger_ThreadName()
    {
#if defined(__linux__)
      pthread_t tid = pthread_self();
      sprintf( mThreadName, "%08X", (int) tid );
#else
      sprintf( mThreadName, "NoTid");
#endif
    }
};

#else

// Contains thread name after constructed.
class FxLogger_CoreName
  {
  public:
    char mCoreName[9];

    FxLogger_CoreName()
      {
      unsigned CoreId = 0;

      #ifndef NO_PK_PLATFORM
        //CoreId = Platform::Core::GetId() ;
        CoreId = PkCoreGetId();
      #else
        #ifdef THREAD_SAFE
          pthread_t  tid = pthread_self() ; //& 0x0000FFFF);
          CoreId = tid & 0x0000FFFF;
        #endif
      #endif

      int SigFigFlag = 0;
      int ci = 0;
      for( int i = 0; i < 8 ; i++ )
        {
        if( CoreId & 0xF0000000 )
          SigFigFlag = 1;
        if( SigFigFlag )
          {
          mCoreName[ci] = itox[ (CoreId >> 28) & 0x0F ];
          ci++;
          }
        CoreId = CoreId << 4;
        }
      if( ci > 0 )
        mCoreName[ ci ] = '\0';
      else
        {
        // ... otherwise, it must be thread 0, right?
        mCoreName[0] = '0';
        mCoreName[1] = '\0';
        }
      }
  };


// Contains thread name after constructed.
class FxLogger_FiberName
  {
  public:
    char mFiberName[9];

    FxLogger_FiberName()
      {
      unsigned FiberId = 0;

      #ifndef NO_PK_PLATFORM
        //FiberId = Platform::Fiber::GetId() ;
        //FiberId = PkFiberGetId();
        FiberId = PkFiberGetNumber();
      #else
        #ifdef THREAD_SAFE
          pthread_t  tid = pthread_self() ; //& 0x0000FFFF);
          FiberId = tid & 0x0000FFFF;
        #endif
      #endif

      int SigFigFlag = 0;
      int ci = 0;
      for( int i = 0; i < 8 ; i++ )
        {
        if( FiberId & 0xF0000000 )
          SigFigFlag = 1;
        if( SigFigFlag )
          {
          mFiberName[ci] = itox[ (FiberId >> 28) & 0x0F ];
          ci++;
          }
        FiberId = FiberId << 4;
        }
      if( ci > 0 )
        mFiberName[ ci ] = '\0';
      else
        {
        // ... otherwise, it must be thread 0, right?
        mFiberName[0] = '0';
        mFiberName[1] = '\0';
        }
      }
  };
#endif

#ifdef PKFXLOG_DUMMY_OUT

    // For BlueLight bringup, logging is not available.
    // Dummy it out here.
    #define BegLogLine(MyLogMask) { \
        LogStream PkLog ;           \
        PkLog                       \

    #define EndLogLine              \
        0 ; }

    class LogStream
      {
      } ;

    template <class X> static inline LogStream& operator<< (LogStream& ls, const X& x)
      {
      return ls ;
      }

    static inline LogStream& operator<< (LogStream& ls, int x)
      {
      return ls ;
      }

#else // FXLOGGING not dummied out.

    // The following macro begins the definition of a log line.
    // This includes opening a '{ ... }' scope.
    // In theory, the 'if( mask stuff )' should drop the scope out if not used.
    // Every log line is includes the address space and thread number as well
    // as a time stamp so they can be merged and sorted.

#ifdef NDEBUG
enum {pkfxlog_debug = 0};
#else
enum {pkfxlog_debug = 1};
#endif

#ifdef __linux__
    #define __PKFXLOG_BASIC_LOG_LINE(__PKFXLOG_ABORT_FLAG)                                \
            {                                                                             \
            int _pkFxLogAbortFlag = __PKFXLOG_ABORT_FLAG;                                 \
            {                                                                             \
            FxLogger_TimeStamp        ts;                                                 \
            FxLogger_NodeName         an;                                                 \
            FxLogger_ProcessName      ap;                                                 \
            FxLogger_ThreadName       at;                                                 \
            LogStream              PkLog;                                                 \
            PkLog                                                                         \
                  << FormatString("%6d")   << ts.mSeconds                                 \
                                           << "."                                         \
                  << FormatString("%09d")  << ts.mNanoseconds                             \
                                           << " "                                         \
                                           << an.mNodeName                                \
                                           << " "                                         \
                                           << ap.mProcessName                             \
                                           << " "                                         \
                                           << at.mThreadName                              \
                                           << " >"
#else
    #define __PKFXLOG_BASIC_LOG_LINE(__PKFXLOG_ABORT_FLAG)                                \
            {                                                                             \
            int _pkFxLogAbortFlag = __PKFXLOG_ABORT_FLAG;                                 \
            {                                                                             \
            FxLogger_TimeStamp        ts;                                                 \
            FxLogger_NodeName         an;                                                 \
            FxLogger_CoreName         ac;                                                 \
            FxLogger_FiberName        af;                                                 \
            LogStream              PkLog;                                                 \
            PkLog                                                                         \
                  << FormatString("%6d")   << ts.mSeconds                                 \
                                           << "."                                         \
                  << FormatString("%09d")  << ts.mNanoseconds                             \
                                           << " "                                         \
                                           << an.mNodeName                                \
                                           << " "                                         \
                                           << ac.mCoreName                                \
                                           << " "                                         \
                                           << af.mFiberName                               \
                                           << " >"
#endif

    #define BegLogLine(MyLogMask) \
      if( ( PkAnyLogging && MyLogMask ) || PKFXLOG_ALL )  \
        __PKFXLOG_BASIC_LOG_LINE(0)

    // Runtime check that is turned off by NDEBUG flag
    #define AssertLogLine(MyAssertCond)                       \
      if( ( pkfxlog_debug ) && ( ! (MyAssertCond) ) ) \
         __PKFXLOG_BASIC_LOG_LINE(1)                                        \
         << " FAILED ASSERT( " << #MyAssertCond << ") >"

#if defined(PK_STRONG_ASSERT_ONLY_TRAP)
// gcc warns on a multiline comment, so macro it out instead
// Runtime check that is turned off by NDEBUG flag
#  define StrongAssertLogLine(MyAssertCond)                             \
    if( ( pkfxlog_debug ) && ( ! (MyAssertCond) ) )                     \
        __PKFXLOG_BASIC_LOG_LINE(1)                                     \
            << " FAILED ASSERT( " << #MyAssertCond << ") >"
#else
// Runtime check that is not turned off by NDEBUG flag
#  define StrongAssertLogLine(MyAssertCond)                             \
    if( ! (MyAssertCond) )                                              \
        __PKFXLOG_BASIC_LOG_LINE(1)                                     \
            << " FAILED ASSERT( " << #MyAssertCond << " ) >"
#endif

#define EndLogLine0    "< "                                             \
    << __FUNCTION__  << "() "                                           \
    << FxLogger_GetStartPointInFilePathName( __FILE__ )  << " "         \
    << __LINE__   << " "; }                                             \
    if(_pkFxLogAbortFlag) __PK_FXLOG_SERIOUS_ABORT }

#define EndLogLine    "< "                                              \
    << __FUNCTION__  << "() "                                           \
    << FxLogger_GetStartPointInFilePathName( __FILE__ )  << " "         \
    << __LINE__   << " "; }                                             \
    if(_pkFxLogAbortFlag) __PK_FXLOG_SERIOUS_ABORT }


class
    LogStreamBuffer
      {
      public:

	char StrBufSpace[ PKLOG_MAXBUF + 1 + 64 ] ;
	char* StrBuf;

        LogStreamBuffer()
          {
          // NEED TO MOVE THIS TO THE CORE CONTROL BLOCK
          // MAXBUF + 1 byte for null term + 64 bytes to keep off same L1 cache line
          // NOTE: This won't work for more than 4 cores
	  #if ( defined( __linux__ ) )
	    // Handle multithreading

            //static char StrBufSpace[ PKLOG_MAXBUF + 1 + 64 ] ;
            StrBuf = StrBufSpace;
            StrBuf[0] = 0;
          #else
            static char StrBufSpace[ 4 ][ PKLOG_MAXBUF + 1 + 64 ] ;
            ///// int procId = a_hard_processor_id() & 3;
            int procId = rts_get_processor_id() & 1;
            StrBuf = StrBufSpace[ procId ];
            StrBuf[0] = 0;
          #endif
          }

      } ;

    class SprintfStream
      {
      public:
        char * mBuffStart;
        char * mCurrentBufferPointer;
        int    mLength;
        int    mRemainingBufferSpace;

        const char *mFormat;
        int   mFormatFlag;

        SprintfStream( char* aBuffer, int aLength )
          {
          mBuffStart            = aBuffer;
          mCurrentBufferPointer = aBuffer;
          mLength               = aLength;
          mRemainingBufferSpace = aLength; // for new line?
          mFormatFlag           = 0;
          }

        ~SprintfStream(){}

        template<typename T>
        inline void
        SprintfToBuffer( T aArg, const char * aFmt, int aStrLen )
          {
	  int rc = snprintf( mCurrentBufferPointer,
			     // mRemainingBufferSpace,
			     aStrLen,
			     aFmt,
			     aArg );

          if( rc < 0 )
            FxLogger_Abort("snprintf failed");

          mCurrentBufferPointer += rc;
          mRemainingBufferSpace -= rc;
          return;
          }


        template<typename T>
        inline void
        SprintfToBuffer( T aArg, const char * aFmt)
          {
          if( mFormatFlag )
            {
            aFmt = mFormat;
            mFormatFlag = 0;
            }

          #ifdef PK_BLADE
            int rc = sprintf( mCurrentBufferPointer,
                              aFmt,
                              aArg );
          #else
            int rc = snprintf( mCurrentBufferPointer,
                               mRemainingBufferSpace,
                               aFmt,
                               aArg );
          #endif

          if( rc < 0 )
            FxLogger_Abort("snprintf failed");

          mCurrentBufferPointer += rc;
          mRemainingBufferSpace -= rc;
          return;
          }

        inline int GetBuffLength()
          {
          return ( mLength - mRemainingBufferSpace );
          }

        inline char* GetStartOfBuffer()
          {
          return mBuffStart;
          }

      };

   class HexDump
      {
      public:
        unsigned char *mAddr;
        int   mSize;
        int   mMax;
        HexDump( void *aAddr, int aSize )
          {
          mAddr = (unsigned char*) aAddr;
          mSize = aSize;
          mMax  = PKLOG_MAXBUF >> 1;
          }

        HexDump( void *aAddr, int aSize, int aMax )
          {
          mAddr = (unsigned char*) aAddr;
          mSize = aSize;
          mMax  = std::min( aMax, (PKLOG_MAXBUF >> 1) );
          }
      };

    static SprintfStream& operator<< (SprintfStream& ls, HexDump hd )__attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, HexDump hd )
      {
      int bytes = hd.mSize;
      if( hd.mSize > hd.mMax )
        bytes = hd.mMax;

      for(int i = 0; i < bytes ; i++ )
        {
        if( i%4 == 0 )
          {
          ls.SprintfToBuffer( hd.mAddr[ i ], " " );
          }
        ls.SprintfToBuffer( hd.mAddr[ i ], "%02X" );
        }
      return ls;
      }

   class FormatString
      {
      public:
        const char *mFmtStr;
        FormatString( const char *aFmtStr )
          {
          mFmtStr = aFmtStr;
          }
      };

    static SprintfStream& operator<< (SprintfStream& ls, FormatString fs ) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, FormatString fs )
      {
      ls.mFormat     = fs.mFmtStr;
      ls.mFormatFlag = 1;
      return ls;
      }

    static SprintfStream& operator<< (SprintfStream& ls, char x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, char x)
      {
      ls.SprintfToBuffer( x, "%c" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, double x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, double x)
      {
      #if defined(PK_BLADE) || defined(PKLOG_HEXREALS)
        int * ip = (int *) &x;
        ls.SprintfToBuffer( ":XD:","%s" );
        ls.SprintfToBuffer( ip[0], "%08X" );
        ls.SprintfToBuffer( ip[1], "%08X" );
      #else
        /////  BGF we need to regularly be looking at more sigfigs.  ls.SprintfToBuffer( x, "%f" );
        // 16.13f should leave space for full precission including decimal place and signe.
        ls.SprintfToBuffer( x, "% 16.13f" );
      #endif
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, float x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, float x)
      {
      #if defined(PK_BLADE) || defined(PKLOG_HEXREALS)
        int * ip = (int *) &x;
        ls.SprintfToBuffer( ":XD:","%s" );
        ls.SprintfToBuffer( ip[0], "%08X" );
      #else
        ls.SprintfToBuffer( x, "%f" );
      #endif
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, short x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, short x)
      {
      ls.SprintfToBuffer( x, "%d" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, int x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, int x)
      {
      ls.SprintfToBuffer( x, "%d" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, long x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, long x)
      {
      ls.SprintfToBuffer( x, "%ld" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, unsigned long long x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, unsigned long long x)
      {
      ///ls.SprintfToBuffer( x, "%lld" );
      /// Mark G. says this is gonna work in AIX/XLC
      ls.SprintfToBuffer( x, "%llu" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, long long x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, long long x)
      {
      ///ls.SprintfToBuffer( x, "%lld" );
      /// Mark G. says this is gonna work in AIX/XLC
      ls.SprintfToBuffer( x, "%Ld" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, unsigned int x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, unsigned int x)
      {
      ls.SprintfToBuffer( x, "%u" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, unsigned long x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, unsigned long x)
      {
      ls.SprintfToBuffer( x, "%lu" );
      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, void* x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, void* x)
      {
#if defined( __32BIT__ )
      ls.SprintfToBuffer( x, "%08X" );
#else
      ls.SprintfToBuffer( x, "%08lX" );
#endif
      return( ls );
      }

//    static SprintfStream& operator<< (SprintfStream& ls, char* x) __attribute__((unused)) ;
//    static SprintfStream& operator<< (SprintfStream& ls, char* x)
//      {
//	if( (int) strlen( x ) > ls.mRemainingBufferSpace )
//	  FxLogger_Abort( "SprintfStream& operator<< (SprintfStream& ls, char* x):  Log line would over run buffer" );
//
//      ls.SprintfToBuffer( x, "%s" );
//      return( ls );
//      }

    static SprintfStream& operator<< (SprintfStream& ls, const char * x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, const char * x)
      {
      if ( NULL == x )
      {
        ls.SprintfToBuffer("(NULL)","%s") ;
      }
      else
      {
        if( (int) strlen( x ) > ls.mRemainingBufferSpace )
          FxLogger_Abort( "SprintfStream& operator<< (SprintfStream& ls, char* x):  Log line would over run buffer" );
        ls.SprintfToBuffer( x, "%s" );
      }

      return( ls );
      }

    static SprintfStream& operator<< (SprintfStream& ls, fxlogger_string_t x) __attribute__((unused)) ;
    static SprintfStream& operator<< (SprintfStream& ls, fxlogger_string_t x)
      {
	if( x.mSize > ls.mRemainingBufferSpace )
	FxLogger_Abort( "SprintfStream& operator<< (SprintfStream& ls, char* x):  Log line would over run buffer" );

	ls.SprintfToBuffer( x.mData, "%s", x.mSize );

	return( ls );
      }

    class
    LogStream : public LogStreamBuffer, public SprintfStream//ostrstream
      {
      public:
        LogStream()
          : SprintfStream( StrBuf, PKLOG_MAXBUF )
          {
          // Hook in the buffer to the ostrstream interface
          }

        ~LogStream()
          {
          // Get a char pointer to the oststream buffer so we can write it out.
          //char *cp = str();

          char *cp = GetStartOfBuffer();

          int bw = sprintf(mCurrentBufferPointer, "\n");
          mRemainingBufferSpace -= bw;

          // Check that we haven't over run the buffer - would be nice to check this earlier.

          if( GetBuffLength() >= PKLOG_MAXBUF  )
            FxLogger_Abort( "~LogStream() : Log line over ran buffer " );

          // Write this log line to log port.
          FxLogger_WriteBufferToLogPort( cp, GetBuffLength() );

          }

       };

// MCP lifted from old fxlogger
#if 0

#define LogHexDump1(Thing)    " 0x" << hex << setw(8) << ((int *)Thing)[0] << " "
#define LogHexDump2(Thing)    " 0x" << hex << setw(8) << ((int *)Thing)[0] << " " \
                           << " 0x" << hex << setw(8) << ((int *)Thing)[1]
#define LogHexDump3(Thing)    " 0x" << hex << setw(8) << ((int *)Thing)[0] << " " \
                           << " 0x" << hex << setw(8) << ((int *)Thing)[1] << " " \
                           << " 0x" << hex << setw(8) << ((int *)Thing)[2] << " "
#define LogHexDump4(Thing)    " 0x" << hex << setw(8) << ((int *)Thing)[0] << " " \
                           << " 0x" << hex << setw(8) << ((int *)Thing)[1] << " " \
                           << " 0x" << hex << setw(8) << ((int *)Thing)[2] << " " \
                           << " 0x" << hex << setw(8) << ((int *)Thing)[3]
#endif

#define LogHexDump1(Thing)    " 0x" << (void *)  ((int *)Thing)[0] << " "
#define LogHexDump2(Thing)    " 0x" << (void *)  ((int *)Thing)[0] << " " \
                           << " 0x" << (void *)  ((int *)Thing)[1]
#define LogHexDump3(Thing)    " 0x" << (void *)  ((int *)Thing)[0] << " " \
                           << " 0x" << (void *) ((int *)Thing)[1] << " " \
                           << " 0x" << (void *)  ((int *)Thing)[2] << " "
#define LogHexDump4(Thing)    " 0x" << (void *)  ((int *)Thing)[0] << " " \
                           << " 0x" << (void *)  ((int *)Thing)[1] << " " \
                           << " 0x" << (void *)  ((int *)Thing)[2] << " " \
                           << " 0x" << (void *)  ((int *)Thing)[3]

#endif
#endif
