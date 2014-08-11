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

#include <FxLogger.hpp>

void
FxLogger_WriteBufferToLogPort( char * aBuffer, int aLen )
  {
//  static int PortOpenedFlag = 0;

  static int FileCreatedFlag = 0;

  static int fd = -1;

  if( FileCreatedFlag == 0 )
    {
//      int Rank = FxLoggerNodeId;
      char FileName[ 1024 ];

      FxLogger_NodeName ThisNode;

      if( FxLoggerProgramName[0] == '\0' )
        sprintf( FileName, "%s.FxLog", ThisNode.mNodeName );
      else
        sprintf( FileName, "/tmp/%s.%s.FxLog", FxLoggerProgramName, ThisNode.mNodeName );

      fd = open( FileName,
                 O_CREAT | O_APPEND | O_WRONLY | O_TRUNC,
                 S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH );

      if( fd < 0 )
        {
          /////char ErrBuff[ 128 ];
          ////sprintf( ErrBuff, "FxLogger Creating file failed. Sendng long lines to stdout. ((( open errno: %d", errno );
          ////FxLogger_Abort( ErrBuff );
          printf("FxLogger: Writing log file to stdout\n" );
          fd = 1;
        }
      else
        {
          // The following if you want a common name w/o job id, etc.
          char FileNameLink[ 1024 ];

          sprintf( FileNameLink, "/tmp/Latest.FxLog.%s", FxLoggerProgramName );

          symlink( FileName, FileNameLink );
//          printf("FxLogger: Writing log file to >%s<\n", FileName );
        }
      FileCreatedFlag = 1;
    }

  int BytesWritten = 0;
  int ErrorCount = 0;
  while( BytesWritten < aLen )
    {

int TryLen = aLen-BytesWritten;
#if  defined(PK_VRNIC)
if( TryLen > 248 )
  TryLen = 248;
#endif

      int wlen = write( fd, & aBuffer[ BytesWritten ], TryLen );  // gotta think about this.

      if( wlen < 0 )
        {
          ErrorCount++;
          printf("FxLogger error: %d TryLen %d Total Len %d\n", errno, TryLen, aLen );
          if( ErrorCount == 32 )
            FxLogger_Abort( "FxLogger_WriteBufferToLogPort:: Aborting on a rc < 0" );
        }
      else
        {
        BytesWritten += wlen;
        }
    }
  }


// C-callable fxlogger, for use with JAM
extern "C" {
  extern void pkfxlog_c(const char * file, int line, const char * logline, int abortflag);
} ;

extern void pkfxlog(const char * file, int line, const char * logline, int abortflag);

void pkfxlog_c(const char * file, int line, const char * logline, int abortflag)
  {
    pkfxlog(file,line,logline,abortflag) ;
  }

void pkfxlog(const char * file, int line, const char * logline, int abortflag)
  {
    if( ( PkAnyLogging ) || PKFXLOG_ALL )
      __PKFXLOG_BASIC_LOG_LINE(abortflag)
          << logline
          << "<    "
          << FxLogger_GetStartPointInFilePathName( file )
          << " "
          << line
          << " ";
        }
      if(_pkFxLogAbortFlag) __PK_FXLOG_SERIOUS_ABORT
    }

  }

int   FxLoggerNodeId = -1;
char  FxLoggerProgramName[ 1024 ] = { '\0' };
