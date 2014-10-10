/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     lschneid - initial implementation
 */

#define __64BIT__

#include <mpi.h>
#include <FxLogger.hpp>
#include <skv/client/skv_client.hpp>

#include <unistd.h>
#include <string>
#include <iostream>
#include <iomanip>

#ifndef SKV_BENCH_LOG
#define SKV_BENCH_LOG ( 0 )
#endif

#ifndef SKV_BENCH_DATA_LOG
#define SKV_BENCH_DATA_LOG ( 0 )
#endif

#define DEFAULT_START ( 1 )
#define DEFAULT_END   ( 2 )
#define DEFAULT_STEP  ( 1 )
#define DEFAULT_QUEUE_DEPTH ( 16 )
#define DEFAULT_BATCH_COUNT ( -4 )
#define DEFAULT_DATA_CHECK ( false )
#define DEFAULT_AVG_ERROR ( 5 )
#define DEFAULT_TIME_LIMIT ( 1 )
#define DEFAULT_PDS_NAME "SKV_BENCH_PDS"
#define DEFAULT_MAX_ATTEMPTS ( 15 )

#define MB_FACTOR ( (1000 * 1000) )




/*************************************************************
 * Output formating macros
 */
#define SB_OUT_KEYLEN (4)
#define SB_OUT_VALLEN (10)
#define SB_OUT_KVLEN (SB_OUT_KEYLEN+SB_OUT_VALLEN )

#define SB_OUT_BWLEN (10)
#define SB_OUT_IOLEN (10)
#define SB_OUT_RQLEN (8)
#define SB_OUT_SETLEN ( SB_OUT_RQLEN+SB_OUT_IOLEN+SB_OUT_BWLEN+1 )

#define SB_OUTBW std::setw( SB_OUT_BWLEN )
#define SB_OUTIO std::setw( SB_OUT_IOLEN )
#define SB_OUTRQ std::setw( SB_OUT_RQLEN )


/*************************************************************
 * Parameter Range Handling
 */
class skv_parameter_range_t
{
  int mStart;
  int mEnd;
  int mStep;

  int (skv_parameter_range_t::*mNext)(int);

public:
  skv_parameter_range_t() : mStart( DEFAULT_START ), mEnd( DEFAULT_END ), mStep( DEFAULT_STEP ), mNext() {}

  int Init( int aStart, int aEnd, int aStep = 1, bool aMultiply = false )
  {
    mStart = aStart;
    mEnd = aEnd;
    if( aEnd < aStart )
      return 1;

    mStep = aStep;

    if( aMultiply )
      mNext = &skv_parameter_range_t::MultiplyOperator;
    else
      mNext = &skv_parameter_range_t::LinearOperator;

    return 0;
  }
  int Init( const std::string &aInStr )
  {
    std::string parse( aInStr );

    // hunt for the Start of Range
    size_t colon = parse.find_first_of( ':' );
    if( colon == 0 )
      mStart = 0;
    else if( colon == std::string::npos )
    {
      int number = atoi ( parse.c_str() );
      Init( number, number, 1, false );
      colon = parse.length()-1;
    }
    else
    {
      std::string startStr( parse, 0, colon );
      mStart = atoi ( startStr.c_str() );
    }
    parse.erase( 0, colon+1 );

    if( parse.empty() )
      return 0;

    // hunt for the End of Range
    colon = parse.find_first_of( ':' );
    if( colon == 0 )
      mEnd = 0;
    else if( colon == std::string::npos )
    {
      mEnd = atoi( parse.c_str() );
      mStep = 1;
      mNext = &skv_parameter_range_t::LinearOperator;
      colon = parse.length()-1;
    }
    else
    {
      std::string endStr( parse, 0, colon );
      mEnd = atoi( endStr.c_str() );
    }
    parse.erase( 0, colon+1 );

    // hunt for the stepping
    if( !parse.empty() )
    {
      if( parse[0] == '*' )
      {
        mNext = &skv_parameter_range_t::MultiplyOperator;
        parse.erase( 0, 1 );   // remove entry
      }
      else
      {
        mNext = &skv_parameter_range_t::LinearOperator;
        if( parse[0] == '+' )
          parse.erase( 0, 1 );
      }
      mStep = atoi( parse.c_str() );
    }

    return 0;
  }

  inline bool OutOfRange( int aIn ) const { return (( aIn < mStart ) || ( aIn > mEnd )); }
  inline int GetStart() const { return mStart; }
  inline int GetEnd() const { return mEnd; }
  inline int GetStep() const { return mStep; }
  inline bool IsLinear() const { return  (mNext == &skv_parameter_range_t::LinearOperator); }
  inline int Next( int aIn ) { return (this->*mNext)( aIn ); }

private:
  int LinearOperator( int aIn )
  {
    return aIn + mStep;
  }
  int MultiplyOperator( int aIn )
  {
    return aIn * mStep;
  }


};

/*************************************************************
 * Configuration of Benchmark
 */
class skv_bench_config_t
{
public:
// Options to change by command line
  skv_parameter_range_t mKeySize;
  skv_parameter_range_t mValueSize;
  double mAvgError;
  int mQueueDepth;
  int mTimeLimit;
  int mBatchSize;
  bool mDataCheck;

// Settings and defaults set internally
  int mRank;
  int mNodeCount;
  std::string mPDSName;

  skv_bench_config_t() :
    mKeySize(),
    mValueSize(),
    mAvgError( DEFAULT_AVG_ERROR ),
    mNodeCount( 1 ),
    mRank( 0 ),
    mQueueDepth( DEFAULT_QUEUE_DEPTH ),
    mBatchSize( DEFAULT_BATCH_COUNT ),
    mDataCheck( DEFAULT_DATA_CHECK ),
    mTimeLimit( DEFAULT_TIME_LIMIT ),
    mPDSName( DEFAULT_PDS_NAME )
  {}

  int Parse( int aArgC, char **aArgV )
  {
    int rc = 0;
    int op;
    while ((op = getopt(aArgC, aArgV, "b:ce:hk:q:t:v:")) != -1)
    {
      char *endp;
      switch(op)
      {
        default:
          std::cerr << "Invalid Options." << std:: endl << std::endl;
          rc = -1;
        case 'h':
        {
          std::cout << "USAGE: skv_bench" << std::endl;
          std::cout << " -k  <start>[:<end>[:[+|*]<step>]]  : Range of Key size" << std::endl;
          std::cout << " -v  <start>[:<end>[:[+|*]<step>]]  : Range of Value size" << std::endl;
          std::cout << " -q  <queue_depth>                  : Number of in-flight requests (default: " << DEFAULT_QUEUE_DEPTH << ")" << std::endl;
          std::cout << " -b  <batch_count>                  : Number of batches to create the in-flight requests (default: " << -DEFAULT_BATCH_COUNT << ")" << std::endl;
          std::cout << " -c                                 : enable data check (default: " << (DEFAULT_DATA_CHECK?"ON":"OFF") << ")"<< std::endl;
          std::cout << " -t <seconds>                       : time limit for each test loop (default: " << (DEFAULT_TIME_LIMIT) << ")" << std::endl;
          std::cout << " -e <percent>                       : max allowed error to achieve converged measurement in percent (default: " << DEFAULT_AVG_ERROR << ")" << std::endl;
          std::cout << std::endl;
          break;
        }
        case 'b':
          mBatchSize = atoi( optarg ) * (-1);  // tempor. make it a neg number to signal that it hold batch COUNT that needs to be turned into SIZE
          break;
        case 'c':
          mDataCheck = true;
          break;
        case 'e':
          mAvgError = (double)atoi( optarg );
          break;
        case 'k':
          rc = mKeySize.Init( optarg );
          break;
        case 'q':
          mQueueDepth = atoi( optarg );
          break;
        case 'v':
          rc = mValueSize.Init( optarg );
          break;
        case 't':
          mTimeLimit = atoi( optarg );
          break;
      }
    }
    if( mBatchSize < 0 )
      mBatchSize = mQueueDepth / (-mBatchSize);

    return rc;
  }
  bool SanityCheck()
  {
    return ( ( mBatchSize > 0)
        && ( mTimeLimit >= 0)
        );
  }
};


template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_bench_config_t& aIn )
{
  os << "Keyrange:   [" << aIn.mKeySize.GetStart() << ":" << aIn.mKeySize.GetEnd() << "], "
     << ((aIn.mKeySize.IsLinear())?"+":"*") << aIn.mKeySize.GetStep() << std::endl;
  os << "ValueRange: [" << aIn.mValueSize.GetStart() << ":" << aIn.mValueSize.GetEnd() << "], "
     << ((aIn.mValueSize.IsLinear())?"+":"*") << aIn.mValueSize.GetStep() << std::endl;
  os << "QueueDepth: " << aIn.mQueueDepth << std::endl;
  os << "Batch Size: " << aIn.mBatchSize << std::endl;
  os << "Data Check: " << (aIn.mDataCheck?"ON":"OFF") << std::endl;
  os << "Time Limit: " << aIn.mTimeLimit << "s"<< std::endl;
  os << "Max Error:  " << aIn.mAvgError << "%"<< std::endl;
  return(os);
}

/*************************************************************
 * Benchmark class
 */
typedef enum {
  SKV_BENCH_STATE_UNKNOWN = 0x0000,
  SKV_BENCH_STATE_ERROR = 0x0001,
  // initialization states
  SKV_BENCH_STATE_RESET       = 0x0100,
  SKV_BENCH_STATE_INITIALIZED = 0x0101,
  SKV_BENCH_STATE_CONNECTED   = 0x0102,
  SKV_BENCH_STATE_PDS_OPEN    = 0x0103,
  // benchmark run states
  SKV_BENCH_STATE_INSERT_COMPLETE   = 0x1000,
  SKV_BENCH_STATE_RETRIEVE_COMPLETE = 0x1001,
  SKV_BENCH_STATE_REMOVE_COMPLETE   = 0x1002
} skv_bench_state_t;

struct skv_bench_measurement_t {
  double mRequests;
  double mTime;
  double mIOPS;
  double mBW;

  void Reset()
  {
    mRequests = 0;
    mTime = 0.0;
    mIOPS = 0.0;
    mBW = 0.0;
  }
  void Calculate( double aTime, int aSizePerOp, double aOpCount )
  {
    mRequests = aOpCount;
    mTime = aTime;
    mBW = ((double)aSizePerOp * mRequests) / mTime / MB_FACTOR;
    mIOPS = mRequests / mTime;
  }
  skv_bench_measurement_t& operator=( const skv_bench_measurement_t &aIn )
  {
    mRequests = aIn.mRequests;
    mTime = aIn.mTime;
    mIOPS = aIn.mIOPS;
    mBW = aIn.mBW;
    return (*this);
  }
};
template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_bench_measurement_t& aIn )
{
  os << SB_OUTRQ << std::setprecision(0) << aIn.mRequests << "x"
     << std::setprecision(1) << SB_OUTIO << aIn.mIOPS << SB_OUTBW << aIn.mBW;
  return(os);
}

struct skv_measurement_set_t {
  skv_bench_measurement_t mInsert;
  skv_bench_measurement_t mRetrieve;
  skv_bench_measurement_t mRemove;

public:
  void Reset()
  {
    mInsert.Reset();
    mRetrieve.Reset();
    mRemove.Reset();
  }
  skv_measurement_set_t& operator=( const skv_measurement_set_t &aIn )
  {
    mInsert = aIn.mInsert;
    mRetrieve = aIn.mRetrieve;
    mRemove = aIn.mRemove;
    return (*this);
  }
};
template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_measurement_set_t& aIn )
{
  os << aIn.mInsert << " |" << aIn.mRetrieve << " |" << aIn.mRemove;
  return(os);
}

#define NEXT_X8_SIZE(x) ( ((((x)-1)>>3) + 1) << 3 )

class skv_bench_t
{
  skv_client_t mClient;
  skv_bench_state_t mState;

  skv_pds_id_t mPDSId;

  const skv_bench_config_t *mConfigRef;

  char *mKeyBuffer;
  char *mValueBuffer;
  skv_client_cmd_ext_hdl_t *mHandleBuffer;

  skv_status_t mExitStatus;
public:
  int mKeySize;
  int mValueSize;

  // Measurements
  uint64_t mOpCount;
  skv_measurement_set_t mLocalData;
  skv_measurement_set_t mGlobalData;

private:
  void InitKeyBuffer( char *aKeyBuffer, int aItems, uint64_t aStart )
  {
    uint64_t keyval = aStart;
    uint64_t skipSize = NEXT_X8_SIZE( mKeySize );

    BegLogLine( SKV_BENCH_DATA_LOG )
      << "KeySettings: start: " << (void*)aStart
      << " skipSize: " << skipSize
      << " buffer@:0x " << (void*)aKeyBuffer
      << EndLogLine;

    for( int idx = 0; (idx < aItems); idx++ )
    {
      char *Key = (char*)aKeyBuffer + ( idx * skipSize );
      *(uint64_t*)Key = keyval;

      if( skipSize > 8 )
      {
        Key = (char*)aKeyBuffer + ( idx * skipSize + 8 );
        *(uint64_t*)Key = keyval+1;
      }

      BegLogLine( SKV_BENCH_DATA_LOG )
        << "Key: " << (void*)(*(uint64_t*)Key) << " from: " << (void*)keyval
        << " cpy: " << (void*)(Key) << ":" << (void*)((char*)&keyval) << ":" << mKeySize
        << EndLogLine;

      keyval += mConfigRef->mNodeCount;
    }
  }
  void InitValueBuffer( char* aValueBuffer, int aItems )
  {
    uint64_t *valbuf = (uint64_t*)aValueBuffer;
    for( size_t n=0; n < (mValueSize * aItems)/sizeof(uint64_t); n++ )
    {
      uint64_t valval = random() << 32 + random();
      valbuf[n] = valval;
    }
  }
public:
  skv_status_t Init( const skv_bench_config_t &aConfig )
  {
    mState = SKV_BENCH_STATE_RESET;
    mExitStatus = SKV_SUCCESS;
    mConfigRef = &aConfig;

    /*****************************************************************************
     * Init the SKV Client
     ****************************************************************************/
    skv_status_t status = mClient.Init( 0,
                                        MPI_COMM_WORLD,
                                        aConfig.mRank );
    if( status == SKV_SUCCESS )
      {
        BegLogLine( SKV_BENCH_LOG )
          << "skv_bench: SKV Client Init succeded "
          << EndLogLine;
        mState = SKV_BENCH_STATE_INITIALIZED;
      }
    else
      {
        BegLogLine( SKV_BENCH_LOG )
          << "skv_bench: SKV Client Init FAILED "
          << " status: " << skv_status_to_string( status )
          << EndLogLine;
      }
    /****************************************************************************/

    if( mState != SKV_BENCH_STATE_INITIALIZED )
      return status;

    /*****************************************************************************
     * Connect to the SKV Server
     ****************************************************************************/
    BegLogLine( SKV_BENCH_LOG )
      << "skv_bench: About to connect "
      << EndLogLine;

    status = mClient.Connect( NULL, 0 );

    if( status == SKV_SUCCESS )
      {
        BegLogLine( SKV_BENCH_LOG )
          << "skv_bench: SKV Client connected"
          << EndLogLine;
        mState = SKV_BENCH_STATE_CONNECTED;
      }
    else
      {
        BegLogLine( SKV_BENCH_LOG )
          << "skv_bench: SKV Client FAILED to connect. "
          << " status: " << skv_status_to_string( status )
          << EndLogLine;
      }
    /****************************************************************************/

    if( mState != SKV_BENCH_STATE_CONNECTED )
      return status;

    /*****************************************************************************
     * Open a test PDS
     ****************************************************************************/
    status = mClient.Open( (char*)aConfig.mPDSName.c_str(),
                           (skv_pds_priv_t) (SKV_PDS_READ | SKV_PDS_WRITE),
                           (skv_cmd_open_flags_t) SKV_COMMAND_OPEN_FLAGS_CREATE,
                           & mPDSId );

    if( status == SKV_SUCCESS )
      {
        BegLogLine( SKV_BENCH_LOG )
          << "skv_bench: SKV Client successfully opened: "
          << aConfig.mPDSName.c_str()
          << " MyPDSId: " << mPDSId
          << EndLogLine;
        mState = SKV_BENCH_STATE_PDS_OPEN;
      }
    else
      {
        BegLogLine( SKV_BENCH_LOG )
          << "skv_bench: SKV Client FAILED to open: "
          << aConfig.mPDSName.c_str()
          << " status: " << skv_status_to_string( status )
          << EndLogLine;
      }
    /****************************************************************************/

    if( mState != SKV_BENCH_STATE_PDS_OPEN )
      return status;

    mKeyBuffer = NULL;
    mValueBuffer = NULL;
    mHandleBuffer = NULL;
    return status;
  }
  skv_status_t Exit()
  {
    skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;

    if( mState > SKV_BENCH_STATE_PDS_OPEN )
      status = mClient.Close( &mPDSId );

    if( mState > SKV_BENCH_STATE_CONNECTED )
      status = mClient.Disconnect();

    if( mState > SKV_BENCH_STATE_INITIALIZED )
      status = mClient.Finalize();
    return status;
  }
  skv_status_t Reset( int aKeySize, int aValueSize )
  {
    mKeySize = aKeySize;
    mValueSize = aValueSize;

    mLocalData.Reset();
    mGlobalData.Reset();

    mExitStatus = SKV_SUCCESS;

    if( mKeyBuffer )
      delete mKeyBuffer;
    mKeyBuffer = new char[ NEXT_X8_SIZE( aKeySize ) * mConfigRef->mQueueDepth ];

    if( mValueBuffer )
      delete mValueBuffer;
    mValueBuffer = new char[ NEXT_X8_SIZE( aValueSize ) * mConfigRef->mQueueDepth ];

    if( mHandleBuffer )
      delete mHandleBuffer;
    mHandleBuffer = new skv_client_cmd_ext_hdl_t[ mConfigRef->mQueueDepth ];

    return SKV_SUCCESS;
  }

  skv_status_t InsertBatch( const char* aKeyBuffer,
                            const char *aValueBuffer,
                            skv_client_cmd_ext_hdl_t *aHandleBuffer,
                            int aBatchSize )
  {
    BegLogLine( SKV_BENCH_DATA_LOG )
      << "INST FIRST HANDLE @" << (void*)aHandleBuffer << EndLogLine;
    skv_status_t status = SKV_SUCCESS;
    size_t keyptr_offset = NEXT_X8_SIZE( mKeySize ) - mKeySize;

    for( int idx = 0; (idx < aBatchSize) && (status == SKV_SUCCESS); idx++ )
    {
      char *Key = (char*)aKeyBuffer + ( idx * NEXT_X8_SIZE( mKeySize ) + keyptr_offset );
      char *Value = (char*)aValueBuffer + ( idx * mValueSize );
      skv_client_cmd_ext_hdl_t *Hndl = &(aHandleBuffer[ idx ]);

      status = mClient.iInsert( &mPDSId,
                                Key,
                                mKeySize,
                                Value,
                                mValueSize,
                                0,
                                SKV_COMMAND_RIU_FLAGS_NONE,
                                Hndl );

      BegLogLine( SKV_BENCH_DATA_LOG )
        << "Insert Key: " << (void*)(*(uint64_t*)Key)
        << " hdl: " << (void*)(*Hndl)
        << " status: " << skv_status_to_string ( GetExitStatus() )
        << EndLogLine;
    }
    return status;
  }
  skv_status_t RetrieveBatch( const char* aKeyBuffer,
                              const char *aValueBuffer,
                              skv_client_cmd_ext_hdl_t *aHandleBuffer,
                              int aBatchSize )
  {
    skv_status_t status = SKV_SUCCESS;
    size_t keyptr_offset = NEXT_X8_SIZE( mKeySize ) - mKeySize;

    for( int idx = 0; (idx < aBatchSize) && (status == SKV_SUCCESS); idx++ )
    {
      char *Key = (char*)aKeyBuffer + ( idx * NEXT_X8_SIZE( mKeySize ) + keyptr_offset );
      char *Value = (char*)aValueBuffer + ( idx * mValueSize );
      skv_client_cmd_ext_hdl_t *Hndl = &(aHandleBuffer[ idx ]);
      int ValueRetrievedSize;

      status = mClient.iRetrieve( &mPDSId,
                                  Key,
                                  mKeySize,
                                  Value,
                                  mValueSize,
                                  &ValueRetrievedSize,
                                  0,
                                  SKV_COMMAND_RIU_FLAGS_NONE,
                                  Hndl );

      BegLogLine( SKV_BENCH_DATA_LOG )
        << "Retrieve Key: " << (void*)(*(uint64_t*)Key)
        << " hdl: " << (void*)(*Hndl)
        << " status: " << skv_status_to_string ( GetExitStatus() )
        << EndLogLine;
    }
    return status;
  }
  skv_status_t RemoveBatch( const char* aKeyBuffer,
                            skv_client_cmd_ext_hdl_t *aHandleBuffer,
                            int aBatchSize )
  {
    skv_status_t status = SKV_SUCCESS;
    size_t keyptr_offset = NEXT_X8_SIZE( mKeySize ) - mKeySize;

    for( int idx = 0; (idx < aBatchSize) && (status == SKV_SUCCESS); idx++ )
    {
      char *Key = (char*)aKeyBuffer + ( idx * NEXT_X8_SIZE( mKeySize ) + keyptr_offset );
      skv_client_cmd_ext_hdl_t *Hndl = &(aHandleBuffer[ idx ]);

      status = mClient.iRemove( &mPDSId,
                                Key,
                                mKeySize,
                                SKV_COMMAND_REMOVE_FLAGS_NONE,
                                Hndl );

      BegLogLine( SKV_BENCH_DATA_LOG )
        << "Remove Key: " << (void*)(*(uint64_t*)Key)
        << " hdl: " << (void*)(*Hndl)
        << " status: " << skv_status_to_string ( GetExitStatus() )
        << EndLogLine;
    }
    return status;
  }
  skv_status_t WaitForBatch( skv_client_cmd_ext_hdl_t *aHandleBuffer, int aItems )
  {
    BegLogLine( SKV_BENCH_DATA_LOG )
      << "WAIT FIRST HANDLE @" << (void*)aHandleBuffer << EndLogLine;
    for( int i=0; ( i < aItems ); i++ )
    {
      SetExitStatus( mClient.Wait( aHandleBuffer[i] ) );

      BegLogLine( SKV_BENCH_DATA_LOG )
        << " hndl: " << (void*)(aHandleBuffer[i])
        << " wait status: " << skv_status_to_string( GetExitStatus() )
        << EndLogLine;
    }
    return GetExitStatus();
  }
  inline
  void SetExitStatus( skv_status_t aNew )
  {
    // only change if there's no error yet
    if( mExitStatus == SKV_SUCCESS )
      mExitStatus = aNew;
  }
  inline skv_status_t GetExitStatus() { return mExitStatus; }

  skv_status_t Run()
  {
    skv_status_t status = SKV_SUCCESS;
    /*********************************************************
     * insert phase
     */
    double TimeLimit = mConfigRef->mTimeLimit;
    double CurrentTime = MPI_Wtime();
    size_t Requests = 0;
    size_t RequestLimit = mKeySize >= 3 ? (1<<20)/mConfigRef->mNodeCount : ((0x1 << (mKeySize * 8)) / mConfigRef->mNodeCount );
    int BatchIdx = 0;
    int MaxBatch = (int)(mConfigRef->mQueueDepth/mConfigRef->mBatchSize);

    BegLogLine( SKV_BENCH_LOG ) << "Starting Insert phase... RequestLimit: " << RequestLimit << EndLogLine;

    // set up lists of pointers for more convenient batch operation
    char **KeyBufferList = new char*[MaxBatch];
    char **ValueBufferList = new char*[MaxBatch];
    skv_client_cmd_ext_hdl_t **HandleBufferList = new skv_client_cmd_ext_hdl_t*[MaxBatch];

    for( int i=0; i<MaxBatch; i++)
    {
      KeyBufferList[i] = &mKeyBuffer[ i * NEXT_X8_SIZE( mKeySize ) * mConfigRef->mBatchSize ];
      ValueBufferList[i] = &mValueBuffer[ i * mValueSize * mConfigRef->mBatchSize ];
      HandleBufferList[i] = &mHandleBuffer[ i * mConfigRef->mBatchSize ];
    }

    double StartTime = MPI_Wtime();
    InitKeyBuffer( mKeyBuffer, mConfigRef->mQueueDepth, mConfigRef->mRank );
    InitValueBuffer( mValueBuffer, mConfigRef->mQueueDepth );

    // fill the pipeline
    for( int batch = 0; batch < MaxBatch; batch++ )
      SetExitStatus( InsertBatch( (const char*)(KeyBufferList[ batch ]),
                                  (const char*)(ValueBufferList[ batch]),
                                  HandleBufferList[ batch ],
                                  mConfigRef->mBatchSize ) );
    Requests += mConfigRef->mQueueDepth;
    /// LOOP
    while( (Requests + mConfigRef->mBatchSize <= RequestLimit) && ( CurrentTime - StartTime < TimeLimit) )
    {
      /// wait for batch of requests
      status = WaitForBatch( HandleBufferList[ BatchIdx ], mConfigRef->mBatchSize );
      SetExitStatus( status );

      /// recreate batch of requests
      InitKeyBuffer( KeyBufferList[ BatchIdx ], mConfigRef->mBatchSize, mConfigRef->mRank + Requests * mConfigRef->mNodeCount );
      InitValueBuffer( ValueBufferList[ BatchIdx ], mConfigRef->mBatchSize );

      /// post batch of requests
      status = InsertBatch( (const char*)(KeyBufferList[ BatchIdx ]),
                            (const char*)(ValueBufferList[ BatchIdx ]),
                            HandleBufferList[ BatchIdx ],
                            mConfigRef->mBatchSize );

      SetExitStatus( status );
      /// END LOOP
      BatchIdx = (BatchIdx + 1) % MaxBatch;
      CurrentTime = MPI_Wtime();
      Requests += mConfigRef->mBatchSize;
    }
    // flush the pipeline
    for( int batch = 0; batch < MaxBatch; batch++ )
      SetExitStatus( WaitForBatch( HandleBufferList[ batch ],
                                   mConfigRef->mBatchSize ) );

    CurrentTime = MPI_Wtime();
    MPI_Barrier( MPI_COMM_WORLD );
    mLocalData.mInsert.Calculate( CurrentTime-StartTime, mValueSize, (double)Requests );

    /*********************************************************
     * retrieve phase
     */
    BegLogLine( SKV_BENCH_LOG ) << "Starting Retrieve phase.." << EndLogLine;

    CurrentTime = MPI_Wtime();
    RequestLimit = Requests;  // Never do more retrieve/remove requests than inserts!
    Requests = 0;
    BatchIdx = 0;
    InitKeyBuffer( mKeyBuffer, mConfigRef->mQueueDepth, mConfigRef->mRank );

    // fill the pipeline
    StartTime = MPI_Wtime();
    for( int batch = 0; batch < MaxBatch; batch++ )
      SetExitStatus( RetrieveBatch( (const char*)(KeyBufferList[ batch ]),
                                    (const char*)(ValueBufferList[ batch]),
                                    HandleBufferList[ batch ],
                                    mConfigRef->mBatchSize ) );
    Requests += mConfigRef->mQueueDepth;
    /// LOOP
    while( (Requests + mConfigRef->mBatchSize <= RequestLimit) && ( CurrentTime - StartTime < TimeLimit) )
    {
      /// wait for batch of requests
      status = WaitForBatch( HandleBufferList[ BatchIdx ], mConfigRef->mBatchSize );
      SetExitStatus( status );

      /// recreate batch of requests
      InitKeyBuffer( KeyBufferList[ BatchIdx ], mConfigRef->mBatchSize, mConfigRef->mRank + Requests * mConfigRef->mNodeCount );

      /// post batch of requests
      status = RetrieveBatch( (const char*)(KeyBufferList[ BatchIdx ]),
                              (const char*)(ValueBufferList[ BatchIdx ]),
                              HandleBufferList[ BatchIdx ],
                              mConfigRef->mBatchSize );
      SetExitStatus( status );

      /// END LOOP
      BatchIdx = (BatchIdx + 1) % MaxBatch;
      CurrentTime = MPI_Wtime();
      Requests += mConfigRef->mBatchSize;
    }
    // flush the pipeline
    for( int batch = 0; batch < MaxBatch; batch++ )
      SetExitStatus( WaitForBatch( HandleBufferList[ batch ],
                                  mConfigRef->mBatchSize ) );

    CurrentTime = MPI_Wtime();
    MPI_Barrier( MPI_COMM_WORLD );
    mLocalData.mRetrieve.Calculate( CurrentTime-StartTime, mValueSize, (double)Requests );

    /*********************************************************
     * remove phase
     */
    BegLogLine( SKV_BENCH_LOG ) << "Starting Remove phase.." << EndLogLine;

    CurrentTime = MPI_Wtime();
    Requests = 0;
    BatchIdx = 0;
    InitKeyBuffer( mKeyBuffer, mConfigRef->mQueueDepth, mConfigRef->mRank );

    // fill the pipeline
    StartTime = MPI_Wtime();
    for( int batch = 0; batch < MaxBatch; batch++ )
      SetExitStatus( RemoveBatch( (const char*)(KeyBufferList[ batch ]),
                                  HandleBufferList[ batch ],
                                  mConfigRef->mBatchSize ) );
    Requests += mConfigRef->mQueueDepth;
    /// LOOP: there can't be a timeout for remove calls since this step needs to clean up all inserted data
    while( (Requests + mConfigRef->mBatchSize <= RequestLimit) )// && ( CurrentTime - StartTime < TimeLimit) )
    {
      /// wait for batch of requests
      status = WaitForBatch( HandleBufferList[ BatchIdx ], mConfigRef->mBatchSize );
      SetExitStatus( status );

      /// recreate batch of requests
      InitKeyBuffer( KeyBufferList[ BatchIdx ], mConfigRef->mBatchSize, mConfigRef->mRank + Requests * mConfigRef->mNodeCount );

      /// post batch of requests
      status = RemoveBatch( (const char*)(KeyBufferList[ BatchIdx ]),
                            HandleBufferList[ BatchIdx ],
                            mConfigRef->mBatchSize );
      SetExitStatus( status );

      /// END LOOP
      BatchIdx = (BatchIdx + 1) % MaxBatch;
      CurrentTime = MPI_Wtime();
      Requests += mConfigRef->mBatchSize;
    }
    // flush the pipeline
    for( int batch = 0; batch < MaxBatch; batch++ )
      SetExitStatus( WaitForBatch( HandleBufferList[ batch ],
                                   mConfigRef->mBatchSize ) );

    CurrentTime = MPI_Wtime();
    MPI_Barrier( MPI_COMM_WORLD );
    mLocalData.mRemove.Calculate( CurrentTime-StartTime, mValueSize, (double)Requests );

    MPI_Allreduce( & mLocalData.mInsert, & mGlobalData.mInsert, 4, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD );
    MPI_Allreduce( & mLocalData.mRetrieve, & mGlobalData.mRetrieve, 4, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD );
    MPI_Allreduce( & mLocalData.mRemove, & mGlobalData.mRemove, 4, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD );

    return GetExitStatus();
  }

};
template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_bench_t& aIn )
{
  os << std::setw(SB_OUT_KEYLEN) << aIn.mKeySize << std::setw(SB_OUT_VALLEN) << aIn.mValueSize << " |*|"
     << std::fixed << std::setprecision(1) << aIn.mLocalData << " |*|" << aIn.mGlobalData;
  return(os);
}

#define SKV_BENCH_STAT_LEN ( 5 )

class skv_bench_gliding_avg_t {
  skv_measurement_set_t mList[ SKV_BENCH_STAT_LEN ];
  int mCurrentIdx;

  double mAvgInsertIOPS;
  double mAvgRetrieveIOPS;
  double mAvgRemoveIOPS;

public:
  void PrintHeader()
  {
    int x;
    std::cout << std::setw(SB_OUT_KVLEN) << "input size  " << " |*|"
              << std::setw(SB_OUT_SETLEN) << "rank0 insert     " << " |"
              << std::setw(SB_OUT_SETLEN) << "rank0 retreive    " << " |"
              << std::setw(SB_OUT_SETLEN) << "rank0 remove     " << " |*|"
              << std::setw(SB_OUT_SETLEN) << "global insert    " << " |"
              << std::setw(SB_OUT_SETLEN) << "global retreive   " << " |"
              << std::setw(SB_OUT_SETLEN) << "global remove    "
              << std::endl;

    std::cout << std::setw(SB_OUT_KEYLEN) << "key" << std::setw(SB_OUT_VALLEN) << "value" << " |*|"
              << SB_OUTRQ << "#req" << " " << SB_OUTIO << "IOPS" << SB_OUTBW << "BW" << " |"
              << SB_OUTRQ << "#req" << " " << SB_OUTIO << "IOPS" << SB_OUTBW << "BW" << " |"
              << SB_OUTRQ << "#req" << " " << SB_OUTIO << "IOPS" << SB_OUTBW << "BW" << " |"
              << "*|"
              << SB_OUTRQ << "#req" << " " << SB_OUTIO << "IOPS" << SB_OUTBW << "BW" << " |"
              << SB_OUTRQ << "#req" << " " << SB_OUTIO << "IOPS" << SB_OUTBW << "BW" << " |"
              << SB_OUTRQ << "#req" << " " << SB_OUTIO << "IOPS" << SB_OUTBW << "BW"
              << std::endl;

    std::cout << std::setfill('-') << std::setw(SB_OUT_KVLEN) << "-" << "-+-+"
              << std::setw(SB_OUT_SETLEN) << "-" << "-+"
              << std::setw(SB_OUT_SETLEN) << "-" << "-+"
              << std::setw(SB_OUT_SETLEN) << "-" << "-+-+"
              << std::setw(SB_OUT_SETLEN) << "-" << "-+"
              << std::setw(SB_OUT_SETLEN) << "-" << "-+"
              << std::setw(SB_OUT_SETLEN) << "-" << "-"
              << std::endl;

    std::cout << std::setfill(' ');
  }
  void Reset()
  {
    mCurrentIdx = 0;
    for( int n=0; n<SKV_BENCH_STAT_LEN; n++ )
      mList[n].Reset();

    mAvgInsertIOPS = 0.0;
    mAvgRetrieveIOPS = 0.0;
    mAvgRemoveIOPS = 0.0;
  }
  void Append( const skv_bench_measurement_t &aInsert,
               const skv_bench_measurement_t &aRetrieve,
               const skv_bench_measurement_t &aRemove )
  {
    mList[ mCurrentIdx ].mInsert = aInsert;
    mList[ mCurrentIdx ].mRetrieve = aRetrieve;
    mList[ mCurrentIdx ].mRemove = aRemove;
    mCurrentIdx = ( mCurrentIdx + 1 ) % SKV_BENCH_STAT_LEN;
  }
  bool Converged( double aEps )
  {
    int rank;
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );

    mAvgInsertIOPS = 0.0;
    mAvgRetrieveIOPS = 0.0;
    mAvgRemoveIOPS = 0.0;

    aEps = aEps / 100;
    for( int n=0; n<SKV_BENCH_STAT_LEN; n++ )
    {
      mAvgInsertIOPS += mList[ n ].mInsert.mIOPS;
      mAvgRetrieveIOPS += mList[ n ].mRetrieve.mIOPS;
      mAvgRemoveIOPS += mList[ n ].mRemove.mIOPS;
    }
    mAvgInsertIOPS = mAvgInsertIOPS / SKV_BENCH_STAT_LEN;
    mAvgRetrieveIOPS = mAvgRetrieveIOPS / SKV_BENCH_STAT_LEN;
    mAvgRemoveIOPS = mAvgRemoveIOPS / SKV_BENCH_STAT_LEN;

    double a,b;
    for( int n=0; n<SKV_BENCH_STAT_LEN; n++ )
    {
      b = mList[ n ].mInsert.mIOPS;
      a = mAvgInsertIOPS;
      // if( rank == 0)
      //   std::cout << "EPS: " << aEps << " ERR: " << ((b - a) * (b - a)) / ( a*a ) << std::endl;
      if( ((b - a) * (b - a)) / ( a*a ) > aEps )
        return false;

      b = mList[ n ].mRetrieve.mIOPS;
      a = mAvgRetrieveIOPS;
      if( ((b - a) * (b - a)) / ( a*a ) > aEps )
        return false;

      b = mList[ n ].mRemove.mIOPS;
      a = mAvgRemoveIOPS;
      if( ((b - a) * (b - a)) / ( a*a ) > aEps )
        return false;
    }
    return true;
  }
};


/*************************************************************
 * MAIN
 */
int
main(int argc, char **argv)
{
  FxLogger_Init( argv[ 0 ] );
  MPI_Init( &argc, &argv );

  skv_bench_config_t config;
  if( config.Parse( argc, argv ) || !config.SanityCheck() )
    return 1;

  MPI_Comm_rank( MPI_COMM_WORLD, &config.mRank );
  MPI_Comm_size( MPI_COMM_WORLD, &config.mNodeCount );

  if( config.mRank == 0 )
    std::cout << config << std::endl;

  skv_status_t status = SKV_SUCCESS;
  skv_bench_t bench;
  skv_bench_gliding_avg_t glAvg;

  status = bench.Init( config );
  if( config.mRank == 0)
    glAvg.PrintHeader();

  for( int k = config.mKeySize.GetStart(); !config.mKeySize.OutOfRange( k ); k= config.mKeySize.Next( k ) )
    for( int v = config.mValueSize.GetStart(); !config.mValueSize.OutOfRange( v ); v = config.mValueSize.Next( v ) )
    {
      bench.Reset( k, v );
      glAvg.Reset();
      int MaxAttempts = DEFAULT_MAX_ATTEMPTS;
      double CurrentTime = MPI_Wtime();
      double StartTime = CurrentTime;
      while( (status == SKV_SUCCESS) && ( MaxAttempts-- > 0) )
      {
        status = bench.Run();
        glAvg.Append( bench.mGlobalData.mInsert,
                      bench.mGlobalData.mRetrieve,
                      bench.mGlobalData.mRemove );

        if( config.mRank == 0 )
          std::cout << ".";
        if( glAvg.Converged( config.mAvgError ) )
        {
          break;
        }

        CurrentTime = MPI_Wtime();
      }
      if( config.mRank == 0 )
        std::cout << "\r" << bench << (MaxAttempts<=0?"!!":"") << std::endl;

      if( status != SKV_SUCCESS )
      {
        std::cout << "\r" << "ERROR during test: " << skv_status_to_string( status ) << std::endl;
        break;
      }
    }

  MPI_Finalize();
  return 0;
}
