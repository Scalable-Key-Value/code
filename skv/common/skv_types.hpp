/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     arayshu, lschneid - initial implementation
 */

#ifndef __SKV_TYPES_HPP__
#define __SKV_TYPES_HPP__

extern "C"
{
#define ITAPI_ENABLE_V21_BINDINGS
#include <it_api.h>
  //#include "ordma_debug.h"
}

#include <cstdint>
#include <FxLogger.hpp>
#include <Trace.hpp>
#include <skv/common/skv_errno.hpp>
#include <skv/common/skv_config.hpp>

#include <inttypes.h>

#ifndef SKV_LOGGING_ALL
#define SKV_LOGGING_ALL ( 0 )
#endif

#ifndef SKV_CLIENT_ENDIAN_LOG
#define SKV_CLIENT_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL)
#endif

#ifndef SKV_SERVER_ENDIAN_LOG
#define SKV_SERVER_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL)
#endif

/*****************
 * Running the skv client/server
 * locally on the host
 ****************/
//#define SKV_RUNNING_LOCAL

// Size of the control message buffers for send/recv
#define SKV_CONTROL_MESSAGE_SIZE                     ( 4096 )
#define IT_TIMEOUT_SMALL                             ( 100 )

/** \brief number of outstanding commands which are really posted to the it_api
 *  \note This must not exceed the number of WQEs that is set for the verbs QP (setting inside IT_API)
 */
#if SKV_USE_VERBS
#define SKV_MAX_COMMANDS_PER_EP                     ( 64 )
#define SKV_SIGNALED_WRITE_INTERVAL_SHIFT           ( 4 )   // 2^# has to be less/equal than SKV_MAX_COMMANDS_PER_EP !!!
#else
#define SKV_MAX_COMMANDS_PER_EP                     ( 256 )
#define SKV_SIGNALED_WRITE_INTERVAL_SHIFT           ( 6 )   // 2^# has to be less/equal than SKV_MAX_COMMANDS_PER_EP !!!
#endif

#define SKV_SERVER_SENDQUEUE_SIZE                   ( SKV_MAX_COMMANDS_PER_EP * 16 )
#define SKV_SERVER_PENDING_EVENTS_PER_EP            ( SKV_MAX_COMMANDS_PER_EP + 4 )
#define SKV_SERVER_COMMAND_SLOTS                    ( SKV_MAX_COMMANDS_PER_EP * 2 )


#define SKV_EVD_SEVD_QUEUE_SIZE                     ( SKV_MAX_COMMANDS_PER_EP * 16 )

// Priority settings for event sources in SKV server
// value determines interval of loops between check for new events  (lower is higher prio, not linear!)
#define SKV_SERVER_COMMAND_SRC_PRIORITY    3
#define SKV_SERVER_LOCAL_KV_SRC_PRIORITY   1
#define SKV_SERVER_NETWORK_SRC_PRIORITY    5
#define SKV_SERVER_INTERNAL_SRC_PRIORITY   2

#define SKV_SERVER_AEVD_EVENTS_MAX_COUNT            ( 128 )
#define SKV_SERVER_EVENTS_MAX_COUNT                 ( 128 )
#define SKV_SERVER_LOCAL_KV_EVENT_MAX_COUNT         ( 128 )

#define MULT_FACTOR                                  ( 8 )
#define MULT_FACTOR_2                                ( 2 )
#define SKV_SERVER_MAX_RDMA_WRITE_SEGMENTS           ( 8 )
#define SKV_MAX_SGE                                  ( 4 )

// #define SKV_SERVER_PORT                             ( 17002 )

#ifndef SKV_STORE_T_LOG
#define SKV_STORE_T_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#include <skv/common/skv_types_ext.hpp>

#define SKV_MAX_DATA_LOAD      ( 1024 ) // MB of heap
#define SKV_KEY_LIMIT          ( 1024 )
#define SKV_VALUE_LIMIT        ( 10 * 1024 * 1024 )
#define SKV_BULK_INSERT_LIMIT  ( 100 * 1024 * 1024 )

#define SKV_MAX_SERVER_PER_NODE         ( 16 )
#define SKV_MAX_SERVER_ADDR_NAME_LENGTH ( 256 )
#define SKV_SERVER_PORT_LENGTH          ( 10 )

typedef enum
{
  SKV_DATA_ID_TYPE_PDS_ID   = 0x0001,
  SKV_DATA_ID_TYPE_INDEX_ID = 0x0002
} skv_data_id_type_t;

struct skv_data_id_t
{
  skv_data_id_type_t            mType;

  union
  {
    skv_pds_id_t                mPdsId;
    void                       *mIndexId;
  };
};

struct skv_server_addr_t
{
  char mName[ SKV_MAX_SERVER_ADDR_NAME_LENGTH ];
  int  mPort;
};

/***
 * KeyT: Is used to represent keys
 ***/
struct skv_store_t
{
  int   mSizeBE;
  char* mData;

  int mSize(void) const
  {
    int rc=ntohl(mSizeBE);
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "Endian-converting " << (void *) (intptr_t)mSizeBE
      << " to " << rc
      << EndLogLine ;
    return rc ;
  }
  int
  GetSize()
  {
    return mSize();
  }

  char*
  GetData()
  {
    return mData;
  }

  //
  void
  Init( char* aData, int aSize )
  {
    AssertLogLine( aSize >= 0 )
      << "skv_store_t::Init():: ERROR: "
      << " aSize: " << aSize
      << EndLogLine;

    mData = aData;
    mSizeBE = htonl(aSize);
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "Endian-converting " << aSize
      << " to " << (void *) (intptr_t)mSizeBE
      << EndLogLine ;
  }

  void
  Finalize()
  {
    mData = NULL;
    mSizeBE = -1;
  }

  skv_store_t&
  operator=( const skv_store_t& aStore )
  {
    mSizeBE = aStore.mSizeBE;
    mData = aStore.mData;

    return (*this);
  }

  // ??? We should allow the user to set a compare function on skv_store_t
  // ??? Default behaviour should be lexicographical order
  bool
  operator==( const skv_store_t& aStore ) const
  {
    BegLogLine( SKV_STORE_T_LOG )
      << "skv_store_t::operator==():: Entering "
      << EndLogLine;

    AssertLogLine( mData != NULL )
      << "skv_store_t::operator==():: ERROR: "
      << " mData != NULL "
      << EndLogLine;

    AssertLogLine( aStore.mData != NULL )
      << "skv_store_t::operator==():: ERROR: "
      << " aStore.mData != NULL "
      << EndLogLine;

    if( mSize() == aStore.mSize() )
    {
      return (memcmp( mData, aStore.mData, mSize() ) == 0);
    }
    else
      return 0;
  }

  // ??? We should allow the user to set a compare function on skv_store_t
  // ??? Default behaviour should be lexicographical order
  bool
  operator<( const skv_store_t& aStore ) const
  {
    BegLogLine( SKV_STORE_T_LOG )
      << "skv_store_t::operator<():: Entering "
      << EndLogLine;

    int MinDataSize = min( mSize(), aStore.mSize() );

    AssertLogLine( mData != NULL )
      << "skv_store_t::operator<():: ERROR: "
      << " mData != NULL "
      << EndLogLine;

    AssertLogLine( aStore.mData != NULL )
      << "skv_store_t::operator<():: ERROR: "
      << " aStore.mData != NULL "
      << EndLogLine;

    int rc = memcmp( mData, aStore.mData, MinDataSize );

    BegLogLine( SKV_STORE_T_LOG )
      << "skv_store_t::operator<():: "
      << " MinDataSize: " << MinDataSize
      << " mSize: " << mSize()
      << " aStore.mSize: " << aStore.mSize()
      << " rc: " << rc
      << EndLogLine;

    return ( rc < 0 );
  }
};

template<class streamclass>
static streamclass& operator<<( streamclass& os, const skv_store_t& aArg )
{
  const int STR_BUFF_SIZE = 128;
  char buff[STR_BUFF_SIZE];
  int Len = 0;
  if( aArg.mData != NULL )
  {
    Len = min( (STR_BUFF_SIZE - 1), aArg.mSize() );
    memcpy( buff, aArg.mData, Len );

    buff[Len] = 0;
  }
  else
  {
    Len = 0;
    buff[0] = 0;
  }

  os << "skv_store_t: [ mSize: " << aArg.mSize();
  os << " mData: ";

#if 1
  os << buff;
#else
  for( int i=0; i<Len; i++ )
    os << FormatString( "%02X" )
       << buff[ i ];
#endif

  os << " ]";
  return os;
}

typedef unsigned int       Bit32;
typedef unsigned long long Bit64;

typedef skv_store_t skv_key_t;
typedef skv_store_t skv_value_t;

#include <map>
#include <set>
#include <queue>
#include <list>

#define STL_QUEUE( T )       std::queue< T, std::deque< T > >
#define STL_LIST( T )        std::list< T >

typedef enum
{
  SKV_COMMAND_NONE            = 0x00000001 ,// = 1,
  SKV_COMMAND_INSERT          = 0x00000002 ,
  SKV_COMMAND_BULK_INSERT     = 0x00000003 ,
  SKV_COMMAND_RETRIEVE        = 0x00000004 ,
  SKV_COMMAND_RETRIEVE_N_KEYS = 0x00000005 ,   // = 5
  SKV_COMMAND_RETRIEVE_DIST   = 0x00000006 ,
  SKV_COMMAND_UPDATE          = 0x00000007 ,
  SKV_COMMAND_REMOVE          = 0x00000008 ,
  SKV_COMMAND_CLOSE           = 0x00000009 ,
  SKV_COMMAND_OPEN            = 0x0000000a ,  // = 10
  SKV_COMMAND_CONN_EST        = 0x0000000b ,
  SKV_COMMAND_ACTIVE_BCAST    = 0x0000000c ,
  SKV_COMMAND_CURSOR_PREFETCH = 0x0000000d ,
  SKV_COMMAND_PDSCNTL         = 0x0000000e // = 14
} skv_command_type_t;

static
const char*
skv_command_type_to_string( skv_command_type_t aCommandType )
{
  switch( aCommandType )
  {
    case SKV_COMMAND_NONE:               { return "SKV_COMMAND_NONE"; }
    case SKV_COMMAND_INSERT:             { return "SKV_COMMAND_INSERT"; }
    case SKV_COMMAND_BULK_INSERT:        { return "SKV_COMMAND_BULK_INSERT"; }
    case SKV_COMMAND_RETRIEVE:           { return "SKV_COMMAND_RETRIEVE"; }
    case SKV_COMMAND_RETRIEVE_N_KEYS:    { return "SKV_COMMAND_RETRIEVE_N_KEYS"; }
    case SKV_COMMAND_RETRIEVE_DIST:      { return "SKV_COMMAND_RETRIEVE_DIST"; }
    case SKV_COMMAND_UPDATE:             { return "SKV_COMMAND_UPDATE"; }
    case SKV_COMMAND_REMOVE:             { return "SKV_COMMAND_REMOVE"; }
    case SKV_COMMAND_CLOSE:              { return "SKV_COMMAND_CLOSE"; }
    case SKV_COMMAND_OPEN:               { return "SKV_COMMAND_OPEN"; }
    case SKV_COMMAND_CONN_EST:           { return "SKV_COMMAND_CONN_EST"; }
    case SKV_COMMAND_ACTIVE_BCAST:       { return "SKV_COMMAND_ACTIVE_BCAST"; }
    case SKV_COMMAND_CURSOR_PREFETCH:    { return "SKV_COMMAND_CURSOR_PREFETCH"; }
    case SKV_COMMAND_PDSCNTL:            { return "SKV_COMMAND_PDSCNTL"; }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_command_type_to_string:: ERROR:: Unrecognized type: "
        << " aCommandType: " << aCommandType
        << EndLogLine;

      return "SKV_COMMAND_UNKNOWN";
    }
  }
}

struct skv_lmr_triplet_t
{
  it_lmr_triplet_t mLMRTriplet;

  it_lmr_triplet_t&
  GetTriplet()
  {
    return mLMRTriplet;
  }

  it_lmr_triplet_t*
  GetTripletPtr()
  {
    return &mLMRTriplet;
  }

  void
  InitRel( it_lmr_handle_t aLmr, it_length_t aRel, it_length_t aLen )
  {
    mLMRTriplet.lmr = aLmr;
    mLMRTriplet.length = aLen;
    mLMRTriplet.addr.rel = aRel;
  }

  void
  InitAbs( it_lmr_handle_t aLmr, char* aAddr, it_length_t aLen )
  {
    mLMRTriplet.lmr = aLmr;
    mLMRTriplet.length = aLen;
    mLMRTriplet.addr.abs = aAddr;
  }

  void
  Init( const skv_lmr_triplet_t &aLMR )
  {
    mLMRTriplet.lmr = aLMR.mLMRTriplet.lmr;
    mLMRTriplet.length = aLMR.mLMRTriplet.length;
    mLMRTriplet.addr.abs = aLMR.mLMRTriplet.addr.abs;
  }
  it_length_t
  GetLen() const
  {
    return mLMRTriplet.length;
  }

  // adjust the length, only possible with shorter length than existing size
  void
  SetLenIfSmaller( it_length_t aLen )
  {
    if( mLMRTriplet.length > aLen )
    {
      mLMRTriplet.length = aLen;
    }
  }

  void
  SetAddr( char *aAddr )
  {
    mLMRTriplet.addr.abs = aAddr;
  }

  uintptr_t
  GetAddr() const
  {
    return (uintptr_t) mLMRTriplet.addr.abs;
  }

  it_lmr_handle_t&
  GetLMRHandle()
  {
    return mLMRTriplet.lmr;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_lmr_triplet_t& aT )
{
  os << "skv_lmr_triplet_t [  "
     << (void *) aT.mLMRTriplet.lmr << ' '
     << (void *) aT.mLMRTriplet.addr.abs << ' '
     << (int) aT.mLMRTriplet.length
     << " ]";

  return(os);
}


struct skv_rmr_triplet_t
{
  it_rmr_context_t         mRMR_Context;
  uint64_t                 mRMR_Addr;
  it_length_t              mRMR_Len;

  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "mRMR_Context=" << mRMR_Context
      << " mRMR_Addr=" << mRMR_Addr
      << " mRMR_Len=" << mRMR_Len
      << EndLogLine ;
    mRMR_Context=htobe64(mRMR_Context) ;
    mRMR_Addr=htobe64(mRMR_Addr) ;
  }
  void
  Init( it_rmr_context_t aRmr, char* aAddr, int aLen )
  {
    mRMR_Context  = aRmr;
    mRMR_Addr     = (uint64_t) ((uintptr_t) aAddr);
    mRMR_Len      = aLen;
  }

  it_length_t
  GetLen()
  {
    return mRMR_Len;
  }

  uint64_t
  GetAddr()
  {
    return mRMR_Addr;
  }

  void
  SetAddr( char * aAddr )
  {
    mRMR_Addr = (uint64_t) ((uintptr_t) aAddr);
  }

  void
  SetLen( it_length_t aLen )
  {
    mRMR_Len = aLen;
  }

  it_rmr_context_t&
  GetRMRContext()
  {
    return mRMR_Context;
  }

  skv_rmr_triplet_t&
  operator=( const skv_rmr_triplet_t &in )
  {
    mRMR_Context = in.mRMR_Context;
    mRMR_Addr    = in.mRMR_Addr;
    mRMR_Len     = in.mRMR_Len;
    return *this;
  }

  skv_rmr_triplet_t&
  operator=( const it_rmr_triplet_t &in )
  {
    mRMR_Context  = (it_rmr_context_t)(in.rmr);
    mRMR_Addr     = (uint64_t)(in.addr.abs); // we use abs address for assignment,
                                 // however, the content shouldn't
                                 // vary if relative addressing is
                                 // used (just make sure that
                                 // mRMR_Addr type matches the largest
                                 // element of it_rmr_triplet_t.addr)
    mRMR_Len      = in.length;
    return *this;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_rmr_triplet_t& aT )
{
  os << "skv_rmr_triplet_t [  "
     << (void *) aT.mRMR_Context << ' '
     << (void *) aT.mRMR_Addr << ' '
     << (int) aT.mRMR_Len
     << " ]";

#if 0
  const int STR_BUFF_SIZE = 32;
  char buff[STR_BUFF_SIZE];
  int Len = 0;
  if( aT.mRMR_Addr != NULL )
  {
    Len = min( (STR_BUFF_SIZE - 1), aT.mRMR_Len );
    memcpy( buff, aT.mRMR_Addr, Len );

    buff[Len] = 0;
  }
  else
  {
    Len = 0;
    buff[0] = 0;
  }

  os << " Data: [ ";
  for( int i=0; i<Len; i++)
    os << FormatString( "%02X" )
       << buff[ i ];
  os << " ]";
#endif

  return(os);
}

typedef char* skv_rec_lock_handle_t;
enum
{
  SKV_UNLOCKED_STATE = 0,
  SKV_LOCKED_STATE
};

/***************************************************
 * Layout of the bulk row
 * { key_size, key, value_size, value }
 **************************************************/
static
int
skv_bulk_insert_get_key_value_refs( char *aRow, char **Key, int &KeyLength, char **Value, int & ValueLength )
{
  int Sint = sizeof(int);

  char * Cursor = aRow;

  int *klen = (int *) Cursor;
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << " &KeyLength= " << (void *) &KeyLength
    << " " << (long) &KeyLength
    << " klen= " << (void *) klen
    << " " << (long) klen
    << " " << EndLogLine ;
  int Starklen = *klen ;
  int LocalKeyLength = ntohl(Starklen) ;
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian-converted from " << (void *) (intptr_t)Starklen
    << " to " << LocalKeyLength
    << EndLogLine ;
  KeyLength = LocalKeyLength;
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian-converting KeyLength from " << (void *) (intptr_t)(*klen)
    << EndLogLine ;

  Cursor += Sint;
  *Key = Cursor;

  Cursor += KeyLength;

  int *vlen = (int *) Cursor;
  ValueLength = ntohl(*vlen);

  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian-converting ValueLength from " << (void *) (intptr_t)(*vlen)
    << EndLogLine ;

  Cursor += Sint;
  *Value = Cursor;

  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "KeyLength=" << KeyLength
    << " ValueLength=" << ValueLength
    << EndLogLine ;

  return KeyLength + ValueLength + 2 * Sint;
}

static
int
skv_bulk_insert_get_total_len( char* aRow )
{
  int TotalSize = 0;
  int Index = 0;

  int* KeyLenPtr = (int *) &aRow[Index];
  TotalSize += *KeyLenPtr;
  TotalSize += sizeof(int);

  int* ValueLenPtr = (int *) &aRow[Index + TotalSize];
  TotalSize += *ValueLenPtr;
  TotalSize += sizeof(int);

  return TotalSize;
}

static
int
skv_bulk_insert_pack( char *aRow, char *Key, int KeyLength, char *Value, int ValueLength )
{
  int TotalSize = 0;

  int* KeyPtr = (int *) &aRow[TotalSize];
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian-converting KeyLength from " << (void *) (intptr_t)KeyLength
    << EndLogLine ;
  *KeyPtr = htonl(KeyLength);
  TotalSize += sizeof(int);

  memcpy( &aRow[TotalSize],
          Key,
          KeyLength );

  TotalSize += KeyLength;

  int* ValuePtr = (int *) &aRow[TotalSize];
  BegLogLine(SKV_CLIENT_ENDIAN_LOG)
    << "Endian-converting ValueLength from " << (void *) (intptr_t)ValueLength
    << EndLogLine ;
  *ValuePtr = htonl(ValueLength);
  TotalSize += sizeof(int);

  memcpy( &aRow[TotalSize],
          Value,
          ValueLength );

  TotalSize += ValueLength;

  return TotalSize;
}
/**************************************************/

#endif
