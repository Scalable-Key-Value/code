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

/* \todo This file has to be split up. There are way to many references to this file which pull in way too much unrelevant stuff!! */

#ifndef __SKV_SERVER_TYPES_HPP__
#define __SKV_SERVER_TYPES_HPP__

#ifndef SKV_SERVER_COMMAND_TRANSIT_LOG
#define SKV_SERVER_COMMAND_TRANSIT_LOG      ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_COMMAND_DISPATCH_LOG
#define SKV_SERVER_COMMAND_DISPATCH_LOG     ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_BUFFER_AND_COMMAND_LOG
#define SKV_SERVER_BUFFER_AND_COMMAND_LOG   ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_PENDING_EVENTS_LOG
#define SKV_SERVER_PENDING_EVENTS_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_CLEANUP_LOG
#define SKV_SERVER_CLEANUP_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_COMMAND_POLLING_LOG
#define SKV_SERVER_COMMAND_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_RDMA_RESPONSE_PLACEMENT_LOG
#define SKV_SERVER_RDMA_RESPONSE_PLACEMENT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_TRACE
#define SKV_SERVER_TRACE ( 0 )
#endif

#include <server/skv_server_event_type.hpp>
#include <server/skv_server_heap_manager.hpp>
// !! FURTHER INCLUDES FURTHER DOWN IN THE FILE !!

static TraceClient gSKVServerDispatchResponseStart;
static TraceClient gSKVServerDispatchResponseFinis;
static TraceClient gSKVServerRDMAResponseStart;
static TraceClient gSKVServerRDMAResponseFinis;

typedef enum
{
  SKV_SERVER_STATE_INIT = 1,
  SKV_SERVER_STATE_RUN,
  SKV_SERVER_STATE_PENDING_EVENTS,
  SKV_SERVER_STATE_EXIT,
  SKV_SERVER_STATE_ERROR
} skv_server_state_t;

static
const char*
skv_server_state_to_string( skv_server_state_t aState )
{
  switch( aState )
  {
    case SKV_SERVER_STATE_INIT: { return "SKV_SERVER_STATE_INIT"; }
    case SKV_SERVER_STATE_RUN:  { return "SKV_SERVER_STATE_RUN"; }
    case SKV_SERVER_STATE_PENDING_EVENTS: { return "SKV_SERVER_STATE_PENDING_EVENTS"; }
    case SKV_SERVER_STATE_ERROR: { return "SKV_SERVER_STATE_ERROR"; }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_server_state_to_string:: ERROR:: Unrecognized state: "
        << " aState: " << aState
        << EndLogLine;
      return "SKV_SERVER_STATE_UNKNOWN";
    }
  }
  return "SKV_SERVER_STATE_UNKNOWN";
}

typedef enum
{
  SKV_SERVER_COMMAND_STATE_INIT = 1,
  SKV_SERVER_COMMAND_STATE_CONN_EST_PENDING,
  SKV_SERVER_COMMAND_STATE_CONN_EST,
  SKV_SERVER_COMMAND_STATE_WAITING_ON_LOCK,
  SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL,
  SKV_SERVER_COMMAND_STATE_WAITING_RDMA_WRITE_CMPL,
  SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP,
  SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP,
  SKV_SERVER_COMMAND_STATE_LOCAL_KV_READY
} skv_server_command_state_t;

static
const char*
skv_server_command_state_to_string( skv_server_command_state_t aState )
{
  switch( aState )
  {
    case SKV_SERVER_COMMAND_STATE_INIT: { return "SKV_SERVER_COMMAND_STATE_INIT"; }
    case SKV_SERVER_COMMAND_STATE_CONN_EST_PENDING: { return "SKV_SERVER_COMMAND_STATE_CONN_EST_PENDING"; }
    case SKV_SERVER_COMMAND_STATE_CONN_EST: { return "SKV_SERVER_COMMAND_STATE_CONN_EST"; }
    case SKV_SERVER_COMMAND_STATE_WAITING_ON_LOCK: { return "SKV_SERVER_COMMAND_STATE_WAITING_ON_LOCK"; }
    case SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL: { return "SKV_SERVER_COMMAND_STATE_WAITING_RDMA_READ_CMPL"; }
    case SKV_SERVER_COMMAND_STATE_WAITING_RDMA_WRITE_CMPL: { return "SKV_SERVER_COMMAND_STATE_WAITING_RDMA_WRITE_CMPL"; }
    case SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP: { return "SKV_SERVER_COMMAND_STATE_LOCAL_KV_INDEX_OP"; }
    case SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP: { return "SKV_SERVER_COMMAND_STATE_LOCAL_KV_DATA_OP"; }
    case SKV_SERVER_COMMAND_STATE_LOCAL_KV_READY: { return "SKV_SERVER_COMMAND_STATE_LOCAL_KV_READY"; }
    default:
    {
      StrongAssertLogLine( 0 )
        << "skv_server_command_state_to_string():: ERROR:: "
        << " aState: " << aState
        << " Not supported"
        << EndLogLine;
      return "SKV_SERVER_COMMAND_STATE_UNKNOWN";
    }
  }
}

typedef union
{
  it_ep_handle_t  mIT_EP;
  int             mMPI_Rank;
} skv_server_ep_handle_t ;

struct skv_server_command_key_t
{
  skv_server_ep_handle_t mEP;
};

/***************************************************************/
// TODO: Figure out how to get rid of this forward declaration.
struct skv_server_ep_state_t;
/***************************************************************/

class skv_server_cookie_t  
{
  struct skv_server_cookie_params_t
  {
    unsigned short                mCookieSeqNo;

    // NOTE: 
    // * In Recv context mOrd is an ordinal of the Recv Buffer in EP state
    // * In Send context mOrd is an ordinal of the skv_server_ccb_t in EP state
    unsigned short                mOrd;

    skv_server_ep_state_t*        mEPState;
  };

  union
  {
    it_dto_cookie_t             mCookie;
    skv_server_cookie_params_t  mParams;
  };

public:

  void
  Init( skv_server_ep_state_t* aEPState,
        unsigned aSeqNo,
        unsigned aOrd )
  {
    mParams.mCookieSeqNo = aSeqNo;
    mParams.mOrd         = aOrd;
    mParams.mEPState     = aEPState;
  }

  it_dto_cookie_t
  GetCookie()
  {
    return mCookie;
  }

  unsigned short
  GetCookieSeqNo()
  {
    return mParams.mCookieSeqNo;
  }

  unsigned short
  GetOrd()
  {
    return mParams.mOrd;
  }

  skv_server_ep_state_t*
  GetEPState()
  {
    return mParams.mEPState;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, skv_server_cookie_t& A )
{
  int SeqNo = A.GetCookieSeqNo();
  int Ord   = A.GetOrd();  

  os << "skv_server_cookie_t: ["
     << SeqNo << ' '
     << Ord << ' '
     << (void *) A.GetEPState()
     << " ]";

  return(os);    
}

typedef void*(*skv_server_rdma_write_cmpl_func_t)(void*);
void*  EPSTATE_CountSendCompletionsCallback( void* Arg );
void*  EPSTATE_RetrieveWriteComplete(        void* Arg );

class skv_server_rdma_write_cmpl_cookie_t
{
  struct skv_server_rdma_write_cmpl_cookie_params_t
  {
    skv_server_rdma_write_cmpl_func_t mFunc;    
    void*                             mContext;

    // Needed to preserve the transaction id for tracing
    int                               mCmdOrd;
    int                               mIsLast;
  };

  union
  {
    it_dto_cookie_t                             mCookie;
    skv_server_rdma_write_cmpl_cookie_params_t  mParams;
  };

public:
  void
  Init( void* aContext,
        skv_server_rdma_write_cmpl_func_t aFunc,
        int aCmdOrd = -1,
        int aIsLast = 0 )
  {
    mParams.mFunc    = aFunc;
    mParams.mContext = aContext;
    mParams.mCmdOrd  = aCmdOrd;
    mParams.mIsLast  = aIsLast;
  }

  int
  GetIsLast()
  {
    return mParams.mIsLast;
  }

  it_dto_cookie_t
  GetCookie()
  {    
    return mCookie;
  }

  int
  GetCmdOrd()
  {
    return mParams.mCmdOrd;
  }

  void*
  GetContext()
  {
    return mParams.mContext;
  }

  skv_server_rdma_write_cmpl_func_t
  GetFunc( )
  {
    return mParams.mFunc;
  }  
};


struct skv_server_command_insert_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_cmd_RIU_flags_t                mFlags;
  skv_rec_lock_handle_t              mRecLockHdl;
};

struct skv_server_command_bulk_insert_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_lmr_triplet_t                  mLocalBuffer;
  skv_pds_id_t                       mPDSId;

  // Needed for debugging
  uint64_t                            mRemoteBufferAddr;
  it_rmr_context_t                    mRemoteBufferRMR;

#ifdef SKV_BULK_LOAD_CHECKSUM 
  uint64_t                            mRemoteBufferChecksum;
#endif  

};

struct skv_server_command_remove_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_cmd_remove_flags_t             mFlags;
  // skv_rec_lock_handle_t              mRecLockHdl;
};

struct skv_server_command_active_bcast_t
{
  skv_server_to_client_cmd_hdr_t       mHdr;

  it_lmr_handle_t                      mBufferLMR;
  skv_c2s_active_broadcast_func_type_t mFuncType;

  char*                                mBufferPtr;
  int                                  mBufferSize;
};

struct skv_server_local_kv_pdsopen_data_t
{
  skv_pds_id_t mPDSId;
};

struct skv_server_local_kv_pdsstat_data_t
{
  skv_pds_attr_t mPDSAttr;
  skv_pdscntl_cmd_t mCntlCmd;
};

struct skv_server_local_kv_distribution_data_t
{
  skv_distribution_t *mDist;
};

struct skv_server_local_kv_lookup_data_t
{
  skv_lmr_triplet_t mValueRepInStore;
};

typedef uint64_t skv_local_kv_req_ctx_t;

struct skv_server_local_kv_twophase_rdma_data_t
{
  skv_lmr_triplet_t mValueRDMADest;
  skv_local_kv_req_ctx_t mReqCtx;
  int mSize;
};

struct skv_server_local_kv_retrieve_n_keys_data_t
{
  skv_lmr_triplet_t *mKeysSizesSegs;
  int mKeysCount;
  int mKeysSizesSegsCount;
};

typedef enum {
  SKV_COMMAND_CLASS_IMMEDIATE = 0,
  SKV_COMMAND_CLASS_MULTI_STAGE
}  skv_server_command_class_t;
    

#include <server/skv_local_kv_types.hpp>

struct skv_server_ccb_t
{
  skv_server_command_state_t   mState;  
  skv_command_type_t           mType;

  skv_lmr_triplet_t*           mCtrlMsgSendTriplet;
  int                          mCtrlMsgSendBuffOrdinal;
  skv_local_kv_cookie_t        mLocalKVCookie;

  skv_server_command_class_t   mCommandClass;

  // Currently holds the state for the rdma_read() commands
  // Used on rdma_read() completion
  union
  {
    skv_server_command_insert_t         mCommandInsert;
    skv_server_command_bulk_insert_t    mCommandBulkInsert;
    skv_server_command_remove_t         mCommandRemove;
    skv_server_command_active_bcast_t   mCommandActiveBcast;
  } mCommandState;

  // holds response data that's returned by the local kv backend
  union
  {
    skv_server_local_kv_pdsopen_data_t mPDSOpen;
    skv_server_local_kv_pdsstat_data_t mPDSStat;
    skv_server_local_kv_distribution_data_t mDistribution;
    skv_server_local_kv_lookup_data_t mLookup;
    skv_server_local_kv_twophase_rdma_data_t mRDMA;
    skv_server_local_kv_retrieve_n_keys_data_t mRetrieveNKeys;
  } mLocalKVData;
  skv_status_t mLocalKVrc;

  char* 
  GetSendBuff()
  {
    AssertLogLine( mCtrlMsgSendTriplet != NULL )
      << "ERROR: "
      << " mCtrlMsgSendTriplet: " << (void *) mCtrlMsgSendTriplet
      << " mCtrlMsgSendBuffOrdinal: " << mCtrlMsgSendBuffOrdinal
      << EndLogLine;

    return (char *) mCtrlMsgSendTriplet->GetAddr();
  } 

  int 
  GetSendBuffSize()
  {
    AssertLogLine( mCtrlMsgSendTriplet != NULL )
      << "ERROR: "
      << " mCtrlMsgSendTriplet: " << (void *) mCtrlMsgSendTriplet
      << " mCtrlMsgSendBuffOrdinal: " << mCtrlMsgSendBuffOrdinal
      << EndLogLine;

    return mCtrlMsgSendTriplet->GetLen();
  } 

  it_lmr_handle_t &
  GetSendLMR()
  {
    AssertLogLine( mCtrlMsgSendTriplet != NULL )
      << "ERROR: "
      << " mCtrlMsgSendTriplet: " << (void *) mCtrlMsgSendTriplet
      << " mCtrlMsgSendBuffOrdinal: " << mCtrlMsgSendBuffOrdinal
      << EndLogLine;

    return mCtrlMsgSendTriplet->GetLMRHandle();
  }

  skv_lmr_triplet_t*
  GetSendTripletPtr()
  {
    return mCtrlMsgSendTriplet;
  }

  int
  GetSendBuffOrdinal()
  {
    return mCtrlMsgSendBuffOrdinal;
  }

  skv_server_command_state_t 
  GetState()
  {
    return mState;
  }  

  skv_command_type_t
  GetType()
  {
    return mType;
  }

  void
  SetType( skv_command_type_t aType )
  {
    mType = aType;
  }

  void
  Transit( skv_server_command_state_t aNewState )
  {
    BegLogLine( SKV_SERVER_COMMAND_TRANSIT_LOG )
      << "skv_server_ccb_t::Transit():: "
      << " CCB: " << (void*)this
      << " from state: " << skv_server_command_state_to_string( mState )
      << " to state: " << skv_server_command_state_to_string( aNewState )
      << EndLogLine;

    mState = aNewState;
  }

  void
  SetSendBuffInfo( skv_lmr_triplet_t* aCtrlMsgSendTriplet,
                   int                 aCtrlMsgSendBuffOrdinal )
  {
    BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_ccb_t::SetSendBuffInfo():"
      << *aCtrlMsgSendTriplet
      << " ord: " << aCtrlMsgSendBuffOrdinal
      << EndLogLine;

    mCtrlMsgSendTriplet     = aCtrlMsgSendTriplet;
    mCtrlMsgSendBuffOrdinal = aCtrlMsgSendBuffOrdinal;
  }

  void 
  Init( it_pz_handle_t      aPZ,
        skv_lmr_triplet_t* aCtrlMsgSendTriplet,
        int                 aCtrlMsgSendBuffOrdinal )
  {
    BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG )
      << "skv_server_ccb_t::Init():: ENTERING"
      << " CCB: " << (void*)this
      << " sendbuf: " << (void*)aCtrlMsgSendTriplet
      << EndLogLine;

    mState = SKV_SERVER_COMMAND_STATE_INIT;
    mType  = SKV_COMMAND_NONE;

    mCtrlMsgSendTriplet     = aCtrlMsgSendTriplet;
    mCtrlMsgSendBuffOrdinal = aCtrlMsgSendBuffOrdinal;


    mCommandClass = SKV_COMMAND_CLASS_IMMEDIATE;
  }    

  skv_server_command_class_t
  GetCommandClass( )
  {
    return mCommandClass;
  }

  void
  SetCommandClass( skv_server_command_class_t aClass )
  {
    mCommandClass = aClass;
  }

  skv_status_t
  DetachPrimaryCmdBuffer( )
  {
    mCtrlMsgSendTriplet = NULL;
    mCtrlMsgSendBuffOrdinal = -1;
    return SKV_SUCCESS;
  }

};

// On disconnect, we need to finalize any EP associated
// state
typedef enum
{
  SKV_SERVER_FINALIZABLE_ASSOCIATED_EP_STATE_CREATE_CURSOR_TYPE = 0x0001,
  SKV_SERVER_FINALIZABLE_ASSOCIATED_EP_STATE_CREATE_INDEX_TYPE  = 0x0002
} skv_server_finalizable_associated_ep_state_type_t;

struct skv_server_finalizable_associated_ep_state_t
{
  skv_server_finalizable_associated_ep_state_type_t      mStateType;
  void *                                                 mState;

  void
  Init( skv_server_finalizable_associated_ep_state_type_t   aStateType,
        void *                                               aState )
  {
    mStateType  = aStateType;
    mState      = aState;
  }

  bool operator==(const skv_server_finalizable_associated_ep_state_t& Arg)    
  {
    return ( ( mStateType == Arg.mStateType ) && 
             ( mState == Arg.mState ) );
  }
};

/*********************************************************************************/
struct skv_server_ep_state_t;  // forward declaration of ep_state

struct skv_server_command_finder_t
{
  skv_server_ep_state_t*  mEPStatePtr;
  int                     mCommandOrd;
};

struct skv_server_conn_finder_t
{
  it_cn_est_identifier_t  mConnEstId;
  int                     mClientOrdInGroup;
  skv_client_group_id_t   mClientGroupId;
  it_rmr_triplet_t        mResponseRMR;
};

struct skv_server_event_t
{
  skv_server_event_type_t   mCmdEventType;    
  skv_server_event_type_t   mEventType;    


  typedef union
  {
    skv_server_ep_handle_t        mEP;

    // Needed by a connection request event
    skv_server_conn_finder_t      mConnFinder;

    skv_server_command_finder_t   mCommandFinder;

    skv_server_rdma_write_cmpl_cookie_t mRdmaWriteCmplCookie;

  } skv_server_event_metadata_t ; 

  skv_server_event_metadata_t mEventMetadata;  

  void 
  Init( skv_server_event_type_t  aEventType,
        skv_server_ep_state_t*   aEPState,
        unsigned                 aCommandOrd,
        skv_server_event_type_t  aCmdEventType )
  {
    mEventType         = aEventType;
    mCmdEventType      = aCmdEventType;

    mEventMetadata.mCommandFinder.mEPStatePtr = aEPState;
    mEventMetadata.mCommandFinder.mCommandOrd = aCommandOrd;
  }

  void 
  Init( skv_server_event_type_t  aEventType,
        skv_server_cookie_t      aCookie )
  {
    mEventType                                = aEventType;
    mCmdEventType                             = aEventType;

    mEventMetadata.mCommandFinder.mEPStatePtr = aCookie.GetEPState();
    mEventMetadata.mCommandFinder.mCommandOrd = aCookie.GetOrd();
  }

  void
  Init( skv_server_event_type_t                 aEventType,
        skv_server_rdma_write_cmpl_cookie_t     aCookie )
  {
    mEventType                           = aEventType;
    mCmdEventType                        = aEventType;
    mEventMetadata.mRdmaWriteCmplCookie  = aCookie;
  }

  void
  Init( skv_server_event_type_t  aEventType,
        skv_server_ep_handle_t   aEP )
  {
    mEventType          = aEventType;
    mEventMetadata.mEP  = aEP;
  }

  void
  Init( skv_server_event_type_t  aEventType,
        it_ep_handle_t            aEP )
  {
    mEventType                 = aEventType;
    mEventMetadata.mEP.mIT_EP  = aEP;
  }

  void
  Init( skv_server_event_type_t  aEventType,
        it_cn_est_identifier_t    aConnEstId,
        int                       aClientOrdInGroup,
        skv_client_group_id_t    aClientGroupId,
        skv_rmr_triplet_t       *aResponseRMR)
  {
    mEventType                                   =  aEventType;
    mEventMetadata.mConnFinder.mConnEstId        =  aConnEstId;
    mEventMetadata.mConnFinder.mClientOrdInGroup =  aClientOrdInGroup;
    mEventMetadata.mConnFinder.mClientGroupId    =  aClientGroupId;

    mEventMetadata.mConnFinder.mResponseRMR.rmr       =  (it_rmr_handle_t)aResponseRMR->GetRMRContext();
    mEventMetadata.mConnFinder.mResponseRMR.addr.abs  =  ( void* )aResponseRMR->GetAddr();
    mEventMetadata.mConnFinder.mResponseRMR.length    =  aResponseRMR->GetLen();
    
  }
};

template <class streamclass>
static streamclass& operator<<( streamclass& os, skv_server_event_t& aArg ) 
{
  os << "skv_server_event_t [ ";
  os << "EventType: " << skv_server_event_type_to_string( aArg.mEventType );
  os << " ]";

  return os;
}

  // +1 for the connection command for the EP 
#define SKV_COMMAND_TABLE_LEN ( SKV_MAX_COMMANDS_PER_EP + 1 )
#define SKV_CONN_COMMAND_ORDINAL ( SKV_MAX_COMMANDS_PER_EP )

#define SKV_RECV_BUFFERS_COUNT ( SKV_MAX_COMMANDS_PER_EP + 2 )
#define SKV_SEND_BUFFERS_COUNT ( SKV_SERVER_SENDQUEUE_SIZE + SKV_MAX_COMMANDS_PER_EP )

#include "common/skv_array_stack.hpp"
#include "common/skv_array_queue.hpp"

/*********************************************************************************/
typedef STL_LIST( skv_server_finalizable_associated_ep_state_t ) skv_server_finalizable_associated_ep_state_list_t;
// typedef STL_QUEUE( int ) skv_server_free_command_slot_list_t;
// typedef STL_QUEUE( skv_server_event_t* ) skv_server_state_pending_events_list_t;
// typedef STL_STACK( int ) skv_server_unposted_recv_buffers_list_t;
// typedef STL_STACK( int ) skv_server_unposted_send_buffers_list_t;

typedef skv_array_queue_t< int,                  SKV_COMMAND_TABLE_LEN+1 >          skv_server_free_command_slot_list_t;
typedef skv_array_queue_t< skv_server_event_t*, SKV_SERVER_PENDING_EVENTS_PER_EP > skv_server_state_pending_events_list_t;

//typedef skv_array_stack_t<int, SKV_RECV_BUFFERS_COUNT+1> skv_server_unposted_recv_buffers_list_t;
//typedef skv_array_stack_t<int, SKV_SEND_BUFFERS_COUNT+1> skv_server_unposted_send_buffers_list_t;


struct skv_server_ep_state_t
{
  skv_server_ccb_t             mCCBTable[ SKV_COMMAND_TABLE_LEN ];  
  skv_lmr_triplet_t            mPrimaryCmdBuffers[ SKV_SERVER_COMMAND_SLOTS ]; // command slots for immediately completable commands (completing in order)
  skv_lmr_triplet_t            mSecondaryCmdBuffers[ SKV_COMMAND_TABLE_LEN ]; // command slots for multi-phase commands (completing out of order)

  skv_server_event_t           mPendingEvents[ SKV_SERVER_PENDING_EVENTS_PER_EP ]; //! hold copies of events
  int                          mLastPending; //! index to last pending event
  int                          mWaitForSQ;

  it_ep_handle_t               mEPHdl;
  it_pz_handle_t               mPZHdl;

  char*                        mCommandBuffs;
  char*                        mCommandBuffsRaw;

  skv_server_free_command_slot_list_t     *mFreeCommandSlotList;
  skv_server_state_pending_events_list_t  *mPendingEventsList;

  int                          mUsedCommandSlotIndex; // indicate the first used/occupied/uncompleted command buffer
  int                          mUnusedCommandSlotIndex;  // indicate the next free usable command buffers
  int                          mDispatchedCommandBufferIndex;  // indicate the latest command buffer that was dispatched

  skv_server_finalizable_associated_ep_state_list_t      *mAssociatedStateList;

  // skv client group info
  int                                                     mClientGroupOrdinal;
  skv_client_group_id_t                                   mClientGroupId;
  skv_rmr_triplet_t                                       mClientResponseRMR;
  int                                                     mClientCurrentResponseSlot;

  it_lmr_handle_t              mCommandBuffLMR;
  skv_rmr_triplet_t            mCommandBuffRMR;
  int                          mCurrentCommandSlot;

  void
  SetClientInfo( int aClientOrdInGroup, skv_client_group_id_t aClientGroupId, it_rmr_triplet_t *aResponseRMR )
  {
    mClientGroupOrdinal = aClientOrdInGroup;
    mClientGroupId      = aClientGroupId;
    mClientResponseRMR  = *aResponseRMR;
  }

  int
  GetClientGroupOrdinal()
  {
    return mClientGroupOrdinal;
  }

  void
  AddToAssociatedState(   skv_server_finalizable_associated_ep_state_type_t      aStateType,
                          void *                                                  aState )
  {    
    skv_server_finalizable_associated_ep_state_t FinalizableState;
    FinalizableState.Init( aStateType, aState );

    mAssociatedStateList->push_back( FinalizableState );

    return;
  }

  void
  RemoveFromAssociatedState(   skv_server_finalizable_associated_ep_state_type_t      aStateType,
                               void *                                                  aState )
  {    
    skv_server_finalizable_associated_ep_state_t FinalizableState;
    FinalizableState.Init( aStateType, aState );

    mAssociatedStateList->remove( FinalizableState );

    return;
  } 

  void
  Finalize()
  {    
    BegLogLine( SKV_SERVER_CLEANUP_LOG )
      << "Finalizing EP: " << (void*)this
      << EndLogLine;

    delete mAssociatedStateList;

    it_lmr_free(mCommandBuffLMR);
    free( mCommandBuffsRaw );
    mCommandBuffsRaw = NULL;

    delete mFreeCommandSlotList;

    delete mPendingEventsList;
    
  }


  skv_lmr_triplet_t*
  GetPrimaryCommandBuff( int aOrdinal )
  {
    AssertLogLine( (aOrdinal >= 0) && (aOrdinal < SKV_SERVER_COMMAND_SLOTS) )
      << "skv_server_ep_state_t::GetPrimaryCommandBuff():: ERROR:: "
      << " aOrdinal: " << aOrdinal
      << " SKV_RECV_BUFFERS_COUNT: " << SKV_SERVER_COMMAND_SLOTS
      << EndLogLine;

    return (& mPrimaryCmdBuffers[ aOrdinal ]);
  }

  skv_lmr_triplet_t*
  GetSecondaryCommandBuff( int aOrdinal )
  {
    AssertLogLine( (aOrdinal >= 0) && (aOrdinal < SKV_COMMAND_TABLE_LEN) )
      << "skv_server_ep_state_t::GetSecondaryCommandBuff():: ERROR:: "
      << " aOrdinal: " << aOrdinal
      << " SKV_RECV_BUFFERS_COUNT: " << SKV_COMMAND_TABLE_LEN
      << EndLogLine;

    return (& mSecondaryCmdBuffers[ aOrdinal ]);
  }

  skv_status_t
  ReplaceAndInitCommandBuffer( skv_server_ccb_t* Command,
                               int aCommandOrdinal )
  {
    // get locations of buffers
    skv_lmr_triplet_t* NewSendBufTriplet = & (mSecondaryCmdBuffers[ aCommandOrdinal ]);
    skv_client_to_server_cmd_hdr_t* oldBuf = (skv_client_to_server_cmd_hdr_t*) (Command->GetSendBuff());
    char* newBuf = (char*) (NewSendBufTriplet->GetAddr());

    // initialize new buffer
    memcpy( newBuf,
            oldBuf,
            SKV_CONTROL_MESSAGE_SIZE );

    // set command class
    Command->SetCommandClass( SKV_COMMAND_CLASS_MULTI_STAGE );

    // replace existing buffer
    Command->SetSendBuffInfo( NewSendBufTriplet,
                              aCommandOrdinal );

    // simulate as if this command was dispatched
    AdvanceDispatched( );
    

    return SKV_SUCCESS;
  }

  int
  GetConnCommandOrdinal()
  {
    // Connection command gets a special/fixed element of the command table
    return SKV_CONN_COMMAND_ORDINAL;
  }

  skv_server_ccb_t* 
  GetCommandForOrdinal( int aOrdinal )
  {
    AssertLogLine( (aOrdinal >= 0) && (aOrdinal < SKV_COMMAND_TABLE_LEN) )
      << "skv_server_ep_state_t::GetCommandForOrdinal:: ERROR:: "
      << " aOrdinal: " << aOrdinal
      << " SKV_COMMAND_TABLE_LEN: " << SKV_COMMAND_TABLE_LEN
      << EndLogLine;

    return (& mCCBTable[ aOrdinal ]);
  }

  // ??? Possibly need a special command for 
  // affiliated and connection events
  void
  Init( it_ep_handle_t   aEP,
        it_pz_handle_t   aPZ )
  {
    // The Ord field in the skv_server_cookie_t is 16 bits
    AssertLogLine( SKV_RECV_BUFFERS_COUNT <= 65536 )
      << "skv_server_ep_state_t::Init(): ERROR: "
      << " SKV_RECV_BUFFERS_COUNT: " << SKV_RECV_BUFFERS_COUNT
      << EndLogLine;

    StrongAssertLogLine( SKV_COMMAND_TABLE_LEN < SKV_SERVER_COMMAND_SLOTS )
      << "skv_server_ep_state_t::Init(): ERROR: "
      << " Command table exceeds command slots. This will not work!"
      << EndLogLine;

    mEPHdl = aEP;
    mPZHdl = aPZ;

    mAssociatedStateList = new skv_server_finalizable_associated_ep_state_list_t;

    StrongAssertLogLine( mAssociatedStateList != NULL ) 
      << "skv_server_ep_state_t::Init(): ERROR: "
      << EndLogLine;
    
    mFreeCommandSlotList = new skv_server_free_command_slot_list_t;

    StrongAssertLogLine( mFreeCommandSlotList != NULL ) 
      << "skv_server_ep_state_t::Init(): ERROR creating Stack for free command slots "
      << EndLogLine;
    
    mUnusedCommandSlotIndex       = 0;
    mUsedCommandSlotIndex         = 0;
    mDispatchedCommandBufferIndex = 0;
    /***********************************************************/

    // create list for tracking of pending/deferred replies
    mPendingEventsList = new skv_server_state_pending_events_list_t;
    
    StrongAssertLogLine( mPendingEventsList != NULL ) 
      << "skv_server_ep_state_t::Init(): ERROR creating FIFO for pending events"
      << EndLogLine;

    memset( mPendingEvents, 0, SKV_SERVER_PENDING_EVENTS_PER_EP * sizeof( skv_server_event_t ) );
    mLastPending = 0;
    UnstallEP();

    mClientCurrentResponseSlot = 0;


#define SAFETY_GAP 256
#define ALIGNMENT ( 256 )
#define ALIGNMENT_MASK ( 0xff )
    /******************************************************************
     * Allocate Buffers
     ******************************************************************/
    int CommandBuffSize = SKV_CONTROL_MESSAGE_SIZE * SKV_SERVER_COMMAND_SLOTS
      + SKV_CONTROL_MESSAGE_SIZE * SKV_COMMAND_TABLE_LEN + SAFETY_GAP;

    mCommandBuffsRaw   = (char *) malloc( CommandBuffSize + ALIGNMENT );
  
    StrongAssertLogLine( mCommandBuffsRaw != NULL )
      << "skv_server_ep_state_t::Init():: ERROR:: not enough memory for "
      << " BuffSize: " << CommandBuffSize
      << EndLogLine;

    // check alignment of send-recv buffers
    if( (uintptr_t) mCommandBuffsRaw & ALIGNMENT_MASK )
    {
      mCommandBuffs = (char *) ((((uintptr_t) mCommandBuffsRaw) & (uintptr_t) (~ALIGNMENT_MASK)) + ALIGNMENT);
    }
    else
    {
      mCommandBuffs = mCommandBuffsRaw;
    }

    it_mem_priv_t   privs;
    it_lmr_flag_t   lmr_flags;

    // TODO: Here Recv and Sendbuffers are registered with the same
    // privileges in the same LMR, later, we create an RMR out of it
    // to allow clients to write the RecvBuffer section. THIS IS
    // DANGEROUS, since clients might overwrite send buffers as well!
    
    
    // TODO: need to recheck the permissions and flags!

    // privs     = IT_PRIV_LOCAL;
    // lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;
    privs     = (it_mem_priv_t) (IT_PRIV_REMOTE_WRITE | IT_PRIV_LOCAL);
    lmr_flags = IT_LMR_FLAG_SHARED;

    it_rmr_context_t  RMR_Context;

    it_status_t status = it_lmr_create( aPZ,
                                        mCommandBuffs,
                                        NULL,
                                        CommandBuffSize,
                                        IT_ADDR_MODE_ABSOLUTE,
                                        privs,
                                        lmr_flags,
                                        0,
                                        & mCommandBuffLMR,
                                        & RMR_Context);
                                        // (it_rmr_context_t *) NULL );

    StrongAssertLogLine( status == IT_SUCCESS )
      << "skv_server_ccb_t::Init():: ERROR:: from it_lmr_create "
      << " status: " << status
      << EndLogLine;      


    BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
      << "skv_server_ep_state_t::Init(): RetrieveBuffs "
      << " start @ " << (void*)mCommandBuffs
      << " size: " << SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE
      << " end @ " << (void*)&(mCommandBuffs[SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE])
      << EndLogLine;

    /******************************************************************
     * Init Primary Command Buffers used for immediate in-order completion of commands
     ******************************************************************/    
    char* PrimaryBuffs = mCommandBuffs;

    // initializse RMR-data for later use
    mCommandBuffRMR.Init( RMR_Context,
                          PrimaryBuffs,
                          SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE );

    mCurrentCommandSlot = 0;
    BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
      << "skv_server_ep_state_t::Init():: "
      << " EPState: " << (void*)this
      << " RecvSlot: " << mCurrentCommandSlot
      << " HexRecvSlot: " << (void*)mCurrentCommandSlot
      << " addr: " << (void*)&mCurrentCommandSlot
      << EndLogLine;


    for( int i = 0; i < SKV_SERVER_COMMAND_SLOTS; i++ )
    {
      int Index = i * SKV_CONTROL_MESSAGE_SIZE;

      mPrimaryCmdBuffers[i].InitAbs( mCommandBuffLMR, &PrimaryBuffs[Index], SKV_CONTROL_MESSAGE_SIZE );
      skv_client_to_server_cmd_hdr_t* req = (skv_client_to_server_cmd_hdr_t*) &(PrimaryBuffs[Index]);

      // since we poll on mem location, make sure the buffers are reset
      req->Reset();
      ((char*) req)[SKV_CONTROL_MESSAGE_SIZE - 1] = SKV_CTRL_MSG_FLAG_EMPTY;

      BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG | SKV_SERVER_PENDING_EVENTS_LOG | SKV_SERVER_BUFFER_AND_COMMAND_LOG )
          << "skv_server_ep_state_t::Init()::PrimaryCmdBuffs"
          << " i: " << i
          << " SB: " << (void*) &mPrimaryCmdBuffers[i] << " " << mPrimaryCmdBuffers[i]
          << EndLogLine;
    }
    /******************************************************************/




    /******************************************************************
     * Init Secondary Command Buffers used for request data of multi-stage out-of-order commands
     ******************************************************************/
    char* SecondaryBuffs = mCommandBuffs + ( SKV_SERVER_COMMAND_SLOTS * SKV_CONTROL_MESSAGE_SIZE );

    for( int i = 0; i < SKV_COMMAND_TABLE_LEN; i++ )
    {
      int Index = i * SKV_CONTROL_MESSAGE_SIZE;

      mSecondaryCmdBuffers[i].InitAbs( mCommandBuffLMR, &SecondaryBuffs[Index], SKV_CONTROL_MESSAGE_SIZE );

      BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG | SKV_SERVER_PENDING_EVENTS_LOG | SKV_SERVER_BUFFER_AND_COMMAND_LOG )
        << "skv_server_ep_state_t::Init()::SecondaryCmdBuffs"
        << " i: " << i
        << " SB: " << (void*)&mSecondaryCmdBuffers[i] << " " << mSecondaryCmdBuffers[i]
        << EndLogLine;
    }
    /*******************************************************************/




    /******************************************************************
     * Init Command Tables
     ******************************************************************/
    for( int i = 0; i < SKV_COMMAND_TABLE_LEN; i++ )
    {
      if( i < SKV_MAX_COMMANDS_PER_EP )
      {
        // mCommandTable[ i ].Init( mPZHdl, & mSecondaryCmdBuffers[ i ], i );
        mCCBTable[i].Init( mPZHdl, NULL, -1 );   // don't initialize any command/response buffers yet
      }
      else
        mCCBTable[i].Init( mPZHdl, NULL, -1 );   // special buffer for cm-events
    }
    /******************************************************************/

    /******************************************************************
     * Init free Command slots list and RecvBuffer list
     ******************************************************************/
    // for( int i = 0; i < SKV_RECV_BUFFERS_COUNT; i++ )
    //   {
    //     mUnpostedRecvBuffersList->push( i ); // mark recv buffers as unposted 
    //   }    
    for( int i = 0; i < SKV_MAX_COMMANDS_PER_EP; i++ )
    {
      mFreeCommandSlotList->push( i );   // fill the free CCB slots list (omit the slot for connReqests)
    }

    /******************************************************************/

  }

  /******************************************************************
   * Managing send counters
   ******************************************************************/
  inline void StallEP()     { mWaitForSQ = true;  }
  inline void UnstallEP()   { mWaitForSQ = false; }
  inline bool EPisStalled() { return mWaitForSQ;  }

  void
  AllSendsComplete( int aSignaledCommandBufferIndex )
  {
    BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
      << "skv_server_ep_state_t::AllSendsComplete(): "
      << " on EP: " << (void*)this
      << " completing from: " << mUsedCommandSlotIndex
      << " until: " << aSignaledCommandBufferIndex
      << " next unused: " << mUnusedCommandSlotIndex
      << " dispatched: " << mDispatchedCommandBufferIndex
      << " unstalling EP"
      << EndLogLine;
    
    if( ( EPisStalled() ) &&
        ( mUsedCommandSlotIndex != aSignaledCommandBufferIndex ) )
    {
      UnstallEP();
    }
    mUsedCommandSlotIndex = aSignaledCommandBufferIndex;
  }
  /******************************************************************
   * Managing list of pending events for this EP
   *
   * To avoid send queue overflow, we avoid processing of certain events
   * and push them to a pending events queue
   ******************************************************************/
  int
  GetPendingEventsCount()
  {
    return mPendingEventsList->size();
  }

  skv_status_t
  AddToPendingEventsList( skv_server_event_t* aEvent )
  {
    skv_status_t status = SKV_SUCCESS;

    while( mPendingEvents[ mLastPending ].mCmdEventType != 0 )
    {
      mLastPending = ( mLastPending + 1 ) % SKV_SERVER_PENDING_EVENTS_PER_EP;
    }

    BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_ep_state_t::AddToPendingEventsList(): "
      << " mLastPending: " << mLastPending
      << EndLogLine;

    skv_server_event_t *newEvent = & mPendingEvents[ mLastPending ]; // newEvent space in array
    skv_server_event_t *oldEvent = aEvent;                  // existing pending Event from event array
    memcpy( newEvent, oldEvent, sizeof( skv_server_event_t ) );    // copy

    mPendingEventsList->push( newEvent );

    BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG | SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_ep_state_t::AddToPendingEventsList()::"
      << " cmd: " << skv_server_event_type_to_string( oldEvent->mEventType )
      << " eventPtr: " << ( void* ) newEvent
      << " pending: " << GetPendingEventsCount()
      << EndLogLine;

    StrongAssertLogLine( GetPendingEventsCount() <= SKV_SERVER_PENDING_EVENTS_PER_EP )
      << "skv_server_ep_state_t::AddToPendingEventsList(): Pending Events Queue Overflow: Increase SKV_SERVER_PENDING_EVENTS_PER_EP"
      << " current size: " << GetPendingEventsCount()
      << EndLogLine;

    return status;
  }

  // retrieves first pending event from event list
  skv_server_event_t*
  GetNextPendingEvent( )
  {
    if( mPendingEventsList->empty() )
      return NULL;

    skv_server_event_t* event = mPendingEventsList->front();
    
    BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG | SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_ep_state_t::GetNextPendingEvent()::"
      << " cmd: " << skv_server_event_type_to_string( event->mEventType )
      << " eventPtr: " << (void*) event
      << " remaining: " << mPendingEventsList->size()
      << EndLogLine;

    return event;
  }
  skv_status_t
  FreeFirstPendingEvent( )
  {
    skv_server_event_t* event = mPendingEventsList->front();
    mPendingEventsList->pop();  // remove from queue

    memset( event, 0, sizeof( skv_server_event_t ) );   // reset event slot/mark as free
    
    BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG | SKV_SERVER_PENDING_EVENTS_LOG )
      << "skv_server_ep_state_t::FreeFirstPendingEvent()::"
      << " remaining: " << mPendingEventsList->size()
      << EndLogLine;

    return SKV_SUCCESS;
  }
  

  /******************************************************************
   * Managing list of free command slots for this EP
   ******************************************************************/
  bool
  ResourceCheck()
  {
    if( mFreeCommandSlotList->size() < 1 )
    {
      BegLogLine( 0 )
        << "ResourceCheck(): ran out of cmd-slots"
        << " free: " << mFreeCommandSlotList->size()
        << EndLogLine;
      return false;
    }

    int nextCommandBufferIndex = (mUnusedCommandSlotIndex + 2) % SKV_SERVER_COMMAND_SLOTS;
    if( nextCommandBufferIndex == mUsedCommandSlotIndex )
    {
      BegLogLine( 0 )
        << "ResourceCheck(): ran out of req-slots"
        << " next unused: " << mUnusedCommandSlotIndex
        << " first used: " << mUsedCommandSlotIndex
        << " next attempt: " << nextCommandBufferIndex
        << EndLogLine;
      return false;
    }

    return true;
  }

  int
  GetNextFreeCommandSlotOrdinal()
  {
    BegLogLine( SKV_SERVER_BUFFER_AND_COMMAND_LOG )
      << "skv_server_ep_state_t::GetNextFreeCommandSlotOrdinal():: "
      << " #free slots: " << mFreeCommandSlotList->size()
      << EndLogLine;

    if( !mFreeCommandSlotList->empty() > 0 )
    {
      int ord = mFreeCommandSlotList->front();
      mFreeCommandSlotList->pop();

      BegLogLine( SKV_SERVER_BUFFER_AND_COMMAND_LOG )
        << "skv_server_ep_state_t::GetNextFreeCommandSlotOrdinal():: "
        << " #remaining free slots: " << mFreeCommandSlotList->size()
        << " ordinal: " << ord
        << EndLogLine;

      return ord;
    }
    return -1;
  }

  skv_status_t
  AddToFreeCommandSlots( skv_server_ccb_t *aCCB, int ord )
  {
    // detach the command buffer from the CCB
    aCCB->DetachPrimaryCmdBuffer( );

    mFreeCommandSlotList->push( ord );

    BegLogLine( SKV_SERVER_BUFFER_AND_COMMAND_LOG )
      << "skv_server_ep_state_t::AddToFreeCommandSlots():: "
      << " added ord: " << ord
      << " #free slots: " << mFreeCommandSlotList->size()
      << EndLogLine;

    return SKV_SUCCESS;
  }

  /******************************************************************
   * Managing list of unposted command buffers for this EP
   ******************************************************************/
  int
  GetNextUnusedCommandBufferOrd()
  {
    int ret = mUnusedCommandSlotIndex;
    mUnusedCommandSlotIndex = (mUnusedCommandSlotIndex + 1) % SKV_SERVER_COMMAND_SLOTS;

    if( mUnusedCommandSlotIndex == mUsedCommandSlotIndex )
    {
      BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
        << "GetNextUnusedCommandBufferOrd(): CommandSlotOverflow: "
        << " unused:" << mUnusedCommandSlotIndex
        << " used: " << mUsedCommandSlotIndex
        << EndLogLine;

      mUnusedCommandSlotIndex = ret;   // reset to previous value
      return -1;
    }

    BegLogLine( SKV_SERVER_BUFFER_AND_COMMAND_LOG )
      << "skv_server_ep_state_t::GetNextUnusedCommandBufferOrd():: "
      << " first used: " << mUsedCommandSlotIndex
      << " dispatched: " << mDispatchedCommandBufferIndex
      << " current: " << ret
      << " next unused: " << mUnusedCommandSlotIndex
      << EndLogLine;

    return ret;
  }

  skv_status_t
  AddToUnusedCommandBuffers( )
  {
    mUsedCommandSlotIndex = (mUsedCommandSlotIndex + 1) % SKV_SERVER_COMMAND_SLOTS;

    BegLogLine( SKV_SERVER_BUFFER_AND_COMMAND_LOG )
      << "skv_server_ep_state_t::AddToUnusedCommandBuffers():: "
      << " first used: " << mUsedCommandSlotIndex
      << " dispatched: " << mDispatchedCommandBufferIndex
      << " next unused: " << mUnusedCommandSlotIndex
      << EndLogLine;

    return SKV_SUCCESS;
  }

  skv_status_t
  AdvanceDispatched( )
  {
    mDispatchedCommandBufferIndex = ( mDispatchedCommandBufferIndex + 1 ) % SKV_SERVER_COMMAND_SLOTS;

    BegLogLine( SKV_SERVER_BUFFER_AND_COMMAND_LOG )
      << "skv_server_ep_state_t::AdvanceDispatched():: "
      << " first used: " << mUsedCommandSlotIndex
      << " dispatched: " << mDispatchedCommandBufferIndex
      << " next unused: " << mUnusedCommandSlotIndex
      << EndLogLine;

    return SKV_SUCCESS;
  }

  /******************************************************************/
  // Recv-Slot Polling
  bool
  CheckForNewCommands()
  {
    if( EPisStalled() )
    {
      BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
        << "skv_server_ep_state_t::CheckForNewCommands():"
        << " EP: " << ( void* )this
        << " is stalled. returning without command"
        << EndLogLine;

      return false;

    }

    int Index = mCurrentCommandSlot * SKV_CONTROL_MESSAGE_SIZE;

    skv_client_to_server_cmd_hdr_t* NewRequest = (skv_client_to_server_cmd_hdr_t*) ( & mCommandBuffs[ Index ] ) ;

    bool found =( ( NewRequest->mCmdType != SKV_COMMAND_NONE ) &&
                  ( ((char*)NewRequest)[SKV_CONTROL_MESSAGE_SIZE-1] == NewRequest->CheckSum() )
                );

    // #define HEXLOG( x )  (  (void*) (*((uint64_t*) &(x)) ) )

    // if( found )
    //   {
    //     char* buf = (char*)NewRequest;
    //     BegLogLine( 1 )
    //       << "Fetch: " << mCurrentCommandSlot << " @:" << (void*)NewRequest
    //       << " buf: " << HEXLOG(buf[0*sizeof(void*)]) << " " << HEXLOG(buf[1*sizeof(void*)]) << " " << HEXLOG(buf[2*sizeof(void*)])  << " " << HEXLOG(buf[3*sizeof(void*)])
    //       << " " << HEXLOG(buf[4*sizeof(void*)]) << " " << HEXLOG(buf[5*sizeof(void*)]) << " " << HEXLOG(buf[6*sizeof(void*)])  << " " << HEXLOG(buf[7*sizeof(void*)])
    //       << " " << HEXLOG(buf[8*sizeof(void*)]) << " " << HEXLOG(buf[9*sizeof(void*)]) << " " << HEXLOG(buf[10*sizeof(void*)]) << " " << HEXLOG(buf[11*sizeof(void*)])
    //       << " last bytes: " << HEXLOG( buf[SKV_CONTROL_MESSAGE_SIZE-sizeof(void*)])
    //       << " EP: " << (void*)this
    //       << EndLogLine;
    //   }

    return found;
  }


  void
  CommandSlotAdvance()
  {
    // reset trailling flag of current command slot (implicitly marked consumed although it isn't yet consumed)
    int Index = mCurrentCommandSlot * SKV_CONTROL_MESSAGE_SIZE;
    skv_client_to_server_cmd_hdr_t* NewRequest = (skv_client_to_server_cmd_hdr_t*) ( & mCommandBuffs[ Index ] ) ;

    BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
      << "skv_server_ep_state_t::CommandSlotAdvance():: "
      << " EP: " << (void*)this
      << " CmdSlot from: " << mCurrentCommandSlot
      << " resetting ChSum: " << (int)(((char*)NewRequest)[SKV_CONTROL_MESSAGE_SIZE-1])
      << EndLogLine;

    ((char*)NewRequest)[SKV_CONTROL_MESSAGE_SIZE-1] = SKV_CTRL_MSG_FLAG_EMPTY;


    // char* buf = (char*)NewRequest;
    // BegLogLine( 1 )
    //   << "Cleanup: " << mCurrentCommandSlot
    //   << " " << HEXLOG(buf[0*sizeof(void*)]) << " " << HEXLOG(buf[1*sizeof(void*)]) << " " << HEXLOG(buf[2*sizeof(void*)])  << " " << HEXLOG(buf[3*sizeof(void*)])
    //   << " " << HEXLOG(buf[4*sizeof(void*)]) << " " << HEXLOG(buf[5*sizeof(void*)]) << " " << HEXLOG(buf[6*sizeof(void*)])  << " " << HEXLOG(buf[7*sizeof(void*)])
    //   << " " << HEXLOG(buf[8*sizeof(void*)]) << " " << HEXLOG(buf[9*sizeof(void*)]) << " " << HEXLOG(buf[10*sizeof(void*)]) << " " << HEXLOG(buf[11*sizeof(void*)])
    //   << " last bytes: " << HEXLOG( buf[SKV_CONTROL_MESSAGE_SIZE-sizeof(void*)])
    //   << EndLogLine;

    // advance slot
    mCurrentCommandSlot = (mCurrentCommandSlot + 1 ) % SKV_SERVER_COMMAND_SLOTS;

    BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
      << "skv_server_ep_state_t::CommandSlotAdvance():: "
      << " EP: " << (void*)this
      << " CmdSlot to: " << mCurrentCommandSlot
      << " resetting ChSum: " << (int)(((char*)NewRequest)[SKV_CONTROL_MESSAGE_SIZE-1])
      << EndLogLine;
  }

  int 
  GetCurrentCommandSlot()
  {
    return mCurrentCommandSlot;
  }

  skv_lmr_triplet_t*
  GetCommandTriplet( int aSlotOrd )
  {
    AssertLogLine( (aSlotOrd >= 0) && (aSlotOrd < SKV_SERVER_COMMAND_SLOTS) )
      << "skv_server_ep_state_t::GetCommandTriplet(): ERROR"
      << " slotNr out of range: " << aSlotOrd
      << EndLogLine;

    return (skv_lmr_triplet_t*) &(mPrimaryCmdBuffers[ aSlotOrd ]);
  }

  int GetCurrentClientResponseBufferSlotOrd()
  {
    return mClientCurrentResponseSlot;
  }

  /** \brief Get the client-side slot address for the next response to write 
   */
  uintptr_t GetClientResponseBufferAddress()
  {
    uintptr_t address = mClientResponseRMR.GetAddr();

    address += mClientCurrentResponseSlot * SKV_CONTROL_MESSAGE_SIZE;

    BegLogLine( SKV_SERVER_RDMA_RESPONSE_PLACEMENT_LOG )
      << "skv_server_ep_state_t::GetNextClientResponseBufferAddress():"
      << " address to write: " << (void*)address
      << " slot: " << mClientCurrentResponseSlot
      << EndLogLine;

    return address;
  }
  void
  ClientResonseBufferAdvance()
  {
    mClientCurrentResponseSlot = (mClientCurrentResponseSlot + 1) % SKV_SERVER_COMMAND_SLOTS;
  }
    

#ifdef SKV_DEBUG_MSG_MARKER
  // duplicating the structs for debugging here because of circular dependencies
struct skv_server_to_client_cmd_hdr_tX
{
  // skv_client_ccb_t*                mCmdCtrlBlk;
  uint64_t                          mCmdCtrlBlk;
  int                               mCmdOrd;
  skv_command_type_t               mCmdType;
  skv_client_event_t               mEvent;
  uint64_t                          mMarker;
};
struct skv_value_in_ctrl_msg_tX
{
  int                           mValueSize;
  char                          mData[ 0 ];  
};
struct skv_cmd_retrieve_resp_t
{
  skv_server_to_client_cmd_hdr_tX     mHdr;  
  skv_status_t                       mStatus;  
  skv_value_in_ctrl_msg_tX            mValue;
};
#endif

  /******************************************************************/

  skv_status_t
  Dispatch( skv_server_ccb_t      *aCCB,
            int                    *aSeqNo,
            int                     aCommandOrdinal )
  {        
    gSKVServerDispatchResponseStart.HitOE( SKV_SERVER_TRACE,
                                           "SKVServerDispatch",
                                           aCommandOrdinal,
                                           gSKVServerDispatchResponseStart );

    skv_status_t status = SKV_SUCCESS;

    // nothing to be done on the post-recv part any more...

    /******************************************************************************************/

    // try to post the first entry of the pending reply list
    skv_lmr_triplet_t* SendLmrTriplet = aCCB->GetSendTripletPtr();

    BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG )
      << "skv_server_ep_state_t::Dispatch():: "
      << " command: " << skv_command_type_to_string (aCCB->GetType())
      << " posting reply seqNo: " << *aSeqNo
      << " DispatchedOrd: " << mDispatchedCommandBufferIndex
      << " Ord: " << aCommandOrdinal
      << " sendlmr: " << *SendLmrTriplet
      << EndLogLine;

#ifdef SKV_DEBUG_MSG_MARKER
    skv_server_to_client_cmd_hdr_t *sendHdr = (skv_server_to_client_cmd_hdr_t*) aCCB->GetSendBuff();
    BegLogLine( 1 )
      << "Sending Marker: " << sendHdr->mMarker
      << " sndBuf: " << (void*)sendHdr
      << EndLogLine;
#endif

    status = PostResponse( SendLmrTriplet,
                           aSeqNo,
                           mDispatchedCommandBufferIndex );

    // if posted successfully, detach CCB and sendBuff and get both back to the free lists
    if( status == SKV_SUCCESS )
    {
      // since the "old" send buffer is detached, we can enable
      // reusage of command slot
      AddToFreeCommandSlots( aCCB, aCommandOrdinal );

      // if this was an immediate command, then we can advance the DispatchedIndex
      if( aCCB->GetCommandClass() == SKV_COMMAND_CLASS_IMMEDIATE )
      {
        AdvanceDispatched();
      }

      BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
        << "skv_server_ep_state_t::Dispatch(): "
        << " EP: " << (void*)this
        << " current DispatchIndex: " << mDispatchedCommandBufferIndex
        << " cmdClass: " << (int)aCCB->GetCommandClass()
        << EndLogLine;

    }
    // check for queue overflow - shouldn't happen any more...
    else if( status == SKV_ERRNO_COMMAND_LIMIT_REACHED )
    {

      AssertLogLine( 0 )
        << "skv_server_ep_state_t::Dispatch():: THIS SHOULD NO LONGER HAPPEN, BUG IN BOOKKEEPING"
        << " cmdOrd: " << aCommandOrdinal
        << " status: " << status
        << EndLogLine;

    }

    gSKVServerDispatchResponseFinis.HitOE( SKV_SERVER_TRACE,
                                           "SKVServerDispatch",
                                           aCommandOrdinal,
                                           gSKVServerDispatchResponseFinis );

    return status;
  }

  skv_status_t
  PostResponse( skv_lmr_triplet_t*  aSendTriplet,
                int*                 aSeqNo,
                int                  aDispatchedIndex )
  {
    // if the server slot is 0 the do a signalled
    skv_server_rdma_write_cmpl_cookie_t Cookie;
    it_dto_flags_t dto_write_flags = (it_dto_flags_t) (0);

    int dest_resp_slot = GetCurrentClientResponseBufferSlotOrd();   // signaling based on response slot because out-of-order commands don't progress dispatched index
    if( (dest_resp_slot % (SKV_SIGNALED_WRITE_INTERVAL)) == 0 )   //
    {
      // first attempts without notification (send still in place)
      dto_write_flags = (it_dto_flags_t) (IT_COMPLETION_FLAG | IT_NOTIFY_FLAG);

      skv_server_rdma_write_cmpl_func_t cbFunc = EPSTATE_CountSendCompletionsCallback;

      Cookie.Init( this, cbFunc, aDispatchedIndex, 1 );

      BegLogLine( SKV_SERVER_PENDING_EVENTS_LOG | SKV_SERVER_COMMAND_POLLING_LOG )
        << "skv_server_ep_state_t::PostResponse():  preparing cookie"
        << " EP: " << ( void* )this
        << " SBord: " << aDispatchedIndex << " "
        << *aSendTriplet
        << " SeqNo: " << (void*)cbFunc
        << EndLogLine;

    }
    else
    {
      Cookie.Init( NULL, NULL );
    }

    it_rdma_addr_t dest_resp_addr = (it_rdma_addr_t) (GetClientResponseBufferAddress());

#ifdef SKV_DEBUG_MSG_MARKER
    skv_cmd_retrieve_resp_t *Resp = (skv_cmd_retrieve_resp_t*) (seg_rx.addr.abs);
    BegLogLine( 1 )
      << "skv_server_ep_state_t::Dispatch(): "
      << " about to send data mBuf: " << (void*)Resp->mValue.mData
      << " mvalue: " << (void*) (*(uint64_t*)(Resp->mValue.mData))
      << " size: " << Resp->mValue.mValueSize
      << " msg: " << Resp->mHdr.mMarker
      << EndLogLine;
#endif

    it_lmr_triplet_t seg_tx;
    seg_tx.addr.abs = (char *) aSendTriplet->GetAddr(); 
    seg_tx.length   = aSendTriplet->GetLen();
    seg_tx.lmr      = aSendTriplet->GetLMRHandle();

    skv_server_to_client_cmd_hdr_t *hdr = (skv_server_to_client_cmd_hdr_t*)seg_tx.addr.abs;
    ((char*)seg_tx.addr.abs)[SKV_CONTROL_MESSAGE_SIZE-1] = hdr->CheckSum();

    BegLogLine( SKV_SERVER_COMMAND_DISPATCH_LOG )
      << "skv_server_ep_state_t::PostResponse(): "
      << " EP: "        << (void*)this
      << " clientCCB: " << (void*)(hdr->mCmdCtrlBlk)
      << EndLogLine;

// #define HEXLOG( x )  (  (void*) (*((uint64_t*) &(x)) ) )
//     uintptr_t *buffer = (uintptr_t*)seg_tx.addr.abs;
//     BegLogLine( 1 )
//       << "responseBuff: " << HEXLOG(buffer[ 0 ])
//       << " " << HEXLOG(buffer[ 1 ])
//       << " " << HEXLOG(buffer[ 2 ])
//       << " " << HEXLOG(buffer[ 3 ])
//       << " " << HEXLOG(buffer[ 4 ])
//       << " TRLFLG: " << HEXLOG(buffer[ SKV_CONTROL_MESSAGE_SIZE / sizeof(uintptr_t) -1 ])
//       << " slot@: " << (void*)buffer
//       << EndLogLine;

    gSKVServerRDMAResponseStart.HitOE( SKV_SERVER_TRACE,
                                       "SKVServerRDMAResponse",
                                       aDispatchedIndex,
                                       gSKVServerRDMAResponseStart );

    it_status_t status = it_post_rdma_write( mEPHdl,
                                             &seg_tx,
                                             1,
                                             Cookie.GetCookie(),
                                             dto_write_flags,
                                             dest_resp_addr,
                                             mClientResponseRMR.GetRMRContext() );

    gSKVServerRDMAResponseFinis.HitOE( SKV_SERVER_TRACE,
                                       "SKVServerRDMAResponse",
                                       aDispatchedIndex,
                                       gSKVServerRDMAResponseFinis );

    if( status == IT_ERR_TOO_MANY_POSTS )
    {
      BegLogLine( 1 )
        << "skv_server_ep_state_t::PostResponse():: "
        << " post queue exhausted"
        << EndLogLine;
      return SKV_ERRNO_COMMAND_LIMIT_REACHED;
    }

    if( status != IT_SUCCESS )
    {
      BegLogLine( 1 )
        << "skv_server_ep_state_t::PostResponse():: "
        << " status: " <<  status
        << EndLogLine;

      return SKV_ERRNO_IT_POST_SEND_FAILED;
    }

    ClientResonseBufferAdvance();
    (*aSeqNo)++;    

    return SKV_SUCCESS;
  }
};

// the list of contexts (CCBs)
typedef std::map< it_ep_handle_t, skv_server_ep_state_t* > EPStateMap_T;

class skv_server_internal_event_manager_if_t
{
  typedef STL_QUEUE( skv_server_event_t )  EventQueue_T;
  EventQueue_T* mQueue;

public:
  void
  Init()
  {
    mQueue = new EventQueue_T;
    StrongAssertLogLine( mQueue != NULL )
      << "skv_server_user_event_manager_if_t::Init():: "
      << EndLogLine;
  }

  void
  Finalize()
  {
    if( mQueue )
    {
      delete mQueue;
      mQueue = NULL;
    }
  }

  skv_status_t
  Enqueue( skv_server_event_t* aEvent )
  {
    mQueue->push( *aEvent );
    BegLogLine( 1 )
      << "skv_server_internal_event_manager_if_t::Enqueue(): "
      << " Event.ccb: " << (void*)aEvent->mEventMetadata.mCommandFinder.mEPStatePtr
      << EndLogLine;
    return SKV_SUCCESS;
  }

  int
  GetEventQueueSize()
  {
    int Size = mQueue->size();

    BegLogLine( 0 )
      << "skv_server_user_event_manager_if_t::GetEventQueueSize():: "
      << " Size: " << Size
      << EndLogLine;

    return Size;
  }

  skv_status_t
  Dequeue( skv_server_event_t* aEvent )
  {
    if( GetEventQueueSize() > 0 )
    {
      *aEvent = mQueue->front();
      mQueue->pop();

      BegLogLine( 1 )
        << "skv_server_user_event_manager_if_t::Dequeue():: "
        << " Popped one"
        << " Event.ccb: " << (void*)aEvent->mEventMetadata.mCommandFinder.mEPStatePtr
        << EndLogLine;

      return SKV_SUCCESS;
    }
    else
      return SKV_ERRNO_NO_EVENT;
  }
};

class skv_server_pending_event_manager_if_t
{
  typedef STL_QUEUE( it_event_t )  EventQueue_T;
  EventQueue_T* mQueue;

public:
  void
  Init()
  {
    mQueue = new EventQueue_T;
    StrongAssertLogLine( mQueue != NULL )
      << "skv_server_pending_event_manager_if_t::Init():: "
      << EndLogLine;
  }

  void
  Finalize()
  {
    if( mQueue )
    {
      delete mQueue;
      mQueue = NULL;
    }
  }

  skv_status_t
  Enqueue( it_event_t* aEvent )
  {
    mQueue->push( *aEvent );
    return SKV_SUCCESS;
  }

  int
  GetEventQueueSize()
  {
    int Size = mQueue->size();

    BegLogLine( 0 )
      << "skv_server_pending_event_manager_if_t::queue_size():: "
      << " Size: " << Size
      << EndLogLine;

    return Size;
  }

  skv_status_t
  Dequeue( it_event_t* aEvent )
  {
    if( GetEventQueueSize() > 0 )
    {
      *aEvent = mQueue->front();
      mQueue->pop();

      BegLogLine( 1 )
        << "skv_server_pending_event_manager_if_t::Dequeue():: "
        << " Popped one"
        << EndLogLine;

      return SKV_SUCCESS;
    }
    else
      return SKV_ERRNO_NO_EVENT;
  }
};

#endif
