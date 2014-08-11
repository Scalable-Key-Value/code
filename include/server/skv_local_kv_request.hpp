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

#ifndef __SKV_LOCAL_KV_REQUEST_HPP__
#define __SKV_LOCAL_KV_REQUEST_HPP__

typedef enum {
  SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN,
  SKV_LOCAL_KV_REQUEST_TYPE_INSERT,
  SKV_LOCAL_KV_REQUEST_TYPE_LOOKUP,
  SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE,
  SKV_LOCAL_KV_REQUEST_TYPE_REMOVE,
  SKV_LOCAL_KV_REQUEST_TYPE_OPEN,
  SKV_LOCAL_KV_REQUEST_TYPE_CLOSE,
  SKV_LOCAL_KV_REQUEST_TYPE_INFO,
  SKV_LOCAL_KV_REQUEST_TYPE_BULK_INSERT,
  SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE_N,
  SKV_LOCAL_KV_REQUEST_TYPE_GET_DISTRIBUTION
} skv_local_kv_request_type_t;

static
const char*
skv_local_kv_request_type_to_string( skv_local_kv_request_type_t aType )
{
  switch( aType )
    {
    case SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN:        { return "SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_INSERT:         { return "SKV_LOCAL_KV_REQUEST_TYPE_INSERT"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_LOOKUP:         { return "SKV_LOCAL_KV_REQUEST_TYPE_LOOKUP"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE:       { return "SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_REMOVE:         { return "SKV_LOCAL_KV_REQUEST_TYPE_REMOVE"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_OPEN:           { return "SKV_LOCAL_KV_REQUEST_TYPE_OPEN"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_CLOSE:          { return "SKV_LOCAL_KV_REQUEST_TYPE_CLOSE"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_INFO:           { return "SKV_LOCAL_KV_REQUEST_TYPE_INFO"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_BULK_INSERT:    { return "SKV_LOCAL_KV_REQUEST_TYPE_BULK_INSERT"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE_N:     { return "SKV_LOCAL_KV_REQUEST_TYPE_RETRIEVE_N"; }
    case SKV_LOCAL_KV_REQUEST_TYPE_GET_DISTRIBUTION: { return "SKV_LOCAL_KV_REQUEST_TYPE_GET_DISTRIBUTION"; }
    default:
      {
        printf( "skv_local_kv_request_type_to_string: ERROR:: type: %d is not recognized\n", aType );
        return "SKV_ERRNO_UNKNOWN_STATE";
      }
    }
}


struct skv_local_kv_open_request_t {
  char *mPDSName;   // only need reference here - data is in command sendbuffer
  skv_pds_priv_t mPrivs;
  skv_cmd_open_flags_t mFlags;
};

struct skv_local_kv_stat_request_t {
  skv_pdscntl_cmd_t mCmd;
  skv_pds_attr_t *mPDSAttr;
};

struct skv_local_kv_lookup_request_t {
  skv_pds_id_t mPDSId;
  char *mKeyData;
  int mKeySize;
  skv_cmd_RIU_flags_t mFlags;
};

struct skv_local_kv_retrieve_request_t {
  skv_pds_id_t mPDSId;
  char* mKeyData;
  int mKeySize;
  int mValueOffset;
  int mValueSize;
  skv_cmd_RIU_flags_t mFlags;
};

struct skv_local_kv_insert_request_t {
  skv_cmd_RIU_req_t *mReqData;
  skv_status_t mCmdStatus;
  skv_lmr_triplet_t mStoredValueRep;
};

struct skv_local_kv_remove_request_t {
  skv_pds_id_t mPDSId;
  char* mKeyData;
  int mKeySize;
};

struct skv_local_kv_bulkinsert_request_t {
  skv_pds_id_t mPDSId;
  skv_lmr_triplet_t mLocalBuffer;
};

struct skv_local_kv_retrieveN_request_t {
  skv_pds_id_t mPDSId;
  char *mStartingKeyData;
  int mStartingKeySize;
  skv_lmr_triplet_t *mRetrievedKeysSizesSegs;
  int mListOfKeysMaxCount;
  skv_cursor_flags_t mFlags;
};

struct skv_local_kv_create_cursor_request_t {
  char* mBuff;
  int aBuffSize;
};

struct skv_local_kv_request_t {
  skv_local_kv_request_type_t mType;
  skv_local_kv_cookie_t *mCookie;
  skv_local_kv_req_ctx_t mReqCtx;

  union {
    skv_local_kv_open_request_t mOpen;
    skv_local_kv_stat_request_t mStat;
    skv_local_kv_insert_request_t mInsert;
    skv_local_kv_lookup_request_t mLookup;
    skv_local_kv_retrieve_request_t mRetrieve;
    skv_local_kv_remove_request_t mRemove;
    skv_local_kv_bulkinsert_request_t mBulkInsert;
    skv_local_kv_retrieveN_request_t mRetrieveN;
    skv_local_kv_create_cursor_request_t mCursor;
  } mRequest;
  char mData[ SKV_CONTROL_MESSAGE_SIZE ];

  void InitCommon( skv_local_kv_request_type_t aType = SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN,
                   skv_local_kv_cookie_t *aCookie = NULL,
                   skv_local_kv_req_ctx_t aReqCtx = 0 )
  {
    mType = aType;
    mCookie = aCookie;
    mReqCtx = aReqCtx;
  }
};


#endif // __SKV_LOCAL_KV_REQUEST_HPP__
