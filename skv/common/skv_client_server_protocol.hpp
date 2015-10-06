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

#ifndef __SKV_CLIENT_SERVER_PROTOCOL_HPP__
#define __SKV_CLIENT_SERVER_PROTOCOL_HPP__

//#define SKV_DEBUG_MSG_MARKER

#include <skv/common/skv_types.hpp>
#include <skv/common/skv_distribution_manager.hpp>
#include <skv/client/skv_client_types.hpp>
#include <skv/server/skv_server_event_type.hpp>

#include <skv/common/skv_client_server_headers.hpp>

//#include <skv/server/skv_server_types.hpp>
//#include <skv/server/skv_server_cursor_manager_if.hpp>
#include <skv/client/skv_client_conn_manager_if.hpp>

#ifndef SKV_CLIENT_ENDIAN_LOG
#define SKV_CLIENT_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_ENDIAN_LOG
#define SKV_SERVER_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL )
#endif

/***************************************************
 * Structures for the CLIENT to SERVER communication
 ***************************************************/

struct skv_cmd_retrieve_dist_req_t
{
  skv_client_to_server_cmd_hdr_t mHdr;

  void
  Init( skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );
    mHdr.SetCmdLength( sizeof(skv_cmd_retrieve_dist_req_t) );
  }
};

struct skv_cmd_open_req_t
{
  skv_client_to_server_cmd_hdr_t        mHdr;

  skv_pds_priv_t                        mPrivs;
  skv_cmd_open_flags_t                  mFlags;
  char                                  mPDSName[ SKV_MAX_PDS_NAME_SIZE];

  void
  EndianConvert(void)
  {
    mPrivs=(skv_pds_priv_t)ntohl(mPrivs) ;
    mFlags=(skv_cmd_open_flags_t)ntohl(mFlags) ;
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mPrivs=" << mPrivs
      << " mFlags=" << mFlags
      << EndLogLine ;
  }
  void
  Init( skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_pds_priv_t aPrivs,
        skv_cmd_open_flags_t aFlags,
        char* aPDSName,
        int aPDSNameSize )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mPrivs = aPrivs;
    mFlags = aFlags;

    AssertLogLine( aPDSNameSize > 0 &&
                   aPDSNameSize <= SKV_MAX_PDS_NAME_SIZE )
      << "skv_cmd_open_req_t::Init():: ERROR:: Length of PDS name is too long. "
      << " NameLen: " << aPDSNameSize
      << " SKV_MAX_PDS_NAME_SIZE: " << SKV_MAX_PDS_NAME_SIZE
      << EndLogLine;

    memcpy( mPDSName, aPDSName, aPDSNameSize );
    mHdr.SetCmdLength( sizeof(skv_cmd_open_req_t) );
  }
};

struct skv_cmd_pdscntl_req_t
{
  skv_client_to_server_cmd_hdr_t     mHdr;

  skv_pdscntl_cmd_t                  mCntlCmd;
  skv_pds_attr_t                     mPDSAttr;

  void
  Init( skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_pdscntl_cmd_t aCntlCmd,
        skv_pds_attr_t *aPDSAttr )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mCntlCmd = aCntlCmd;
    mPDSAttr = *aPDSAttr;

    mHdr.SetCmdLength( sizeof(skv_cmd_pdscntl_req_t) );
  }
  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "Endian conversion mCntlCmd=" << mCntlCmd
      << EndLogLine ;
    mCntlCmd=(skv_pdscntl_cmd_t)htonl(mCntlCmd) ;
  }
};


struct skv_key_value_in_ctrl_msg_t
{
  int                           mKeySize;
  int                           mValueSize;
  char                          mData[ 0 ];
  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "mKeySize=" << mKeySize
      << " mValueSize=" << mValueSize
      << EndLogLine ;
    mKeySize=ntohl(mKeySize) ;
    mValueSize=ntohl(mValueSize) ;
  }
};

struct skv_value_in_ctrl_msg_t
{
  int                           mValueSize;
  char                          mData[ 0 ];
  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "mValueSize=" << mValueSize
      << EndLogLine ;
    mValueSize=ntohl(mValueSize) ;
  }
};


template<class streamclass>
static streamclass& operator<<( streamclass& os, const skv_key_value_in_ctrl_msg_t& aArg )
{
  const int STR_BUFF_SIZE = 64;
  char buff[STR_BUFF_SIZE];

  int Len = min( (STR_BUFF_SIZE - 1), aArg.mKeySize );
  memcpy( buff, aArg.mData, Len );

  buff[Len] = 0;

  os << "skv_key_value_in_ctrl_msg_t: [ mKeySize: " << aArg.mKeySize;
  os << " mValueSize: " << aArg.mValueSize;
  os << " mData: ";

  for( int i = 0; i < Len; i++ )
    os << FormatString( "%02X" )
      << buff[i];
  os << " ]";
  return os;
}


// TODO: Can optimize sizing of max key in message data
struct skv_cmd_RIU_req_t
{
  skv_client_to_server_cmd_hdr_t       mHdr;
  skv_cmd_RIU_flags_t                  mFlags;

  int                                  mOffset;
  skv_pds_id_t                         mPDSId;

  skv_rmr_triplet_t                    mRMRTriplet;

  /// CAREFUL!!! KeyInCtrlMsg can be a char[ 0 ]
  // This has to be the last field.
  // NEED: Check that the key is not larger then the
  // message buffer
  skv_key_value_in_ctrl_msg_t          mKeyValue;

  void
  Init( int aNodeId,
        skv_client_conn_manager_if_t* aConnMgr,
        skv_pds_id_t* aPDSId,
        skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        int aOffset,
        skv_cmd_RIU_flags_t aFlags,
        int aKeySize,
        char* aKeyData,
        int aValueSize,
        char* aValueData,
        it_pz_handle_t aPZHdl,
        it_lmr_handle_t* aKeyLMR,
        it_lmr_handle_t* aValueLMR )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mPDSId = *aPDSId;
    mOffset = aOffset;
    mFlags = aFlags;

    mKeyValue.mKeySize = aKeySize;
    mKeyValue.mValueSize = aValueSize;

    int inlineDataSize = sizeof( mKeyValue.mKeySize ) + sizeof( mKeyValue.mValueSize );

    // get the key into the request
    if( aFlags & (SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG |
                  SKV_COMMAND_RIU_RETRIEVE_VALUE_FIT_IN_CTL_MSG |
                  SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG) )
    {
      memcpy( mKeyValue.mData,
              aKeyData,
              aKeySize );

      inlineDataSize += aKeySize;
    }
    else
    {
      StrongAssertLogLine( 0 )
        << "skv_cmd_RIU_req_t::Init(): ERROR:: Keys that don't fit "
        << " into a control message are not yet supported."
        << EndLogLine;
    }

    // get the value into request only for inserts
    if( aFlags & SKV_COMMAND_RIU_INSERT_KEY_VALUE_FIT_IN_CTL_MSG )
    {
      memcpy( & mKeyValue.mData[ aKeySize ],
              aValueData,
              aValueSize );

      inlineDataSize += aValueSize;
    }

    // create lmr for larger requests
    if( aFlags & SKV_COMMAND_RIU_INSERT_KEY_FITS_IN_CTL_MSG )
    {
      it_lmr_handle_t lmrHandle;
      it_rmr_context_t rmrHandle;   // watch out for ambiguous usage of this: with lmr_create it's the handle, with rmr_get_context it's the rkey!!

      it_mem_priv_t privs = (it_mem_priv_t) (IT_PRIV_LOCAL | IT_PRIV_REMOTE);
      it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

      it_status_t status = it_lmr_create( aPZHdl,
                                          aValueData,
                                          NULL,
                                          aValueSize,
                                          IT_ADDR_MODE_ABSOLUTE,
                                          privs,
                                          lmr_flags,
                                          0,
                                          &lmrHandle,
                                          &rmrHandle );

      StrongAssertLogLine( status == IT_SUCCESS )
          << "skv_cmd_retrieve_req_t::Init():: "
          << " status: " << status
          << EndLogLine;

      *aValueLMR = lmrHandle;

      /**
       * Need to get the actual rmr context for a given end point (ep)
       * Since the rmr key is specific to the network device
       * The network device is disambiguated by the ep handle
       */
      it_ep_handle_t epHandle;
      skv_status_t pstatus = aConnMgr->GetEPHandle( aNodeId, &epHandle );
      AssertLogLine( pstatus == SKV_SUCCESS )
        << "ERROR: skv status: " << skv_status_to_string( pstatus )
        << EndLogLine;

      status = itx_get_rmr_context_for_ep( epHandle, lmrHandle, & rmrHandle );  // here we put in the rmrHandle and get back the rkey!!

      AssertLogLine( status == IT_SUCCESS )
        << "ERROR: status: " << status
        << EndLogLine;

      mRMRTriplet.Init( rmrHandle, aValueData, aValueSize );
    }

    mHdr.SetCmdLength( sizeof(skv_cmd_RIU_req_t) + inlineDataSize );
  }
  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "Endian conversion from mKeySize=" << mKeyValue.mKeySize
      << " mValueSize=" << mKeyValue.mValueSize
      << " mOffset=" << mOffset
      << " mFlags=" << mFlags
      << EndLogLine ;
    mKeyValue.mKeySize=htonl(mKeyValue.mKeySize) ;
    mKeyValue.mValueSize=htonl(mKeyValue.mValueSize) ;
    mOffset=htonl(mOffset) ;
    mFlags=(skv_cmd_RIU_flags_t)htonl(mFlags) ;
  }

  static size_t GetMaxPayloadSize()
  {
    return SKV_CONTROL_MESSAGE_SIZE - SKV_CHECKSUM_BYTES -
           sizeof( skv_cmd_RIU_req_t ) -
           sizeof( skv_key_value_in_ctrl_msg_t::mKeySize ) -
           sizeof( skv_key_value_in_ctrl_msg_t::mValueSize );
  }
};

struct skv_cmd_remove_req_t
{
  skv_client_to_server_cmd_hdr_t       mHdr;
  skv_cmd_remove_flags_t               mFlags;

  skv_pds_id_t                         mPDSId;

  /// CAREFUL!!! KeyInCtrlMsg can be a char[ 0 ]
  // This has to be the last field.
  // NEED: Check that the key is not larger then the
  // message buffer
  skv_key_value_in_ctrl_msg_t          mKeyValue;

  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "mFlags=" << mFlags
      << EndLogLine ;
    mFlags=(skv_cmd_remove_flags_t)htonl(mFlags) ;
    mKeyValue.EndianConvert() ;
  }
  void
  Init( int aNodeId,
        skv_client_conn_manager_if_t* aConnMgr,
        skv_pds_id_t* aPDSId,
        skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_cmd_remove_flags_t aFlags,
        int aKeySize,
        char* aKeyData )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mPDSId = *aPDSId;
    mFlags = aFlags;

    mKeyValue.mKeySize = aKeySize;

    if( aFlags & SKV_COMMAND_REMOVE_KEY_FITS_IN_CTL_MSG )
    {
      memcpy( mKeyValue.mData,
              aKeyData,
              aKeySize );

    }
    else
    {
      StrongAssertLogLine( 0 )
        << "skv_cmd_RIU_req_t::Init(): ERROR:: Keys that don't fit "
        << " into a control message are not yet supported."
        << EndLogLine;
    }

    mHdr.SetCmdLength( sizeof(skv_cmd_remove_req_t) + aKeySize );
  }
};

struct skv_cmd_bulk_insert_req_t
{
  skv_client_to_server_cmd_hdr_t       mHdr;

  skv_pds_id_t                         mPDSId;
  int                                  mBufferSize;
  it_rmr_context_t                     mBufferRMR;
  uint64_t                             mBuffer;

#ifdef SKV_BULK_LOAD_CHECKSUM
  uint64_t                             mBufferChecksum;
#endif

  void
  Init( int aNodeId,
        skv_client_conn_manager_if_t* aConnMgr,
        skv_pds_id_t* aPDSId,
        skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        char* aBuffer,
        int aBufferSize,
        it_lmr_handle_t aBufferLMR,
        // it_rmr_context_t               aBufferRMR,
        uint64_t aBufferChecksum )
  {
    AssertLogLine( sizeof( skv_cmd_bulk_insert_req_t ) < SKV_CONTROL_MESSAGE_SIZE )
      << "ERROR: "
      << " sizeof( skv_cmd_bulk_insert_req_t ): " << sizeof( skv_cmd_bulk_insert_req_t )
      << " SKV_CONTROL_MESSAGE_SIZE: " << SKV_CONTROL_MESSAGE_SIZE
      << EndLogLine;

#ifdef SKV_BULK_LOAD_CHECKSUM
    mBufferChecksum = aBufferChecksum;
    BegLogLine( 1 )
      // << "On Client mBufferChecksum: " << mBufferChecksum
      << "On Client aBufferChecksum: " << mBufferChecksum
      << EndLogLine;
#endif

    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mPDSId = *aPDSId;
    mBuffer = (uint64_t) ((uintptr_t) aBuffer);
    mBufferSize = aBufferSize;

    /**
     * Need to get the actual rmr context for a given end point (ep)
     * Since the rmr key is specific to the network device
     * The network device is disambiguated by the ep handle
     */
    it_ep_handle_t epHandle;
    skv_status_t pstatus = aConnMgr->GetEPHandle( aNodeId, &epHandle );
    AssertLogLine( pstatus == SKV_SUCCESS )
      << "ERROR: skv status: " << skv_status_to_string( pstatus )
      << EndLogLine;

    it_status_t status = itx_get_rmr_context_for_ep( epHandle, aBufferLMR, & mBufferRMR );

    AssertLogLine( status == IT_SUCCESS )
      << "ERROR: status: " << status
      << EndLogLine;

    mHdr.SetCmdLength( sizeof(skv_cmd_bulk_insert_req_t) );
  }
  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "Endian convert mBufferSize=" << mBufferSize
      << EndLogLine ;
    mBufferSize=htonl(mBufferSize) ;
  }
};

struct skv_cmd_retrieve_n_keys_req_t
{
  skv_client_to_server_cmd_hdr_t         mHdr;
  int                                    mIsKeyInCtrlMsg;

  skv_pds_id_t                           mPDSId;
  skv_cursor_flags_t                     mFlags;

  union
  {
    it_lmr_handle_t                       mKeysDataCacheLMR;

    // If we had the RDMA Read the RMR is all we would need
    // The rmr could both be used to RDMA Read the starting key from the client
    // in case it doesn't fit into the buffer. Could also be used
    // for the server to RDMA Write the list of keys to the client
    it_rmr_context_t                      mKeysDataCacheRMR;
  } mKeyDataCacheMemReg ;

  int                                     mKeysDataListMaxCount;
  uint64_t                                mKeysDataList;

  int                                     mStartingKeySize;
  char                                    mStartingKeyData[ 0 ];

  void
  Init( int aNodeId,
        skv_client_conn_manager_if_t* aConnMgr,
        skv_pds_id_t aPDSId,
        skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_cursor_flags_t aFlags,
        char* aStartingKeyBuffer,
        int aStartingKeyBufferSize,
        int aKeyFitsInBuff,
        it_lmr_handle_t aKeysDataCacheLMR,
        it_rmr_context_t aKeysDataCacheRMR,
        char* aCachedKeysBuff,
        int aMaxCachedKeysCount )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mPDSId = aPDSId;
    mFlags = aFlags;

    AssertLogLine( aKeyFitsInBuff == 1 )
      << "skv_cmd_retrieve_n_keys_req_t::Init():: ERROR:: "
      << " aKeyFitsInBuff: " << aKeyFitsInBuff
      << EndLogLine;

    mIsKeyInCtrlMsg = aKeyFitsInBuff;

    /**
     * Need to get the actual rmr context for a given end point (ep)
     * Since the rmr key is specific to the network device
     * The network device is disambiguated by the ep handle
     */
    it_ep_handle_t epHandle;
    skv_status_t pstatus = aConnMgr->GetEPHandle( aNodeId, &epHandle );
    AssertLogLine( pstatus == SKV_SUCCESS )
      << "ERROR: skv status: " << skv_status_to_string( pstatus )
      << EndLogLine;

    it_status_t status = itx_get_rmr_context_for_ep( epHandle,
                                                     aKeysDataCacheLMR,
                                                     & mKeyDataCacheMemReg.mKeysDataCacheRMR );

    AssertLogLine( status == IT_SUCCESS )
      << "ERROR: status: " << status
      << EndLogLine;

    AssertLogLine( aMaxCachedKeysCount <= SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE )
      << "skv_cmd_retrieve_n_keys_req_t::Init():: ERROR:: "
      << " aMaxCachedKeysCount: " << aMaxCachedKeysCount
      << " SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE: " << SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE
      << EndLogLine;

    mKeysDataList = (uint64_t) (uintptr_t) aCachedKeysBuff;

    mKeysDataListMaxCount = aMaxCachedKeysCount;

    mStartingKeySize = aStartingKeyBufferSize;

    memcpy( mStartingKeyData,
            aStartingKeyBuffer,
            mStartingKeySize );

    mHdr.SetCmdLength( sizeof(skv_cmd_retrieve_n_keys_req_t) + mStartingKeySize );
    return;
  }
  void EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "Endian convert mFlags=" << mFlags
      << " mKeysDataListMaxCount=" << mKeysDataListMaxCount
      << " mStartingKeySize=" << mStartingKeySize
      << " mKeysDataList=" << (void *) mKeysDataList
      << " mKeysDataCacheRMR=" << (void *) mKeyDataCacheMemReg.mKeysDataCacheRMR
      << EndLogLine ;
    mFlags=(skv_cursor_flags_t)htonl(mFlags) ;
    mKeysDataListMaxCount=htonl(mKeysDataListMaxCount) ;
    mStartingKeySize=htonl(mStartingKeySize) ;
    mKeysDataList=htobe64(mKeysDataList) ;
    mKeyDataCacheMemReg.mKeysDataCacheRMR=htobe64(mKeyDataCacheMemReg.mKeysDataCacheRMR) ;
  }
};

struct skv_cmd_active_bcast_req_t
{
  skv_client_to_server_cmd_hdr_t       mHdr;
  skv_c2s_active_broadcast_func_type_t mFuncType;
  skv_rmr_triplet_t                    mBufferRep;

  void
  Init( int aNodeId,
        skv_client_conn_manager_if_t* aConnMgr,
        skv_command_type_t aCmdType,
        skv_server_event_type_t aEventType,
        skv_client_ccb_t* aCmdCtrlBlk,
        skv_c2s_active_broadcast_func_type_t aFuncType,
        int aBuffSize,
        char* aBuff,
        it_pz_handle_t aPZHdl,
        it_lmr_handle_t* aLmrHandle )
  {
    mHdr.Init( aEventType, aCmdCtrlBlk, aCmdType );

    mFuncType = aFuncType;

    it_lmr_handle_t lmrHandle;
    it_rmr_context_t rmrHandle;

    it_mem_priv_t privs = (it_mem_priv_t) (IT_PRIV_LOCAL | IT_PRIV_REMOTE);
    it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

    it_status_t status = it_lmr_create( aPZHdl,
                                        aBuff,
                                        NULL,
                                        aBuffSize,
                                        IT_ADDR_MODE_ABSOLUTE,
                                        privs,
                                        lmr_flags,
                                        0,
                                        &lmrHandle,
                                        &rmrHandle );

    StrongAssertLogLine( status == IT_SUCCESS )
      << "skv_cmd_retrieve_req_t::Init():: "
      << " status: " << status
      << EndLogLine;

    *aLmrHandle = lmrHandle;

    /**
     * Need to get the actual rmr context for a given end point (ep)
     * Since the rmr key is specific to the network device
     * The network device is disambiguated by the ep handle
     */
    it_ep_handle_t epHandle;
    skv_status_t pstatus = aConnMgr->GetEPHandle( aNodeId, &epHandle );
    AssertLogLine( pstatus == SKV_SUCCESS )
      << "ERROR: skv status: " << skv_status_to_string( pstatus )
      << EndLogLine;

    status = itx_get_rmr_context_for_ep( epHandle, lmrHandle, &rmrHandle );

    AssertLogLine( status == IT_SUCCESS )
      << "ERROR: status: " << status
      << EndLogLine;

    mBufferRep.Init( rmrHandle, aBuff, aBuffSize );
    mHdr.SetCmdLength( sizeof(skv_cmd_active_bcast_req_t) );

    return;
  }
};
/***************************************************/

/***************************************************
 * Structures for the SERVER to CLIENT communication
 ***************************************************/
/**************************************************************************/

/**************************************************************************
 * Open Response
 **************************************************************************/
struct skv_cmd_open_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_status_t                       mStatus;

  skv_pds_id_t                       mPDSId;
  void EndianConvert(void)
  {
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    // pdsid shouldn't get endian-converted! Client just puts it in as the server sent it
    mStatus=skv_status_byte_swap( mStatus );
    mHdr.EndianConvert() ;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_cmd_open_resp_t& A )
{
  os << "skv_cmd_open_resp_t: ["
     << A.mHdr << ' '
     << (int) A.mStatus << ' '
     << A.mPDSId
     << " ]";

  return(os);
}
/**************************************************************************/

/**************************************************************************
 * PDS Control Response
 **************************************************************************/
struct skv_cmd_pdscntl_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_status_t                       mStatus;

  skv_pds_attr_t                     mPDSAttr;

  void EndianConvert(void)
  {
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    mStatus=skv_status_byte_swap( mStatus );
    mHdr.EndianConvert() ;
    mPDSAttr.EndianConvert() ;
  }
};
/**************************************************************************/

/**************************************************************************
 * Retrieve Dist Response
 **************************************************************************/
struct skv_cmd_retrieve_dist_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_status_t                       mStatus;

  skv_distribution_t                 mDist;
  void EndianConvert(void)
  {
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    mStatus=skv_status_byte_swap( mStatus );
    mHdr.EndianConvert() ;
    mDist.EndianConvert() ;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_cmd_retrieve_dist_resp_t& A )
{
  os << "skv_cmd_retrieve_dist_resp_t: ["
     << A.mHdr << ' '
     << (int) A.mStatus << ' '
     << A.mDist
     << " ]";

  return (os);
}
/**************************************************************************/

struct skv_cmd_err_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;
  uint64_t                           mChecksum;

  void EndianConvert(void)
  {
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    mStatus = skv_status_byte_swap( mStatus );
    mHdr.EndianConvert();
    mChecksum = be64toh( mChecksum );
  }
};

struct skv_cmd_insert_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;
};

struct skv_cmd_insert_cmpl_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;
  void EndianConvert(void)
  {
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    mStatus=skv_status_byte_swap( mStatus );
    mHdr.EndianConvert() ;
  }
};


/******************
 *    Remove
 ******************/
struct skv_cmd_remove_cmpl_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;
  void EndianConvert(void)
  {
    BegLogLine(SKV_SERVER_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    mStatus=skv_status_byte_swap( mStatus );
    mHdr.EndianConvert() ;
  }
};

/******************
 *    Retrieve
 ******************/
struct skv_cmd_retrieve_value_rdma_write_ack_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;

  /// CAREFUL!!! KeyInCtrlMsg can be a char[ 0 ]
  // This has to be the last field.
  // NEED: Check that the key is not larger then the
  // message buffer
  skv_value_in_ctrl_msg_t            mValue;
  void
  EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << EndLogLine ;
    mStatus=skv_status_byte_swap( mStatus );
    mValue.EndianConvert() ;
    mHdr.EndianConvert() ;
  }
};
/******************/

/******************
 * Retrieve N Keys
 ******************/
struct skv_cmd_retrieve_n_keys_rdma_write_ack_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;
  skv_status_t                       mStatus;

  int                                mCachedKeysCount;
  void EndianConvert(void)
  {
    BegLogLine(SKV_CLIENT_ENDIAN_LOG)
      << "mStatus=" << mStatus
      << " mCachedKeysCount=" << mCachedKeysCount
      << EndLogLine ;
    mStatus=skv_status_byte_swap( mStatus );
    mCachedKeysCount=ntohl(mCachedKeysCount);
    mHdr.EndianConvert() ;
  }
};
/************/

/****************************
 * Active Bcast Response
 ***************************/
struct skv_cmd_active_bcast_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;

  union
  {
    uint64_t                         mServerHandle;
  };

};
/***************************/

/****************************
 * Cursor Prefetch Response
 ***************************/
struct skv_cmd_cursor_prefetch_resp_t
{
  skv_server_to_client_cmd_hdr_t     mHdr;

  skv_status_t                       mStatus;

  int                                mFilledBufferSize;

  int                                mReqSeqNo;
  int                                mRespSrc;
};
/***************************/

/***************************************************/

#endif
