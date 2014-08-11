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

#ifndef __SKV_CLIENT_CURSOR_MANAGER_IF_HPP__
#define __SKV_CLIENT_CURSOR_MANAGER_IF_HPP__

#ifndef SKV_CLIENT_CURSOR_LOG 
#define SKV_CLIENT_CURSOR_LOG ( 0 | SKV_LOGGING_ALL )
#endif

struct skv_client_cursor_control_block_t
{
  skv_pds_id_t                  mPdsId;
  int                            mCurrentNodeId;

  it_lmr_handle_t                mKeysDataLMRHdl;
  it_rmr_context_t               mKeysDataRMRHdl;

#define SKV_CACHED_KEYS_BUFFER_SIZE (SKV_CLIENT_MAX_CURSOR_KEYS_TO_CACHE * SKV_KEY_LIMIT)
  char                           mCachedKeys[ SKV_CACHED_KEYS_BUFFER_SIZE ];
  int                            mCachedKeysCount;

  int                            mCurrentCachedKeyIdx;

  char*                          mCurrentCachedKey;
  char*                          mPrevCachedKey;

  void
  ResetCurrentCachedState()
  {
    mCurrentCachedKeyIdx = 0;
    mCachedKeysCount = 0;
    mCurrentCachedKey = mCachedKeys;
    mPrevCachedKey = NULL;
  }

  void
  SetNodeId( int aNodeId )
  {
    mCurrentNodeId = aNodeId;
  }

  int
  GetNodeId()
  {
    return mCurrentNodeId;
  }

  void
  Init( it_pz_handle_t  aPZ_Hdl, 
        int             aNodeId, 
        skv_pds_id_t*  aPdsId )
  {
    SetNodeId( aNodeId );

    mPdsId          = *aPdsId;
    mCachedKeysCount = 0;
    mCurrentCachedKeyIdx = 0;

    mCurrentCachedKey = & mCachedKeys[ 0 ];    

    it_mem_priv_t privs     = (it_mem_priv_t) ( IT_PRIV_LOCAL | IT_PRIV_REMOTE );
    it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

    int SizeOfKeyDataBuffer = sizeof( char ) * SKV_CACHED_KEYS_BUFFER_SIZE;

    it_status_t status = it_lmr_create( aPZ_Hdl, 
                                        & mCachedKeys[ 0 ],
                                        NULL,
                                        SizeOfKeyDataBuffer,
                                        IT_ADDR_MODE_ABSOLUTE,
                                        privs,
                                        lmr_flags,
                                        0,
                                        & mKeysDataLMRHdl,
                                        & mKeysDataRMRHdl );

    BegLogLine( SKV_CLIENT_CURSOR_LOG )
      << "skv_client_cursor_control_block_t::Init():: Leaving..."
      << " & mCachedKeys[ 0 ]: " << (void *) & mCachedKeys[ 0 ]
      << " mKeysDataLMRHdl: " << (void *) mKeysDataLMRHdl
      << EndLogLine;

    StrongAssertLogLine( status == IT_SUCCESS )
      << "skv_client_cursor_control_block_t::Init():: ERROR:: "
      << " status: " << status
      << EndLogLine;
  }

  void
  Finalize()
  {
    BegLogLine( SKV_CLIENT_CURSOR_LOG )
      << "skv_client_cursor_control_block_t::Finalize():: Entering..."
      << " mKeysDataLMRHdl: " << (void *) mKeysDataLMRHdl
      << EndLogLine;

    it_status_t status = it_lmr_free( mKeysDataLMRHdl );

    StrongAssertLogLine( status == IT_SUCCESS )
      << "skv_client_cursor_control_block_t::Finalize():: ERROR:: "
      << " status: " << status
      << EndLogLine;    

    BegLogLine( SKV_CLIENT_CURSOR_LOG )
      << "skv_client_cursor_control_block_t::Finalize():: Leaving..."
      << EndLogLine;
  }
};

typedef skv_client_cursor_control_block_t* skv_client_cursor_handle_t;

class skv_client_cursor_manager_if_t
{
public:
  skv_client_cursor_manager_if_t() {}
  ~skv_client_cursor_manager_if_t() {}

  void
  InitCursorHdl( it_pz_handle_t               aPZ_Hdl,   
                 int                          aNodeId, 
                 skv_pds_id_t*               aPdsId,
                 skv_client_cursor_handle_t* aCursorHdl )
  {

    *aCursorHdl = (skv_client_cursor_handle_t) malloc( sizeof( skv_client_cursor_control_block_t ) );

    StrongAssertLogLine( *aCursorHdl != NULL )
      << "skv_client_cursor_manager_if::InitCursorId():: ERROR:: "
      << " *aCursorHdl != NULL"
      << EndLogLine;

    skv_client_cursor_control_block_t* CursorCCB = *aCursorHdl;
    CursorCCB->Init( aPZ_Hdl, aNodeId, aPdsId );        
  } 

  void 
  FinalizeCursorHdl( skv_client_cursor_handle_t aCursorHdl )
  {
    if( aCursorHdl != NULL )      
      {
        aCursorHdl->Finalize();
        free( aCursorHdl );
      }
  }
};

#endif
