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

#ifndef __SKV_BULK_INSERTER_MANAGER_IF_HPP__
#define __SKV_BULK_INSERTER_MANAGER_IF_HPP__

#define SKV_CLIENT_BULK_INSERT_BUFFER_SIZE ( 64 * 1024 )
#define SKV_CLIENT_BULK_INSERT_BUFFERS_PER_NODE ( 4 )
// #define SKV_CLIENT_BULK_INSERT_BUFFER_SIZE ( 512 * 1024 )
// #define SKV_CLIENT_BULK_INSERT_BUFFERS_PER_NODE ( 2 )

#ifndef SKV_BULK_INSERTER_MANAGER_IF_LOG
#define SKV_BULK_INSERTER_MANAGER_IF_LOG ( 0 | SKV_LOGGING_ALL )
#endif

typedef enum
  {
    SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_PENDING = 0x0001,
    SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_READY   = 0x0002
  } skv_client_bulk_inserter_buffer_state_type_t;

struct skv_client_bulk_insert_buffer_t
{
  int                                           mBufferSize;
  int                                           mCurrentIndex;
  it_rmr_context_t                              mBufferRMR;
  it_lmr_handle_t                               mBufferLMR;

  skv_client_cmd_hdl_t                         mCommandHandle;

  skv_client_bulk_insert_buffer_t*             mNext;
  skv_client_bulk_insert_buffer_t*             mPrev;

  skv_client_bulk_inserter_buffer_state_type_t mState;

  char                                          mBufferData[ 0 ];

  void
  Init( int                               aBufferSize,
        it_lmr_handle_t                   aBufferLMR,
        it_rmr_context_t                  aBufferRMR,
        skv_client_bulk_insert_buffer_t *aNext,
        skv_client_bulk_insert_buffer_t *aPrev )
  {
    mState        = SKV_CLIENT_BULK_INSERTER_BUFFER_STATE_READY;
    mBufferSize   = aBufferSize;
    mBufferLMR    = aBufferLMR;
    mBufferRMR    = aBufferRMR;
    mNext         = aNext;
    mPrev 	  = aPrev;
    mCurrentIndex = 0;

    BegLogLine( SKV_BULK_INSERTER_MANAGER_IF_LOG )
      << "skv_client_bulk_insert_buffer_t::Init(): "
      << " mState: " << mState
      << " mBufferSize: " << mBufferSize
      << " mCurrentIndex: " << mCurrentIndex
      << " mNext: " << (void *) mNext
      << " mPrev: " << (void *) mPrev
      << " mBufferRMR: " << (void *) aBufferRMR
      << " mBufferLMR: " << (void *) aBufferLMR
      << EndLogLine;
  }
};

struct skv_client_bulk_insert_buffer_list_t
{
  skv_client_bulk_insert_buffer_t*  mHead;

  skv_client_bulk_insert_buffer_t*  mCurrentBuffer;

  int                                mBufferCount;

  void
  Init( char*            aWorkingBuffer, 
        int              aSizePerBuffer, 
        int              aDataSizePerBuffer, 
        int              aBufferCount,
        it_lmr_handle_t  aBufferLMR,
        it_rmr_context_t aBufferRMR )
  {
    BegLogLine( SKV_BULK_INSERTER_MANAGER_IF_LOG )
      << "skv_client_bulk_insert_buffer_list_t::Init(): "
      << " aWorkingBuffer: " << (void *) aWorkingBuffer
      << " aSizePerBuffer: " << aSizePerBuffer
      << " aDataSizePerBuffer: " << aDataSizePerBuffer
      << " aBufferCount: " << aBufferCount
      << " aBufferRMR: " << aBufferRMR
      << EndLogLine;

    AssertLogLine( aBufferCount > 0 )
      << "skv_client_bulk_insert_buffer_list_t::Init(): ERROR: "
      << " aBufferCount: " << aBufferCount
      << EndLogLine;

    mBufferCount = aBufferCount;

    mHead = (skv_client_bulk_insert_buffer_t *) aWorkingBuffer;
    mCurrentBuffer = mHead;

    skv_client_bulk_insert_buffer_t * Buffer  = (skv_client_bulk_insert_buffer_t *) (aWorkingBuffer);

    Buffer->Init( aDataSizePerBuffer,
                  aBufferLMR,
                  aBufferRMR,
                  ( aBufferCount == 1 ) ? NULL : (skv_client_bulk_insert_buffer_t *) (aWorkingBuffer + aSizePerBuffer),
                  NULL );

    for( int i = 1; i < aBufferCount; i++ )
      {
        Buffer = (skv_client_bulk_insert_buffer_t *) (aWorkingBuffer + i * aSizePerBuffer);

        if( i == (aBufferCount-1) )
          {
            Buffer->Init( aDataSizePerBuffer,
                          aBufferLMR,
                          aBufferRMR,
                          NULL,
                          (skv_client_bulk_insert_buffer_t *) (aWorkingBuffer + (i-1) * aSizePerBuffer) );

          }
        else
          {
            Buffer->Init( aDataSizePerBuffer,
                          aBufferLMR,
                          aBufferRMR,
                          (skv_client_bulk_insert_buffer_t *) (aWorkingBuffer + (i+1) * aSizePerBuffer),
                          (skv_client_bulk_insert_buffer_t *) (aWorkingBuffer + (i-1) * aSizePerBuffer) );
          }
      }
  }

  void
  Finalize()
  {
    mHead          = NULL;
    mCurrentBuffer = NULL;
  }  
};

struct skv_client_bulk_insert_control_block_t
{
  skv_pds_id_t                             mPDSId;

  skv_bulk_inserter_flags_t                mFlags;

  int                                       mServerNodeCount;

  skv_client_bulk_insert_buffer_list_t*    mBufferListPerServer;

  int                                       mWorkingBufferSize;
  char*                                     mWorkingBuffer;
  it_lmr_handle_t                           mWorkingBuffer_LMRHdl;
  it_rmr_context_t                          mWorkingBuffer_RMRHdl;

public:

  void
  Init( it_pz_handle_t                           aPZ_Hdl, 
        skv_pds_id_t*                           aPDSId,
        skv_bulk_inserter_flags_t               aFlags,
        int                                      aServerNodeCount )
  {
    mFlags = aFlags;
    mPDSId = *aPDSId;
    mServerNodeCount = aServerNodeCount;

    mBufferListPerServer = (skv_client_bulk_insert_buffer_list_t *) 
      malloc( sizeof( skv_client_bulk_insert_buffer_list_t ) * mServerNodeCount );

    StrongAssertLogLine( mBufferListPerServer )
      << "skv_client_bulk_insert_control_block_t::Init(): ERROR: "
      << EndLogLine;

    int SizeOfBufferMetadata = sizeof( skv_client_bulk_insert_buffer_t );
    // int SizeOfBufferData     = SKV_CLIENT_BULK_INSERT_BUFFER_SIZE;
    // double MultFactor        = 4.0 / aServerNodeCount;
    // double MultFactor        = 0.25;
    //int SizeOfBufferData     = (int) ( MultFactor * SKV_CLIENT_BULK_INSERT_BUFFER_SIZE );
    int SizeOfBufferData     = SKV_CLIENT_BULK_INSERT_BUFFER_SIZE;
    int SizePerBuffer        = SizeOfBufferMetadata + SizeOfBufferData;

    int BuffersPerNodeCount  = SKV_CLIENT_BULK_INSERT_BUFFERS_PER_NODE;
    int SizePerNode          = SizePerBuffer * BuffersPerNodeCount;

    mWorkingBufferSize       = aServerNodeCount * SizePerNode;
    mWorkingBuffer           = (char *) malloc( mWorkingBufferSize );

    StrongAssertLogLine( mWorkingBuffer )
      << "skv_client_bulk_insert_control_block_t::Init(): ERROR:: "
      << " SizeOfBufferMetadata: " << SizeOfBufferMetadata
      << " SizeOfBufferData: " << SizeOfBufferData
      << " SizePerBuffer: " << SizePerBuffer
      << " BuffersPerNodeCount: " << BuffersPerNodeCount
      << " SizePerNode: " << SizePerNode
      << " mWorkingBufferSize: " << mWorkingBufferSize
      << EndLogLine;

    it_mem_priv_t privs     = (it_mem_priv_t) ( IT_PRIV_LOCAL | IT_PRIV_REMOTE );
    it_lmr_flag_t lmr_flags = IT_LMR_FLAG_NON_SHAREABLE;

    it_status_t status = it_lmr_create( aPZ_Hdl, 
                                        mWorkingBuffer,
                                        NULL,
                                        mWorkingBufferSize,
                                        IT_ADDR_MODE_ABSOLUTE,
                                        privs,
                                        lmr_flags,
                                        0,
                                        & mWorkingBuffer_LMRHdl,
                                        & mWorkingBuffer_RMRHdl );

    StrongAssertLogLine( status == IT_SUCCESS )
      << "skv_client_bulk_insert_control_block_t::Init(): ERROR:: "
      << " status: " << status
      << EndLogLine;

    for( int i = 0; i < mServerNodeCount; i++ )
      {
        int WorkingBufferIndex = i * SizePerNode;

        mBufferListPerServer[ i ].Init( & mWorkingBuffer[ WorkingBufferIndex ],
                                        SizePerBuffer,
                                        SizeOfBufferData,
                                        BuffersPerNodeCount,
                                        mWorkingBuffer_LMRHdl,
                                        mWorkingBuffer_RMRHdl );
      }
  }

  void
  Finalize()
  {
    BegLogLine( SKV_BULK_INSERTER_MANAGER_IF_LOG )
      << "skv_client_bulk_insert_control_block_t::Finalize(): Entering "
      << EndLogLine;

    for( int i = 0; i < mServerNodeCount; i++ )
      {
        mBufferListPerServer[ i ].Finalize();
      }

    free( mBufferListPerServer );
    mBufferListPerServer = NULL;

    it_lmr_free( mWorkingBuffer_LMRHdl );

    free( mWorkingBuffer );
    mWorkingBuffer = NULL;
  }
};

typedef skv_client_bulk_insert_control_block_t* skv_client_bulk_inserter_hdl_t;
#endif
