/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_local_kv_rocksdb_access.hpp
 *
 *  Created on: Jun 24, 2014
 *      Author: lschneid
 */

#ifndef SKV_LOCAL_KV_ROCKSDB_ACCESS_HPP_
#define SKV_LOCAL_KV_ROCKSDB_ACCESS_HPP_

#define SKV_LOCAL_KV_ROCKSDB_MIN_GET_SIZE ( 65536 )

typedef basic_string< char, char_traits<char>, skv_rdma_buffer_allocator_t<char>> skv_rdma_string;

#include <rocksdb/table.h>

class skv_local_kv_rocksdb_access_t {

  rocksdb::DB *mPDSDBHndl;   // handle for server PDS to maintain existing user PDSs
  rocksdb::DB *mDataDBHndl;  // handle for the user PDSs
  rocksdb::ReadOptions mPDSrdopts = rocksdb::ReadOptions();
  rocksdb::ReadOptions mIteratorOpts = rocksdb::ReadOptions();
  rocksdb::WriteOptions mPDSwropts = rocksdb::WriteOptions();
  rocksdb::WriteOptions mDeleteOpts = rocksdb::WriteOptions();

public:
  skv_local_kv_rocksdb_access_t() : mPDSDBHndl(), mDataDBHndl()
  {
    mDataDBHndl = NULL;
    mPDSDBHndl = NULL;
  }
  ~skv_local_kv_rocksdb_access_t() {}

  rocksdb::DB* GetPDSDBHndl()
  {
    return mPDSDBHndl;
  }
  rocksdb::DB* GetDataDBHndl()
  {
    return mDataDBHndl;
  }

  skv_status_t Init( const std::string &aPDSBaseName, int aRank )
  {
    skv_status_t status = SKV_ERRNO_UNSPECIFIED_ERROR;

    rocksdb::Options dbopts = rocksdb::Options();
    dbopts.create_if_missing = true;
    dbopts.use_adaptive_mutex = true;
    dbopts.write_buffer_size = 16 * 1048576;
    dbopts.compression = rocksdb::kNoCompression;

    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_size = 8192;
    dbopts.table_factory.reset(NewBlockBasedTableFactory(table_opts));

    dbopts.max_background_compactions = SKV_LOCAL_KV_WORKER_POOL_SIZE;
    // dbopts.IncreaseParallelism( SKV_LOCAL_KV_WORKER_POOL_SIZE );
    dbopts.env->SetBackgroundThreads( SKV_LOCAL_KV_WORKER_POOL_SIZE, rocksdb::Env::Priority::HIGH );

    // open the node's PDS table for later retrieval of PDS metadata
    std::string PDSName = aPDSBaseName;
    PDSName.append( ".PDSentries.");
    PDSName.append( std::to_string( aRank ) );

    if ( ! rocksdb::DB::Open(dbopts,
                             PDSName,
                             &mPDSDBHndl).ok() )
    {
      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "skv_local_kv_request_t: ERROR opening rocksDB"
        << EndLogLine;
      status = SKV_ERRNO_PDS_DOES_NOT_EXIST;
    }

    // open the node's data table
    PDSName = aPDSBaseName;
    PDSName.append( ".PDSdata.");
    PDSName.append( std::to_string( aRank ) );

    if ( ! rocksdb::DB::Open(dbopts,
                             PDSName,
                             &mDataDBHndl).ok() )
    {
      BegLogLine( SKV_LOCAL_KV_BACKEND_LOG )
        << "skv_local_kv_request_t: ERROR opening rocksDB"
        << EndLogLine;
      status = SKV_ERRNO_PDS_DOES_NOT_EXIST;
    }

    mDeleteOpts.sync = false;

    return status;
  }

  skv_status_t Exit()
  {
    if( mPDSDBHndl )
      delete mPDSDBHndl;
    if( mDataDBHndl )
      delete mDataDBHndl;
    return SKV_SUCCESS;
  }


  rocksdb::Status GetPDS( const rocksdb::Slice &aKey, std::string *aValue )
  {
    return mPDSDBHndl->Get( mPDSrdopts, aKey, aValue );
  }
  rocksdb::Status DeletePDS( const rocksdb::Slice &aPDSName, const rocksdb::Slice &aPDSId )
  {
    rocksdb::Status rs = mPDSDBHndl->Delete( mPDSwropts, aPDSName );
    if( rs.ok() )
      rs = mPDSDBHndl->Delete( mPDSwropts, aPDSId );
    return rs;
  }
  rocksdb::Status CreatePDS( const rocksdb::Slice &aPDSName,
                             const rocksdb::Slice &aPDSId,
                             const rocksdb::Slice &aPDSData )
  {
    rocksdb::Status rs = mPDSDBHndl->Put( mPDSwropts,
                                          aPDSName,
                                          aPDSData );

    if( rs.ok() )
    {
      rs = mPDSDBHndl->Put( mPDSwropts,
                            aPDSId,
                            aPDSData );
    }
    return rs;
  }
  rocksdb::Status ClosePDS( const rocksdb::Slice &aPDSId )
  {
    // we currently ignore the pdsid because we don't use separate file per PDS. This might change later!!

    rocksdb::FlushOptions flOpts = rocksdb::FlushOptions();
    rocksdb::Status rs = mPDSDBHndl->Flush( flOpts );
    if( rs.ok() )
      rs = mDataDBHndl->Flush( flOpts );
    return rs;
  }
  bool DataKeyMayExist( const rocksdb::Slice &aKey )
  {
    bool getval = false;
    std::string value;
    return mDataDBHndl->KeyMayExist( mPDSrdopts, aKey, &value, &getval );
  }
  /* since rocksdb doesn't have the option to retrieve the value size,
   * we'll start with a min-size and adjust after the first get failed
   */
  rocksdb::Status GetData( const rocksdb::Slice &aKey, skv_rdma_string *aValue, size_t aExpSize )
  {
    rocksdb::Status rs;
    const char *oldp = aValue->data();
    size_t getSize = SKV_LOCAL_KV_ROCKSDB_MIN_GET_SIZE > aExpSize ? SKV_LOCAL_KV_ROCKSDB_MIN_GET_SIZE : aExpSize + (aExpSize>>2);
    do
    {
      aValue->resize( getSize );
      oldp = aValue->data();
      rs = mDataDBHndl->Get( mPDSrdopts, aKey, reinterpret_cast<std::string*>(aValue) );

#if (SKV_LOCAL_KV_ROCKSDB_PROCESSING_LOG != 0)
      if( oldp != aValue->data() )
        BegLogLine( 1 )
          << "skv_local_kv_rocksdb_access_t: need to resize value to " << aValue->length()
          << EndLogLine;
#endif

    } while( oldp != aValue->data() );

    return rs;
  }
  rocksdb::Status LookupData( const rocksdb::Slice &aKey, std::string *aValue )
  {
    rocksdb::Status rs = mDataDBHndl->Get( mPDSrdopts, aKey, aValue );
    return rs;
  }
  rocksdb::Status PutData( const rocksdb::Slice &aKey, const rocksdb::Slice &aValue )
  {
    return mDataDBHndl->Put( mPDSwropts, aKey, aValue );
  }
  rocksdb::Status DeleteData( const rocksdb::Slice &aKey )
  {
    return mDataDBHndl->Delete( mDeleteOpts, aKey );
  }
  rocksdb::Iterator* NewIterator( const rocksdb::Slice *aStartKey, const rocksdb::Slice *aPDSId )
  {
#if (ROCKSDB_MAJOR < 3 )
    mIteratorOpts.prefix = NULL;
#endif
    mIteratorOpts.tailing = true;

    rocksdb::Iterator *iter = mDataDBHndl->NewIterator( mIteratorOpts );
    if( aStartKey != NULL )
    {
      iter->Seek( *aStartKey );
    }
    else
    {
      iter->SeekToFirst();
    }
    return iter;
  }
};



#endif /* SKV_LOCAL_KV_ROCKSDB_ACCESS_HPP_ */
