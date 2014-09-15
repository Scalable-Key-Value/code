/*
 * skv_rdma_buffer_allocator.hpp
 *
 *  Created on: Jul 2, 2014
 *      Author: lschneid
 */

#ifndef SKV_RDMA_BUFFER_ALLOCATOR_HPP_
#define SKV_RDMA_BUFFER_ALLOCATOR_HPP_

#ifndef SKV_RDMA_BUFFER_ALLOCATOR_LOG
#define SKV_RDMA_BUFFER_ALLOCATOR_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#include <sstream>
#include <unordered_map>

class skv_thread_id_map_t
{
  static skv_thread_id_map_t *mThreadMap;
  std::unordered_map< std::thread::id, skv_local_kv_rdma_data_buffer_t* > mRDBMap;

  skv_thread_id_map_t() : mRDBMap() {}

public:
  static skv_thread_id_map_t* GetThreadIdMap( )
  {
    if( !mThreadMap )
    {
      mThreadMap = new skv_thread_id_map_t;
    }
    return mThreadMap;
  }

  skv_local_kv_rdma_data_buffer_t* GetRDB( const std::thread::id &aID )
  {
#if( SKV_RDMA_BUFFER_ALLOCATOR_LOG != 0 )
    std::stringstream s;
    s << std::this_thread::get_id();

    BegLogLine( SKV_RDMA_BUFFER_ALLOCATOR_LOG )
      << "skv_thread_id_map_t: retrieving rdb for thread_id: " << s
      << EndLogLine;
#endif
    return mRDBMap.find( aID )->second;
  }
  skv_status_t InsertRDB( skv_local_kv_rdma_data_buffer_t *aRDB )
  {
#if( SKV_RDMA_BUFFER_ALLOCATOR_LOG != 0 )
    std::stringstream s;
    s << std::this_thread::get_id();

    BegLogLine( SKV_RDMA_BUFFER_ALLOCATOR_LOG )
      << "skv_thread_id_map_t: inserting rdb@" << (void*)aRDB
      << " for thread_id: " << s
      << EndLogLine;
#endif
    std::pair<std::thread::id, skv_local_kv_rdma_data_buffer_t*> entry ( std::this_thread::get_id(), aRDB );
    if( mRDBMap.insert( entry ).second == true )
      return SKV_SUCCESS;
    else
      return SKV_ERRNO_NOT_DONE;
  }
  skv_status_t RemoveRDB( )
  {
    if( mRDBMap.erase( std::this_thread::get_id() ) != 0 )
      return SKV_SUCCESS;
    else
      return SKV_ERRNO_KEY_NOT_FOUND;
  }
  skv_status_t ResetRDB()
  {
    mRDBMap.clear();
    return SKV_SUCCESS;
  }
};

template< class skv_rdma_content_t >
class skv_rdma_buffer_allocator_t
{
public:
  typedef skv_rdma_content_t value_type;
  typedef skv_rdma_content_t* pointer;
  typedef skv_rdma_content_t& reference;
  typedef const skv_rdma_content_t* const_pointer;
  typedef const skv_rdma_content_t& const_reference;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;

  skv_rdma_buffer_allocator_t() throw() {};
  skv_rdma_buffer_allocator_t(const skv_rdma_buffer_allocator_t<skv_rdma_content_t>& alloc) throw() {};
  template <class U> skv_rdma_buffer_allocator_t(const skv_rdma_buffer_allocator_t<U>& alloc) throw() {};
  // references and pointers
  const_pointer address( const_reference aRef ) const
  {
    return &aRef;
  }

  pointer address( reference aRef ) const
  {
    return &aRef;
  }

  // initialize+construct and destroy
  void construct( pointer aPtr, const_reference aValue )
  {
    new( aPtr ) value_type( aValue );
  }

  void destroy( pointer aPtr )
  {
    aPtr->~value_type();
  }

  // max number of elements to allocate
  size_type max_size() const throw()
  {
    skv_thread_id_map_t *tm = skv_thread_id_map_t::GetThreadIdMap();
    skv_local_kv_rdma_data_buffer_t *RDB = tm->GetRDB( std::this_thread::get_id() );
    return RDB->GetAllocSize();
  }

  // enable typedef ...
  template< class ITEM >
  struct rebind
  {
    typedef skv_rdma_buffer_allocator_t< ITEM > other;
  };

  // allocation and deallocation
  pointer allocate( size_type aCount, const void* aHint=0 )
  {
    pointer retval = NULL;
    skv_thread_id_map_t *tm = skv_thread_id_map_t::GetThreadIdMap();
    skv_local_kv_rdma_data_buffer_t *RDB = tm->GetRDB( std::this_thread::get_id() );
    RDB->AcquireDataAreaPtr( aCount, &retval);
    return retval;
  }

  void deallocate( pointer aPtr, size_type aCount )
  {
    skv_thread_id_map_t *tm = skv_thread_id_map_t::GetThreadIdMap();
    skv_local_kv_rdma_data_buffer_t *RDB = tm->GetRDB( std::this_thread::get_id() );
    RDB->ReleaseDataAreaPtr( aCount, aPtr );
  }
};

template<class skv_rdma_content_t>
bool operator==(const skv_rdma_buffer_allocator_t< skv_rdma_content_t>& a,
                const skv_rdma_buffer_allocator_t< skv_rdma_content_t>& b) throw()
{
  return ((void*)&a == (void*)&b);
}

template<class skv_rdma_content_t>
bool operator!=(const skv_rdma_buffer_allocator_t< skv_rdma_content_t>& a,
                const skv_rdma_buffer_allocator_t< skv_rdma_content_t>& b) throw()
{
  return ((void*)&a != (void*)&b);
}

#endif /* SKV_RDMA_BUFFER_ALLOCATOR_HPP_ */
