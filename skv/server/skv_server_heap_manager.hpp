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

#ifndef __SKV_SERVER_HEAP_MANAGER_HPP__
#define __SKV_SERVER_HEAP_MANAGER_HPP__

#ifndef SKV_CLIENT_UNI
#include <mpi.h>
#endif

#include <limits>
#include <iostream>
#include <dlmalloc.h>
#include <sys/mman.h>
#include <unistd.h>
#include <math.h>

#ifndef SKV_SERVER_HEAP_MANAGER_LOG
#define SKV_SERVER_HEAP_MANAGER_LOG ( 0 )
#endif

#define PERSISTENT_FILEPATH_MAX_SIZE            512
// #define PERSISTENT_FILENAME                     "beef_slab"
//#define PERSISTENT_FILE_LOCAL_PATH              "/tmp/beef_slab"
#define PERSISTENT_MAGIC_NUMBER                 0xfaceb0b1
#define IONODE_IP                               "10.255.255.254"
#define MY_HOSTNAME_SIZE 128

//#define PERSISTENT_IMAGE_MAX_LEN                ( 2u * 1024u * 1024u * 1024u )
#define PERSISTENT_IMAGE_MAX_LEN                ( 3u * 1024u * 1024u * 1024u )
//#define PERSISTENT_IMAGE_MAX_LEN                ( 1024u * 1024u * 1024u )
//#define PERSISTENT_IMAGE_MAX_LEN                ( 1724u * 1024u * 1024u )

static
int
read_from_file( int sock, char * buff, int len )
{
  int BytesRead = 0;
  for( ; BytesRead < len; )
  {
    int read_rc = read( sock,
                        (((char *) buff) + BytesRead),
                        len - BytesRead );
    if( read_rc < 0 )
    {
      StrongAssertLogLine( 0 )
        << "read_from_file:: ERROR:: "
        << "failed to write to file: " << sock
        << " errno: " << errno
        << EndLogLine;
    }
    else if( read_rc == 0 )
      return 0;

    BytesRead += read_rc;
  }

  return BytesRead;
}

struct skv_server_persistance_heap_hdr_t
{
  void*             mDataMap;

  void*             mPDSNameTable;
  void*             mPDSIdTable;
  int               mLocalPDSCount;

  void*             mMmapAddr;
  unsigned long     mMagicNum;
  unsigned long     mNodeId;

  unsigned long long mRowDataChecksum;

  char*              mMspaceBase;
  unsigned long      mMspaceLen;
  mspace             mMspace;
};

typedef enum
{
  SKV_PERSISTANCE_FLAG_INIT    = 0x0001,
  SKV_PERSISTANCE_FLAG_RESTART = 0x0002
} skv_persistance_flag_t;

class skv_server_heap_manager_t
{
  static  mspace   mMspace;
  static  char*    mMspaceBase;
  static  uint32_t mMspaceLen;

  static  uint32_t mTotalLen;
  static  char*    mMemoryAllocation;

  static  int      mFd;

public:

  static
  void*    
  Allocate( int bytes )
  {
    void* Ptr = mspace_malloc( mMspace, bytes );

    StrongAssertLogLine( Ptr != NULL )
      << "Allocate(): ERROR: Not enough memory. "
      << " bytes: " << bytes
      << EndLogLine;

    return Ptr;
  }

  static
  void     
  Free( void * addr )
  {
    mspace_free( mMspace, addr );
  }

  static
  void
  Sync()
  {
    // Flush the mmaped file
    int rc = msync( mMemoryAllocation, mTotalLen, MS_SYNC );
    StrongAssertLogLine( rc == 0 )
      << "ERROR: rc: " << rc
      << " errno: " << errno
      << EndLogLine;

    rc = fsync( mFd );
    StrongAssertLogLine( rc == 0 )
      << "ERROR: rc: " << rc
      << " errno: " << errno
      << EndLogLine;
  }
  static
  void
  Dump( char* aPath )
  {
    Sync();
    int MyRank = 0;
#ifndef SKV_CLIENT_UNI
    MPI_Comm_rank( MPI_COMM_WORLD, & MyRank );
#endif
    skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

    char persistentFilename[ PERSISTENT_FILEPATH_MAX_SIZE ];
    bzero( persistentFilename, PERSISTENT_FILEPATH_MAX_SIZE );

    sprintf( persistentFilename, "%s/%s.%d", aPath, config->GetServerPersistentFilename(), MyRank );

    char ScpCommand[ 512 ];
    bzero( ScpCommand, 512 );

    char MyHostname[ MY_HOSTNAME_SIZE ];
    bzero( MyHostname, MY_HOSTNAME_SIZE );
    gethostname( MyHostname, MY_HOSTNAME_SIZE );

    if( strcmp( MyHostname, "asfhost00" ) == 0 )
    {
      sprintf( ScpCommand,
               "tar -czf - %s.%d | dd of=%s",
               //"cp %s.%d %s",
               config->GetServerPersistentFileLocalPath(),
               MyRank,
               persistentFilename );
    }
    else
    {
      // tar -czf - myfile | rsh 10.255.255.254 dd of=/gpfs/gpfs/packedfile.mynode
#if 0
      sprintf( ScpCommand,
               // "tar -czf - %s.%d | rsh %s dd of=%s",
               "tar -czf - %s.%d | rsh %s \"cat > %s\"",
               config->GetServerPersistentFileLocalPath(),
               MyRank,
               IONODE_IP,
               persistentFilename );
#else
      sprintf( ScpCommand,
               // "rcp %s.%d %s:%s",
               "tar -czf %s %s.%d",
               persistentFilename,
               config->GetServerPersistentFileLocalPath(),
               MyRank );
#endif
    }
    //     sprintf( ScpCommand, 
    //              "rcp %s %s:%s", 
    //              PERSISTENT_FILE_LOCAL_PATH,
    //              IONODE_IP, 
    //              persistentFilename );

    BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
      << "Dump(): "
      << " ScpCommand: " << ScpCommand
      << EndLogLine;

    system( ScpCommand );

    BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
      << "Dump(): rcp finished "
      << EndLogLine;

#if 0
    bzero( mMemoryAllocation,  mTotalLen );
    rc = munmap( mMemoryAllocation, mTotalLen );
    StrongAssertLogLine( rc == 0 )
      << "ERROR: rc: " << rc
      << " errno: " << errno
      << EndLogLine;

    rc = close( mFd );
    StrongAssertLogLine( rc == 0 )
      << "ERROR: rc: " << rc
      << " errno: " << errno
      << EndLogLine;

    char PersistentLocalFilePath[ 128 ];
    sprintf( PersistentLocalFilePath, "%s.%d", config->GetServerPersistentFileLocalPath(), MyRank );

    unlink( PersistentLocalFilePath );

    // Copy the file from the ionode

    bzero( ScpCommand, 512 );

    char MyHostname[ MY_HOSTNAME_SIZE ];
    bzero( MyHostname, MY_HOSTNAME_SIZE );
    gethostname( MyHostname, MY_HOSTNAME_SIZE );

    if( strcmp( MyHostname, "asfhost00" ) == 0 )
    {
      sprintf( ScpCommand,
               "cat %s | tar -zxOf - > %s.%d",
               //"cp %s %s.%d",
               persistentFilename,
               config->GetServerPersistentFileLocalPath(),
               MyRank );
    }
    else
    {
      sprintf( ScpCommand,
               "rsh %s cat %s | tar -zxOf - > %s.%d",
               IONODE_IP,
               persistentFilename,
               config->GetServerPersistentFileLocalPath(),
               MyRank );
    }

    BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
      << "Init(): "
      << " ScpCommand: " << ScpCommand
      << EndLogLine;

    system( ScpCommand );

    mFd = open( PersistentLocalFilePath, O_RDWR );

    StrongAssertLogLine( mFd >= 0 )
      << "ERROR: Failed to open file: "
      << " PERSISTENT_FILE_LOCAL_PATH: " << config->GetServerPersistentFileLocalPath()
      << " PersistentLocalFilePath: " << PersistentLocalFilePath
      << " mFd: " << mFd
      << " errno: " << errno
      << EndLogLine;

    skv_server_persistance_heap_hdr_t Hdr;

    read_from_file( mFd, (char *) & Hdr, sizeof( skv_server_persistance_heap_hdr_t ) );

    StrongAssertLogLine( Hdr.mMagicNum == PERSISTENT_MAGIC_NUMBER )
      << "ERROR: Magic number mismatch: "
      << " Hdr.mMagicNum: " << (void *) Hdr.mMagicNum
      << " PERSISTENT_MAGIC_NUMBER: " << (void *) PERSISTENT_MAGIC_NUMBER
      << EndLogLine;

    StrongAssertLogLine( Hdr.mNodeId == MyRank )
      << "ERROR: NodeId mismatch: "
      << " MyRank: " << MyRank
      << " Hdr.mNodeId: " << Hdr.mNodeId
      << EndLogLine;

    char* persistentFileMapAddress = (char *) Hdr.mMmapAddr;
    int mmapFlags = MAP_FIXED | MAP_SHARED;

    mMemoryAllocation = (char *) mmap( persistentFileMapAddress,
                                       mTotalLen, 
                                       PROT_READ | PROT_WRITE, 
                                       mmapFlags,
                                       mFd,
                                       0 );

    if( mMemoryAllocation == MAP_FAILED )
    {
      char CatPid[256];
      bzero( CatPid, 256 );
      sprintf( CatPid, "cat /proc/%d/maps > /tmp/skv_server_proc_maps.%d ", getpid(), getpid() );
      system( CatPid );

      StrongAssertLogLine( 0 )
        << "Init(): ERROR: mmap failed "
        << " mTotalLen: " << mTotalLen
        << " errno: " << errno
        << EndLogLine;
    }
#endif

  }

  static
  void
  Init( int                      aHeapMegs, 
        char**                   aPersistanceMetadataStart, 
        char*                    aRestartImagePath,
        skv_persistance_flag_t  aFlag )
  {  
    //mTotalLen = aHeapMegs * 1024 * 1024; 
    mTotalLen = PERSISTENT_IMAGE_MAX_LEN ;
    //mTotalLen = 2147483648u;

    StrongAssertLogLine( mTotalLen > 0 )
      << "Init(): ERROR: "
      << " mTotalLen: " << mTotalLen
      << EndLogLine;

    BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
      << "Init(): Trying to allocate "
      << " mTotalLen: " << mTotalLen
      << EndLogLine;

    mFd = -1;
    char* persistentFileMapAddress = NULL;
    int  mmapFlags = MAP_SHARED;

    int MyRank = 0;
#ifndef SKV_CLIENT_UNI
    MPI_Comm_rank( MPI_COMM_WORLD, & MyRank );
#endif
    skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

    char PersistentLocalFilePath[ 128 ];
    sprintf( PersistentLocalFilePath, "%s.%d", config->GetServerPersistentFileLocalPath(), MyRank );

    if( aFlag & SKV_PERSISTANCE_FLAG_INIT )
    {
      mFd = open( PersistentLocalFilePath,
                  O_RDWR | O_CREAT | O_TRUNC,
                  S_IROTH | S_IWOTH | S_IRUSR | S_IWUSR );

      StrongAssertLogLine( mFd >= 0 )
        << "ERROR: Failed to open file: "
        << " PersistentLocalFilePath: " << PersistentLocalFilePath
        << " mFd: " << mFd
        << " errno: " << errno
        << EndLogLine;

      int rc = ftruncate64( mFd, mTotalLen );

      StrongAssertLogLine( rc == 0 )
        << " ftruncate failed: "
        << " errno: " << errno
        << EndLogLine;
    }
    else if( aFlag & SKV_PERSISTANCE_FLAG_RESTART )
    {
      char persistentFilename[ PERSISTENT_FILEPATH_MAX_SIZE ];
      bzero( persistentFilename, PERSISTENT_FILEPATH_MAX_SIZE );

      sprintf( persistentFilename, "%s/%s.%d", aRestartImagePath, config->GetServerPersistentFilename(), MyRank );

      // Remove the file from the local node
      unlink( PersistentLocalFilePath );

        // Copy the file from the ionode

      char ScpCommand[ 512 ];
      bzero( ScpCommand, 512 );

      // rsh 10.255.255.254 cat /gpfs/gpfs/packedfile.mynode | tar -xzf -
      char MyHostname[ MY_HOSTNAME_SIZE ];
      bzero( MyHostname, MY_HOSTNAME_SIZE );
      gethostname( MyHostname, MY_HOSTNAME_SIZE );

      if( strcmp( MyHostname, "asfhost00" ) == 0 )
      {
        sprintf( ScpCommand,
                 "cat %s | tar -zxOf - > %s.%d",
                 // "cp %s %s.%d",
                 persistentFilename,
                 config->GetServerPersistentFileLocalPath(),
                 MyRank );
      }
      else
      {
#if 0            
        sprintf( ScpCommand,
                 "rsh %s cat %s | tar -zxOf - > %s.%d",
                 IONODE_IP,
                 persistentFilename,
                 config->GetServerPersistentFileLocalPath(),
                 MyRank );
#else
        sprintf( ScpCommand,
                 // "rcp %s:%s %s.%d",
                 "cat %s | tar -zxOf - > %s.%d",
                 persistentFilename,
                 config->GetServerPersistentFileLocalPath(),
                 MyRank );
#endif
      }

      BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
        << "Init(): "
        << " ScpCommand: " << ScpCommand
        << EndLogLine;

      system( ScpCommand );

      mFd = open( PersistentLocalFilePath, O_RDWR );

      StrongAssertLogLine( mFd >= 0 )
        << "ERROR: Failed to open file: "
        << " PERSISTENT_FILE_LOCAL_PATH: " << config->GetServerPersistentFileLocalPath()
        << " PersistentLocalFilePath: " << PersistentLocalFilePath
        << " mFd: " << mFd
        << " errno: " << errno
        << EndLogLine;

      skv_server_persistance_heap_hdr_t Hdr;

      read_from_file( mFd, (char *) & Hdr, sizeof( skv_server_persistance_heap_hdr_t ) );

      StrongAssertLogLine( Hdr.mMagicNum == PERSISTENT_MAGIC_NUMBER )
        << "ERROR: Magic number mismatch: "
        << " Hdr.mMagicNum: " << (void *) Hdr.mMagicNum
        << " PERSISTENT_MAGIC_NUMBER: " << (void *) PERSISTENT_MAGIC_NUMBER
        << EndLogLine;

      StrongAssertLogLine( Hdr.mNodeId == (unsigned long)MyRank )
        << "ERROR: NodeId mismatch: "
        << " MyRank: " << MyRank
        << " Hdr.mNodeId: " << Hdr.mNodeId
        << EndLogLine;

      persistentFileMapAddress = (char *) Hdr.mMmapAddr;
      mmapFlags |= MAP_FIXED;

      lseek( mFd, 0, SEEK_SET );
    }
    else
    {
      StrongAssertLogLine( 0 )
        << "Init(): ERROR: "
        << " aFlag: " << aFlag
        << EndLogLine;
    }

    mMemoryAllocation = (char *) mmap( persistentFileMapAddress,
                                       mTotalLen, 
                                       PROT_READ | PROT_WRITE, 
                                       mmapFlags,
                                       mFd,
                                       0 );

    if( mMemoryAllocation == MAP_FAILED )
    {
      char CatPid[256];
      bzero( CatPid, 256 );
      sprintf( CatPid, "cat /proc/%d/maps > /tmp/skv_server_proc_maps.%d ", getpid(), getpid() );
      system( CatPid );

      StrongAssertLogLine( 0 )
        << "Init(): ERROR: mmap failed "
        << " mTotalLen: " << mTotalLen
        << " errno: " << errno
        << EndLogLine;
    }

    BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
      << "Init(): "
      << " mMemoryAllocation: " << (void *) mMemoryAllocation
      << EndLogLine;

    skv_server_persistance_heap_hdr_t * Hdr = 
        (skv_server_persistance_heap_hdr_t *) mMemoryAllocation;

    if( aFlag & SKV_PERSISTANCE_FLAG_RESTART )
    {
      StrongAssertLogLine( Hdr->mMmapAddr == mMemoryAllocation )
        << "skv_server_heap_manager_t::Init(): ERROR: "
        << " Hdr->mMmapAddr: " << (void *) Hdr->mMmapAddr
        << " mMemoryAllocation: " << (void *) mMemoryAllocation
        << EndLogLine;
    }
    else
    {
      BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
        << "skv_server_heap_manager_t::Init(): before bzero() "
        << " mTotalLen: " << mTotalLen
        << EndLogLine;

      int page_size = getpagesize();

      int page_count = (int) ceil( (1.0*mTotalLen)  / (1.0*page_size) );

      for( int page_idx = 0; page_idx < page_count; page_idx++ )
      {
        int byte_idx = page_idx * page_size + 1;
        mMemoryAllocation[ byte_idx ] = 0;
      }

      // bzero( mMemoryAllocation, mTotalLen );

      Hdr->mMmapAddr = mMemoryAllocation;
      Hdr->mMagicNum = PERSISTENT_MAGIC_NUMBER;
      Hdr->mNodeId   = MyRank;

      // Save space for state variables
      Hdr->mMspaceBase = mMemoryAllocation + sizeof( skv_server_persistance_heap_hdr_t );
      Hdr->mMspaceLen  = mTotalLen - sizeof( skv_server_persistance_heap_hdr_t );

      BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
        << "skv_server_heap_manager_t::Init(): Allocated "
        << " Hdr->mMspaceLen: " << Hdr->mMspaceLen
        << EndLogLine;

      Hdr->mMspace = create_mspace_with_base( Hdr->mMspaceBase, Hdr->mMspaceLen, 0 );

      StrongAssertLogLine( Hdr->mMspace != NULL )
        << "skv_server_heap_manager_t::Init(): ERROR: No memory!!!"
        << " mMspaceBase: " << (void *) mMspaceBase
        << " mMspaceLen: " << mMspaceLen
        << EndLogLine;
    }

    mMspaceBase = Hdr->mMspaceBase;
    mMspaceLen  = Hdr->mMspaceLen;
    mMspace     = Hdr->mMspace;

    *aPersistanceMetadataStart = mMemoryAllocation;

    BegLogLine( SKV_SERVER_HEAP_MANAGER_LOG )
      << "skv_server_heap_manager_t::Init(): Exiting"
      << EndLogLine;    

    return;
  }

  static
  void
  GetDataStartAndLen( char** aStart, size_t* aLen )
  {
    *aStart = mMspaceBase;
    *aLen   = mMspaceLen;
  }
};

template <class SKV_INMEM_CLASS>
class skv_allocator_t {
public:

  typedef SKV_INMEM_CLASS value_type;
  typedef SKV_INMEM_CLASS* pointer;
  typedef SKV_INMEM_CLASS& reference;
  typedef const SKV_INMEM_CLASS* const_pointer;
  typedef const SKV_INMEM_CLASS& const_reference;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;

  // construction/destruction
  skv_allocator_t() throw() {}
  template< class ITEM > skv_allocator_t( const skv_allocator_t< ITEM >& aIn ) throw() {}
  ~skv_allocator_t() throw() {}

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
    new( aPtr ) SKV_INMEM_CLASS( aValue );
  }

  void destroy( pointer aPtr )
  {
    aPtr->~SKV_INMEM_CLASS();
  }

  // max number of elements to allocate
  // \todo: pretty stupid approach so far, should watch the memory limits and also OFED limits for memory registration
  size_type max_size() const throw()
  {
    return (size_t)(-1)/sizeof(SKV_INMEM_CLASS);
  }

  // enable typedef ...
  template< class ITEM >
  struct rebind
  {
    typedef skv_allocator_t< ITEM > other;
  };


  // allocation and deallocation
  pointer allocate( size_type aCount, const void* aHint=0 )
  {
    return (pointer) skv_server_heap_manager_t::Allocate( sizeof( SKV_INMEM_CLASS ) * aCount );
  }

  void deallocate( pointer aPtr, size_type aCount )
  {
    skv_server_heap_manager_t::Free( (void *) aPtr );
  }

};

template< class SKV_INMEM_CLASS >
bool operator==(const skv_allocator_t< SKV_INMEM_CLASS>& a, const skv_allocator_t< SKV_INMEM_CLASS>& b) throw()
{
  return ((void*)&a == (void*)&b);
}

template< class SKV_INMEM_CLASS >
bool operator!=(const skv_allocator_t< SKV_INMEM_CLASS>& a, const skv_allocator_t< SKV_INMEM_CLASS>& b) throw()
{
  return ((void*)&a != (void*)&b);
}


#endif
