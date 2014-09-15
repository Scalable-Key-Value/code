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

#ifndef __SKV_DISTRIBUTION_MANAGER_HPP__
#define __SKV_DISTRIBUTION_MANAGER_HPP__

#include <skv/common/skv_types.hpp>
#include <skv/common/skv_utils.hpp>

typedef unsigned int HashKeyT;

#ifndef SKV_HASH_DISTRIBUTION_LOG
#define SKV_HASH_DISTRIBUTION_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_CLIENT_ENDIAN_LOG
#define SKV_CLIENT_ENDIAN_LOG ( 0 || SKV_LOGGING_ALL )
#endif

struct skv_hash_func_t
{
  unsigned long long mP;
  unsigned int mA;
  unsigned int mB;

  void
  Init()
  {
    // ( 2^32 - 267 ) is a prime according to
    // http://primes.utm.edu/lists/2small/0bit.html
    mP = 4294967029ull;

    // According to:
    // J. Lawrence Carter and Mark N. Wegman's original paper "Universal Classes of Hash Functions"
    // If one picks random value for A and B then f( x ) = ( a * x + b ) % p makes for a
    // reasonable hash function
    mA = 65413;
    mB = 16777049;
  }
  void EndianConvert(void)
    {
      mP=be64toh(mP) ;
      mA=ntohl(mA) ;
      mB=ntohl(mB) ;
      BegLogLine(SKV_CLIENT_ENDIAN_LOG)
        << "Endian-converting hash function to mP=" << mP
        << " mA=" << mA
        << " mB=" << mB
        << EndLogLine ;
    }

  HashKeyT
  GetHashUINT( unsigned int aX ) const
  {
    unsigned long long R0 = mA;
    unsigned long long R1 = R0 * aX;
    unsigned long long R2 = R1 + mB;


    unsigned int R3 = R2 % mP;

    return R3;
  }

  HashKeyT
  GetHashSimple( const char* aData, int aLen )
  {      
    AssertLogLine( aData != NULL )
      << "skv_hash_func_t::GetHash() "
      << " aData != NULL "
      << EndLogLine;

    AssertLogLine( aLen > 0 )
      << "skv_hash_func_t::GetHash() "
      << " aLen: " << aLen
      << EndLogLine;

    // Fold in the last non-integer aligned block into the last
    // integer aligned block using XOR
    int NumberOfFullBlocks = aLen / sizeof( unsigned int );
    int NumberInLastBlock  = aLen % sizeof( unsigned int );

    unsigned int* DataInput = (unsigned int *) aData;

    HashKeyT hashValue = 0;

    if( NumberOfFullBlocks > 0 )
    {
      hashValue = GetHashUINT( DataInput[0] );

      for( int i = 1; i < NumberOfFullBlocks; i++ )
      {
        hashValue ^= GetHashUINT( DataInput[i] );
      }
    }

    if( NumberInLastBlock )
    {
      unsigned int TempInt = 0;
      memcpy( &TempInt,
              &DataInput[NumberOfFullBlocks],
              NumberInLastBlock );

      if( NumberOfFullBlocks == 0 )
        hashValue = GetHashUINT( TempInt );
      else
        hashValue ^= GetHashUINT( TempInt );
    }

    return hashValue;
  }

  HashKeyT 
  GetHash( const char* aData, int aLen )
  {
    AssertLogLine( aData != NULL )
      << "skv_hash_func_t::GetHash() "
      << " aData != NULL "
      << EndLogLine;

    AssertLogLine( aLen > 0 )
      << "skv_hash_func_t::GetHash() "
      << " aLen: " << aLen
      << EndLogLine;

#ifdef USE_BOB_JENKINS_HASH_FUNCTION
    HashKeyT hashValue = hashbig( aData, aLen, 0 );
#else
    HashKeyT hashValue = GetHashSimple( aData, aLen );
#endif

    return hashValue;
  }

  void
  GetRange( unsigned int &aLow, unsigned int &aHigh ) const
  {
#ifdef USE_BOB_JENKINS_HASH_FUNCTION
    aLow  = 0;
    aHigh = UINT_MAX;
#else
    aLow = 0;
    aHigh = mP - 1;
#endif
  }

};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_hash_func_t& A )
{
  os << "skv_hash_func_t [ "
     << A.mP << ' '
     << A.mA << ' '
     << A.mB
     << " ]";

  return(os);
}

struct skv_distribution_hash_t
{
  int                 mCount;
  skv_hash_func_t     mHashFunc;

  skv_status_t Init( int aCount );
  skv_status_t Finalize();
  void EndianConvert(void)
    {
      mCount=ntohl(mCount) ;
      BegLogLine(SKV_CLIENT_ENDIAN_LOG)
        << "mCount endian-converted to " << mCount
        << EndLogLine ;
      mHashFunc.EndianConvert() ;
    }
  int GetNode( skv_key_t* aKey ) const;

  // Input is a list of points to data
  // with a parallel list of data lengths
  int GetNode( char** aListOfDataElem, int* aListOfSizesOfData, int aListElementCount ) const;
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_distribution_hash_t& A )
{
  os << "skv_distribution_hash_t [ "
     << A.mCount << ' '
     << A.mHashFunc
     << " ]";

  return(os);
}


struct skv_distribution_random_t
{
  int mCount;

  skv_status_t Init( int aCount );
  skv_status_t Finalize();
  int GetNode( skv_key_t* aKey ) const;
  int GetNode( char* aData, int aSize ) const;
};

typedef skv_distribution_hash_t skv_distribution_t;

#endif
