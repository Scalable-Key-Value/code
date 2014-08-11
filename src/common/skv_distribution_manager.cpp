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

#include <common/skv_distribution_manager.hpp>

#include <math.h>
#include <stdlib.h>

#ifndef SKV_DISTRIBUTION_MANAGER_LOG
#define SKV_DISTRIBUTION_MANAGER_LOG ( 0 | SKV_LOGGING_ALL )
#endif

/******************
 * Hash based distirbution
 *****************/

/***
 * DistributionManagerHash::Finalize::
 * Desc: Finalize the state of the distribution manager
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_distribution_hash_t::
Finalize()
{
  return SKV_SUCCESS;
}

/***
 * skv_distribution_hash_t::Init::
 * Desc: Initiate the state of the distribution manager
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_distribution_hash_t::
Init( int aCount )
{
  BegLogLine( SKV_DISTRIBUTION_MANAGER_LOG )
    << "skv_distribution_hash_t::Init():: Entering "
    << EndLogLine;

  mCount = aCount;
  mHashFunc.Init();

  BegLogLine( SKV_DISTRIBUTION_MANAGER_LOG )
    << "skv_distribution_hash_t::Init():: Leaving "
    << EndLogLine;

  return SKV_SUCCESS;
}

/***
 * skv_distribution_hash_t::GetNode::
 * Desc: Initiate the state of the distribution manager
 * input: 
 * aRangeCount -> 
 * returns: NodeId that owns aKey
 ***/
int
skv_distribution_hash_t::
GetNode( skv_key_t* aKey )
{    
  AssertLogLine( aKey != NULL )
    << "skv_distribution_hash_t::GetNode():: ERROR: "
    << " aKey != NULL"
    << EndLogLine;    

  char* Data = aKey->GetData();
  int   DataSize = aKey->GetSize();

  return GetNode( & Data, & DataSize, 1 );
}

/***
 * skv_distribution_hash_t::GetNode::
 * returns: NodeId that owns aKey
 ***/
int
skv_distribution_hash_t::
GetNode( char** aListOfDataElem, int* aListOfSizesOfData, int aListElementCount )
{        
  AssertLogLine( aListElementCount >= 1 )
    << EndLogLine;

  HashKeyT hashValue = mHashFunc.GetHash( aListOfDataElem[ 0 ], aListOfSizesOfData[ 0 ] );

  for( int i = 1; i < aListElementCount; i++ )
  {
    hashValue ^= mHashFunc.GetHash( aListOfDataElem[i], aListOfSizesOfData[i] );
  }

  unsigned int NodeId = hashValue % mCount;

  BegLogLine( SKV_DISTRIBUTION_MANAGER_LOG )
    << "skv_distribution_hash_t::GetNode():: "
    << " hashValue: " << hashValue
    << " mCount: " << mCount
    << " NodeId: " << NodeId
    << " aListElementCount: " << aListElementCount
    << EndLogLine;

  return  ( hashValue % mCount );
}

/******************
 * Random-based distirbution
 *****************/

/***
 * skv_distribution_random_t::Finalize::
 * Desc: Finalize the state of the distribution manager
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_distribution_random_t::
Finalize()
{
  return SKV_SUCCESS;
}

/***
 * skv_distribution_random_t::Init::
 * Desc: Initiate the state of the distribution manager
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_distribution_random_t::
Init( int aCount )
{
  mCount = aCount;

  srand( GetBGPRank() );

  return SKV_SUCCESS;
}

/***
 * skv_distribution_random_t::GetNode::
 * Desc: Initiate the state of the distribution manager
 * input: 
 * returns: NodeId that owns aKey
 ***/
int
skv_distribution_random_t::
GetNode( skv_key_t* aKey )
{
  AssertLogLine( aKey != NULL )
    << "skv_distribution_random_t::GetNode():: ERROR: "
    << " aKey != NULL"
    << EndLogLine;        

  return ((int) (mCount * ( rand() / (RAND_MAX + 1.0) ) ));
}
