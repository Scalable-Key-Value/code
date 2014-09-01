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

#ifndef __SKV_TYPES_EXT_HPP__
#define __SKV_TYPES_EXT_HPP__

#include <common/skv_errno.hpp>

#ifndef SKV_CLIENT_UNI
#include <mpi.h>
#endif

#ifdef SKV_CLIENT_FILEBASED
// \todo: check if this is outdated
struct skv_pds_id_t
{
  FILE* mPDSFileFd;
};
#else
struct skv_pds_id_t
{
  unsigned int mOwnerNodeId;
  unsigned int mIdOnOwner;

  void
  Init()
  {
    mOwnerNodeId = 0;
    mIdOnOwner = 0;
  }

  bool
  operator==(const skv_pds_id_t & aPDSId ) const
  {
    return (( mOwnerNodeId == aPDSId.mOwnerNodeId ) &&
            ( mIdOnOwner == aPDSId.mIdOnOwner ) );
  }

  bool
  operator!=(const skv_pds_id_t & aPDSId ) const
  {
    return (( mOwnerNodeId != aPDSId.mOwnerNodeId ) ||
            ( mIdOnOwner != aPDSId.mIdOnOwner ) );
  }

  bool
  operator<( const skv_pds_id_t & aPDSId ) const
             {
    if( mOwnerNodeId < aPDSId.mOwnerNodeId )
      return 1;
    else if( mOwnerNodeId == aPDSId.mOwnerNodeId )
    {
      return (mIdOnOwner < aPDSId.mIdOnOwner);
    }
    else
      return 0;
  }
};

template<class streamclass>
static  
streamclass& 
operator<<(streamclass& os, const skv_pds_id_t& aT )
{
  os << "skv_pds_id_t [  "
     << aT.mOwnerNodeId << ' '
     << aT.mIdOnOwner
     << " ]";

  return(os);
}
#endif  // SKV_CLIENT_FILEBASED


#include <skv.h>

struct skv_pds_attr_t
{
  uint64_t             mSize;
  skv_pds_priv_t       mPrivs;
  skv_pdsname_string_t mPDSName[ SKV_MAX_PDS_NAME_SIZE ];
  skv_pds_id_t         mPDSId;
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, const skv_pds_attr_t& aT )
{
  os << "skv_pds_attr_t [ "
     << aT.mPDSId << ' '
     << (unsigned) aT.mPrivs << " s:"
     << aT.mSize << " n:"
     << aT.mPDSName << " ]";

  return (os);
}

#endif // __SKV_TYPES_EXT_HPP__

