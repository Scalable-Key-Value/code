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

#include <client/skv_client_internal.hpp>

#include <sched.h>

#ifndef SKV_CLIENT_WAIT_LOG
#define SKV_CLIENT_WAIT_LOG ( 0 | SKV_LOGGING_ALL )
#endif

/***
 * skv_client_internal_t::TestAny::
 * Desc: Check if any command is done
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_command_manager_if_t::
TestAny( skv_client_cmd_hdl_t* aCmdHdl )
{
  skv_status_t status = SKV_SUCCESS;

  mConnMgrIF->ProcessConnectionsRqSq();  

  skv_client_ccb_t* CCB = mCCBMgrIF->RemoveFromFrontDoneCCBQueue();

  if( CCB == NULL )
  {
    // No block is done
    status = SKV_ERRNO_NOT_DONE;
  }
  else
  {
    *aCmdHdl = CCB;

    mCCBMgrIF->AddToFreeCCBQueue( CCB );

    status = CCB->mStatus;
  }

  return status;
}

skv_status_t
skv_client_command_manager_if_t::
ReleaseAssumeDone( skv_client_cmd_hdl_t aCmdHdl )
{
  // Return the command ctrl block back to the free queue

  mCCBMgrIF->RemoveFromDoneCCBQueue( aCmdHdl );

  aCmdHdl->Transit( SKV_CLIENT_COMMAND_STATE_IDLE );

  mCCBMgrIF->AddToFreeCCBQueue( aCmdHdl );  

  return SKV_SUCCESS;
}

skv_status_t
skv_client_command_manager_if_t::
Reserve( skv_client_cmd_hdl_t* aCmdHdl )
{
  skv_client_cmd_hdl_t NewCommand = mCCBMgrIF->RemoveFromFrontFreeCCBQueue();
  if( NewCommand == NULL )
    return SKV_ERRNO_COMMAND_LIMIT_REACHED;

  *aCmdHdl = NewCommand;

  return SKV_SUCCESS;
}

/***
 * skv_client_internal_t::Test::
 * Desc: Check if a command is done
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_command_manager_if_t::
Test( skv_client_cmd_hdl_t aCmdHdl )
{
  // BegLogLine( SKV_CLIENT_WAIT_LOG )
  //   << "skv_client_internal_t::Test():: Entering " 
  //   << EndLogLine;

  mConnMgrIF->ProcessConnectionsRqSq();
  // mConnMgrIF->ProcessConnections();

  skv_status_t status = SKV_SUCCESS;
  if( aCmdHdl->mState == SKV_CLIENT_COMMAND_STATE_DONE )
  {
    status = aCmdHdl->mStatus;
    ReleaseAssumeDone( aCmdHdl );
  }
  else
  {
    status = SKV_ERRNO_NOT_DONE;
  }

  // BegLogLine( SKV_CLIENT_WAIT_LOG )
  //   << "skv_client_internal_t::Test():: Leaving " 
  //   << EndLogLine;

  return status;
}

/***
 * skv_client_internal_t::WaitAny::
 * Desc: Wait on any command handle
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t
skv_client_command_manager_if_t::
WaitAny( skv_client_cmd_hdl_t* aCmdHdl )
{
  while( 1 )
  {
    skv_status_t status = TestAny( aCmdHdl );

    if( status == SKV_ERRNO_NOT_DONE )
    {
      continue;
    }
    else
      return status;
  }

  return SKV_SUCCESS;
}


/***
 * skv_client_command_manager_if_t::Wait::
 * Desc: Wait on a command handle
 * input: 
 * returns: SKV_SUCCESS on success or error code
 ***/
skv_status_t 
skv_client_command_manager_if_t::
Wait( skv_client_cmd_hdl_t aCmdHdl )
{
  BegLogLine( SKV_CLIENT_WAIT_LOG )
    << "skv_client_internal_t::Wait():: Entering " 
    << EndLogLine;

  while( 1 )
  {
    skv_status_t status = Test( aCmdHdl );

    if( status == SKV_ERRNO_NOT_DONE )
    {
      //sleep( 1 );

      // sched_yield();

      continue;
    }
    else
      return status;
  }

  BegLogLine( SKV_CLIENT_WAIT_LOG )
    << "skv_client_internal_t::Wait():: Leaving " 
    << EndLogLine;

  return SKV_SUCCESS;
}


skv_status_t 
skv_client_command_manager_if_t::
Dispatch( int aNodeId, skv_client_ccb_t* aCCB )
{
  return mConnMgrIF->Dispatch( aNodeId, aCCB );  
}
