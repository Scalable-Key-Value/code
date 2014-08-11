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

#ifndef __SKV_CLIENT_COMMAND_MANAGER_IF_HPP__
#define __SKV_CLIENT_COMMAND_MANAGER_IF_HPP__

typedef skv_client_ccb_t* skv_client_cmd_hdl_t;

#define SKV_CLIENT_COMMAND_HANDLE_NULL ((skv_client_cmd_hdl_t) NULL)

class skv_client_command_manager_if_t
{
public:

  skv_client_conn_manager_if_t*             mConnMgrIF;  
  skv_client_ccb_manager_if_t*              mCCBMgrIF;

  void
  Init( skv_client_conn_manager_if_t*             aConnMgrIF,  
        skv_client_ccb_manager_if_t*              aCCBMgrIF )
  {
    mConnMgrIF = aConnMgrIF;
    mCCBMgrIF = aCCBMgrIF;
  }

  void  Finalize() {} 

  // Returns a handle that has completed
  skv_status_t WaitAny( skv_client_cmd_hdl_t* aCmdHdl );

  skv_status_t Wait( skv_client_cmd_hdl_t aCmdHdl );

  // Test
  // Returns a completed handle on success
  skv_status_t TestAny( skv_client_cmd_hdl_t* aCmdHdl );

  skv_status_t Test( skv_client_cmd_hdl_t aCmdHdl );

  skv_status_t Dispatch( int aNodeId, skv_client_ccb_t* aCCB );

  skv_status_t ReleaseAssumeDone( skv_client_cmd_hdl_t aCmdHdl );
  skv_status_t Reserve( skv_client_cmd_hdl_t* aCmdHdl );


};
#endif
