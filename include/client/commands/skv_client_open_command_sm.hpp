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

#ifndef __SKV_CLIENT_OPEN_COMMAND_SM_HPP__
#define __SKV_CLIENT_OPEN_COMMAND_SM_HPP__

class skv_client_open_command_sm
{
public:

  static
  skv_status_t
  Execute( skv_client_server_conn_t*    aConn, 
           skv_client_ccb_t*            aCCB )
  {
    char* RecvBuff = aCCB->GetRecvBuff();
    skv_server_to_client_cmd_hdr_t* Hdr = (skv_server_to_client_cmd_hdr_t *) RecvBuff;

    skv_client_command_state_t State = aCCB->GetState();    
    skv_client_event_t         Event = Hdr->mEvent;
    skv_status_t status = SKV_SUCCESS;

    switch( State )
      {
      case SKV_CLIENT_COMMAND_STATE_IDLE:
      case SKV_CLIENT_COMMAND_STATE_DONE:
        {
          StrongAssertLogLine( 0 )
            << "SKV_Client_Conn_ManagerIF::ProcessCCB:: ERROR:: Invalid State: "
            << " State: " << State
            << EndLogLine;

          break;
        }
      case SKV_CLIENT_COMMAND_STATE_PENDING:
        {
          switch( Event )
            {
            case SKV_CLIENT_EVENT_CMD_COMPLETE:	    
              { 
                // Return the status and set the PDSId 
                skv_cmd_open_resp_t* Resp = (skv_cmd_open_resp_t *) RecvBuff;

                *(aCCB->mCommand.mCommandBundle.mCommandOpen.mPDSId) = Resp->mPDSId;
                aCCB->mStatus                                        = Resp->mStatus;

                // Command is completed, release resources 
                int CommandOrd = Resp->mHdr.mCmdOrd;  
                aConn->ReleaseCmdOrdinal( CommandOrd );

                aCCB->mCCBMgrIF->AddToDoneCCBQueue( aCCB );

                aCCB->Transit( SKV_CLIENT_COMMAND_STATE_DONE );
                status = SKV_SUCCESS;
                break;
              }	    
            default:
              {
                StrongAssertLogLine( 0 )
                  << "SKV_Client_Conn_ManagerIF::ProcessCCB:: ERROR:: Invalid State: "
                  << " State: " << State
                  << " Event: " << Event
                  << EndLogLine;

                break;
              }
            }

          break;
        }
      default:
        {
          StrongAssertLogLine( 0 )
            << "SKV_Client_Conn_ManagerIF::ProcessCCB:: ERROR:: Invalid State: "
            << " State: " << State
            << EndLogLine;

          break;
        }
      }
    return status;
  }
};

#endif
