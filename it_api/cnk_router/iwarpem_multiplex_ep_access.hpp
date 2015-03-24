/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * iwarpem_multiplex_ep_access.hpp
 *
 *  Created on: Jan 26, 2015
 *      Author: lschneid
 */

#ifndef IT_API_CNK_ROUTER_IWARPEM_MULTIPLEX_EP_ACCESS_HPP_
#define IT_API_CNK_ROUTER_IWARPEM_MULTIPLEX_EP_ACCESS_HPP_


static
inline
iWARPEM_Status_t
RecvRaw( iWARPEM_Object_EndPoint_t *aEP, char *buff, int len, int* wlen, bool aWithClientHdr )
{
  iWARPEM_Status_t status = IWARPEM_SUCCESS;
  int sock = aEP->ConnFd;

  if( aEP->ConnType == IWARPEM_CONNECTION_TYPE_VIRUTAL )
  {
    iWARPEM_StreamId_t client;
    iWARPEM_Router_Endpoint_t *rEP = (iWARPEM_Router_Endpoint_t*)( gSockFdToEndPointMap[ sock ]->connect_sevd_handle );
    if( aWithClientHdr )
      status = rEP->ExtractRawData( buff, len, wlen, &client );
    else
      status = rEP->ExtractRawData( buff, len, wlen );
  }
  else
    status = read_from_socket( sock, buff, len, wlen );
  return status;
}

static
inline
iWARPEM_Status_t
SendMsg( iWARPEM_Object_EndPoint_t *aEP, char * buff, int len, int* wlen, const bool aFlush )
{
  iWARPEM_Status_t status = IWARPEM_SUCCESS;
  int sock = aEP->ConnFd;

  if( aEP->ConnType == IWARPEM_CONNECTION_TYPE_VIRUTAL )
  {
    iWARPEM_Router_Endpoint_t *rEP = (iWARPEM_Router_Endpoint_t*)( gSockFdToEndPointMap[ sock ]->connect_sevd_handle );
    iWARPEM_Message_Hdr_t *hdr = (iWARPEM_Message_Hdr_t*)buff;
    char *data = buff + sizeof( iWARPEM_Message_Hdr_t );
    len -= sizeof( iWARPEM_Message_Hdr_t );

    status = rEP->InsertMessage( aEP->ClientId, hdr, buff, len );
    if( status == IWARPEM_SUCCESS )
      *wlen = len + sizeof( iWARPEM_Message_Hdr_t );
    else
      *wlen = 0;
    if( aFlush )
      rEP->FlushSendBuffer();
  }
  else
    status = write_to_socket( sock, buff, len, wlen );
  return status;
}

static
inline
iWARPEM_Status_t
SendVec( iWARPEM_Object_EndPoint_t *aEP, struct iovec *iov, int iov_count, size_t totalen, int* wlen, const bool aFlush = false )
{
  iWARPEM_Status_t status = IWARPEM_SUCCESS;
  int sock = aEP->ConnFd;

  if( aEP->ConnType == IWARPEM_CONNECTION_TYPE_VIRUTAL )
  {
    iWARPEM_Router_Endpoint_t *rEP = (iWARPEM_Router_Endpoint_t*)( gSockFdToEndPointMap[ sock ]->connect_sevd_handle );
    status = rEP->InsertMessageVector( aEP->ClientId, iov, iov_count, wlen, true );
    if( aFlush )
      rEP->FlushSendBuffer();
  }
  else
    status = write_to_socket( sock, iov, iov_count, totalen, wlen );
  return status;
}

#endif /* IT_API_CNK_ROUTER_IWARPEM_MULTIPLEX_EP_ACCESS_HPP_ */
