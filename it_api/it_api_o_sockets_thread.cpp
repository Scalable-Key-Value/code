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

#include <Trace.hpp>
#include <it_api_o_sockets_thread.h>

#ifndef IT_API_O_SOCKETS_THREAD_LOG
#define IT_API_O_SOCKETS_THREAD_LOG ( 0 )
#endif

#ifndef IT_API_O_SOCKETS_THREAD_TRACE
#define IT_API_O_SOCKETS_THREAD_TRACE ( 0 )
#endif

static TraceClient gITAPI_RDMA_READ_AT_SOCKETS;
extern int gTraceRank;

/*
 * There is no code in this file. It is only here to satisfy the
 * build process
 */
