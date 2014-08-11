/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     lschneid - initial implementation
 *
 * Created on: Jan 13, 2014
 */

#ifndef SKV_TRACE_CLIENTS_HPP_
#define SKV_TRACE_CLIENTS_HPP_

static TraceClient gSKVServerInsertEnter;
static TraceClient gSKVServerInsertSendingRDMAReadAck;

static TraceClient gSKVServerInsertInTreeStart;
static TraceClient gSKVServerInsertInTreeFinis;

static TraceClient gSKVServerInsertRetrieveFromTreeStart;
static TraceClient gSKVServerInsertRetrieveFromTreeFinis;

static TraceClient gSKVServerInsertRDMAReadStart;
static TraceClient gSKVServerInsertRDMAReadFinis;

static TraceClient gSKVServerRetrieveEnter;
static TraceClient gSKVServerRetrieveRDMAWriteStart;
static TraceClient gSKVServerRetrieveRDMAWriteFinis;

static TraceClient gSKVServerRetrieveSendingRDMAWriteAck;

static TraceClient gSKVServerRemoveEnter;
static TraceClient gSKVServerRemoveLeave;

static TraceClient gSKVServerBulk_InsertSendingRDMAReadAck;
static TraceClient gSKVServerBulk_InsertAboutToRDMARead;

#endif /* SKV_TRACE_CLIENTS_HPP_ */
