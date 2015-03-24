/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * test_skv_server_command_buffer.cpp
 *
 *  Created on: Dec 29, 2014
 *      Author: lschneid
 */

#include <FxLogger.hpp>
#include <pthread.h>
#include <skv/common/skv_errno.hpp>
#include <skv/common/skv_types.hpp>
#include <skv/common/skv_client_server_headers.hpp>
#include <skv/server/skv_server_types.hpp>

#include <skv/server/skv_server_command_event_buffer.hpp>
#include <skv/server/skv_server_command_event_buffer.cpp>

int main()
{
  skv_server_command_event_buffer_t TestBuffer;

  return TestBuffer.test_fill();
}
