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

#ifndef __SKV_C2S_ACTIVE_BROADCAST_HPP__
#define __SKV_C2S_ACTIVE_BROADCAST_HPP__

typedef enum
  {
    SKV_ACTIVE_BCAST_CREATE_CURSOR_FUNC_TYPE           = 0x0001,
    SKV_ACTIVE_BCAST_CREATE_INDEX_FUNC_TYPE            = 0x0002,
    SKV_ACTIVE_BCAST_DUMP_PERSISTENCE_IMAGE_FUNC_TYPE  = 0x0003
  } skv_c2s_active_broadcast_func_type_t;

static
const char* 
skv_c2s_active_broadcast_func_type_to_string(skv_c2s_active_broadcast_func_type_t aFuncType)
{
  switch( aFuncType )
    {
    case SKV_ACTIVE_BCAST_CREATE_CURSOR_FUNC_TYPE: {return "SKV_ACTIVE_BCAST_CREATE_CURSOR_FUNC_TYPE";}
    case SKV_ACTIVE_BCAST_CREATE_INDEX_FUNC_TYPE: {return "SKV_ACTIVE_BCAST_CREATE_INDEX_FUNC_TYPE";}
    case SKV_ACTIVE_BCAST_DUMP_PERSISTENCE_IMAGE_FUNC_TYPE: { return "SKV_ACTIVE_BCAST_DUMP_PERSISTENCE_IMAGE_FUNC_TYPE"; }
    default:
      StrongAssertLogLine( 0 )
        << "skv_c2s_active_broadcast_func_type_to_string(): ERROR: "
        << " aFuncType: " << aFuncType
        << EndLogLine;
    }
}


#ifndef SKV_C2S_ACTIVE_BROADCAST_LOG 
#define SKV_C2S_ACTIVE_BROADCAST_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#endif
