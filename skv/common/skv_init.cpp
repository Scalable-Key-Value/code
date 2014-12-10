/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     Stefan.Eilemann@epfl.ch - initial implementation
 */

#include "skv_init.hpp"

static bool initialized = false;

bool skv_common_init()
{
    if( initialized )
        return false;
    initialized = true;
    return true;
}

bool skv_common_exit()
{
    initialized = false;
}
