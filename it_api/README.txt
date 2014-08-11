########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
########################################
#
# Contributors:
#     arayshu, lschneid - initial implementation

/*****************
* General Notes
******************/

This module implements a subset of IT_API that's needed by SKV.

There's an IT_API over sockets and IT_API over ofed verbs implementations.

The verbs implementation runs multi-threaded on the server side of the impl.
The client side runs single threaded. 

To accommodate the verbs IT_API needed to be expanded by a few additional functions. 
These functions are at the bottom of it_api.h and start with itx_*. The main nudge
was related the it_api over verb layer handling multiple independent devices. Since
devices are treated separately, each device has it's own memory region registration
context. The context therefore depends on the device, the sender uses to communicate
with the peer. Often this context is used advertised to peers and used in rdma_read,
rdma_write operations.

The it_api over verbs implements a lazy initialization of state. This design decision
is largely due to knowledge about devices comes at the point of connection establishment.
Once a connection is established, we're ready to initiate a number of ofed structures
which require an ibv_context* (generally device->verbs pointer) to be available. 

This implies that we need to do a lazy initiation of MR regions as well. MR region metadata
is initiated to NULL at first. Once the region is used or needed, the region is initiated. 
Assuming the proper connection already exists, initiation happens once. 

NOTE: Memory region gets initiated for every device.

UNUSUAL BEHAVIOUR: This is from line 1900 of it_api_o_verbs.  

      /********************************************************
       * SPECIAL NOTE: Below is logic is neccessary
       * to be able to specify a local port 
       * to the rdma_route_addr(). Specifying 
       * the local port is needed because OFED
       * has a tendency to choosing an already 
       * used port, cause rdma_connect() to 
       * return with errno=98
       ********************************************************/

