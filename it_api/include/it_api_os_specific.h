/*
 * it_api_os_specific.h - OS specific file to support it_api.h on Linux
 *
 * Copyright (C) 2005 The Open Group
 * All rights reserved.
 *
 * The copyright owner hereby grants permission for all or part of this
 * publication to be reproduced, stored in a retrieval system, or transmitted,
 * in any form or by any means, electronic, mechanical, photocopying,
 * recording, or otherwise, provided that it remains unchanged and that this
 * copyright statement is included in all copies or substantial portions of the
 * publication.
 *
 * For any software code contained within this specification, permission is
 * hereby granted, free of charge, to any person obtaining a copy of this
 * specification (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the above
 * copyright notice and this permission notice being included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * Permission is granted for implementers to use the names, labels, etc.
 * contained within the specification. The intent of publication of the
 * specification is to encourage implementations of the specification.
 * This specification has not been verified for avoidance of possible
 * third-party proprietary rights. In implementing this specification, usual
 * procedures to ensure the respect of possible third-party intellectual
 * property rights should be followed.
 *
 * History:
 * - Based on IT-API v2.1
 * - 2006, 2007: Patches by Fredy Neeser <nfd@zurich.ibm.com>, see NFD-...
 */

#ifndef _IT_API_OS_SPECIFIC_H_
#define _IT_API_OS_SPECIFIC_H_

#if defined(__KERNEL__)

#include <linux/types.h>

#include <linux/in.h>		/* in_addr */
#include <linux/in6.h>		/* in6_addr */

#else

/* !defined(__KERNEL__) */

/*
 * <stdint.h> contains the definitions of uint8_t ... uint64_t for userspace.
 *
 * Portability Warning:
 * <stdint.h> defines uint64_t differently depending on __WORDSIZE:
 * - 32 bits: uint64_t == 'unsigned long long int'
 * - 64 bits: uint64_t == 'unsigned long int'
 * This is a nuisance with printf()/scanf() formats, which expect
 * '%llu' for the former and '%lu' for the latter.
 *
 * It causes the compiler to complain on x86_64 that the printf format
 * specifier 'llx' does not match with 'uint64_t', while it is happy on i386.
 * In effect, this means that printing a 64-bit variable would require
 * different printf formats for x86_64 and i386.
 *
 * => Uses of uint64_t in userspace should be carefully revised or abandoned
 * => For userspace, we should introduce and use the more portable
 *    (between architectures) types __u64 and __s64
 *    (as defined by the kernel in various include/asm-<arch>/types.h),
 *    which would always be defined as
 *			__u64 == 'unsigned long long int'
 *			__s64 == 'signed long long int'
 *    regardless of __WORDSIZE.
 *
 *  See also: ordma_platform_specific.h, rnicpi_osd.h
 */
#include <stdint.h>

/* These are the same for all architectures: */
//typedef unsigned long long __u64;
//typedef signed   long long __s64;

#include <sys/types.h>

#include <netinet/in.h>		/* in_addr, in6_addr */

#endif

#if defined(__i386__) || defined(__x86_64__)

/*
 * Implementation for Intel architecture.
 */
static inline uint64_t it_hton64(uint64_t hostint)
{
  uint64_t netint;
  netint = htonl((uint32_t)(hostint & 0x00000000ffffffff));
  netint <<= 32;
  netint |= htonl((uint32_t)(hostint >> 32));
  return netint;
}

/*
 * Implementation for Intel architecture.
 */
static inline uint64_t it_ntoh64(uint64_t netint)
{
  uint64_t hostint;
  hostint = ntohl((uint32_t)(netint & 0x00000000ffffffff));
  hostint <<= 32;
  hostint |= ntohl((uint32_t)(netint >> 32));
  return hostint;
}
#elif defined(__powerpc__)
/*
 * Implementation for IBM architecture.
 */
static inline uint64_t it_hton64(uint64_t hostint)
{
  return hostint ;
}

/*
 * Implementation for IBM architecture.
 */
static inline uint64_t it_ntoh64(uint64_t netint)
{
  return netint ;
}

#endif

#define IT_NO_ADDR			NULL

#define	IT_INTERFACE_NAME_SIZE		128

#define IT_MAX_SEND_SEGMENTS		10
#define IT_MAX_RDMA_READ_SEGMENTS	10
#define IT_MAX_RDMA_WRITE_SEGMENTS	10
#define IT_MAX_RECV_SEGMENTS		10

#define IT_RDMA_INITIATOR_DEPTH    32
#define IT_RDMA_RESPONDER_RESOUCES 16

#ifdef __KERNEL__

/* handle struct definitions for kAL: */
#define it_addr_handle_s	kal_ah
#define it_ep_handle_s		kal_ep
#define it_evd_handle_s		kal_evd
#define it_ia_handle_s		kal_ia
#define it_listen_handle_s	kal_lp
#define it_lmr_handle_s		kal_mr
#define it_pz_handle_s		kal_pz
#define it_rmr_handle_s		kal_mw
#define it_srq_handle_s		kal_srq
#define it_ud_svc_req_handle_s	kal_ud_svc_req

#else

/* handle struct definitions for uAL: */
#define it_addr_handle_s	ual_ah
#define it_ep_handle_s		ual_ep
#define it_evd_handle_s		ual_evd
#define it_ia_handle_s		ual_ia
#define it_listen_handle_s	ual_lp
#define it_lmr_handle_s		ual_mr
#define it_pz_handle_s		ual_pz
#define it_rmr_handle_s		ual_mw
#define it_srq_handle_s		ual_srq
#define it_ud_svc_req_handle_s	ual_ud_svc_req

#endif

#endif
