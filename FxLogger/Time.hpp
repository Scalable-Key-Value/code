/************************************************
 * Copyright (c) IBM Corp. 1997-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 *************************************************/

#ifndef __PK_TIME_HPP__
#define __PK_TIME_HPP__

#include <time.h>
#include <sys/time.h>
#include <inttypes.h>

struct pk_time_t
{
  unsigned int mSeconds;
  unsigned int mNanoseconds;
};

#define TIME_BGP_GETTIMEBASE 1
#define TIME_CLOCK_GETTIME   2
#define TIME_GETTIMEOFDAY    3

// #define TIME_TYPE ( TIME_BGP_GETTIMEBASE )
// #define TIME_TYPE ( TIME_CLOCK_GETTIME )

static void GetPkTime( pk_time_t* aPkTime ) ;

inline
unsigned long long
PkTimeGetNanos() ;

#if defined(__powerpc__)

// typedef unsigned long long uint64_t ;
// typedef unsigned int uint32_t ;
#ifdef VRNIC_CNK
#include "/bgsys/drivers/ppcfloor/arch/include/bpcore/ppc450_inlines.h"
#define TIME_TYPE ( TIME_BGP_GETTIMEBASE )
#else

#if !defined(PK_BGP)
#define PK_ASFHOST00
#endif
#define TIME_TYPE ( TIME_BGP_GETTIMEBASE )

/***********
 * Timing
 ***********/
#define SPRN_TBRL       0x10C       // Time Base Read Lower Register (user & sup R/O)
#define SPRN_TBRU       0x10D       // Time Base Read Upper Register (user & sup R/O)

#define _bgp_mfspr( SPRN )\
({\
   unsigned int tmp;\
   do {\
      asm volatile ("mfspr %0,%1" : "=&r" (tmp) : "i" (SPRN) : "memory" );\
      }\
      while(0);\
   tmp;\
})\

extern inline uint64_t _bgp_GetTimeBase( void )
{
  union
  {
    uint32_t ul[2];
    uint64_t ull;
  }
  hack;
  uint32_t utmp;

  do
    {
      utmp       = _bgp_mfspr( SPRN_TBRU );
      hack.ul[1] = _bgp_mfspr( SPRN_TBRL );
      hack.ul[0] = _bgp_mfspr( SPRN_TBRU );
    }
  while( utmp != hack.ul[0] );

  return( hack.ull );
}

#endif

static inline uint64_t get_rtc(void)
 {
   unsigned int hi, lo, hi2;

   do {
     asm volatile("mfrtcu %0; mfrtcl %1; mfrtcu %2"
		  : "=r" (hi), "=r" (lo), "=r" (hi2));
   } while (hi2 != hi);
   return (uint64_t)hi * 1000000000 + lo;
 }
#else
#if defined(__powerpc__)
#define TIME_TYPE ( TIME_CLOCK_GETTIME )
#else
#define TIME_TYPE ( TIME_GETTIMEOFDAY )
#endif

#endif


static
void
GetPkTime( pk_time_t* aPkTime )
{

#if ( TIME_TYPE == TIME_BGP_GETTIMEBASE )
  uint64_t TimeBase = _bgp_GetTimeBase(); 

#ifdef PK_ASFHOST00
  // double TimeVal = TimeBase / 4 704 000 000.0;
  double TimeVal = TimeBase /  512000000.0;
#else
  double TimeVal = TimeBase /  850000000.0;
#endif
  aPkTime->mSeconds = (int) TimeVal;
  aPkTime->mNanoseconds = (int) ( (TimeVal - aPkTime->mSeconds) * 1000000000.0 );
#endif

#if ( TIME_TYPE == TIME_CLOCK_GETTIME )
  struct timespec TimeNow;

  clock_gettime( CLOCK_REALTIME, & TimeNow );

  aPkTime->mSeconds     = TimeNow.tv_sec;
  aPkTime->mNanoseconds = TimeNow.tv_nsec;
#endif

#if ( TIME_TYPE == TIME_GETTIMEOFDAY )
  struct timeval TimeNow;

  gettimeofday( & TimeNow, NULL );
  aPkTime->mSeconds     = TimeNow.tv_sec;
  aPkTime->mNanoseconds = TimeNow.tv_usec;
#endif

  return;
}


inline
unsigned long long
PkTimeGetNanos()
  {
#if ( TIME_TYPE == TIME_BGP_GETTIMEBASE )
  uint64_t TimeBase = _bgp_GetTimeBase();
#ifdef PK_ASFHOST00
  // unsigned long long nanos = (unsigned long long) (TimeBase / 4.704) ;
  unsigned long long nanos = (unsigned long long) (TimeBase / 0.512) ;
#elif defined (__powerpc64__) // BG/Q
  //  unsigned long long nanos = (unsigned long long) (TimeBase / 0.85) ;   // BG/P
  //  unsigned long long nanos = (unsigned long long) (TimeBase / 1.6) ;
  unsigned long long nanos = (unsigned long long) (TimeBase * 0.625 ) ;  // multiply by 1/1.6
#else
  unsigned long long nanos = (unsigned long long) (TimeBase / 0.85) ;
#endif
#endif

#if ( TIME_TYPE == TIME_CLOCK_GETTIME )
  struct timespec TimeNow;

  clock_gettime( CLOCK_REALTIME, & TimeNow );

  unsigned long long nanos = TimeNow.tv_sec * 1000000000 + TimeNow.tv_nsec;
#endif

#if ( TIME_TYPE == TIME_GETTIMEOFDAY )
  struct timeval TimeNow;

  gettimeofday( & TimeNow, NULL );
  unsigned long long nanos = TimeNow.tv_sec * 1000000000 + TimeNow.tv_usec * 1000;
#endif

  return(nanos);
  }


/************/
#endif
