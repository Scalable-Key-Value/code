/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     tjcw - initial implementation
 */
#ifndef _MAPEPOLL_H_
#if defined(USE_EPOLL)
#include <sys/epoll.h>
typedef int mapepfd_t ;
static mapepfd_t mapepoll_create(int size) { return epoll_create(size) ; }
static int mapepoll_ctl(mapepfd_t epfd, int op, int fd, struct epoll_event *event)
  {
    return epoll_ctl(epfd, op, fd, event) ;
  }
static int mapepoll_wait(mapepfd_t epfd, struct epoll_event *events, int maxevents, int timeout)
  {
    return epoll_wait(epfd, events, maxevents, timeout) ;
  }
#else
#include <sys/epoll.h>
#include <poll.h>

#ifndef FXLOG_MAPEPOLL
#define FXLOG_MAPEPOLL (0)
#endif

#ifndef FXLOG_MAPEPOLL_CONTROL
#define FXLOG_MAPEPOLL_CONTROL (0)
#endif

typedef int mapepfd_t ;
typedef struct {
  int fd ;
  struct epoll_event event ;
} mapepoll_event_t ;
static mapepoll_event_t *mapepoll_event ;
static int mapepoll_event_count ;
static int mapepoll_size ;
static mapepfd_t mapepoll_create(int size)
  {
    BegLogLine(FXLOG_MAPEPOLL_CONTROL)
        << "mapepoll_create(size=" << size << ")"
        << EndLogLine ;
     mapepoll_event = (mapepoll_event_t *)malloc(size*sizeof(mapepoll_event_t )) ;
     mapepoll_size = size ;
     mapepoll_event_count = 0 ;
     return 0 ;
  }
static int mapepoll_ctl(mapepfd_t epfd, int op, int fd, struct epoll_event *event)
  {
    BegLogLine(FXLOG_MAPEPOLL_CONTROL)
        << "mapepoll_ctl(epfd=" << epfd
        << ",op=" << op
        << ",fd=" << fd
        << ",event=" << event
        << ") mapepoll_event_count=" << mapepoll_event_count
        << EndLogLine ;
    switch ( op )
      {
    case EPOLL_CTL_ADD :
      StrongAssertLogLine(mapepoll_event_count < mapepoll_size) << EndLogLine ;
      mapepoll_event[mapepoll_event_count].event = *event ;
      mapepoll_event[mapepoll_event_count].fd = fd ;
      mapepoll_event_count += 1 ;
      break ;
    case EPOLL_CTL_DEL :
      for(int a=0; a<mapepoll_event_count;a+=1)
        {
          if (mapepoll_event[a].fd == fd)
            {
              for ( int b = a; b < mapepoll_event_count-1;b+=1)
                {
                  mapepoll_event[b]=mapepoll_event[b+1] ;
                }
              mapepoll_event_count = mapepoll_event_count-1 ;
              break ;
            }
        }
      break ;
    default :
      StrongAssertLogLine(0)
        << "Unknown epoll op " << op
        << EndLogLine ;
      }
    return 0 ;
  }
static int mapepoll_wait(mapepfd_t epfd, struct epoll_event *events, int maxevents, int timeout)
  {
    struct pollfd pollfds[mapepoll_event_count] ;
    BegLogLine(0)
      << "epfd=" << epfd
      << " events=" << events
      << " maxevents=" << maxevents
      << " timeout=" << timeout
      << " mapepoll_event_count=" << mapepoll_event_count
      << EndLogLine ;
    for(int a=0;a<mapepoll_event_count;a+=1)
      {
        pollfds[a].fd = mapepoll_event[a].fd ;
        pollfds[a].events = POLLIN ;
      }
    int rc=poll(pollfds,mapepoll_event_count,timeout) ;
    if ( rc <= 0 ) return rc ;
    BegLogLine(FXLOG_MAPEPOLL)
      << "poll rc=" << rc
      << EndLogLine ;
    int c = 0 ;
    for(int b=0;b<mapepoll_event_count; b += 1)
      {
        int revents = pollfds[b].revents ;
        if(revents != 0 )
          {
            events[c].data=mapepoll_event[b].event.data ;
            events[c].events = (( revents & POLLIN) && EPOLLIN )
                                | ((revents & POLLHUP) && EPOLLRDHUP);
            BegLogLine(FXLOG_MAPEPOLL)
              << "events[" << c
              << "]=(data.fd=" << events[c].data.fd
              << ",events=" << events[c].events
              << ")"
              << EndLogLine ;
            c += 1 ;
            if ( c >= maxevents) break ;
          }
      }
    return rc ;
  }
#endif
#endif
