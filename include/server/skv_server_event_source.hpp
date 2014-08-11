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

#ifndef __SKV_SERVER_EVENT_SOURCE_HPP__
#define __SKV_SERVER_EVENT_SOURCE_HPP__

#define SKV_SERVER_EVENT_SOURCE_MAX_PRIORITY 64
#define SKV_SERVER_EVENT_SOURCE_DEFAULT_PRIO 1   // default priority (will cause check for events every time)


// generic base class for unified event sources
class skv_server_generic_event_source_t
{
  int              mPriority;

public:
  virtual
  skv_status_t GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount ) = 0;

  inline
  void SetPriority( int aPrio )
  {
    AssertLogLine( (aPrio > 0) && (aPrio < SKV_SERVER_EVENT_SOURCE_MAX_PRIORITY) )
      << "skv_server_event_source_t::SetPriority(): Priority out of range:"
      << " requested: " << aPrio
      << " limits: min="<< 0 << " max=" << SKV_SERVER_EVENT_SOURCE_MAX_PRIORITY
      << EndLogLine;

    mPriority = aPrio;
  }

  inline
  int GetPriority() { return mPriority; }

  virtual ~skv_server_generic_event_source_t() { };
};


template<class event_manager_t>
class skv_server_event_source_t :
  public skv_server_generic_event_source_t
{
protected:
  event_manager_t  *mEventManager;

public:
  skv_status_t Init( event_manager_t *aEVMgr,
                      int aPrio = SKV_SERVER_EVENT_SOURCE_DEFAULT_PRIO )
  {
    SetPriority( aPrio );
    SetEventManager( aEVMgr );
    return SKV_SUCCESS;
  }

  inline
  void SetEventManager( event_manager_t *aEVMgr )
  {
    mEventManager = aEVMgr;
  }

  inline
  event_manager_t
  GetEventManager() { return mEventManager; }

  // since the check for new content depends on the chunk-class, we force users to overload this routine
  virtual skv_status_t
  GetEvent( skv_server_event_t* aEvents, int* aEventCount, int aMaxEventCount ) = 0;
};

#endif // __SKV_SERVER_EVENT_SOURCE_HPP__
