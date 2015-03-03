/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_server_command_event_buffer.hpp
 *
 *  Created on: Dec 22, 2014
 *      Author: lschneid
 */

#ifndef SKV_SERVER_SKV_SERVER_COMMAND_EVENT_BUFFER_HPP_
#define SKV_SERVER_SKV_SERVER_COMMAND_EVENT_BUFFER_HPP_

#ifndef SKV_SERVER_EVENT_BUFFER_LOG
#define SKV_SERVER_EVENT_BUFFER_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_GET_COMMAND_LOG
#define SKV_SERVER_GET_COMMAND_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#ifndef SKV_SERVER_COMMAND_POLLING_LOG
#define SKV_SERVER_COMMAND_POLLING_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#define SKV_SERVER_COMMAND_EVENT_BUFFER_COUNT 2048
#define POLL_PER_EP SKV_MAX_COMMANDS_PER_EP

/*
 * class for command event buffer for consistent event extraction out of endpoints
 * intentionally, member functions are not thread safe
 * to all for thread safe operation, the class contains a mutex and member functions
 * to control the flow between threads (AllowFill, PreventFill, StartFill, StopFill, TryStartFill)
 */

class skv_server_command_event_buffer_t
{
  skv_server_event_t *mEventBuffer;
  int mEventEntries;
  volatile int mEventIndex;    // needs to point to the first empty slot

public:
  skv_server_command_event_buffer_t( int aEventEntries = SKV_SERVER_EVENTS_MAX_COUNT )
  {
    mEventBuffer = new skv_server_event_t[ aEventEntries ];

    BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
      << "Creating new event buffer: @" << (void*)mEventBuffer
      << " entries: " << aEventEntries
      << " alignment: " << (int)((uintptr_t)mEventBuffer & (uintptr_t)0xffff)
      << " len: " << aEventEntries * sizeof(skv_server_event_t)
      << EndLogLine;

    mEventEntries = aEventEntries;
    mEventIndex = 0;
  }
  ~skv_server_command_event_buffer_t()
  {
    BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
      << "Destroying event buffer: @" << (void*)mEventBuffer
      << EndLogLine;
    delete mEventBuffer;
  }
  inline int GetSpace() const { return mEventEntries - mEventIndex; }
  inline skv_server_event_t* GetEntry() const { return &( mEventBuffer[ mEventIndex ] ); }
  inline int GetEventCount() const { return mEventIndex; }
  inline skv_server_event_t* GetBuffer() const { return mEventBuffer; }
  inline bool Advance()
  {
    bool ret = ( GetSpace() > 0 );
    if( GetSpace() > 0 )
      mEventIndex++;
    return ret;
  }
  inline void StepBackOne() { if( mEventIndex > 0) mEventIndex--; }
  inline void Reset()
  {
    mEventIndex = 0;
//    memset( mEventBuffer, 0, sizeof( skv_server_event_t ) * SKV_SERVER_EVENTS_MAX_COUNT );
  }
  inline skv_server_event_t* GetEntryAndAdvance()
  {
    if( GetSpace() > 0 )
    {
      skv_server_event_t *event = &( mEventBuffer[ mEventIndex ] );
      mEventIndex++;
      return event;
    }
    return NULL;
  }

#ifdef SKV_UNIT_TEST
  int test_fill();
#endif // SKV_UNIT_TEST
};



class skv_server_command_event_buffer_list_t
{
  skv_server_command_event_buffer_t *mBuffers;
  skv_server_command_event_buffer_t *mCurrentBuffer;
  skv_server_command_event_buffer_t *mReadyBuffer;
  int mBufferCount;
  volatile int mCurrentBufferIndex;
  volatile int mReadyBufferIndex;
  skv_mutex_t mUpdateMutex;
public:
  volatile uint64_t mCmdCounter;
  volatile uint64_t mFtcCounter;

public:
  skv_server_command_event_buffer_list_t( int aBufferCount = SKV_SERVER_COMMAND_EVENT_BUFFER_COUNT )
  {
    mBuffers = new skv_server_command_event_buffer_t[ aBufferCount ];
    mBufferCount = aBufferCount;
    mCurrentBufferIndex = 0;
    mReadyBufferIndex = 0;
    mCurrentBuffer = mBuffers;
    mReadyBuffer = mCurrentBuffer;
    mCmdCounter = 0;
    mFtcCounter = 0;
  }
  ~skv_server_command_event_buffer_list_t()
  {
    delete mBuffers;
  }
private:
  /*
   * has to stay private since it will require synchronization with other parts of the buffer mgmt
   */
  inline void ReadyBufferAdvance()
  {
    mReadyBufferIndex = ( mReadyBufferIndex + 1) % mBufferCount;
    mReadyBuffer = &( mBuffers[ mReadyBufferIndex ] );
  }

public:
  inline bool CurrentBufferAdvance()
  {
    int newCurrentBufferIndex = ( mCurrentBufferIndex + 1) % mBufferCount;
    bool ret = ( newCurrentBufferIndex != mReadyBufferIndex );
    if( ret )
      {
      mCurrentBufferIndex = newCurrentBufferIndex;
      mCurrentBuffer = &( mBuffers[ mCurrentBufferIndex ] );
      }
    return ret;
  }

  /*
   * gets called by the SKV main process whenever it is ready to process the next set of events
   * it needs to be synchronized with the preparation of events to prevent change of buffers during preparation
   * function returns the pointer to the "ready-buffer"
   */
  skv_server_command_event_buffer_t* GetAndFreezeReadyBuffer()
  {
    mUpdateMutex.lock();
    int evts = mReadyBuffer->GetEventCount();

    if( evts )
    {
      BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
        << "Frozen ..."
        << "Freeze: Found: "<< mCmdCounter
        << " tb:Fetched: " << mFtcCounter+evts
        << " Now: " << evts
        << " : " << mReadyBuffer->GetEventCount()
        << " cIDX:rIDX: " << mCurrentBufferIndex << ":" << mReadyBufferIndex
        << EndLogLine;

      // if currentbuffer is the same, then use the next buffer to continue fetching
      if( mReadyBufferIndex == mCurrentBufferIndex )
      {
        CurrentBufferAdvance();
        BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
          << "GetAndFreeze buffer collision. cIDX:rIDX: " << mCurrentBufferIndex << ":" << mReadyBufferIndex
          << EndLogLine;
      }
    }
    return mReadyBuffer;
  }
  void UnfreezeAndAdvanceReadyBuffer()
  {
    int evts = mReadyBuffer->GetEventCount();
    if( evts != 0 )
    {
      mReadyBuffer->Reset();
      ReadyBufferAdvance();
      BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
        << "Unfreeze+Advance... Updated Found: "<< mCmdCounter
        << " Fetched: " << mFtcCounter
        << " cIDX:rIDX: " << mCurrentBufferIndex << ":" << mReadyBufferIndex
        << EndLogLine;
    }
    mUpdateMutex.unlock();
  }
  inline skv_server_command_event_buffer_t* GetReadyEventBuffer() const
  {
    return mReadyBuffer;
  }
  inline skv_server_command_event_buffer_t* GetCurrentEventBuffer() const
  {
    return mCurrentBuffer;
  }
  inline int GetBufferDistance() const
  {
    return ( mCurrentBufferIndex+SKV_SERVER_COMMAND_EVENT_BUFFER_COUNT - mReadyBufferIndex ) % SKV_SERVER_COMMAND_EVENT_BUFFER_COUNT;
  }
  inline bool CurrentBufferIsFull() const
  {
    return ( mCurrentBuffer->GetSpace() < 1 );
  }

  int FillCurrentEventBuffer( skv_server_ep_state_t *aEP )
  {
    mUpdateMutex.lock();
#if (SKV_SERVER_EVENT_BUFFER_LOG != 0)
    static int state_changed = 0;
    bool print = ( state_changed != mCurrentBuffer->GetEventCount() );

    BegLogLine( (SKV_SERVER_EVENT_BUFFER_LOG & print) )
      << " Starting to fill buffer:"
      << " cIDX:rIDX: " << mCurrentBufferIndex << ":" << mReadyBufferIndex
      << " EP: 0x" << (void*)aEP
      << " EPCmdIdx: " << aEP->GetCurrentCommandSlot()
      << EndLogLine;
#endif

    skv_server_command_event_buffer_t *currentBuffer = mCurrentBuffer;
    int cbi = mCurrentBufferIndex;

    int cmdCount = 0;
    while( ( aEP->CheckForNewCommands() )   // got a valid command?
        && ( !CurrentBufferIsFull() )       // space to store event?
        && ( cmdCount < POLL_PER_EP ) )     // balance between EPs
    {
      skv_server_event_t *Event = currentBuffer->GetEntryAndAdvance();

      // shouldn't happen since we test for GetSpace() < 1. However, better save than sorry...
      if( !Event ) break;

      BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
        << "FillCurrentEventBuffer: Found: "<< mCmdCounter
        << " Fetched: " << mFtcCounter
        << " cIDX:CIDX:rIDX: " << cbi << ":" << mCurrentBufferIndex << ":" << mReadyBufferIndex
        << " evcnt: " << currentBuffer->GetEventCount()
        << " cBuf@: " << (void*)currentBuffer
        << EndLogLine;

      skv_status_t status = PrepareEvent( Event, aEP );
      if( status == SKV_ERRNO_COMMAND_LIMIT_REACHED )
      {
        BegLogLine( SKV_SERVER_COMMAND_POLLING_LOG )
          << "skv_server_command_evnet_buffer::FillCurrentEventBuffer(): "
          << " CMD Limit reached. Stalling EP: @" << (void*)aEP
          << EndLogLine;

        status = SKV_SUCCESS;
        currentBuffer->StepBackOne();

        break;   // continue attempt with next EP
      }

      StrongAssertLogLine( status == SKV_SUCCESS )
        << "skv_server_t::GetCommand(): "
        << "ERROR initializing fetched command at slot: " << aEP->GetCurrentCommandSlot()
        << EndLogLine;
#if (SKV_SERVER_COMMAND_POLLING_LOG != 0)
        if( ( status == SKV_SUCCESS ) || ( status == SKV_ERRNO_COMMAND_LIMIT_REACHED ) )
        {
          static int state_has_changed = 1;
        }
#endif

      // set index to next slot
      aEP->CommandSlotAdvance();
      cmdCount++;
      mCmdCounter++;

      BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
        << "FillCurrentEventBuffer: found event."
        << " filling into:" << cbi
        << " rIdx:" << mReadyBufferIndex
        << " cmdCnt:" << cmdCount
        << " ev@" << (void*)Event
        << " EP:" << (void*)aEP
        << " EvType:" << skv_server_event_type_to_string( Event->mEventType )
        << EndLogLine;
    }

//    print = ( state_changed != mCurrentBuffer->GetEventCount() );
//    BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG & print )
//      << " finished fill, unlocking. cmds: " << cmdCount
//      << " evts: " << mCurrentBuffer->GetEventCount()
//      << " cIDX:rIDX: " << ":" << mCurrentBufferIndex << ":" << mReadyBufferIndex
//      << EndLogLine;

#if (SKV_SERVER_EVENT_BUFFER_LOG != 0)
    state_changed = mCurrentBuffer->GetEventCount();
#endif

    if( CurrentBufferIsFull() )
    {
      if( ! CurrentBufferAdvance() )
      {
        BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
          << "Event buffers full..."
          << EndLogLine;
        mUpdateMutex.unlock();
        usleep( 100 );
        mUpdateMutex.lock();
      }

      BegLogLine( SKV_SERVER_EVENT_BUFFER_LOG )
        << "Buffer is full. advancing to: " << mCurrentBufferIndex
        << EndLogLine;
    }

    mUpdateMutex.unlock();
    return cmdCount;
  }

private:
  skv_status_t
  PrepareEvent( skv_server_event_t *currentEvent, skv_server_ep_state_t *aEPState );
};

#endif /* SKV_SERVER_SKV_SERVER_COMMAND_EVENT_BUFFER_HPP_ */
