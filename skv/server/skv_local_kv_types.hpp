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
 */

#ifndef SKV_LOCAL_KV_TYPES_HPP_
#define SKV_LOCAL_KV_TYPES_HPP_

class skv_local_kv_request_ctx_t
{
private:
  int mReqOrdinal;

public:
  inline int GetOrdinal() const { return mReqOrdinal; }

  inline void SetOrdinal( int reqOrdinal ) { mReqOrdinal = reqOrdinal; }
};

class skv_local_kv_cookie_t
{
private:
  unsigned short mOrd;
  skv_server_ep_state_t* mEPState;
  /* \todo
   * EPstate + Ordinal define the command control block
   * This is the current way used by SKV because of tight integration of the EPState
   * it should and will change at a later stage...
   */
public:
  skv_local_kv_cookie_t(unsigned short aOrd = 0,
                        skv_server_ep_state_t *aEPState = NULL)
  {
    mOrd = aOrd;
    mEPState = aEPState;
  }
  inline
  void Set(unsigned short aOrd = 0,
      skv_server_ep_state_t *aEPState = NULL)
  {
    mOrd = aOrd;
    mEPState = aEPState;
  }
  inline
  unsigned GetOrdinal()
  {
    return mOrd;
  }
  inline
  skv_server_ep_state_t* GetEPState()
  {
    return mEPState;
  }
};

template<class streamclass>
static streamclass&
operator<<( streamclass& os, skv_local_kv_cookie_t& A )
{
  skv_server_ep_state_t *EPState = A.GetEPState();
  int Ord   = A.GetOrdinal();

  os << "kv_cookie["
     << Ord << ' '
     << (void *)EPState
     << " ]";

  return(os);
}

struct skv_local_kv_event_t
{
  skv_server_event_type_t mType;
  skv_local_kv_cookie_t mCookie;
};

#endif /* SKV_LOCAL_KV_TYPES_HPP_ */
