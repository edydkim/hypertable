/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Declarations for ConnectionManager.
 * This file contains type declarations for ConnectionManager, a class for
 * establishing and maintaining TCP connections.
 */

#ifndef HYPERTABLE_CONNECTIONMANAGER_H
#define HYPERTABLE_CONNECTIONMANAGER_H

#include <queue>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "Common/HashMap.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/Timer.h"

#include "Comm.h"
#include "CommAddress.h"
#include "ConnectionInitializer.h"
#include "DispatchHandler.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  class Event;

  /**
   * Establishes and maintains a set of TCP connections.  If any of the
   * connections gets broken, then this class will continuously attempt
   * to re-establish the connection, pausing for a while in between attempts.
   */
  class ConnectionManager : public DispatchHandler {

  public:

    /** Per-connection state.
     */
    class ConnectionState : public ReferenceCount {
    public:
      /// Set to <i>true</i> if connected
      bool                connected;
      /// Set when connection is removed, prevents connect retry attempts
      bool                decomissioned;
      /// Connection address supplied to #add method
      CommAddress         addr;
      /// Local address to bind to
      CommAddress         local_addr;
      /// Address initialized from Event object
      InetAddr            inet_addr;
      /// Retry connection attempt after this many milliseconds
      uint32_t            timeout_ms;
      /// Registered connection handler
      DispatchHandlerPtr  handler;
      /// Connection initializer
      ConnectionInitializerPtr initializer;
      /// Set to <i>true</i> if initialization handshake is complete
      bool                initialized;
      /// Mutex to serialize concurrent access
      Mutex               mutex;
      /// Condition variable used to signal connection state change
      boost::condition    cond;
      /// Absolute time of next connect attempt
      boost::xtime        next_retry;
      /// Service name of connection for log messages
      std::string         service_name;
    };
    /// Smart pointer to ConnectionState
    typedef intrusive_ptr<ConnectionState> ConnectionStatePtr;

    /** StringWeakOrdering for connection retry heap
     */
    struct LtConnectionState {
      bool operator()(const ConnectionStatePtr &cs1,
                      const ConnectionStatePtr &cs2) const {
        return xtime_cmp(cs1->next_retry, cs2->next_retry) > 0;
      }
    };

    /** Connection manager state for sharing between Connection manager objects.
     */
    class SharedImpl : public ReferenceCount {
    public:

      /** Destructor.
       */
      ~SharedImpl() {
        shutdown = true;
        retry_cond.notify_one();
        thread->join();
      }

      /// Pointer to Comm layer
      Comm *comm;
      /// Mutex to serialize concurrent access
      Mutex mutex;
      /// Condition variable to signal if anything is on the retry heap
      boost::condition retry_cond;
      /// Pointer to connection manager thread object
      boost::thread *thread;
      /// InetAddr-to-ConnectionState map
      SockAddrMap<ConnectionStatePtr> conn_map;
      /// Proxy-to-ConnectionState map
      hash_map<String, ConnectionStatePtr> conn_map_proxy;
      /// Connect retry heap
      std::priority_queue<ConnectionStatePtr, std::vector<ConnectionStatePtr>,
          LtConnectionState> retry_queue;
      /// Set to <i>true</i> to prevent connect failure log message
      bool quiet_mode;
      /// Set to <i>true</i> to signal shutdown in progress
      bool shutdown;
    };

    /// Smart pointer to SharedImpl object
    typedef boost::intrusive_ptr<SharedImpl> SharedImplPtr;

    /**
     * Constructor.  Creates a thread to do connection retry attempts.
     *
     * @param comm Pointer to the comm object
     */
    ConnectionManager(Comm *comm = 0) {
      m_impl = new SharedImpl;
      m_impl->comm = comm ? comm : Comm::instance();
      m_impl->quiet_mode = false;
      m_impl->shutdown = false;
      m_impl->thread = new boost::thread(*this);
    }

    /**
     * Copy Constructor.  Shares the implementation with object being copied.
     */
    ConnectionManager(const ConnectionManager &cm) {
      m_impl = cm.m_impl;
      intrusive_ptr_add_ref(this);
    }

    /**
     * Destructor.
     */
    virtual ~ConnectionManager() { }

    /**
     * Adds a connection to the connection manager.  The <code>addr</code>
     * parameter holds an address that the connection manager should maintain a
     * connection to.  This method first checks to see if the address is
     * already registered with the connection manager and returns immediately
     * if it is.  Otherwise, it adds the address to an internal connection map,
     * attempts to establish a connection to the address, and then returns.
     * From here on out, the internal manager thread will maintian the
     * connection by continually re-establishing the connection if it ever gets
     * broken.
     *
     * @param addr The address to maintain a connection to
     * @param timeout_ms When connection dies, wait this many milliseconds
     *        before attempting to reestablish
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     */
    void add(const CommAddress &addr, uint32_t timeout_ms,
             const char *service_name);

    /**
     * Same as above method except installs a dispatch handler on the connection
     *
     * @param addr The address to maintain a connection to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     */
    void add(const CommAddress &addr, uint32_t timeout_ms,
             const char *service_name, DispatchHandlerPtr &handler);

    /**
     * Same as above method except installs a connection initializer
     *
     * @param addr Address to maintain a connection to
     * @param timeout_ms Timeout value (in milliseconds) that gets passed into
     * Comm#connect and also used as the waiting period between connection
     * attempts
     * @param service_name Name of the serivce at the other end of the
     * connection used for descriptive log messages
     * @param handler Default event handler for connection.
     * @param initializer Connection initialization handshake driver
     */
    void add_with_initializer(const CommAddress &addr, uint32_t timeout_ms,
                              const char *service_name,
                              DispatchHandlerPtr &handler,
                              ConnectionInitializerPtr &initializer);

    /**
     * Adds a connection to the connection manager with a specific local
     * address.  The address structure addr holds an address that the
     * connection manager should maintain a connection to.  This method first
     * checks to see if the address is already registered with the connection
     * manager and returns immediately if it is.  Otherwise, it adds the
     * address to an internal connection map, attempts to establish a
     * connection to the address, and then returns.  From here on out, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms When connection dies, wait this many
     *        milliseconds before attempting to reestablish
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     */
    void add(const CommAddress &addr, const CommAddress &local_addr,
             uint32_t timeout_ms, const char *service_name);

    /**
     * Same as above method except installs a dispatch handler on the connection
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     */
    void add(const CommAddress &addr, const CommAddress &local_addr,
             uint32_t timeout_ms, const char *service_name,
             DispatchHandlerPtr &handler);

    /**
     * Removes a connection from the connection manager
     *
     * @param addr remote address of connection to remove
     * @return Error code (Error::OK on success)
     */
    int remove(const CommAddress &addr);

    /**
     * This method blocks until the connection to the given address is
     * established.  The given address must have been previously added with a
     * call to Add.  If the connection is not established within
     * max_wait_ms, then the method returns false.
     *
     * @param addr the address of the connection to wait for
     * @param max_wait_ms The maximum time to wait for the connection before
     *        returning
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(const CommAddress &addr, uint32_t max_wait_ms);

    /**
     * This method blocks until the connection to the given address is
     * established.  The given address must have been previously added with a
     * call to Add.  If the connection is not established before the timer
     * expires, then the method returns false.
     *
     * @param addr the address of the connection to wait for
     * @param timer running timer object
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(const CommAddress &addr, Timer &timer);

    /**
     * Returns the Comm object associated with this connection manager
     *
     * @return the assocated comm object
     */
    Comm *get_comm() { return m_impl->comm; }

    /**
     * This method sets a 'quiet_mode' flag which can disable the generation
     * of log messages upon failed connection attempts.  It is set to false
     * by default.
     *
     * @param mode The new value for the quiet_mode flag
     */
    void set_quiet_mode(bool mode) { m_impl->quiet_mode = mode; }

    /**
     * This is the comm layer dispatch callback method.  It should only get
     * called by the AsyncComm subsystem.
     */
    virtual void handle(EventPtr &event);

    /**
     * This is the Boost thread "run" method.  It is called by the manager
     * thread when it starts up.
     */
    void operator()();

  private:

    void add_internal(const CommAddress &addr, const CommAddress &local_addr,
                      uint32_t timeout_ms, const char *service_name,
                      DispatchHandlerPtr &handler,
                      ConnectionInitializerPtr &initializer);

    bool wait_for_connection(ConnectionState *conn_state, Timer &timer);

    void send_connect_request(ConnectionState *conn_state);

    void set_retry_state(ConnectionState *conn_state, EventPtr &event);

    SharedImplPtr m_impl;

  };
  /// Smart pointer to ConnectionManager
  typedef boost::intrusive_ptr<ConnectionManager> ConnectionManagerPtr;

  /** @}*/
}

#endif // HYPERTABLE_CONNECTIONMANAGER_H
