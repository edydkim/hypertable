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
 * Declarations for HandlerMap.
 * This file contains type declarations for HandlerMap, a data structure
 * for mapping socket addresses to I/O handlers.
 */

#ifndef HYPERTABLE_HANDLERMAP_H
#define HYPERTABLE_HANDLERMAP_H

#include <cassert>

//#define HT_DISABLE_LOG_DEBUG

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/Time.h"
#include "Common/Timer.h"

#include "CommAddress.h"
#include "CommBuf.h"
#include "IOHandlerData.h"
#include "IOHandlerDatagram.h"
#include "ProxyMap.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  class IOHandlerAccept;

  /** Data structure for mapping socket addresses to I/O handlers.
   * An I/O handler is associated with each socket connection and is used
   * to respond to polling events on the socket descriptor. This class
   * maintains three maps, one for TCP socket connections, UDP socket
   * connections, and one for accept sockets.  The Comm methods use
   * this map to locate the I/O handler for a given address.
   */
  class HandlerMap : public ReferenceCount {

  public:

    /** Constructor. */
    HandlerMap() : m_proxies_loaded(false) { }

    /** Inserts an accept handler.
     * Uses IOHandler#m_local_addr as the key
     * @param handler Accept I/O handler to insert
     * @return Error::OK
     */
    int32_t insert_handler(IOHandlerAccept *handler);

    /** Inserts a data (TCP) handler.
     * Uses IOHandler#m_addr as the key.  If program is the proxy master,
     * a proxy map update message with the new mapping is broadcast to
     * all connections.
     * @param handler Data (TCP) I/O handler to insert
     * @return Error::OK on success, or Error::COMM_BROKEN_CONNECTION on
     * proxy map broadcast failure.
     */
    int32_t insert_handler(IOHandlerData *handler);

    /** Inserts a datagram (UDP) handler.
     * Uses IOHandler#m_local_addr as the key.
     * @param handler Datagram (UDP) I/O handler to insert
     * @return Error::OK
     */
    int32_t insert_handler(IOHandlerDatagram *handler);

    /** Checks out accept I/O handler associated with <code>addr</code>.
     * First translates <code>addr</code> to socket address and then
     * looks up translated address in accept map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return Error::OK on success, Error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no translation
     * exists, or Error::COMM_NOT_CONNECTED if no mapping found for
     * translated address.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerAccept **handler);

    /** Checks out data (TCP) I/O handler associated with <code>addr</code>.
     * First translates <code>addr</code> to socket address and then
     * looks up translated address in data map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return Error::OK on success, Error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no translation
     * exists, or Error::COMM_NOT_CONNECTED if no mapping found for
     * translated address.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerData **handler);

    /** Checks out datagram (UDP) I/O handler associated with <code>addr</code>.
     * First translates <code>addr</code> to socket address and then
     * looks up translated address in datagram map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return Error::OK on success, Error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no translation
     * exists, or Error::COMM_NOT_CONNECTED if no mapping found for
     * translated address.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerDatagram **handler);

    /** Checks to see if <code>addr</code> is contained in map.
     * First translates <code>addr</code> to socket address and then
     * looks up translated address in data map.
     * @param addr Connection address
     * @return Error::OK if found, Error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no translation
     * exists, or Error::COMM_NOT_CONNECTED if no mapping found for
     * translated address.
     */
    int contains_data_handler(const CommAddress &addr);

    void decrement_reference_count(IOHandler *handler);

    int set_alias(const InetAddr &addr, const InetAddr &alias);

    int remove_handler(IOHandler *handler);

    void decomission_handler(IOHandler *handler);

    void decomission_all();

    bool destroy_ok(IOHandler *handler);

    bool translate_proxy_address(const CommAddress &proxy_addr, CommAddress &addr);

    void purge_handler(IOHandler *handler);

    void wait_for_empty();

    int add_proxy(const String &proxy, const String &hostname, const InetAddr &addr);

    /**
     * Returns the proxy map
     *
     * @param proxy_map reference to proxy map to be filled in
     */
    void get_proxy_map(ProxyMapT &proxy_map);

    void update_proxy_map(const char *message, size_t message_len);

    bool wait_for_proxy_map(Timer &timer);

  private:

    int propagate_proxy_map(ProxyMapT &mappings);

    /**
     * Translates CommAddress into InetAddr (IP address)
     */
    int translate_address(const CommAddress &addr, InetAddr *inet_addr);

    int remove_handler_unlocked(IOHandler *handler);

    IOHandlerAccept *lookup_accept_handler(const InetAddr &addr);

    IOHandlerData *lookup_data_handler(const InetAddr &addr);

    IOHandlerDatagram *lookup_datagram_handler(const InetAddr &addr);

    /// %Mutex for serializing concurrent access
    Mutex m_mutex;

    /// Condition variable for signalling empty map
    boost::condition m_cond;

    /// Condition variable for signalling proxy map load
    boost::condition m_cond_proxy;

    /// Accept map (InetAddr-to-IOHandlerAccept)
    SockAddrMap<IOHandlerAccept *> m_accept_handler_map;

    /// Data (TCP) map (InetAddr-to-IOHandlerData)
    SockAddrMap<IOHandlerData *> m_data_handler_map;

    /// Datagram (UDP) map (InetAddr-to-IOHandlerDatagram)
    SockAddrMap<IOHandlerDatagram *> m_datagram_handler_map;

    /// Decomissioned handler set
    std::set<IOHandler *> m_decomissioned_handlers;

    /// Proxy map
    ProxyMap m_proxy_map;

    /// Flag indicating if proxy map has been loaded
    bool m_proxies_loaded;
  };

  /// Smart pointer to handler map
  typedef boost::intrusive_ptr<HandlerMap> HandlerMapPtr;

  /** @}*/
}


#endif // HYPERTABLE_HANDLERMAP_H
