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
 * Definitions for ProxyMap.
 * This file contains method defintions for ProxyMap, a class that provides
 * a mapping from a set of proxy names (e.g. "rs1") to their associated IP
 * addresses.
 */

#include "Common/Compat.h"
#include "Common/StringExt.h"

extern "C" {
#include <string.h>
}

#include "ProxyMap.h"

using namespace Hypertable;

void ProxyMap::update_mapping(const String &proxy, const String &hostname,
                              const InetAddr &addr, ProxyMapT &invalidated_map,
			      ProxyMapT &new_map) {
  ScopedLock lock(m_mutex);
  ProxyMapT::iterator iter = m_forward_map.find(proxy);
  if (iter != m_forward_map.end() && (*iter).second.addr == addr) {
    if ((*iter).second.hostname != hostname)
      (*iter).second.hostname = hostname;
    return;
  }
  invalidate(proxy, addr, invalidated_map);
  new_map[proxy] = ProxyAddressInfo(hostname, addr);
  m_forward_map[proxy] = ProxyAddressInfo(hostname, addr);
  m_reverse_map[addr] = proxy;
}


void ProxyMap::update_mappings(String &mappings, ProxyMapT &invalidated_map,
			       ProxyMapT &new_map) {
  ScopedLock lock(m_mutex);
  char *line, *proxy, *hostname, *addr_str, *end_nl, *end_tab;

  for (line = strtok_r((char *)mappings.c_str(), "\n", &end_nl); line;
       line = strtok_r(0, "\n", &end_nl)) {
    proxy = strtok_r(line, "\t", &end_tab);
    HT_ASSERT(proxy);
    hostname = strtok_r(0, "\t", &end_tab);
    HT_ASSERT(hostname);
    addr_str = strtok_r(0, "\t", &end_tab);
    HT_ASSERT(addr_str);
    InetAddr addr(addr_str);

    ProxyMapT::iterator iter = m_forward_map.find(proxy);
    if (iter != m_forward_map.end() && (*iter).second.addr == addr) {
      if ((*iter).second.hostname != hostname)
        (*iter).second.hostname = hostname;
      continue;
    }

    invalidate(proxy, addr, invalidated_map);
    new_map[proxy] = ProxyAddressInfo(hostname, addr);
    m_forward_map[proxy] = ProxyAddressInfo(hostname, addr);
    m_reverse_map[addr] = proxy;
  }
}



bool ProxyMap::get_mapping(const String &proxy, String &hostname, InetAddr &addr) {
  ScopedLock lock(m_mutex);
  ProxyMapT::iterator iter = m_forward_map.find(proxy);
  if (iter == m_forward_map.end())
    return false;
  addr = (*iter).second.addr;
  hostname = (*iter).second.hostname;
  return true;
}

String ProxyMap::get_proxy(InetAddr &addr) {
  ScopedLock lock(m_mutex);
  SockAddrMap<String>::iterator iter = m_reverse_map.find(addr);
  if (iter != m_reverse_map.end())
    return (*iter).second;
  return "";
}


CommBuf *ProxyMap::create_update_message() {
  ScopedLock lock(m_mutex);
  String payload;
  CommHeader header;
  header.flags |= CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE;
  for (ProxyMapT::iterator iter = m_forward_map.begin(); iter != m_forward_map.end(); ++iter)
    payload += (*iter).first + "\t" + (*iter).second.hostname + "\t" + (*iter).second.addr.format() + "\n";
  CommBuf *cbuf = new CommBuf(header, payload.length());
  if (payload.length())
    cbuf->append_bytes((uint8_t *)payload.c_str(), payload.length());
  return cbuf;
}


void ProxyMap::invalidate(const String &proxy, const InetAddr &addr,
			  ProxyMapT &invalidated_map) {
  ProxyMapT::iterator iter;
  SockAddrMap<String>::iterator rev_iter;

  // Invalidate entries from forward map
  if ((iter = m_forward_map.find(proxy)) != m_forward_map.end()) {
    if ((*iter).second.addr != addr) {
      invalidated_map[(*iter).first] = (*iter).second;
      m_reverse_map.erase( (*iter).second.addr );
      m_forward_map.erase((*iter).first);
    }
  }

  // Invalidate entries from reverse map
  if ((rev_iter = m_reverse_map.find(addr)) != m_reverse_map.end()) {
    if ((*rev_iter).second != proxy) {
      invalidated_map[(*rev_iter).second] = ProxyAddressInfo("unknown", (*rev_iter).first);
      m_forward_map.erase((*rev_iter).second);
      m_reverse_map.erase((*rev_iter).first);
    }
  }

}
