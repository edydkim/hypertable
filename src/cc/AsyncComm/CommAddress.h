/* -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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
 * Declarations for CommAddress.
 * This file contains type declarations for CommAddress, and abstraction class
 * for holding an arbitrary address type.
 */

#ifndef HYPERTABLE_COMMADDRESS_H
#define HYPERTABLE_COMMADDRESS_H

#include <set>

#include "Common/HashMap.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/String.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Address abstraction to hold either proxy name or IPv4:port address.
   * Proxy names are string mnemonics referring to IPv4:port addresses and are
   * defined or modified via Comm#add_proxy method.  Most Comm methods that
   * operate on an address are abstracted to accept either a proxy name
   * (translated internally) or an IPv4:port address.  The CommAddress class is
   * used to facilitate that abstraction.
   */
  class CommAddress {
  public:

    /** Enumeration for address type.
     */
    enum AddressType { 
      NONE=0, /**< Uninitialized */
      PROXY,  /**< Proxy name type */
      INET    /**< IPv4:port address type */
    };

    /** Constructs an uninitialized CommAddress object.
     */
    CommAddress() : m_type(NONE) { }

    /** Constructs a CommAddress object of type CommAddress#INET.
     * @param addr IPv4:port address
     */
    CommAddress(const sockaddr_in addr) : m_type(INET) { inet=addr; }

    /** Sets address type to CommAddress#PROXY and #proxy name to
     * <code>p</code>.
     * @param str proxy name
     */
    void set_proxy(const String &str) { proxy = str; m_type=PROXY; }

    /** Sets address type to CommAddress#INET and #inet value to
     * <code>addr</code>.
     * @param addr IPv4:port address
     */
    void set_inet(sockaddr_in addr) { inet = addr; m_type=INET; }

    /** Sets address type to CommAddress#INET and #inet value to
     * <code>addr</code>.
     * @param addr IPv4:port address
     * @return reference to this CommAddress object
     */
    CommAddress &operator=(sockaddr_in addr) 
      { inet = addr; m_type=INET; return *this; }

    /** Equality operator.
     * If address is of type CommAddress#PROXY,
     * <code>std::string::compare</code> is used to compare #proxy members.  If
     * address is of type CommAddress#INET, InetAddr#operator== is used to
     * compare #inet members.  If both addresses are of type CommAddress#NONE,
     * <i>true</i> is returned.  <i>false</i> is returned if the addresses
     * are of different types.
     * @param other object to compare to
     * @return <i>true</i> if addresses are equal, <i>false</i> otherwise
     */
    bool operator==(const CommAddress &other) const {
      if (m_type != other.m_type)
        return false;
      if (m_type == PROXY)
	return proxy.compare(other.proxy) == 0;
      else if (m_type == INET)
        return inet == other.inet;
      return true;
    }

    /** Inequality operator.
     * Returns the exact opposite of what is returned by #operator==
     * @param other object to compare to
     * @return <i>true</i> if addresses are not equal, <i>false</i> otherwise
     */
    bool operator!=(const CommAddress &other) const {
      return !(*this == other);
    }

    /** Less than operator.
     * If address types differ, then an integer less than comparison of the
     * AddressType values (#m_type) is returned.  If addresses are of type
     * CommAddress#PROXY, then string less than comparison of the #proxy
     * members is returned.  If addresses are of type CommAddress#INET then
     * Inet#operator< is used to compare the IP addresses, and if addresses are
     * of type CommAddress#NONE, <i>false</i> is returned.
     * @param other object on right-hand side of comparison
     * @return <i>true</i> if address is less than <code>other</code>,
     * <i>false</i> otherwise
     */
    bool operator<(const CommAddress &other) const {
      if (m_type != other.m_type)
	return m_type < other.m_type;
      if (m_type == PROXY)
	return proxy < other.proxy;
      else if (m_type == INET)
        return inet < other.inet;
      HT_ASSERT(m_type == NONE && other.m_type == NONE);
      return false;
    }

    /** Returns <i>true</i> if address is of type CommAddress#PROXY.
     * @return <i>true</i> if address is of type CommAddress#PROXY,
     * <i>false</i> otherwise
     */
    bool is_proxy() const { return m_type == PROXY; }

    /** Returns <i>true</i> if address is of type CommAddress#INET.
     * @return <i>true</i> if address is of type CommAddress#INET, <i>false</i>
     * otherwise
     */
    bool is_inet() const { return m_type == INET; }

    /** Returns <i>true</i> if address has been initialized.
     * @return <i>true</i> if address is of type CommAddress#PROXY or
     * CommAddress#INET, <i>false</i> otherwise
     */
    bool is_set() const { return m_type == PROXY || m_type == INET; }

    /** Clears address to uninitialized state.
     * Sets address type to CommAddress#NONE
     */
    void clear() { proxy=""; m_type=NONE; }

    /** Returns string representation of address.
     * If address is of type CommAddress#PROXY, the #proxy is returned.  If
     * address is of type CommAddress#INET, InetAddr#format is used to return a
     * string representation of #inet.  If address is of type CommAddress#NONE,
     * the string "[NULL]" is returned.
     * @return string representation of address
     */
    String to_str() const;
    
    String proxy;   //!< Proxy name
    InetAddr inet;  //!< IPv4:port address

  private:
    int32_t m_type; //!< Address type
  };

  /** Hash function (functor) for CommAddress objets.
   * This class is defined for use with STL template classes that require a
   * hash functor (e.g. <code>std::hash_map</code>).
   */
  class CommAddressHash {
  public:
    /** Parenthesis operator with a single CommAddress parameter.
     * This method returns the hash value for the object specified in the
     * <code>addr</code> parameter.  If the address type is CommAddress#INET,
     * then the hash code is computed as the bitwise exclusive OR of the IP
     * address and the port.  If the address is of type CommAddress#PROXY, then
     * the hash code is computed with <code>__gnu_cxx::hash<const char *></code>.
     * Otherwise the hash value is 0.
     * @return hash value
     */
    size_t operator () (const CommAddress &addr) const {
      if (addr.is_inet())
	return (size_t)(addr.inet.sin_addr.s_addr ^ addr.inet.sin_port);
      else if (addr.is_proxy()) {
	__gnu_cxx::hash<const char *> cchash;
	return cchash(addr.proxy.c_str());
      }
      return 0;
    }
  };

  /// Parameterized hash map for mapping CommAddress to arbitrary type.
  template<typename TypeT, typename addr=CommAddress>
  class CommAddressMap : public hash_map<addr, TypeT, CommAddressHash> {
  };

  /// Set of CommAddress objects
  typedef std::set<CommAddress> CommAddressSet;
  /** @}*/
} // namespace Hypertable

#endif // HYPERTABLE_COMMADDRESS_H
