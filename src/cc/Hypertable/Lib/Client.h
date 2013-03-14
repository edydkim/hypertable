/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_CLIENT_H
#define HYPERTABLE_CLIENT_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "AsyncComm/ApplicationQueueInterface.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#include "MasterClient.h"
#include "NameIdMapper.h"
#include "NamespaceCache.h"
#include "TableCache.h"
#include "Namespace.h"

namespace Hypertable {

  /** @defgroup libHypertable libHypertable
   * %Client library.
   * The libHypertable module contains the Hypertable client library
   * which contains Hypertable specific code that is shared by clients,
   * the RangeServer and the Master.
   * @{
   */

  class Comm;
  class HqlInterpreter;

  class Client : public ReferenceCount {
  public:

    /**
     * Constructs the object using the specified config file
     *
     * @param install_dir path to Hypertable installation directory
     * @param config_file name of configuration file
     * @param default_timeout_ms default method call timeout in milliseconds
     */
    Client(const String &install_dir, const String &config_file,
           uint32_t default_timeout_ms=0);

    /**
     * Constructs the object using the default config file
     *
     * @param install_dir path to Hypertable installation directory
     * @param default_timeout_ms default method call timeout in milliseconds
     */
    Client(const String &install_dir = String(), uint32_t default_timeout_ms=0);
    ~Client() {}

    /**
     * Creates a namespace
     *
     * @param name name of the namespace to create
     * @param base optional base Namespace (if specified the name parameter will be relative
     *        to base)
     * @param create_intermediate if true then create all non-existent intermediate namespaces
     * @param if_not_exists don't throw an exception if namespace does exist
     */
    void create_namespace(const String &name, Namespace *base=NULL,
                          bool create_intermediate=false,
                          bool if_not_exists=false);

    /**
     * Opens a Namespace
     *
     * @param name name of the Namespace
     * @param base optional base Namespace (if specified the name parameter will be relative
     *        to base)
     * @return pointer to the Namespace object
     */
    NamespacePtr open_namespace(const String &name, Namespace *base=NULL);

    /**
     * Checks if the namespace exists
     *
     * @param name name of namespace
     * @param base optional base Namespace (if specified the name parameter will be relative
     *        to base)
     * @return true if namespace exists false ow
     */
    bool exists_namespace(const String &name, Namespace *base=NULL);

    /**
     * Removes a namespace.  This command instructs the Master to
     * remove a namespace from the system. The namespace must be empty, ie all tables and
     * namespaces under it must have been dropped already
     *
     * @param name namespace name
     * @param base optional base Namespace (if specified the name parameter will be relative
     *        to base)
     * @param if_exists don't throw an exception if table does not exist
     */
    void drop_namespace(const String &name, Namespace *base=NULL, bool if_exists=false);

    Hyperspace::SessionPtr& get_hyperspace_session();

    MasterClientPtr get_master_client();

    NameIdMapperPtr get_nameid_mapper();

    void close();
    void shutdown();

    /**
     *
     * @param immutable_namespace if true then the namespace can't be modified once its set
     */
    HqlInterpreter *create_hql_interpreter(bool immutable_namespace=true);

  private:

    void initialize();

    PropertiesPtr           m_props;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    ApplicationQueueInterfacePtr m_app_queue;
    Hyperspace::SessionPtr  m_hyperspace;
    NameIdMapperPtr         m_namemap;
    MasterClientPtr         m_master_client;
    RangeLocatorPtr         m_range_locator;
    uint32_t                m_timeout_ms;
    String                  m_install_dir;
    TableCachePtr           m_table_cache;
    NamespaceCachePtr       m_namespace_cache;
    Mutex                   m_mutex;
    bool                    m_hyperspace_reconnect;
    String                  m_toplevel_dir;
  };

  typedef intrusive_ptr<Client> ClientPtr;

} // namespace Hypertable

#endif // HYPERTABLE_CLIENT_H
