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

/** @file
 * Contains main function for Master server
 * This file contains the definition for the main function for the Master
 * server
 */

#include "Common/Compat.h"

extern "C" {
#include <poll.h>
}

#include "Common/FailureInducer.h"
#include "Common/Init.h"
#include "Common/ScopeGuard.h"
#include "Common/System.h"
#include "Common/Thread.h"
#include "Common/md5.h"

#include "AsyncComm/Comm.h"

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/MetaLogReader.h"
#include "DfsBroker/Lib/Client.h"

#include "ConnectionHandler.h"
#include "Context.h"
#include "LoadBalancer.h"
#include "MetaLogDefinitionMaster.h"
#include "OperationBalance.h"
#include "OperationInitialize.h"
#include "OperationProcessor.h"
#include "OperationRecover.h"
#include "OperationRecoveryBlocker.h"
#include "OperationSystemUpgrade.h"
#include "OperationTimedBarrier.h"
#include "OperationWaitForServers.h"
#include "ReferenceManager.h"
#include "ResponseManager.h"
#include "BalancePlanAuthority.h"

using namespace Hypertable;
using namespace Config;

namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      alias("port", "Hypertable.Master.Port");
      alias("workers", "Hypertable.Master.Workers");
      alias("reactors", "Hypertable.Master.Reactors");
    }
  };

  typedef Meta::list<GenericServerPolicy, DfsClientPolicy,
                     HyperspaceClientPolicy, DefaultCommPolicy, AppPolicy> Policies;

  class HandlerFactory : public ConnectionHandlerFactory {
  public:
    HandlerFactory(ContextPtr &context) : m_context(context) {
      m_handler = new ConnectionHandler(m_context);
    }

    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = m_handler;
    }

  private:
    ContextPtr m_context;
    DispatchHandlerPtr m_handler;
  };

  struct ltrsc {
    bool operator()(const RangeServerConnectionPtr &rsc1, const RangeServerConnectionPtr &rsc2) const {
      return strcmp(rsc1->location().c_str(), rsc2->location().c_str()) < 0;
    }
  };


} // local namespace

/** @defgroup Master Master
 * %Master server.
 * The Master module contains all of the definitions that make up the Master
 * server process which is responsible for handling meta operations such
 * the following
 *   - Creating, altering, and dropping tables
 *   - CellStore garbage collection
 *   - RangeServer failover orchestration
 *   - Load balancing
 * @{
 */

void obtain_master_lock(ContextPtr &context);

int main(int argc, char **argv) {
  ContextPtr context = new Context();

  // Register ourselves as the Comm-layer proxy master
  ReactorFactory::proxy_master = true;

  try {
    init_with_policies<Policies>(argc, argv);
    uint16_t port = get_i16("port");

    context->comm = Comm::instance();
    context->conn_manager = new ConnectionManager(context->comm);
    context->props = properties;
    context->hyperspace = new Hyperspace::Session(context->comm, context->props);
    context->rsc_manager = new RangeServerConnectionManager();

    context->toplevel_dir = properties->get_str("Hypertable.Directory");
    boost::trim_if(context->toplevel_dir, boost::is_any_of("/"));
    context->toplevel_dir = String("/") + context->toplevel_dir;

    // the "state-file" signals that we're currently acquiring the hyperspace
    // lock; it is required for the serverup tool (issue 816)
    String state_file = System::install_dir + "/run/Hypertable.Master.state";
    String state_text = "acquiring";

    if (FileUtils::write(state_file, state_text) < 0)
      HT_INFOF("Unable to write state file %s", state_file.c_str());

    obtain_master_lock(context);

    if (!FileUtils::unlink(state_file))
      HT_INFOF("Unable to delete state file %s", state_file.c_str());

    context->namemap = new NameIdMapper(context->hyperspace, context->toplevel_dir);
    context->dfs = new DfsBroker::Client(context->conn_manager, context->props);
    context->mml_definition =
        new MetaLog::DefinitionMaster(context, format("%s_%u", "master", port).c_str());
    context->monitoring = new Monitoring(context.get());
    context->request_timeout = (time_t)(context->props->get_i32("Hypertable.Request.Timeout") / 1000);

    if (get_bool("Hypertable.Master.Locations.IncludeMasterHash")) {
      char location_hash[33];
      md5_string(format("%s:%u", System::net_info().host_name.c_str(), port).c_str(), location_hash);
      context->location_hash = String(location_hash).substr(0, 8);
    }
    context->max_allowable_skew = context->props->get_i32("Hypertable.RangeServer.ClockSkew.Max");
    context->monitoring_interval = context->props->get_i32("Hypertable.Monitoring.Interval");
    context->gc_interval = context->props->get_i32("Hypertable.Master.Gc.Interval");
    context->timer_interval = std::min(context->monitoring_interval, context->gc_interval);

    HT_ASSERT(context->monitoring_interval > 1000);
    HT_ASSERT(context->gc_interval > 1000);

    time_t now = time(0);
    context->next_monitoring_time = now + (context->monitoring_interval/1000) - 1;
    context->next_gc_time = now + (context->gc_interval/1000) - 1;

    if (has("induce-failure")) {
      if (FailureInducer::instance == 0)
        FailureInducer::instance = new FailureInducer();
      FailureInducer::instance->parse_option(get_str("induce-failure"));
    }

    /**
     * Read/load MML
     */
    std::vector<MetaLog::EntityPtr> entities;
    std::vector<OperationPtr> operations;
    std::map<String, OperationPtr> recovery_operations;
    MetaLog::ReaderPtr mml_reader;
    OperationPtr operation;
    RangeServerConnectionPtr rsc;
    StringSet locations;
    String log_dir = context->toplevel_dir + "/servers/master/log/"
        + context->mml_definition->name();
    size_t added_servers = 0;

    mml_reader = new MetaLog::Reader(context->dfs, context->mml_definition,
            log_dir);
    mml_reader->get_entities(entities);

    // Uniq-ify the RangeServerConnection objects
    {
      std::vector<MetaLog::EntityPtr> entities2;
      std::set<RangeServerConnectionPtr, ltrsc> rsc_set;

      entities2.reserve(entities.size());
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        rsc = dynamic_cast<RangeServerConnection *>(entity.get());
        if (rsc) {
          if (rsc_set.count(rsc.get()) > 0)
            rsc_set.erase(rsc.get());
          rsc_set.insert(rsc.get());
        }
        else
          entities2.push_back(entity);
      }
      foreach_ht (const RangeServerConnectionPtr &rsc, rsc_set)
        entities2.push_back(rsc.get());
      entities.swap(entities2);
    }

    context->mml_writer = new MetaLog::Writer(context->dfs, context->mml_definition,
                                              log_dir, entities);

    context->reference_manager = new ReferenceManager();

    /** Response Manager */
    ResponseManagerContext *rmctx = 
        new ResponseManagerContext(context->mml_writer);
    context->response_manager = new ResponseManager(rmctx);
    Thread response_manager_thread(*context->response_manager);

    int worker_count  = get_i32("workers");
    context->op = new OperationProcessor(context, worker_count);

    // First do System Upgrade
    operation = new OperationSystemUpgrade(context);
    context->op->add_operation(operation);
    context->op->wait_for_empty();

    // Then reconstruct state and start execution
    for (size_t i = 0; i < entities.size(); i++) {
      operation = dynamic_cast<Operation *>(entities[i].get());
      if (operation) {
        if (operation->remove_explicitly())
          context->reference_manager->add(operation);
        // master was interrupted in the middle of rangeserver failover
        if (dynamic_cast<OperationRecover *>(operation.get())) {
          HT_INFO("Recovery was interrupted; continuing");
          OperationRecover *op =
              dynamic_cast<OperationRecover *>(operation.get());
          recovery_operations[op->location()] = operation;
        }
        else
          operations.push_back(operation);
      }
      else if (dynamic_cast<RangeServerConnection *>(entities[i].get())) {
        rsc = dynamic_cast<RangeServerConnection *>(entities[i].get());
        HT_ASSERT(rsc);
        context->rsc_manager->add_server(rsc);
        locations.insert(rsc->location());
        if (recovery_operations.find(rsc->location())
                == recovery_operations.end())
          recovery_operations[rsc->location()] =
            new OperationRecover(context, rsc, OperationRecover::RESTART);
        added_servers++;
      }
      else {
        BalancePlanAuthority *bpa
            = dynamic_cast<BalancePlanAuthority *>(entities[i].get());
        HT_ASSERT(bpa);
        if (!bpa->is_empty()) {
          HT_INFO_OUT << "Loading BalancePlanAuthority: " << *bpa << HT_END;
          bpa->set_mml_writer(context->mml_writer);
          context->set_balance_plan_authority(bpa);
        }
      }
    }
    context->balancer = new LoadBalancer(context);

    std::map<String, OperationPtr>::iterator recovery_it = recovery_operations.begin();
    while (recovery_it != recovery_operations.end()) {
      operations.push_back(recovery_it->second);
      ++recovery_it;
    }
    recovery_operations.clear();

    if (operations.empty()) {
      OperationInitializePtr init_op = new OperationInitialize(context);
      if (context->namemap->exists_mapping("/sys/METADATA", 0))
        init_op->set_state(OperationState::CREATE_RS_METRICS);
      context->reference_manager->add(init_op.get());
      operations.push_back( init_op );
    }
    else {
      if (context->metadata_table == 0)
        context->metadata_table = new Table(context->props,
                context->conn_manager, context->hyperspace, context->namemap,
                TableIdentifier::METADATA_NAME);

      if (context->rs_metrics_table == 0)
        context->rs_metrics_table = new Table(context->props,
                context->conn_manager, context->hyperspace, context->namemap,
                "sys/RS_METRICS");
    }

    // Add PERPETUAL operations
    operation = new OperationWaitForServers(context);
    operations.push_back(operation);
    context->recovery_barrier_op = new OperationTimedBarrier(context, Dependency::RECOVERY, Dependency::RECOVERY_BLOCKER);
    operation = new OperationRecoveryBlocker(context);
    operations.push_back(operation);

    context->op->add_operations(operations);

    ConnectionHandlerFactoryPtr hf(new HandlerFactory(context));
    InetAddr listen_addr(INADDR_ANY, port);

    context->comm->listen(listen_addr, hf);

    context->op->join();
    context->mml_writer->close();
    context->comm->close_socket(listen_addr);

    context->response_manager->shutdown();
    response_manager_thread.join();
    delete rmctx;
    delete context->response_manager;

    context = 0;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  return 0;
}

using namespace Hyperspace;

void obtain_master_lock(ContextPtr &context) {

  try {
    uint64_t handle = 0;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &handle);

    /**
     *  Create TOPLEVEL directory if not exist
     */
    context->hyperspace->mkdirs(context->toplevel_dir);

    /**
     * Create /hypertable/master if not exist
     */
    if (!context->hyperspace->exists( context->toplevel_dir + "/master" )) {
      handle = context->hyperspace->open( context->toplevel_dir + "/master",
                                   OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);
      context->hyperspace->close(handle);
      handle = 0;
    }

    {
      uint32_t lock_status = LOCK_STATUS_BUSY;
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
      LockSequencer sequencer;
      bool reported = false;
      uint32_t retry_interval = context->props->get_i32("Hypertable.Connection.Retry.Interval");

      context->master_file_handle = context->hyperspace->open(context->toplevel_dir + "/master", oflags);

      while (lock_status != LOCK_STATUS_GRANTED) {

        context->hyperspace->try_lock(context->master_file_handle, LOCK_MODE_EXCLUSIVE,
                                      &lock_status, &sequencer);

        if (lock_status != LOCK_STATUS_GRANTED) {
          if (!reported) {
            HT_INFOF("Couldn't obtain lock on '%s/master' due to conflict, entering retry loop ...",
                     context->toplevel_dir.c_str());
            reported = true;
          }
          poll(0, 0, retry_interval);
        }
      }

      HT_INFOF("Obtained lock on '%s/master'", context->toplevel_dir.c_str());

      /**
       * Write master location in 'address' attribute, format is IP:port
       */
      uint16_t port = context->props->get_i16("Hypertable.Master.Port");
      InetAddr addr(System::net_info().primary_addr, port);
      String addr_s = addr.format();
      context->hyperspace->attr_set(context->master_file_handle, "address",
                             addr_s.c_str(), addr_s.length());

      if (!context->hyperspace->attr_exists(context->master_file_handle, "next_server_id"))
        context->hyperspace->attr_set(context->master_file_handle, "next_server_id", "1", 2);
    }

    context->hyperspace->mkdirs(context->toplevel_dir + "/servers");
    context->hyperspace->mkdirs(context->toplevel_dir + "/tables");

    // Create /hypertable/root
    handle = context->hyperspace->open(context->toplevel_dir + "/root",
        OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);

    HT_INFO("Successfully Initialized.");
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
}

/** @}*/
