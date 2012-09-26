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

#ifndef HYPERTABLE_RANGESERVERHYPERSPACECALLBACK_H
#define HYPERTABLE_RANGESERVERHYPERSPACECALLBACK_H

#include "Hyperspace/Session.h"

#include "AddRecoveryOperationTimerHandler.h"
#include "Context.h"
#include "RangeServerConnection.h"

namespace Hypertable {

  /**
   * RangeServerHyperspaceCallback is a Hyperspace handle callback
   * that is installed to handle a LOCK RELEASED event on a RangeServers
   * Hyperspace file.  It sets up a timer to add an OperationRecover
   * operation after Hypertable.Failover.GracePeriod milliseconds have
   * elapsed.
   */
  class RangeServerHyperspaceCallback : public Hyperspace::HandleCallback {
  public:
    RangeServerHyperspaceCallback(ContextPtr &context, RangeServerConnectionPtr &rsc)
    : Hyperspace::HandleCallback(Hyperspace::EVENT_MASK_LOCK_RELEASED),
      m_context(context), m_rsc(rsc) { }

    virtual void lock_released() {
      if (m_context->rsc_manager->disconnect_server(m_rsc)) {
        uint32_t millis = m_context->props->get_i32("Hypertable.Failover.GracePeriod");
        HT_INFOF("Scheduling recovery operation for %s in %ld milliseconds",
                 m_rsc->location().c_str(), (long)millis);
        DispatchHandlerPtr handler
          = new AddRecoveryOperationTimerHandler(m_context, m_rsc);
        m_context->comm->set_timer(millis, handler.get());
      }
    }

  private:
    ContextPtr m_context;
    RangeServerConnectionPtr m_rsc;
  };

}

#endif // HYPERTABLE_RANGESERVERHYPERSPACECALLBACK_H
