/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#ifndef HYPERTABLE_COMMITLOG_H
#define HYPERTABLE_COMMITLOG_H

#include <deque>
#include <map>
#include <stack>

#include <boost/thread/xtime.hpp>

#include "Common/Mutex.h"
#include "Common/DynamicBuffer.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"
#include "Common/Config.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Types.h"

#include "CommitLogBase.h"
#include "CommitLogBlockStream.h"

namespace Hypertable {

  typedef struct {
    uint32_t distance;
    uint64_t cumulative_size;
  } LogFragmentPriorityData;

  typedef std::map<int64_t, LogFragmentPriorityData> LogFragmentPriorityMap;

  /**
   * Commit log for persisting range updates.  The commit log is a directory
   * that contains a growing number of files that contain compressed blocks of
   * "commits".  The files are named starting with '0' and will periodically
   * roll, which means that a trailer is written to the end of the file, the
   * file is closed, and then the numeric name is incremented by one and
   * opened.  Periodically when old parts of the log are no longer needed, they
   * get purged.  The size of each log fragment file is determined by the
   * following config file property:
   *<pre>
   * Hypertable.RangeServer.CommitLog.RollLimit
   *</pre>
   */

  class CommitLog : public CommitLogBase {
  public:

    /**
     * Constructs a CommitLog object using supplied properties.
     *
     * @param fs filesystem to write log into
     * @param log_dir directory of the commit log
     * @param props reference to properties map
     * @param init_log base log to pull fragments from
     */
    CommitLog(Filesystem *fs, const String &log_dir,
              PropertiesPtr &props, CommitLogBase *init_log = 0)
      : CommitLogBase(log_dir) {
      initialize(fs, log_dir, props, init_log);
    }

    /**
     * Constructs a CommitLog object using default properties.
     *
     * @param fs filesystem to write log into
     * @param log_dir directory of the commit log
     */
    CommitLog(Filesystem *fs, const String &log_dir) : CommitLogBase(log_dir) {
      initialize(fs, log_dir, Config::properties, 0);
    }

    virtual ~CommitLog();

    /**
     * Atomically obtains a timestamp
     *
     * @return microseconds since the epoch
     */
    int64_t get_timestamp();

    /** Writes a block of updates to the commit log.
     *
     * @param buffer block of updates to commit
     * @param revision most recent revision in buffer
     * @return Error::OK on success or error code on failure
     */
    int write(DynamicBuffer &buffer, int64_t revision);

    /** Links an external log into this log.
     *
     * @param log_base pointer to commit log object to link in
     * @return Error::OK on success or error code on failure
     */
    int link_log(CommitLogBase *log_base);

    /** Closes the log.  Writes the trailer and closes the file
     *
     * @return Error::OK on success or error code on failure
     */
    int close();

    /** Purges the log.  Removes all of the log fragments that have
     * a revision that is less than the given revision.
     *
     * @param revision real cutoff revision
     */
    int purge(int64_t revision);

    /** Fills up a map of per-fragment priority information.  One
     * entry per log fragment is inserted into this map.  The key
     * is the revision of the fragment (e.g. the real revision
     * of the most recent data in the fragment file).  The value
     * is a structure that contains information regarding how
     * expensive it is to keep this fragment around.
     *
     * @param frag_map reference to map of log fragment priority data
     */
    void load_fragment_priority_map(LogFragmentPriorityMap &frag_map);

    /**
     * Returns the maximum size of each log fragment file
     */
    int64_t get_max_fragment_size() { return m_max_fragment_size; }

    static const char MAGIC_DATA[10];
    static const char MAGIC_LINK[10];

  private:
    void initialize(Filesystem *, const String &log_dir,
                    PropertiesPtr &, CommitLogBase *init_log);
    int roll();
    int compress_and_write(DynamicBuffer &input, BlockCompressionHeader *header,
                           int64_t revision);

    Mutex                   m_mutex;
    Filesystem             *m_fs;
    BlockCompressionCodec  *m_compressor;
    String                  m_cur_fragment_fname;
    int64_t                 m_cur_fragment_length;
    uint32_t                m_cur_fragment_num;
    int64_t                 m_max_fragment_size;
    int32_t                 m_fd;
    uint32_t                m_flush_flag;
    bool                    m_needs_roll;
  };

  typedef intrusive_ptr<CommitLog> CommitLogPtr;

} // namespace Hypertable

#endif // HYPERTABLE_COMMITLOG_H
