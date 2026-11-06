/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.queryengine.execution.fragment.QueryStatistics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Verifies the one-call statistics copy used as the snapshot consistency boundary. */
public class RuntimeSnapshotStatisticsBarrierTest {

  @Test
  public void testSnapshotCopiesAllCountersAtOneCallBoundary() {
    QueryStatistics statistics = new QueryStatistics();
    statistics.getLoadChunkFromDiskCount().set(7);
    statistics.getRowScanFilteredRows().set(11);

    org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics snapshot = statistics.snapshotToThrift();

    assertEquals(7, snapshot.loadChunkFromDiskCount);
    assertEquals(11, snapshot.rowScanFilteredRows);
  }
}
