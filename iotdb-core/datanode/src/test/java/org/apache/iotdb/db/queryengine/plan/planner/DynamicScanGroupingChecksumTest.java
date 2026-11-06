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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class DynamicScanGroupingChecksumTest {
  @Test
  public void provenanceChangesEIdentityWithoutChangingDIdentity() {
    ScanParallelPlanner.Input in = DynamicScanGroupingTest.input(12, 0, 4, false, false);
    DynamicScanGrouping a =
        new DynamicScanGroupingBuilder()
            .build(in)
            .withCosts(
                Collections.nCopies(12, 3L),
                Collections.nCopies(12, DynamicCostSource.MEASURED_BUCKET));
    DynamicScanGrouping b =
        a.withCosts(a.getCosts(), Collections.nCopies(12, DynamicCostSource.BUCKET_AVERAGE));
    ScanParallelPlanner planner = new ScanParallelPlanner();
    ScanParallelPlan pa = planner.planWithGrouping(in, a, 4),
        pb = planner.planWithGrouping(in, b, 4);
    assertEquals(pa.getPlanIdentityChecksum(), pb.getPlanIdentityChecksum());
    assertNotEquals(
        DynamicScanGroupingChecksum.compute(a, pa), DynamicScanGroupingChecksum.compute(b, pb));
  }

  @Test
  public void supportBitIsDeterministicAndEncodedInDIdentity() {
    ScanParallelPlanner.Input in = DynamicScanGroupingTest.input(12, 0, 4, false, false);
    ScanParallelPlanner planner = new ScanParallelPlanner();
    DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
    String measured = null;
    for (DynamicCostSource source : DynamicCostSource.values()) {
      DynamicScanGrouping v =
          g.withCosts(Collections.nCopies(12, 3L), Collections.nCopies(12, source));
      ScanParallelPlan p = planner.planWithGrouping(in, v, 4);
      for (ScanParallelPlan.WorkUnit u : p.getWorkUnits()) {
        assertEquals(source != DynamicCostSource.EQUAL_WORK, u.isEstimateSupported());
      }
      if (source == DynamicCostSource.MEASURED_BUCKET) {
        measured = p.getPlanIdentityChecksum();
      }
      if (source == DynamicCostSource.EQUAL_WORK) {
        assertNotEquals(measured, p.getPlanIdentityChecksum());
      }
    }
  }
}
