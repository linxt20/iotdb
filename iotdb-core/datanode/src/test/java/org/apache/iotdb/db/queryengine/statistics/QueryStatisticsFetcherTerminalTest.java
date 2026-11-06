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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotFetchResult;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class QueryStatisticsFetcherTerminalTest {
  @Test
  public void waitsForTerminalRatherThanRenderingFlushingSample() throws Exception {
    Fixture f = new Fixture();
    AtomicInteger calls = new AtomicInteger();
    when(f.client.fetchFragmentInstanceStatistics(any()))
        .thenAnswer(invocation -> response(f.first, calls.incrementAndGet() >= 3));
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllTerminalStatisticsBestEffort(
            List.of(f.first), f.manager, 5000);
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.getResponses().get(f.first.getId()).isIsTerminal());
    assertEquals(3, calls.get());
  }

  @Test
  public void oneNonterminalTimeoutRetainsOtherCompletedSource() throws Exception {
    Fixture f = new Fixture();
    when(f.client.fetchFragmentInstanceStatistics(any()))
        .thenAnswer(
            invocation -> {
              var request =
                  (org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq)
                      invocation.getArgument(0);
              return request.getFragmentInstanceId().getFragmentId() == 1
                  ? response(f.first, false)
                  : response(f.second, true);
            });
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllTerminalStatisticsBestEffort(
            List.of(f.first, f.second), f.manager, 1000);
    assertEquals("TIMEOUT", result.getErrors().get(f.first.getId()));
    assertEquals(1, result.getResponses().size());
    assertTrue(result.getResponses().get(f.second.getId()).isIsTerminal());
  }

  @Test
  public void ordinaryLiveFetchRemainsSingleSample() throws Exception {
    Fixture f = new Fixture();
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(response(f.first, false));
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllStatisticsBestEffort(List.of(f.first), f.manager, 5000);
    assertFalse(result.getResponses().get(f.first.getId()).isIsTerminal());
    verify(f.client, times(1)).fetchFragmentInstanceStatistics(any());
  }

  @Test
  public void ignoredFragmentDoesNotWaitForItsOwnExplainDriver() throws Exception {
    Fixture f = new Fixture();
    TFetchFragmentInstanceStatisticsResp ignored = response(f.first, false);
    ignored.setIgnoredFragmentReason("EXPLAIN_CURRENT_INSTANCE");
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(ignored);
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllTerminalStatisticsBestEffort(
            List.of(f.first), f.manager, 5000);
    assertEquals(
        "EXPLAIN_CURRENT_INSTANCE",
        result.getResponses().get(f.first.getId()).getIgnoredFragmentReason());
    verify(f.client, times(1)).fetchFragmentInstanceStatistics(any());
  }

  @Test
  public void legacyResponseKeepsCompatibilityWithoutInventingTerminalState() throws Exception {
    Fixture f = new Fixture();
    TFetchFragmentInstanceStatisticsResp legacy = response(f.first, false);
    legacy.unsetSchemaVersion();
    legacy.unsetIsTerminal();
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(legacy);
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllTerminalStatisticsBestEffort(
            List.of(f.first), f.manager, 5000);
    assertFalse(result.getResponses().get(f.first.getId()).isSetIsTerminal());
    verify(f.client, times(1)).fetchFragmentInstanceStatistics(any());
  }

  @Test
  public void duplicateFragmentCannotOverwriteAnotherEndpointFuture() throws Exception {
    Fixture f = new Fixture();
    FragmentInstance duplicate =
        new FragmentInstance(
            null, f.first.getId(), null, QueryType.READ, 60000, null, false, false);
    duplicate.setHostDataNode(
        new TDataNodeLocation().setInternalEndPoint(new TEndPoint("192.0.2.2", 41231)));
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(response(f.second, true));
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllTerminalStatisticsBestEffort(
            List.of(f.first, duplicate, f.second), f.manager, 5000);
    assertEquals("DUPLICATE_FRAGMENT_INSTANCE", result.getErrors().get(f.first.getId()));
    assertEquals(1, result.getResponses().size());
    assertTrue(result.getResponses().containsKey(f.second.getId()));
    verify(f.client, times(1)).fetchFragmentInstanceStatistics(any());
  }

  @Test
  public void responseFromAnotherFragmentIsRejectedWithoutLosingOtherSource() throws Exception {
    Fixture f = new Fixture();
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(response(f.second, true));
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllStatisticsBestEffort(
            List.of(f.first, f.second), f.manager, 5000);
    assertEquals("FRAGMENT_INSTANCE_MISMATCH", result.getErrors().get(f.first.getId()));
    assertEquals(1, result.getResponses().size());
    assertTrue(result.getResponses().containsKey(f.second.getId()));
  }

  @Test
  public void connectedEndpointMustMatchResponseOrigin() throws Exception {
    Fixture f = new Fixture();
    TFetchFragmentInstanceStatisticsResp response = response(f.first, true);
    response.setSourceEndpoint("192.0.2.99:41230");
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(response);
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllStatisticsBestEffort(List.of(f.first), f.manager, 5000);
    assertEquals("ENDPOINT_MISMATCH", result.getErrors().get(f.first.getId()));
    assertTrue(result.getResponses().isEmpty());
  }

  @Test
  public void versionedResponseNeedsSourceIdentity() throws Exception {
    Fixture f = new Fixture();
    TFetchFragmentInstanceStatisticsResp response = response(f.first, true);
    response.unsetSourceEndpoint();
    when(f.client.fetchFragmentInstanceStatistics(any())).thenReturn(response);
    RuntimeSnapshotFetchResult result =
        QueryStatisticsFetcher.fetchAllStatisticsBestEffort(List.of(f.first), f.manager, 5000);
    assertEquals("MISSING_SOURCE_ENDPOINT", result.getErrors().get(f.first.getId()));
    assertTrue(result.getResponses().isEmpty());
  }

  private static TFetchFragmentInstanceStatisticsResp response(
      FragmentInstance fi, boolean terminal) {
    TFetchFragmentInstanceStatisticsResp r =
        new TFetchFragmentInstanceStatisticsResp(new TSStatus(200));
    r.setFragmentInstanceId(fi.getId().toThrift());
    r.setSchemaVersion(1);
    r.setSnapshotGeneration(1);
    r.setCapturedAtNanos(1);
    r.setState(terminal ? "FINISHED" : "FLUSHING");
    r.setIsTerminal(terminal);
    r.setSourceNodeId("1");
    r.setSourceEndpoint("192.0.2.1:41230");
    return r;
  }

  private static final class Fixture {
    final FragmentInstance first = instance(1), second = instance(2);
    final SyncDataNodeInternalServiceClient client =
        Mockito.mock(SyncDataNodeInternalServiceClient.class);
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager =
        Mockito.mock(IClientManager.class);

    Fixture() throws Exception {
      when(manager.borrowClient(any())).thenReturn(client);
    }

    private static FragmentInstance instance(int n) {
      FragmentInstance fi =
          new FragmentInstance(
              null,
              new FragmentInstanceId(new PlanFragmentId(new QueryId("terminal_fetch"), n), "0"),
              null,
              QueryType.READ,
              60000,
              null,
              false,
              false);
      fi.setHostDataNode(
          new TDataNodeLocation().setInternalEndPoint(new TEndPoint("192.0.2.1", 41230)));
      return fi;
    }
  }
}
