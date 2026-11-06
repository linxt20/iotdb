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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceFetchException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotFetchResult;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueryStatisticsFetcher {

  private static final String LOCAL_HOST_IP =
      IoTDBDescriptor.getInstance().getConfig().getInternalAddress();

  private static final int LOCAL_HOST_PORT =
      IoTDBDescriptor.getInstance().getConfig().getInternalPort();

  private QueryStatisticsFetcher() {
    // allowed to do nothing
  }

  public static Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> fetchAllStatistics(
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager)
      throws FragmentInstanceFetchException {
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> fragmentInstanceStatistics =
        new HashMap<>();
    for (FragmentInstance instance : instances) {
      fragmentInstanceStatistics.put(instance.getId(), fetchStatistics(instance, clientManager));
    }
    return fragmentInstanceStatistics;
  }

  /**
   * Fetches each fragment independently. A timeout or malformed response is recorded for that
   * fragment and does not turn other fragment responses into zero-valued successes.
   */
  public static RuntimeSnapshotFetchResult fetchAllStatisticsBestEffort(
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
      long timeoutMillis) {
    return fetchAllStatisticsBestEffort(instances, clientManager, timeoutMillis, false);
  }

  /** Waits for terminal responses within each fragment's existing fetch deadline. */
  public static RuntimeSnapshotFetchResult fetchAllTerminalStatisticsBestEffort(
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
      long timeoutMillis) {
    return fetchAllStatisticsBestEffort(instances, clientManager, timeoutMillis, true);
  }

  private static RuntimeSnapshotFetchResult fetchAllStatisticsBestEffort(
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
      long timeoutMillis,
      boolean terminalOnly) {
    if (timeoutMillis <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_TIMEOUTMILLIS_MUST_BE_POSITIVE_B8EC8967);
    }
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> responses = new LinkedHashMap<>();
    Map<FragmentInstanceId, String> errors = new LinkedHashMap<>();
    if (instances == null || instances.isEmpty()) {
      return new RuntimeSnapshotFetchResult(responses, errors);
    }
    Map<FragmentInstanceId, FragmentInstance> uniqueInstances = new LinkedHashMap<>();
    java.util.Set<FragmentInstanceId> duplicates = new java.util.HashSet<>();
    for (FragmentInstance instance : instances) {
      if (uniqueInstances.putIfAbsent(instance.getId(), instance) != null) {
        duplicates.add(instance.getId());
        errors.put(instance.getId(), "DUPLICATE_FRAGMENT_INSTANCE");
      }
    }
    ExecutorService executor =
        Executors.newFixedThreadPool(
            Math.min(instances.size(), Runtime.getRuntime().availableProcessors()));
    try {
      Map<FragmentInstanceId, Future<TFetchFragmentInstanceStatisticsResp>> futures =
          new LinkedHashMap<>();
      for (FragmentInstance instance : uniqueInstances.values()) {
        if (duplicates.contains(instance.getId())) continue;
        futures.put(
            instance.getId(),
            executor.submit(
                () ->
                    terminalOnly
                        ? fetchTerminalStatistics(instance, clientManager, timeoutMillis)
                        : fetchStatistics(instance, clientManager)));
      }
      for (Map.Entry<FragmentInstanceId, Future<TFetchFragmentInstanceStatisticsResp>> entry :
          futures.entrySet()) {
        try {
          TFetchFragmentInstanceStatisticsResp response =
              entry.getValue().get(timeoutMillis, TimeUnit.MILLISECONDS);
          if (response == null || response.status == null) {
            errors.put(entry.getKey(), "EMPTY_RESPONSE");
          } else if (response.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            errors.put(entry.getKey(), "STATUS_" + response.status.getCode());
          } else {
            TEndPoint endpoint =
                uniqueInstances.get(entry.getKey()).getHostDataNode().getInternalEndPoint();
            if (response.isSetFragmentInstanceId()
                && !entry.getKey().toThrift().equals(response.getFragmentInstanceId())) {
              errors.put(entry.getKey(), "FRAGMENT_INSTANCE_MISMATCH");
            } else if (response.isSetSchemaVersion() && !response.isSetFragmentInstanceId()) {
              errors.put(entry.getKey(), "MISSING_FRAGMENT_IDENTITY");
            } else if (response.isSetSourceEndpoint()
                && !(endpoint.getIp() + ":" + endpoint.getPort())
                    .equals(response.getSourceEndpoint())) {
              errors.put(entry.getKey(), "ENDPOINT_MISMATCH");
            } else if (response.isSetSchemaVersion() && !response.isSetSourceEndpoint()) {
              errors.put(entry.getKey(), "MISSING_SOURCE_ENDPOINT");
            } else {
              responses.put(entry.getKey(), response);
            }
          }
        } catch (TimeoutException e) {
          entry.getValue().cancel(true);
          errors.put(entry.getKey(), "TIMEOUT");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          errors.put(entry.getKey(), "INTERRUPTED");
        } catch (ExecutionException e) {
          errors.put(
              entry.getKey(),
              e.getCause() instanceof TimeoutException
                  ? "TIMEOUT"
                  : e.getCause() == null ? "ERROR" : e.getCause().toString());
        }
      }
    } finally {
      executor.shutdownNow();
    }
    return new RuntimeSnapshotFetchResult(responses, errors);
  }

  private static TFetchFragmentInstanceStatisticsResp fetchTerminalStatistics(
      FragmentInstance instance,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
      long timeoutMillis)
      throws FragmentInstanceFetchException, InterruptedException, TimeoutException {
    long started = System.nanoTime();
    long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (true) {
      if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
      if (System.nanoTime() - started >= timeoutNanos) throw new TimeoutException();
      TFetchFragmentInstanceStatisticsResp response = fetchStatistics(instance, clientManager);
      if (response == null
          || response.isSetIgnoredFragmentReason()
          || !response.isSetSchemaVersion()
          || !response.isSetIsTerminal()
          || (response.isSetIsTerminal() && response.isIsTerminal())) return response;
      TimeUnit.MILLISECONDS.sleep(1);
    }
  }

  private static TFetchFragmentInstanceStatisticsResp fetchStatistics(
      FragmentInstance instance,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager)
      throws FragmentInstanceFetchException {
    TEndPoint endPoint = instance.getHostDataNode().internalEndPoint;

    TFetchFragmentInstanceStatisticsResp fragmentInstanceStatistics;
    // If instance is running on the local host, we can directly fetch the statistics from the local
    // host
    if (LOCAL_HOST_IP.equals(endPoint.getIp()) && LOCAL_HOST_PORT == endPoint.getPort()) {
      // Get statistics for current instance
      fragmentInstanceStatistics =
          FragmentInstanceManager.getInstance().getFragmentInstanceStatistics(instance.getId());
    } else {
      try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        fragmentInstanceStatistics =
            client.fetchFragmentInstanceStatistics(
                new TFetchFragmentInstanceStatisticsReq(instance.getId().toThrift()));
        if (fragmentInstanceStatistics.status.getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new FragmentInstanceFetchException(fragmentInstanceStatistics.status);
        }
      } catch (TException | ClientManagerException e) {
        TSStatus status = new TSStatus();
        status.setCode(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        status.setMessage(e.getMessage());
        throw new FragmentInstanceFetchException(status);
      }
    }
    return fragmentInstanceStatistics;
  }
}
