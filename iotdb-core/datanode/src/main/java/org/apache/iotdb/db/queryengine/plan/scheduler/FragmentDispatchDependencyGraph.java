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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

/** Consumer-first dispatch for shared hash sources whose exchange edges form a DAG. */
final class FragmentDispatchDependencyGraph {
  private FragmentDispatchDependencyGraph() {}

  static boolean required(List<FragmentInstance> instances) {
    return instances.stream()
        .anyMatch(
            instance ->
                instance.getFragment().getPlanNodeTree()
                        instanceof TableHashPartitioningShuffleSinkNode
                    && ((MultiChildrenSinkNode) instance.getFragment().getPlanNodeTree())
                            .getDownStreamChannelLocationList()
                            .size()
                        > 1);
  }

  static List<List<FragmentInstance>> layers(List<FragmentInstance> instances) {
    Map<FragmentInstanceId, FragmentInstance> byId = new LinkedHashMap<>();
    for (FragmentInstance instance : instances) {
      if (byId.put(instance.getId(), instance) != null) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages
                    .EXCEPTION_DUPLICATE_FRAGMENT_INSTANCE_IN_DISPATCH_GRAPH_ARG_EF1BADDC,
                instance.getId()));
      }
    }
    Map<FragmentInstanceId, Set<FragmentInstanceId>> consumers = new LinkedHashMap<>();
    for (FragmentInstance instance : instances) {
      if (!(instance.getFragment().getPlanNodeTree() instanceof MultiChildrenSinkNode)) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages
                    .EXCEPTION_FRAGMENT_HAS_NO_EXCHANGE_SINK_IN_DISPATCH_GRAPH_ARG_D95FD948,
                instance.getId()));
      }
      Set<FragmentInstanceId> targets = new LinkedHashSet<>();
      for (DownStreamChannelLocation location :
          ((MultiChildrenSinkNode) instance.getFragment().getPlanNodeTree())
              .getDownStreamChannelLocationList()) {
        if (location.getRemoteFragmentInstanceId() == null) {
          throw new IllegalArgumentException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_FRAGMENT_HAS_AN_UNBOUND_DOWNSTREAM_INSTANCE_ARG_9361033B,
                  instance.getId()));
        }
        FragmentInstanceId target =
            FragmentInstanceId.fromThrift(location.getRemoteFragmentInstanceId());
        if (byId.containsKey(target)) {
          targets.add(target);
        } else if (!instance.getFragment().isRoot()
            || !target.getQueryId().equals(instance.getId().getQueryId())
            || target.getFragmentId().getId() != 0
            || !target.getInstanceId().equals("0")) {
          throw new IllegalArgumentException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_FRAGMENT_ARG_REFERENCES_AN_UNKNOWN_DOWNSTREAM_INSTANCE_ARG_8EA16E6A,
                  instance.getId(),
                  target));
        }
      }
      consumers.put(instance.getId(), targets);
    }
    Set<FragmentInstanceId> completed = new LinkedHashSet<>();
    List<List<FragmentInstance>> result = new ArrayList<>();
    while (completed.size() < instances.size()) {
      List<FragmentInstance> ready = new ArrayList<>();
      for (FragmentInstance instance : instances) {
        if (!completed.contains(instance.getId())
            && completed.containsAll(consumers.get(instance.getId()))) {
          ready.add(instance);
        }
      }
      if (ready.isEmpty()) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages
                .EXCEPTION_FRAGMENT_DISPATCH_DEPENDENCIES_CONTAIN_A_CYCLE_E133EC40);
      }
      result.add(Collections.unmodifiableList(ready));
      ready.forEach(instance -> completed.add(instance.getId()));
    }
    return Collections.unmodifiableList(result);
  }

  static FragInstanceDispatchResult dispatch(
      List<FragmentInstance> instances,
      ExecutorService executor,
      Function<FragmentInstance, FragInstanceDispatchResult> dispatch)
      throws InterruptedException, ExecutionException {
    // Freeze dependencies before workers clear their plan trees.
    for (List<FragmentInstance> layer : layers(instances)) {
      List<Future<FragInstanceDispatchResult>> futures = new ArrayList<>();
      try {
        for (FragmentInstance instance : layer)
          futures.add(executor.submit(() -> dispatch.apply(instance)));
        FragInstanceDispatchResult failure = null;
        ExecutionException unexpected = null;
        for (Future<FragInstanceDispatchResult> future : futures) {
          try {
            FragInstanceDispatchResult result = future.get();
            if (!result.isSuccessful() && failure == null) failure = result;
          } catch (ExecutionException e) {
            if (unexpected == null) unexpected = e;
          }
        }
        if (unexpected != null) throw unexpected;
        if (failure != null) return failure;
      } catch (InterruptedException | RuntimeException e) {
        futures.forEach(future -> future.cancel(true));
        throw e;
      }
    }
    return new FragInstanceDispatchResult(true);
  }
}
