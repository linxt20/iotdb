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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

/** Fair, unordered fan-in for table plans. Only the driver thread accesses child operators. */
public class TableReadyCollectOperator implements Operator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableReadyCollectOperator.class);

  private static final long AUXILIARY_INSTANCE_SIZE =
      3L * MemoryEstimationHelper.ARRAY_LIST_INSTANCE_SIZE
          + RamUsageEstimator.shallowSizeOfInstance(AtomicReference.class)
          + RamUsageEstimator.shallowSizeOfInstance(SettableFuture.class)
          + RamUsageEstimator.shallowSizeOfInstance(
              Futures.nonCancellationPropagating(SettableFuture.create()).getClass());

  private final CommonOperatorContext operatorContext;
  private final List<Operator> children;
  private final List<ListenableFuture<?>> childBlocked;
  private final List<ListenableFuture<?>> listenedBlocked;
  private final AtomicReference<SettableFuture<Void>> wakeupTarget = new AtomicReference<>();
  private int remaining;
  private int nextIndex;
  private SettableFuture<Void> wakeup;
  private ListenableFuture<Void> blockedView;

  public TableReadyCollectOperator(CommonOperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = requireNonNull(operatorContext);
    this.children = new ArrayList<>(children);
    this.children.forEach(java.util.Objects::requireNonNull);
    this.remaining = children.size();
    this.childBlocked = new ArrayList<>(Collections.nCopies(children.size(), null));
    this.listenedBlocked = new ArrayList<>(Collections.nCopies(children.size(), null));
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return operatorContext;
  }

  /** Check remembered completions before asking a child for a new future. */
  private boolean probeChildren() {
    boolean ready = false;
    for (int i = 0; i < children.size(); i++) {
      Operator child = children.get(i);
      if (child == null) {
        continue;
      }
      ListenableFuture<?> blocked = childBlocked.get(i);
      if (blocked == null || blocked.isDone()) {
        if (blocked != null) {
          checkCompletedFuture(blocked);
        }
        blocked =
            requireNonNull(
                child.isBlocked(),
                DataNodeQueryMessages.EXCEPTION_CHILD_RETURNED_A_NULL_BLOCKED_FUTURE_41941EB3);
        childBlocked.set(i, blocked);
      }
      if (blocked.isDone()) {
        checkCompletedFuture(blocked);
        ready = true;
      }
    }
    return ready;
  }

  private static void checkCompletedFuture(ListenableFuture<?> future) {
    try {
      Futures.getDone(future);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_TABLE_COLLECT_CHILD_FAILED_FBD7C950, e.getCause());
    } catch (CancellationException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_TABLE_COLLECT_CHILD_WAS_CANCELLED_A320FDF9, e);
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (remaining == 0 || probeChildren()) {
      return NOT_BLOCKED;
    }
    if (wakeup == null || wakeup.isDone()) {
      final SettableFuture<Void> newWakeup = SettableFuture.create();
      wakeup = newWakeup;
      blockedView = null;
      wakeupTarget.set(newWakeup);
      // A long-idle input must not accumulate one listener for every other input's block.
      final AtomicReference<SettableFuture<Void>> target = wakeupTarget;
      for (int i = 0; i < children.size(); i++) {
        if (children.get(i) == null) {
          continue;
        }
        ListenableFuture<?> blocked = childBlocked.get(i);
        if (listenedBlocked.get(i) != blocked) {
          listenedBlocked.set(i, blocked);
          blocked.addListener(
              () -> {
                SettableFuture<Void> current = target.get();
                if (current != null) {
                  current.set(null);
                }
              },
              directExecutor());
        }
        // Completion can race between the probe and publishing this generation's wakeup.
        if (blocked.isDone()) {
          newWakeup.set(null);
        }
      }
    }
    if (blockedView == null || blockedView.isCancelled()) {
      // Driver cancellation must never cancel a shared exchange/queue future.
      blockedView = Futures.nonCancellationPropagating(wakeup);
    }
    return blockedView;
  }

  @Override
  public boolean hasNext() {
    return remaining != 0;
  }

  @Override
  public TsBlock next() throws Exception {
    if (remaining == 0 || !probeChildren()) {
      return null;
    }
    for (int offset = 0; offset < children.size(); offset++) {
      int index = (nextIndex + offset) % children.size();
      Operator child = children.get(index);
      if (child == null || !childBlocked.get(index).isDone()) {
        continue;
      }
      nextIndex = (index + 1) % children.size();
      childBlocked.set(index, null);
      if (child.hasNextWithTimer()) {
        return child.nextWithTimer();
      }
      closeChild(index);
      // Bound work per driver invocation, including streams that yield only EOF.
      return null;
    }
    return null;
  }

  private void closeChild(int index) throws Exception {
    Operator child = children.set(index, null);
    childBlocked.set(index, null);
    listenedBlocked.set(index, null);
    if (child != null) {
      remaining--;
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    return remaining == 0;
  }

  @Override
  public void close() throws Exception {
    SettableFuture<Void> pending = wakeupTarget.getAndSet(null);
    if (pending != null) {
      pending.set(null);
    }
    Throwable failure = null;
    for (int i = 0; i < children.size(); i++) {
      try {
        closeChild(i);
      } catch (Throwable t) {
        if (failure == null) {
          failure = t;
        } else if (failure != t) {
          failure.addSuppressed(t);
        }
      }
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure != null) {
      throw (Exception) failure;
    }
  }

  private static long nonNegative(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_NEGATIVE_TABLE_COLLECT_MEMORY_ESTIMATE_ARG_FC2C4F0B,
              bytes));
    }
    return bytes;
  }

  private static long add(long left, long right) {
    return Math.addExact(nonNegative(left), nonNegative(right));
  }

  @Override
  public long calculateMaxPeekMemory() {
    long retained = calculateRetainedSizeAfterCallingNext();
    long peak = add(retained, calculateMaxReturnSize());
    for (Operator child : children) {
      if (child != null) {
        long otherRetained = retained - nonNegative(child.calculateRetainedSizeAfterCallingNext());
        peak = Math.max(peak, add(otherRetained, child.calculateMaxPeekMemoryWithCounter()));
      }
    }
    return peak;
  }

  @Override
  public long calculateMaxReturnSize() {
    long max = 0;
    for (Operator child : children) {
      if (child != null) {
        max = Math.max(max, nonNegative(child.calculateMaxReturnSize()));
      }
    }
    return max;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long retained = 0;
    for (Operator child : children) {
      if (child != null) {
        retained = add(retained, child.calculateRetainedSizeAfterCallingNext());
      }
    }
    return retained;
  }

  @Override
  public long ramBytesUsed() {
    long bytes = add(INSTANCE_SIZE, AUXILIARY_INSTANCE_SIZE);
    bytes = add(bytes, RamUsageEstimator.sizeOfObjectArray(children.size()));
    bytes = add(bytes, RamUsageEstimator.sizeOfObjectArray(childBlocked.size()));
    bytes = add(bytes, RamUsageEstimator.sizeOfObjectArray(listenedBlocked.size()));
    bytes = add(bytes, MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext));
    for (Operator child : children) {
      bytes = add(bytes, MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child));
    }
    return bytes;
  }

  @TestOnly
  public List<Operator> getChildren() {
    return Collections.unmodifiableList(children);
  }
}
