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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.BlockCredit;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

/** This is not thread safe class, the caller should ensure multi-threads safety. */
@NotThreadSafe
public class SharedTsBlockQueue {

  /** Immutable memory-account identity captured when this physical queue is created. */
  public record MemoryReservationKey(
      String queryId, String fragmentInstanceId, String planNodeId) {}

  private static final Logger LOGGER = LoggerFactory.getLogger(SharedTsBlockQueue.class);

  private final TFragmentInstanceId localFragmentInstanceId;

  private final String localPlanNodeId;

  private final String fullFragmentInstanceId;

  private final LocalMemoryManager localMemoryManager;

  private boolean noMoreTsBlocks = false;

  private long bufferRetainedSizeInBytes = 0L;

  private final Queue<Pair<TsBlock, Long>> queue = new LinkedList<>();

  private SettableFuture<Void> blocked = SettableFuture.create();

  /**
   * this is completed after calling isBlocked for the first time which indicates that this queue
   * needs to output data.
   */
  private final SettableFuture<Void> canAddTsBlock = SettableFuture.create();

  private ListenableFuture<Void> blockedOnMemory;

  private volatile boolean closed = false;
  private boolean alreadyRegistered = false;

  private volatile LocalSourceHandle sourceHandle;
  private volatile LocalSinkChannel sinkChannel;

  private final RuntimeFrontierReservation frontierReservation;
  private final int frontierLaneIndex;
  private BlockCredit frontierCredit;
  private volatile boolean frontierAborted;
  private final Queue<Runnable> frontierNotifications = new LinkedList<>();
  private boolean drainingFrontierNotifications;

  private final java.util.concurrent.atomic.AtomicLong maxBytesCanReserve =
      new java.util.concurrent.atomic.AtomicLong(
          IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance());
  private final boolean isHighestPriority;

  private volatile Throwable abortedCause = null;

  // used for SharedTsBlockQueue listener
  private final ExecutorService executorService;

  @TestOnly
  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId,
      String planNodeId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService) {
    this(fragmentInstanceId, planNodeId, localMemoryManager, executorService, false);
  }

  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId,
      String planNodeId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      boolean isHighestPriority) {
    this(
        fragmentInstanceId,
        planNodeId,
        localMemoryManager,
        executorService,
        isHighestPriority,
        null,
        -1,
        false);
  }

  /**
   * Construct a queue backed by a prepaid, fixed lane. Closing this queue returns its block credit;
   * the enclosing group remains responsible for closing the reservation.
   */
  public SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId,
      String planNodeId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      boolean isHighestPriority,
      RuntimeFrontierReservation frontierReservation,
      int frontierLaneIndex) {
    this(
        fragmentInstanceId,
        planNodeId,
        localMemoryManager,
        executorService,
        isHighestPriority,
        Objects.requireNonNull(frontierReservation),
        frontierLaneIndex,
        true);
  }

  private SharedTsBlockQueue(
      TFragmentInstanceId fragmentInstanceId,
      String planNodeId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      boolean isHighestPriority,
      RuntimeFrontierReservation frontierReservation,
      int frontierLaneIndex,
      boolean frontierMode) {
    this.localFragmentInstanceId =
        Validate.notNull(
            fragmentInstanceId,
            DataNodeQueryMessages.EXCEPTION_FRAGMENT_INSTANCE_ID_CANNOT_BE_NULL_4BE84F40);
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);
    this.localPlanNodeId =
        Validate.notNull(
            planNodeId, DataNodeQueryMessages.EXCEPTION_PLANNODE_ID_CANNOT_BE_NULL_F91303CD);
    this.localMemoryManager =
        Validate.notNull(
            localMemoryManager,
            DataNodeQueryMessages.EXCEPTION_LOCAL_MEMORY_MANAGER_CANNOT_BE_NULL_54701481);
    this.executorService =
        Validate.notNull(
            executorService,
            DataNodeQueryMessages.EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_220C966B);
    this.isHighestPriority = isHighestPriority;
    this.frontierReservation = frontierReservation;
    this.frontierLaneIndex = frontierLaneIndex;
    if (frontierMode) {
      MemoryReservationKey account = frontierReservation.getMemoryReservationKey();
      if (frontierReservation.getMemorySide()
              != org.apache.iotdb.db.queryengine.execution.memory.MemorySide.OUTPUT
          || frontierLaneIndex < 0
          || frontierLaneIndex >= frontierReservation.getLaneCount()
          || !frontierReservation.isBackedBy(localMemoryManager.getQueryPool())
          || !localFragmentInstanceId.getQueryId().equals(account.queryId())
          || !fullFragmentInstanceId.equals(account.fragmentInstanceId())) {
        throw new IllegalArgumentException();
      }
      frontierReservation.claimQueueLane(frontierLaneIndex, this);
    }
    ResourceAuditManager.getInstance()
        .registerObject(
            fragmentInstanceId.getQueryId(),
            fullFragmentInstanceId,
            this,
            "LOCAL_QUEUE",
            this::isClosed,
            getQueueIdentityKey(),
            getActualAccountKey());
  }

  public boolean hasNoMoreTsBlocks() {
    if (isFrontierMode()) {
      synchronized (this) {
        return noMoreTsBlocks;
      }
    }
    return noMoreTsBlocks;
  }

  public long getBufferRetainedSizeInBytes() {
    if (isFrontierMode()) {
      synchronized (this) {
        return bufferRetainedSizeInBytes;
      }
    }
    return bufferRetainedSizeInBytes;
  }

  public SettableFuture<Void> getCanAddTsBlock() {
    return canAddTsBlock;
  }

  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve.accumulateAndGet(maxBytesCanReserve, Math::min);
  }

  public long getMaxBytesCanReserve() {
    return maxBytesCanReserve.get();
  }

  /** Queue identity retained for compatibility with existing resource-audit records. */
  public MemoryReservationKey getMemoryReservationKey() {
    return new MemoryReservationKey(
        localFragmentInstanceId.getQueryId(), fullFragmentInstanceId, localPlanNodeId);
  }

  public MemoryReservationKey getQueueIdentityKey() {
    return getMemoryReservationKey();
  }

  /** The physical pool account, shared by all prepaid lanes of a group. */
  public MemoryReservationKey getActualAccountKey() {
    return isFrontierMode()
        ? frontierReservation.getMemoryReservationKey()
        : getMemoryReservationKey();
  }

  public boolean isFrontierMode() {
    return frontierReservation != null;
  }

  /**
   * Distinguishes abort without a cause from normal close in the prepaid queue's first terminal.
   */
  public boolean wasFrontierAborted() {
    return frontierAborted;
  }

  public int getFrontierLaneIndex() {
    return frontierLaneIndex;
  }

  /**
   * Enqueue a frontier notification without invoking user code. Handles call flushNotifications
   * after leaving both their own monitor and the queue monitor.
   */
  public void deferNotification(Runnable notification) {
    if (!isFrontierMode()) {
      throw new IllegalStateException();
    }
    synchronized (this) {
      frontierNotifications.add(Objects.requireNonNull(notification));
    }
  }

  /**
   * Drain outside all exchange monitors, including with a direct executor. Reentrant actions append
   * to this same outbox. A failed callback does not prevent later resource returns or
   * notifications.
   */
  public void flushNotifications() {
    LocalSinkChannel sink = sinkChannel;
    LocalSourceHandle source = sourceHandle;
    if (!isFrontierMode()
        || Thread.holdsLock(this)
        || (sink != null && Thread.holdsLock(sink))
        || (source != null && Thread.holdsLock(source))) {
      return;
    }
    synchronized (this) {
      if (drainingFrontierNotifications) {
        return;
      }
      drainingFrontierNotifications = true;
    }
    Throwable failure = null;
    while (true) {
      Runnable notification;
      synchronized (this) {
        notification = frontierNotifications.poll();
        if (notification == null) {
          drainingFrontierNotifications = false;
          break;
        }
      }
      try {
        notification.run();
      } catch (Throwable t) {
        if (failure == null) {
          failure = t;
        } else if (failure != t) {
          failure.addSuppressed(t);
        }
      }
    }
    if (failure != null) {
      rethrowNotificationFailure(failure);
    }
  }

  private static void rethrowNotificationFailure(Throwable failure) {
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    throw new RuntimeException(failure);
  }

  /** Allow adding data to queue manually. */
  public void allowAddingTsBlock() {
    if (isFrontierMode()) {
      try {
        deferNotification(() -> canAddTsBlock.set(null));
      } finally {
        flushNotifications();
      }
      return;
    }
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
  }

  public ListenableFuture<Void> isBlocked() {
    if (isFrontierMode()) {
      try {
        synchronized (this) {
          frontierNotifications.add(() -> canAddTsBlock.set(null));
          return blocked;
        }
      } finally {
        flushNotifications();
      }
    }
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
    return blocked;
  }

  public boolean isEmpty() {
    if (isFrontierMode()) {
      synchronized (this) {
        return queue.isEmpty();
      }
    }
    return queue.isEmpty();
  }

  public boolean isClosed() {
    return closed;
  }

  public int getNumOfBufferedTsBlocks() {
    if (isFrontierMode()) {
      synchronized (this) {
        return queue.size();
      }
    }
    return queue.size();
  }

  public void setSinkChannel(LocalSinkChannel sinkChannel) {
    this.sinkChannel = sinkChannel;
  }

  public void setSourceHandle(LocalSourceHandle sourceHandle) {
    this.sourceHandle = sourceHandle;
  }

  /** Notify no more TsBlocks will be added to the queue. */
  public void setNoMoreTsBlocks(boolean noMoreTsBlocks) {
    if (isFrontierMode()) {
      try {
        synchronized (this) {
          if (closed || this.noMoreTsBlocks || !noMoreTsBlocks) {
            return;
          }
          this.noMoreTsBlocks = true;
          SettableFuture<Void> ready = blocked;
          frontierNotifications.add(() -> ready.set(null));
          if (sourceHandle != null) {
            frontierNotifications.add(sourceHandle::checkAndInvokeOnFinished);
          }
        }
      } finally {
        flushNotifications();
      }
      return;
    }
    LOGGER.debug(DataNodeQueryMessages.SIGNAL_NO_MORE_TSBLOCK_ON_QUEUE);
    if (closed) {
      LOGGER.debug(DataNodeQueryMessages.QUEUE_DESTROYED_WHEN_SET_NO_MORE_TSBLOCKS);
      return;
    }
    this.noMoreTsBlocks = noMoreTsBlocks;
    if (!blocked.isDone()) {
      blocked.set(null);
    }
    if (this.sourceHandle != null) {
      this.sourceHandle.checkAndInvokeOnFinished();
    }
  }

  /**
   * Remove a TsBlock from the head of the queue and return. Should be invoked only when the future
   * returned by {@link #isBlocked()} completes.
   *
   * @throws IllegalStateException Cannot remove TsBlock from closed queue.
   */
  public TsBlock remove() {
    if (isFrontierMode()) {
      return removeFrontier();
    }
    if (closed) {
      // try throw underlying exception instead of "Source handle is aborted."
      if (abortedCause != null) {
        throw new IllegalStateException(abortedCause);
      }
      try {
        blocked.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
      }
      throw new IllegalStateException(DataNodeQueryMessages.QUEUE_HAS_BEEN_DESTROYED);
    }
    Pair<TsBlock, Long> tsBlockWithReservedBytes = queue.remove();
    long reservedBytes = tsBlockWithReservedBytes.right;
    if (reservedBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              reservedBytes);
      bufferRetainedSizeInBytes -= reservedBytes;
    }
    // Every time LocalSourceHandle consumes a TsBlock, it needs to send the event
    // to
    // corresponding LocalSinkChannel.
    if (sinkChannel != null) {
      sinkChannel.checkAndInvokeOnFinished();
    }
    if (blocked.isDone() && queue.isEmpty() && !noMoreTsBlocks) {
      blocked = SettableFuture.create();
    }
    return tsBlockWithReservedBytes.left;
  }

  /**
   * Add TsBlocks to the queue. Except the first invocation, this method should be invoked only when
   * the returned future of last invocation completes.
   */
  public ListenableFuture<Void> add(TsBlock tsBlock) {
    if (isFrontierMode()) {
      return addFrontier(tsBlock);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(DataNodeQueryMessages.ADD_TSBLOCK, CommonUtils.toString(tsBlock));
    }
    if (closed) {
      // queue may have been closed
      return immediateVoidFuture();
    }

    Validate.notNull(tsBlock, DataNodeQueryMessages.EXCEPTION_TSBLOCK_CANNOT_BE_NULL_E7EA3BDA);
    Validate.isTrue(
        blockedOnMemory == null || blockedOnMemory.isDone(),
        DataNodeQueryMessages.EXCEPTION_SHAREDTSBLOCKQUEUE_IS_FULL_87493E26);
    if (!alreadyRegistered) {
      localMemoryManager
          .getQueryPool()
          .registerPlanNodeIdToQueryMemoryMap(
              localFragmentInstanceId.queryId,
              fullFragmentInstanceId,
              localPlanNodeId,
              MemorySide.OUTPUT);
      alreadyRegistered = true;
    }
    MemoryReservationResult reserveResult =
        localMemoryManager
            .getQueryPool()
            .reserveWithPriority(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                tsBlock.getSizeInBytes(),
                maxBytesCanReserve.get(),
                isHighestPriority);
    blockedOnMemory = reserveResult.getFuture();
    long reservedBytes = reserveResult.getReservedBytes();
    bufferRetainedSizeInBytes += reservedBytes;

    // reserve memory failed, we should wait until there is enough memory
    if (!reserveResult.isReserveSuccess()) {
      SettableFuture<Void> channelBlocked = SettableFuture.create();
      blockedOnMemory.addListener(
          () -> {
            synchronized (this) {
              // If the queue has been closed or aborted before this listener executes,
              // we must not add the TsBlock. The memory reserved for this TsBlock has
              // already been freed by abort()/close() via bufferRetainedSizeInBytes.
              // Adding it would cause a downstream NPE in MemoryPool.free() when
              // the consumer calls remove(), because the upstream FI's memory mapping
              // has already been deregistered.
              if (closed) {
                channelBlocked.set(null);
                return;
              }
              queue.add(new Pair<>(tsBlock, reservedBytes));
              if (!blocked.isDone()) {
                blocked.set(null);
              }
              channelBlocked.set(null);
            }
          },
          // Use directExecutor() here could lead to deadlock. Thread A holds lock of
          // SharedTsBlockQueueA and tries to invoke the listener of
          // SharedTsBlockQueueB(when freeing memory to complete MemoryReservationFuture)
          // while
          // Thread B holds lock of SharedTsBlockQueueB and tries to invoke the listener
          // of
          // SharedTsBlockQueueA
          executorService);
      return channelBlocked;
    } else { // reserve memory succeeded, add the TsBlock directly
      queue.add(new Pair<>(tsBlock, reservedBytes));
      if (!blocked.isDone()) {
        blocked.set(null);
      }
      return blockedOnMemory;
    }
  }

  /** Destroy the queue and complete the future. Should only be called in normal case */
  public void close() {
    if (isFrontierMode()) {
      terminateFrontier(null, false, true);
      return;
    }
    if (closed) {
      return;
    }
    closed = true;
    if (!blocked.isDone()) {
      blocked.set(null);
    }
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
    }
    queue.clear();
    if (bufferRetainedSizeInBytes > 0L) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    if (sinkChannel != null) {
      // attention: LocalSinkChannel of this SharedTsBlockQueue could be null when we
      // close
      // LocalSourceHandle(with limit clause it's possible) before constructing the
      // corresponding
      // LocalSinkChannel.
      // If this close method is invoked by LocalSourceHandle, listener of
      // LocalSourceHandle will
      // remove the LocalSourceHandle from the map of MppDataExchangeManager and later
      // when
      // LocalSinkChannel is initialized, it will construct a new SharedTsBlockQueue.
      // It is still safe that we let the LocalSourceHandle close successfully in this
      // case. Because
      // the QueryTerminator will do the final cleaning logic.
      sinkChannel.close();
    }
  }

  /** Destroy the queue and cancel the future. Should only be called in abnormal case */
  public void abort() {
    if (isFrontierMode()) {
      terminateFrontier(null, true, false);
      return;
    }
    if (closed) {
      return;
    }
    closed = true;
    if (!blocked.isDone()) {
      blocked.cancel(true);
    }
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
    }
    queue.clear();
    if (bufferRetainedSizeInBytes > 0L) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
  }

  /** Destroy the queue and cancel the future. Should only be called in abnormal case */
  public void abort(Throwable t) {
    if (isFrontierMode()) {
      terminateFrontier(Objects.requireNonNull(t), true, false);
      return;
    }
    if (closed) {
      return;
    }
    abortedCause = t;
    closed = true;
    if (!blocked.isDone()) {
      blocked.setException(t);
    }
    if (!canAddTsBlock.isDone()) {
      canAddTsBlock.set(null);
    }
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
    }
    queue.clear();
    if (bufferRetainedSizeInBytes > 0L) {
      localMemoryManager
          .getQueryPool()
          .free(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              localPlanNodeId,
              bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
  }

  private ListenableFuture<Void> addFrontier(TsBlock tsBlock) {
    try {
      synchronized (this) {
        if (closed) {
          return immediateVoidFuture();
        }
        Objects.requireNonNull(tsBlock);
        if (noMoreTsBlocks || frontierCredit != null || !queue.isEmpty()) {
          throw new IllegalStateException(
              DataNodeQueryMessages.EXCEPTION_SHAREDTSBLOCKQUEUE_IS_FULL_87493E26);
        }
        long retainedBytes = tsBlock.getRetainedSizeInBytes();
        BlockCredit credit = frontierReservation.acquire(frontierLaneIndex, this, retainedBytes);
        try {
          queue.add(new Pair<>(tsBlock, retainedBytes));
        } catch (Throwable t) {
          frontierNotifications.add(() -> frontierReservation.release(credit));
          throw t;
        }
        frontierCredit = credit;
        bufferRetainedSizeInBytes = retainedBytes;
        SettableFuture<Void> ready = blocked;
        frontierNotifications.add(() -> ready.set(null));
        return credit.getWritableFuture();
      }
    } finally {
      flushNotifications();
    }
  }

  private TsBlock removeFrontier() {
    try {
      synchronized (this) {
        if (closed) {
          if (abortedCause != null) {
            throw new IllegalStateException(abortedCause);
          }
          throw new IllegalStateException(DataNodeQueryMessages.QUEUE_HAS_BEEN_DESTROYED);
        }
        if (queue.isEmpty() && noMoreTsBlocks) {
          return null;
        }
        Pair<TsBlock, Long> block = queue.remove();
        releaseFrontierCredit();
        if (sinkChannel != null) {
          frontierNotifications.add(sinkChannel::checkAndInvokeOnFinished);
        }
        if (queue.isEmpty() && !noMoreTsBlocks) {
          blocked = SettableFuture.create();
        }
        return block.left;
      }
    } finally {
      flushNotifications();
    }
  }

  // Only detach under the queue monitor. The outbox returns the credit outside every handle lock.
  private void releaseFrontierCredit() {
    BlockCredit credit = frontierCredit;
    frontierCredit = null;
    bufferRetainedSizeInBytes = 0L;
    if (credit != null) {
      frontierNotifications.add(() -> frontierReservation.release(credit));
    }
  }

  private void terminateFrontier(Throwable cause, boolean abort, boolean notifySink) {
    try {
      synchronized (this) {
        if (closed) {
          return;
        }
        abortedCause = cause;
        frontierAborted = abort;
        closed = true;
        queue.clear();
        releaseFrontierCredit();
        SettableFuture<Void> ready = blocked;
        frontierNotifications.add(() -> completeFrontierTerminal(ready, cause, abort));
        frontierNotifications.add(() -> canAddTsBlock.set(null));
        if (notifySink && sinkChannel != null) {
          frontierNotifications.add(sinkChannel::close);
        }
      }
    } finally {
      flushNotifications();
    }
  }

  private static void completeFrontierTerminal(
      SettableFuture<Void> future, Throwable cause, boolean abort) {
    if (cause != null) {
      future.setException(cause);
    } else if (abort) {
      future.cancel(true);
    } else {
      future.set(null);
    }
  }

  /**
   * Seal first, then obtain the sink's abort cause outside exchange monitors. Even a throwing cause
   * supplier cannot prevent the detached credit and all terminal futures from being settled.
   */
  public void abortWithCauseSupplier(Supplier<Optional<Throwable>> causeSupplier) {
    if (!isFrontierMode()) {
      throw new IllegalStateException();
    }
    Objects.requireNonNull(causeSupplier);
    try {
      synchronized (this) {
        if (closed) {
          return;
        }
        frontierAborted = true;
        closed = true;
        queue.clear();
        releaseFrontierCredit();
        SettableFuture<Void> ready = blocked;
        frontierNotifications.add(
            () -> {
              Throwable cause = null;
              Throwable failure = null;
              try {
                cause = Objects.requireNonNull(causeSupplier.get()).orElse(null);
              } catch (Throwable t) {
                cause = t;
                failure = t;
              }
              Throwable finalCause = cause;
              synchronized (this) {
                abortedCause = cause;
                frontierNotifications.add(() -> completeFrontierTerminal(ready, finalCause, true));
                frontierNotifications.add(() -> canAddTsBlock.set(null));
              }
              if (failure != null) {
                rethrowNotificationFailure(failure);
              }
            });
      }
    } finally {
      flushNotifications();
    }
  }

  public Optional<Throwable> getAbortedCause() {
    return Optional.ofNullable(abortedCause);
  }
}
