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

package org.apache.iotdb.db.queryengine.execution.exchange.source;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserDataTransferErrorCode;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.audit.DataNodeUserDataTransferAuditor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.RemoteShufflePayloadByteTracker;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.createFullIdFrom;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.GET_DATA_BLOCK_TASK_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SOURCE_HANDLE_GET_TSBLOCK_REMOTE;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.GET_DATA_BLOCK_NUM_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER;

public class SourceHandle
    implements ISourceHandle,
        org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;
  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000;

  private final TEndPoint remoteEndpoint;
  private final TEndPoint localEndpoint;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final TFragmentInstanceId localFragmentInstanceId;

  private final String fullFragmentInstanceId;
  private final String localPlanNodeId;

  private final int indexOfUpstreamSinkHandle;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final TsBlockSerde serde;
  private final SourceHandleListener sourceHandleListener;

  private final Map<Integer, Long> sequenceIdToDataBlockSize = new HashMap<>();
  private final Map<Integer, ByteBuffer> sequenceIdToTsBlock = new HashMap<>();

  private final String threadName;
  private long retryIntervalInMs;

  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;

  private SettableFuture<Void> blocked = SettableFuture.create();

  private ListenableFuture<Void> blockedOnMemory;

  /** The actual buffered memory in bytes, including the amount of memory being reserved. */
  private long bufferRetainedSizeInBytes = 0L;

  private int currSequenceId = 0;
  private int nextSequenceId = 0;
  private int lastSequenceId = Integer.MAX_VALUE;
  private boolean aborted = false;

  private boolean closed = false;

  // Guarded by this. Future completion and terminal listener notifications happen outside this
  // lock.
  private final SettableFuture<Void> coordinatedCloseFuture = SettableFuture.create();
  private int inFlightEndpointTasks;
  private boolean coordinatedCloseRequested;
  private boolean coordinatedNotificationClaimed;
  private boolean terminalCleanupStarted;
  private boolean terminalCleanupFinished;
  private Throwable endpointFailure;
  private Throwable terminalActionFailure;
  private final RuntimeEndpointFrontier frontier;
  private final org.apache.iotdb.db.queryengine.execution.exchange.LocalResultExchange
      localResultExchange;
  private boolean localNativeSettlementPending;
  private boolean localNativeSettlementClaimed;
  private boolean localNativeCancellationKnown;

  private InputLease frontierInput;
  private int inFlightConsumers;
  private int pendingCreditReturns;

  private static final class InputLease {
    private final int sequence;
    private RuntimeFrontierReservation.BlockCredit credit;
    private ByteBuffer data;
    private boolean fetchActive = true;
    private boolean mapOwner;
    private boolean consuming;

    private InputLease(int sequence, RuntimeFrontierReservation.BlockCredit credit) {
      this.sequence = sequence;
      this.credit = credit;
    }
  }

  private boolean remoteCloseRequired;
  private boolean remoteClosePublished;

  /** max bytes this SourceHandle can reserve. */
  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();

  /**
   * this is set to true after calling isBlocked() at least once which indicates that this
   * SourceHandle needs to output data.
   */
  private boolean canGetTsBlockFromRemote = false;

  private final boolean isHighestPriority;

  private static final DataExchangeCostMetricSet DATA_EXCHANGE_COST_METRIC_SET =
      DataExchangeCostMetricSet.getInstance();
  private static final DataExchangeCountMetricSet DATA_EXCHANGE_COUNT_METRIC_SET =
      DataExchangeCountMetricSet.getInstance();
  private static final RemoteShufflePayloadByteTracker REMOTE_SHUFFLE_PAYLOAD_BYTE_TRACKER =
      RemoteShufflePayloadByteTracker.getInstance();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SourceHandle.class)
          + RamUsageEstimator.shallowSizeOfInstance(TFragmentInstanceId.class) * 2;

  @TestOnly
  @SuppressWarnings("squid:S107")
  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this(
        remoteEndpoint,
        remoteFragmentInstanceId,
        localFragmentInstanceId,
        localPlanNodeId,
        indexOfUpstreamSinkHandle,
        localMemoryManager,
        executorService,
        serde,
        sourceHandleListener,
        false,
        mppDataExchangeServiceClientManager);
  }

  @SuppressWarnings("squid:S107")
  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      boolean isHighestPriority,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this(
        remoteEndpoint,
        remoteFragmentInstanceId,
        localFragmentInstanceId,
        localPlanNodeId,
        indexOfUpstreamSinkHandle,
        localMemoryManager,
        executorService,
        serde,
        sourceHandleListener,
        isHighestPriority,
        mppDataExchangeServiceClientManager,
        null);
  }

  @SuppressWarnings("squid:S107")
  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      boolean isHighestPriority,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager,
      RuntimeEndpointFrontier frontier) {
    this(
        remoteEndpoint,
        remoteFragmentInstanceId,
        localFragmentInstanceId,
        localPlanNodeId,
        indexOfUpstreamSinkHandle,
        localMemoryManager,
        executorService,
        serde,
        sourceHandleListener,
        isHighestPriority,
        mppDataExchangeServiceClientManager,
        frontier,
        null);
  }

  @SuppressWarnings("squid:S107")
  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      boolean isHighestPriority,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager,
      RuntimeEndpointFrontier frontier,
      org.apache.iotdb.db.queryengine.execution.exchange.LocalResultExchange localResultExchange) {
    this.frontier = frontier;
    this.localResultExchange = localResultExchange;
    this.remoteEndpoint =
        Validate.notNull(
            remoteEndpoint,
            DataNodeQueryMessages.EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_DE2B5885);
    this.localEndpoint =
        new TEndPoint(
            IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
            IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    this.remoteFragmentInstanceId =
        Validate.notNull(
            remoteFragmentInstanceId,
            DataNodeQueryMessages.EXCEPTION_REMOTEFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_C2449A29);
    this.localFragmentInstanceId =
        Validate.notNull(
            localFragmentInstanceId,
            DataNodeQueryMessages.EXCEPTION_LOCALFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_37F5917D);
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);
    this.localPlanNodeId =
        Validate.notNull(
            localPlanNodeId,
            DataNodeQueryMessages.EXCEPTION_LOCALPLANNODEID_CAN_NOT_BE_NULL_DOT_44A34A33);
    this.indexOfUpstreamSinkHandle = indexOfUpstreamSinkHandle;
    this.localMemoryManager =
        Validate.notNull(
            localMemoryManager,
            DataNodeQueryMessages.EXCEPTION_LOCALMEMORYMANAGER_CAN_NOT_BE_NULL_DOT_7A46C6CE);
    this.executorService =
        Validate.notNull(
            executorService,
            DataNodeQueryMessages.EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_BC459BD4);
    this.serde =
        Validate.notNull(serde, DataNodeQueryMessages.EXCEPTION_SERDE_CAN_NOT_BE_NULL_DOT_D46F66E7);
    this.sourceHandleListener =
        Validate.notNull(
            sourceHandleListener,
            DataNodeQueryMessages.EXCEPTION_SOURCEHANDLELISTENER_CAN_NOT_BE_NULL_DOT_01817F52);
    this.isHighestPriority = isHighestPriority;
    this.bufferRetainedSizeInBytes = 0L;
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
    this.threadName = createFullIdFrom(localFragmentInstanceId, localPlanNodeId);
    if (localResultExchange != null) {
      if (frontier != null)
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_LOCAL_RESULT_CONSUMER_RETAINS_ITS_NATIVE_INPUT_ACCOUNT_7EC1BD48);
      if (!org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode(remoteEndpoint))
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_LOCAL_RESULT_EXCHANGE_MUST_REMAIN_IN_THE_SAME_DATANODE_5CA2F875);
      org.apache.iotdb.db.queryengine.execution.exchange.RuntimeLocalExchangeLink.Edge edge =
          localResultExchange.getEdge();
      if (!edge.consumer().equals(FragmentInstanceId.fromThrift(localFragmentInstanceId))
          || !edge.producer().equals(FragmentInstanceId.fromThrift(remoteFragmentInstanceId))
          || edge.channel() != indexOfUpstreamSinkHandle
          || !edge.consumerPlanNode().equals(localPlanNodeId))
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_LOCAL_RESULT_EXCHANGE_BELONGS_TO_ANOTHER_EDGE_5EF70208);
    }
    if (frontier == null) {
      localMemoryManager
          .getQueryPool()
          .registerPlanNodeIdToQueryMemoryMap(
              localFragmentInstanceId.queryId,
              fullFragmentInstanceId,
              localPlanNodeId,
              MemorySide.INPUT);
    } else {
      frontier.bind(
          localMemoryManager.getQueryPool(), localFragmentInstanceId, MemorySide.INPUT, this);
    }
  }

  @Override
  public TsBlock receive() {
    if (frontier != null) return receiveFrontier();
    return receiveLegacy();
  }

  private synchronized TsBlock receiveLegacy() {
    ByteBuffer tsBlock = getSerializedTsBlock();
    if (tsBlock != null) {
      long startTime = System.nanoTime();
      try {
        return serde.deserialize(tsBlock);
      } finally {
        DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
            localResultExchange == null
                ? SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE
                : DataExchangeCostMetricSet.SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL,
            System.nanoTime() - startTime);
      }
    } else {
      return null;
    }
  }

  @Override
  public synchronized ByteBuffer getSerializedTsBlock() {
    if (frontier != null) {
      throw new UnsupportedOperationException("Runtime INPUT requires typed receive ownership");
    }
    long startTime = System.nanoTime();
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      checkState();

      if (!blocked.isDone()) {
        throw new IllegalStateException(DataNodeQueryMessages.SOURCE_HANDLE_IS_BLOCKED);
      }

      ByteBuffer tsBlock = sequenceIdToTsBlock.remove(currSequenceId);
      if (tsBlock == null) {
        return null;
      }
      Long retainedSize = sequenceIdToDataBlockSize.remove(currSequenceId);
      if (retainedSize == null) {
        throw new IllegalStateException(DataNodeQueryMessages.RESERVED_DATA_BLOCK_SIZE_IS_NULL);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.GET_TSBLOCK_FROM_BUFFER, currSequenceId, retainedSize);
      }
      currSequenceId += 1;
      if (retainedSize > 0) {
        bufferRetainedSizeInBytes -= retainedSize;
        localMemoryManager
            .getQueryPool()
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                retainedSize);
      }

      if (sequenceIdToTsBlock.isEmpty() && !isFinished()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(DataNodeQueryMessages.WAIT_FOR_MORE_TSBLOCK);
        }
        blocked = SettableFuture.create();
      }
      if (isFinished()) {
        sourceHandleListener.onFinished(this);
      }
      trySubmitGetDataBlocksTask();
      return tsBlock;
    } finally {
      DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
          localResultExchange == null
              ? SOURCE_HANDLE_GET_TSBLOCK_REMOTE
              : DataExchangeCostMetricSet.SOURCE_HANDLE_GET_TSBLOCK_LOCAL,
          System.nanoTime() - startTime);
    }
  }

  private synchronized void trySubmitGetDataBlocksTask() {
    if (aborted || closed || coordinatedCloseRequested || terminalCleanupStarted) {
      return;
    }
    if (frontier != null) {
      trySubmitFrontierFetch();
      return;
    }
    if (blockedOnMemory != null && !blockedOnMemory.isDone()) {
      return;
    }

    final int startSequenceId = nextSequenceId;
    int endSequenceId = nextSequenceId;
    long reservedBytes = 0L;
    Pair<ListenableFuture<Void>, Boolean> pair = null;
    long blockedSize = 0L;
    while (sequenceIdToDataBlockSize.containsKey(endSequenceId)) {
      Long bytesToReserve = sequenceIdToDataBlockSize.get(endSequenceId);
      if (bytesToReserve == null) {
        throw new IllegalStateException(DataNodeQueryMessages.DATA_BLOCK_SIZE_IS_NULL);
      }
      MemoryReservationResult reserveResult =
          localMemoryManager
              .getQueryPool()
              .reserveWithPriority(
                  localFragmentInstanceId.getQueryId(),
                  fullFragmentInstanceId,
                  localPlanNodeId,
                  bytesToReserve,
                  maxBytesCanReserve,
                  isHighestPriority);
      pair = new Pair<>(reserveResult.getFuture(), reserveResult.isReserveSuccess());
      // actually reserve size is not equals raw size, update the actually reserve size to the map
      if (reserveResult.getReservedBytes() != bytesToReserve) {
        sequenceIdToDataBlockSize.put(endSequenceId, reserveResult.getReservedBytes());
      }
      bufferRetainedSizeInBytes += reserveResult.getReservedBytes();
      endSequenceId += 1;
      reservedBytes += reserveResult.getReservedBytes();
      if (!Boolean.TRUE.equals(pair.right)) {
        blockedSize = bytesToReserve;
        break;
      }
    }

    if (pair == null) {
      // Next data block not generated yet. Do nothing.
      return;
    }
    nextSequenceId = endSequenceId;

    if (!Boolean.TRUE.equals(pair.right)) {
      endSequenceId--;
      reservedBytes -= blockedSize;
      // The future being not completed indicates,
      //   1. Memory has been reserved for blocks in [startSequenceId, endSequenceId).
      //   2. Memory reservation for block whose sequence ID equals endSequenceId - 1 is blocked.
      //   3. Have not reserve memory for the rest of blocks.
      //
      //  startSequenceId          endSequenceId - 1  endSequenceId
      //         |-------- reserved --------|--- blocked ---|--- not reserved ---|

      // Schedule another call of trySubmitGetDataBlocksTask for the rest of blocks.
      blockedOnMemory = pair.left;
      final int blockedSequenceId = endSequenceId;
      final long blockedRetainedSize = blockedSize;
      // Register ownership before listener publication, so terminal close also waits for a
      // pending-memory continuation. MemoryPool completes/cancels futures under their monitor:
      // its listener executor must only enqueue, never acquire this handle or run inline work.
      OwnedTask memoryReadyTask =
          new OwnedTask(
              () ->
                  org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager
                      .getInstance()
                      .trackFuture(
                          localFragmentInstanceId.getQueryId(),
                          fullFragmentInstanceId,
                          submitOwnedTask(
                              new GetDataBlocksTask(
                                  blockedSequenceId, blockedSequenceId + 1, blockedRetainedSize))));
      inFlightEndpointTasks++;
      blockedOnMemory.addListener(
          () -> executeOwnedTask(memoryReadyTask), java.util.concurrent.ForkJoinPool.commonPool());
    }

    if (endSequenceId > startSequenceId) {
      org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
          .trackFuture(
              localFragmentInstanceId.getQueryId(),
              fullFragmentInstanceId,
              submitOwnedTask(
                  new GetDataBlocksTask(startSequenceId, endSequenceId, reservedBytes)));
    }
  }

  /** Observation only: a legacy closed bit never manufactures a coordinated receipt. */
  public synchronized ListenableFuture<Void> getCoordinatedCloseFuture() {
    return coordinatedCloseRequested ? nonCancellationPropagating(coordinatedCloseFuture) : null;
  }

  public boolean isRuntimeFrontierMode() {
    return frontier != null;
  }

  public RuntimeEndpointFrontier getRuntimeFrontier() {
    return frontier;
  }

  public synchronized int getRuntimeInFlightTaskCount() {
    return inFlightEndpointTasks;
  }

  public synchronized int getRuntimeConsumerCount() {
    return inFlightConsumers;
  }

  public synchronized int getRuntimeResponsePinCount() {
    return 0;
  }

  public synchronized int getRuntimePendingCreditReturnCount() {
    return pendingCreditReturns;
  }

  /** Buffered payload only; active fetch/decoder/transport ownership is reported separately. */
  public synchronized Long getRuntimeBufferedPayloadBytes() {
    if (frontier == null && localResultExchange == null) return null;
    return sequenceIdToTsBlock.values().stream().mapToLong(ByteBuffer::remaining).sum();
  }

  public boolean isFrontierMode() {
    return frontier != null;
  }

  public RuntimeFrontierReservation getFrontierReservation() {
    return frontier == null ? null : frontier.reservation();
  }

  public int getFrontierSlotIndex() {
    return frontier == null ? -1 : frontier.slot();
  }

  public MemoryReservationKey getActualAccountKey() {
    return frontier == null
        ? new MemoryReservationKey(
            localFragmentInstanceId.getQueryId(), fullFragmentInstanceId, localPlanNodeId)
        : frontier.reservation().getMemoryReservationKey();
  }

  private void trySubmitFrontierFetch() {
    if (endpointFailure != null
        || frontierInput != null
        || !sequenceIdToDataBlockSize.containsKey(nextSequenceId)) return;
    long announced = sequenceIdToDataBlockSize.get(nextSequenceId);
    if (announced <= 0 || announced > frontier.bounds().maxLogicalBytes()) {
      throw new IllegalArgumentException("Remote block exceeds its admitted logical bound");
    }
    int sequence = nextSequenceId;
    int next = Math.incrementExact(sequence);
    InputLease lease =
        new InputLease(sequence, frontier.reservation().acquireEnvelope(frontier.slot(), this));
    frontierInput = lease;
    nextSequenceId = next;
    submitOwnedTask(new FrontierFetchTask(lease));
  }

  /** Claim only after every task/buffer/consumer reference has genuinely exited. */
  private RuntimeFrontierReservation.BlockCredit claimInputReturn(InputLease lease) {
    if (lease.credit == null || lease.fetchActive || lease.mapOwner || lease.consuming) return null;
    RuntimeFrontierReservation.BlockCredit credit = lease.credit;
    lease.credit = null;
    lease.data = null;
    // Keep the slot occupied until the real RFR.release has finished outside this monitor.
    pendingCreditReturns++;
    return credit;
  }

  private void returnInputCredit(RuntimeFrontierReservation.BlockCredit credit) {
    if (credit == null) return;
    runOutsideMonitor(
        () -> {
          try {
            frontier.reservation().release(credit);
          } catch (Throwable failure) {
            recordEndpointFailure(failure);
          } finally {
            Throwable nextFailure = null;
            synchronized (SourceHandle.this) {
              pendingCreditReturns--;
              if (frontierInput != null
                  && frontierInput.credit == null
                  && !frontierInput.fetchActive
                  && !frontierInput.mapOwner
                  && !frontierInput.consuming) {
                frontierInput = null;
              }
              if (endpointFailure == null
                  && !closed
                  && !aborted
                  && !terminalCleanupStarted
                  && canGetTsBlockFromRemote) {
                try {
                  trySubmitGetDataBlocksTask();
                } catch (Throwable failure) {
                  nextFailure = failure;
                }
              }
            }
            if (nextFailure != null) notifyFailure(nextFailure);
            advanceCoordinatedClose();
          }
        });
  }

  private TsBlock receiveFrontier() {
    InputLease lease;
    ByteBuffer encoded;
    boolean finished;
    synchronized (this) {
      checkState();
      if (!blocked.isDone())
        throw new IllegalStateException(DataNodeQueryMessages.SOURCE_HANDLE_IS_BLOCKED);
      lease = frontierInput;
      if (lease == null || !lease.mapOwner || lease.data == null) return null;
      encoded = lease.data;
      lease.data = null;
      lease.mapOwner = false;
      lease.consuming = true;
      inFlightConsumers++;
      sequenceIdToTsBlock.remove(currSequenceId);
      sequenceIdToDataBlockSize.remove(currSequenceId);
      currSequenceId++;
      bufferRetainedSizeInBytes = 0;
      finished = remoteTsBlockedConsumedUp();
      if (!finished) blocked = SettableFuture.create();
    }
    try {
      RuntimeExchangeBlockCodec.validateEncoded(encoded, frontier.bounds());
      TsBlock decoded = serde.deserialize(encoded);
      if (decoded.getRetainedSizeInBytes() > frontier.bounds().maxDecodedBytes()) {
        throw new IllegalStateException("Serde violated admitted decoded bound");
      }
      if (finished) runOutsideMonitor(() -> sourceHandleListener.onFinished(this));
      return decoded;
    } catch (Throwable failure) {
      notifyFailure(failure);
      rethrowEndpointFailure(failure);
      return null;
    } finally {
      encoded = null;
      RuntimeFrontierReservation.BlockCredit credit;
      synchronized (this) {
        lease.consuming = false;
        inFlightConsumers--;
        credit = claimInputReturn(lease);
      }
      returnInputCredit(credit);
      advanceCoordinatedClose();
    }
  }

  /** One fixed-size assembly and one current response, never a batch of fetched blocks. */
  private final class FrontierFetchTask implements Runnable {
    private final InputLease lease;

    private FrontierFetchTask(InputLease lease) {
      this.lease = lease;
    }

    private void settledAfterTaskExit() {
      RuntimeFrontierReservation.BlockCredit credit;
      synchronized (SourceHandle.this) {
        lease.fetchActive = false;
        credit = claimInputReturn(lease);
      }
      returnInputCredit(credit);
    }

    @Override
    public void run() {
      byte[] assembly = null;
      int received = 0;
      int total = -1;
      try {
        for (int attempt = 1; attempt <= MAX_ATTEMPT_TIMES; attempt++) {
          try (org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease client =
              openExchangeClient()) {
            while (true) {
              synchronized (SourceHandle.this) {
                if (closed || aborted) return;
              }
              TGetDataBlockRequest request =
                  new TGetDataBlockRequest(
                          remoteFragmentInstanceId,
                          lease.sequence,
                          lease.sequence + 1,
                          indexOfUpstreamSinkHandle)
                      .setOffset(received);
              TGetDataBlockResponse response = client.getDataBlock(request);
              if (!client.isLocal())
                REMOTE_SHUFFLE_PAYLOAD_BYTE_TRACKER.recordReceived(
                    localFragmentInstanceId,
                    remoteFragmentInstanceId,
                    indexOfUpstreamSinkHandle,
                    response.getTsBlocks());
              synchronized (SourceHandle.this) {
                if (closed || aborted) return;
              }
              if (response.getTsBlocks() == null || response.getTsBlocksSize() != 1) {
                throw new IllegalArgumentException(
                    "One runtime input credit requires exactly one block");
              }
              ByteBuffer fragment = response.getTsBlocks().get(0).duplicate();
              int bytes = fragment.remaining();
              if (bytes <= 0 || bytes > frontier.bounds().maxSerializedBytes()) {
                throw new IllegalArgumentException(
                    "Remote fragment exceeds admitted encoded bound");
              }
              if (assembly == null) {
                total = response.isSetOffset() ? response.getTotalLength() : bytes;
                if ((response.isSetOffset() && !response.isSetTotalLength())
                    || total < bytes
                    || total > frontier.bounds().maxSerializedBytes()
                    || total <= 0) {
                  throw new IllegalArgumentException("Invalid admitted remote totalLength");
                }
                assembly = new byte[total];
              } else if (response.isSetTotalLength() && response.getTotalLength() != total) {
                throw new IllegalArgumentException("Remote totalLength changed");
              }
              int next = Math.addExact(received, bytes);
              if (next > total
                  || (response.isSetOffset() && (response.getOffset() != next || next >= total))
                  || (!response.isSetOffset() && next != total)) {
                throw new IllegalArgumentException("Invalid admitted fragment offset/length");
              }
              fragment.get(assembly, received, bytes);
              received = next;
              boolean complete = !response.isSetOffset();
              fragment = null;
              response = null;
              if (complete) {
                ByteBuffer payload = ByteBuffer.wrap(assembly);
                SettableFuture<Void> wake;
                synchronized (SourceHandle.this) {
                  if (closed || aborted) return;
                  lease.data = payload;
                  lease.mapOwner = true;
                  sequenceIdToTsBlock.put(lease.sequence, payload);
                  bufferRetainedSizeInBytes = payload.remaining();
                  wake = blocked;
                }
                submitOwnedTask(
                    new SendAcknowledgeDataBlockEventTask(lease.sequence, lease.sequence + 1));
                wake.set(null);
                return;
              }
            }
          } catch (IllegalArgumentException | IllegalStateException failure) {
            throw failure;
          } catch (Exception failure) {
            if (attempt == MAX_ATTEMPT_TIMES) throw failure;
            Thread.sleep(retryIntervalInMs);
          }
        }
      } catch (Throwable failure) {
        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
        notifyFailure(failure);
      }
    }
  }

  @Override
  public synchronized ListenableFuture<?> isBlocked() {
    checkState();
    if (!canGetTsBlockFromRemote) {
      canGetTsBlockFromRemote = true;
      // submit get data task once isBlocked is called to ensure that the blocked future will be
      // completed in case that trySubmitGetDataBlocksTask() is not called.
      trySubmitGetDataBlocksTask();
    }
    if (org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
        .isEnabled())
      org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
          .trackFuture(
              localFragmentInstanceId.getQueryId(),
              org.apache.iotdb.db.queryengine.common.FragmentInstanceId
                  .createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
              blocked);
    return nonCancellationPropagating(blocked);
  }

  public synchronized void setNoMoreTsBlocks(int lastSequenceId) {
    if (aborted || closed) {
      return;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(DataNodeQueryMessages.RECEIVE_NO_MORE_TSBLOCK_EVENT);
    }
    this.lastSequenceId = lastSequenceId;
    if (!blocked.isDone() && remoteTsBlockedConsumedUp()) {
      blocked.set(null);
    }
    if (isFinished()) {
      sourceHandleListener.onFinished(this);
    }
  }

  public void updatePendingDataBlockInfo(int startSequenceId, List<Long> dataBlockSizes) {
    try {
      synchronized (this) {
        if (aborted || closed) {
          return;
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.RECEIVENEWTSBLOCKNOTIFICATION_ARG_ARG_EACH_SIZE_IS_ARG,
              startSequenceId,
              startSequenceId + dataBlockSizes.size(),
              dataBlockSizes);
        }
        for (int i = 0; i < dataBlockSizes.size(); i++) {
          sequenceIdToDataBlockSize.put(i + startSequenceId, dataBlockSizes.get(i));
        }
        if (canGetTsBlockFromRemote) {
          trySubmitGetDataBlocksTask();
        }
      }
    } catch (RuntimeException | Error t) {
      // This method runs in the inbound RPC thread. Mark the local FI failed before the same
      // exception is returned to the upstream FI through the existing Thrift error channel.
      notifyFailure(t);
      throw t;
    }
  }

  private void notifyFailure(Throwable t) {
    recordEndpointFailure(t);
    try {
      sourceHandleListener.onFailure(this, t);
    } catch (Throwable callbackFailure) {
      if (callbackFailure != t) {
        t.addSuppressed(callbackFailure);
      }
    }
  }

  @Override
  public void abort() {
    terminateEndpoint(true, null);
  }

  @Override
  public void abort(Throwable cause) {
    terminateEndpoint(true, cause);
  }

  @Override
  public void close() {
    terminateEndpoint(false, null);
  }

  private void terminateEndpoint(boolean abort, Throwable cause) {
    SettableFuture<Void> wake;
    ListenableFuture<Void> pendingMemory;
    RuntimeFrontierReservation.BlockCredit abandoned;
    boolean first;
    synchronized (this) {
      first = !terminalCleanupStarted;
      wake = blocked;
      pendingMemory = blockedOnMemory;
      abandoned = null;
      if (first) {
        terminalCleanupStarted = true;
        aborted = abort;
        closed = !abort;
        remoteCloseRequired |= !abort;
        sequenceIdToTsBlock.clear();
        sequenceIdToDataBlockSize.clear();
        if (frontier != null && frontierInput != null) {
          frontierInput.mapOwner = false;
          frontierInput.data = null;
          abandoned = claimInputReturn(frontierInput);
        }
        if (!abort) {
          currSequenceId = lastSequenceId + 1;
        }
      }
    }
    if (first) {

      RuntimeFrontierReservation.BlockCredit discarded = abandoned;
      runOutsideMonitor(
          () -> {
            try {
              if (localResultExchange != null) {
                try {
                  localResultExchange.markConsumerClosed();
                } catch (Throwable failure) {
                  recordTerminalActionFailure(failure);
                }
              }
              if (frontier == null && localResultExchange != null) {
                boolean known = cancelPendingNativeMemory(pendingMemory);
                synchronized (SourceHandle.this) {
                  localNativeCancellationKnown = known;
                  localNativeSettlementPending = true;
                }
              } else if (frontier == null) settleTerminalMemory(pendingMemory);
              else {
                returnInputCredit(discarded);
                synchronized (SourceHandle.this) {
                  bufferRetainedSizeInBytes = 0;
                }
              }
              try {
                if (wake != null && !wake.isDone()) {
                  if (abort && cause == null) {
                    wake.cancel(true);
                  } else if (abort) {
                    wake.setException(cause);
                  } else {
                    wake.set(null);
                  }
                }
              } catch (Throwable failure) {
                recordTerminalActionFailure(failure);
              }
              try {
                if (abort) {
                  sourceHandleListener.onAborted(this);
                } else {
                  sourceHandleListener.onFinished(this);
                }
              } catch (Throwable failure) {
                recordTerminalActionFailure(failure);
              }
            } finally {
              synchronized (this) {
                terminalCleanupFinished = true;
              }
              advanceCoordinatedClose();
            }
          });
    } else {
      advanceCoordinatedClose();
    }
    Throwable failure;
    synchronized (this) {
      failure = first ? terminalActionFailure : null;
    }
    rethrowEndpointFailure(failure);
  }

  private void settleTerminalMemory(ListenableFuture<Void> pendingMemory) {
    if (cancelPendingNativeMemory(pendingMemory)) freeNativeTerminalMemory();
  }

  private boolean cancelPendingNativeMemory(ListenableFuture<Void> pendingMemory) {
    boolean cancellationKnown = true;
    if (pendingMemory != null) {
      try {
        long cancelled = localMemoryManager.getQueryPool().tryCancel(pendingMemory);
        synchronized (this) {
          bufferRetainedSizeInBytes -= cancelled;
        }
      } catch (Throwable failure) {
        recordTerminalActionFailure(failure);
        cancellationKnown = false;
      }
    }
    return cancellationKnown;
  }

  private void freeNativeTerminalMemory() {
    long retained;
    synchronized (this) {
      retained = bufferRetainedSizeInBytes;
    }
    if (retained > 0) {
      try {
        localMemoryManager
            .getQueryPool()
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                retained);
        synchronized (this) {
          bufferRetainedSizeInBytes = 0;
        }
      } catch (Throwable failure) {
        // A void free may have changed accounting before throwing. Never retry it.
        recordTerminalActionFailure(failure);
      }
    }
  }

  private boolean settleLocalNativeMemoryIfReady() {
    boolean known;
    synchronized (this) {
      if (!localNativeSettlementPending) return false;
      if (!terminalCleanupFinished || inFlightEndpointTasks != 0 || localNativeSettlementClaimed)
        return true;
      localNativeSettlementClaimed = true;
      known = localNativeCancellationKnown;
    }
    runOutsideMonitor(
        () -> {
          try {
            if (known) freeNativeTerminalMemory();
          } finally {
            synchronized (SourceHandle.this) {
              localNativeSettlementPending = false;
            }
            advanceCoordinatedClose();
          }
        });
    return true;
  }

  private void recordEndpointFailure(Throwable failure) {
    synchronized (this) {
      if (endpointFailure == null) {
        endpointFailure = failure;
      } else if (endpointFailure != failure) {
        endpointFailure.addSuppressed(failure);
      }
    }
  }

  private void recordTerminalActionFailure(Throwable failure) {
    synchronized (this) {
      if (terminalActionFailure == null) {
        terminalActionFailure = failure;
      }
    }
    recordEndpointFailure(failure);
  }

  private static void rethrowEndpointFailure(Throwable failure) {
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure != null) {
      throw new RuntimeException(failure);
    }
  }

  /** Count publication before execute, and release only after actual run exit or pre-run cancel. */
  private final class OwnedTask extends java.util.concurrent.FutureTask<Void> {
    private final Runnable delegate;
    private final java.util.concurrent.atomic.AtomicBoolean started =
        new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicBoolean settled =
        new java.util.concurrent.atomic.AtomicBoolean();

    private OwnedTask(Runnable task) {
      super(
          () -> {
            try {
              task.run();
            } catch (Throwable failure) {
              recordEndpointFailure(failure);
              rethrowEndpointFailure(failure);
            }
            return null;
          });
      this.delegate = task;
    }

    @Override
    public void run() {
      // Caller-runs/direct executors must not execute RPC callbacks under the submitter's lock.
      runOutsideMonitor(this::runOutsideHandleMonitor);
    }

    private void runOutsideHandleMonitor() {
      if (!started.compareAndSet(false, true)) {
        return;
      }
      try {
        super.run();
      } finally {
        settle();
      }
    }

    @Override
    protected void done() {
      if (isCancelled()) {
        recordEndpointFailure(
            new java.util.concurrent.CancellationException(
                "Endpoint task was cancelled before actual settlement"));
      }
      // FutureTask cancellation while running does not prove that the RPC/callback has exited.
      if (!started.get()) {
        settle();
      }
    }

    private void settle() {
      if (settled.compareAndSet(false, true)) {
        try {
          if (delegate instanceof FrontierFetchTask) {
            ((FrontierFetchTask) delegate).settledAfterTaskExit();
          } else if (localResultExchange != null && delegate instanceof GetDataBlocksTask) {
            ((GetDataBlocksTask) delegate).settledAfterTaskExit();
          }
        } catch (Throwable failure) {
          recordEndpointFailure(failure);
        } finally {
          synchronized (SourceHandle.this) {
            inFlightEndpointTasks--;
          }
          advanceCoordinatedClose();
        }
      }
    }
  }

  private java.util.concurrent.Future<?> submitOwnedTask(Runnable task) {
    OwnedTask owned;
    synchronized (this) {
      if (terminalCleanupStarted || coordinatedCloseRequested) {
        return com.google.common.util.concurrent.Futures.immediateVoidFuture();
      }
      inFlightEndpointTasks++;
      owned = new OwnedTask(task);
    }
    executeOwnedTask(owned);
    return owned;
  }

  private void executeOwnedTask(OwnedTask task) {
    try {
      executorService.execute(task);
    } catch (Throwable failure) {
      recordEndpointFailure(failure);
      task.cancel(false);
      rethrowEndpointFailure(failure);
    }
  }

  private void completeCoordinatedNotification(Throwable failure) {
    Runnable notification =
        () -> {
          if (failure == null) {
            coordinatedCloseFuture.set(null);
          } else {
            coordinatedCloseFuture.setException(failure);
          }
        };
    runOutsideMonitor(notification);
  }

  private void runOutsideMonitor(Runnable action) {
    if (Thread.holdsLock(this)) {
      // Direct executors and reentrant callers may still own an outer handle monitor.
      java.util.concurrent.ForkJoinPool.commonPool()
          .execute(
              () -> {
                synchronized (SourceHandle.this) {
                  // Wait for the outermost caller to release its monitor before notifications.
                }
                action.run();
              });
    } else {
      action.run();
    }
  }

  /**
   * Acknowledges this handle's tracked work, not ByteBuffers already handed to an RPC transport.
   */
  public ListenableFuture<Void> requestCoordinatedClose() {
    return requestCoordinatedTermination(false, null);
  }

  public ListenableFuture<Void> requestCoordinatedAbort() {
    return requestCoordinatedTermination(true, null);
  }

  public ListenableFuture<Void> requestCoordinatedAbort(Throwable cause) {
    return requestCoordinatedTermination(true, cause);
  }

  private ListenableFuture<Void> requestCoordinatedTermination(boolean abort, Throwable cause) {
    synchronized (this) {
      coordinatedCloseRequested = true;
      remoteCloseRequired = true;
    }
    try {
      if (abort) {
        if (cause == null) {
          abort();
        } else {
          abort(cause);
        }
      } else {
        close();
      }
    } catch (Throwable failure) {
      recordEndpointFailure(failure);
    } finally {
      advanceCoordinatedClose();
    }
    return nonCancellationPropagating(coordinatedCloseFuture);
  }

  private void advanceCoordinatedClose() {
    if (settleLocalNativeMemoryIfReady()) return;
    OwnedTask closeEvent = null;
    boolean notify = false;
    Throwable failure = null;
    synchronized (this) {
      if (!terminalCleanupFinished
          || inFlightEndpointTasks != 0
          || inFlightConsumers != 0
          || pendingCreditReturns != 0) {
        return;
      }
      if (remoteCloseRequired && !remoteClosePublished) {
        remoteClosePublished = true;
        inFlightEndpointTasks++;
        closeEvent = new OwnedTask(new SendCloseSinkChannelEventTask());
      } else if (coordinatedCloseRequested && !coordinatedNotificationClaimed) {
        coordinatedNotificationClaimed = true;
        failure = endpointFailure;
        notify = true;
      }
    }
    if (closeEvent != null) {
      executeOwnedTask(closeEvent);
    } else if (notify) {
      completeCoordinatedNotification(failure);
    }
  }

  @Override
  public boolean isFinished() {
    return remoteTsBlockedConsumedUp();
  }

  // Return true indicates two points:
  //   1. Remote SinkHandle has told SourceHandle the total count of TsBlocks by lastSequenceId
  //   2. All the TsBlocks has been consumed up
  private synchronized boolean remoteTsBlockedConsumedUp() {
    return currSequenceId - 1 == lastSequenceId;
  }

  public TEndPoint getRemoteEndpoint() {
    return remoteEndpoint;
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId.deepCopy();
  }

  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  public String getLocalPlanNodeId() {
    return localPlanNodeId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = Math.min(this.maxBytesCanReserve, maxBytesCanReserve);
  }

  @Override
  public synchronized boolean isClosed() {
    return closed;
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  private void checkState() {
    if (aborted) {
      if (blocked.isDone()) {
        // try throw underlying exception instead of "Source handle is aborted."
        try {
          blocked.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        } catch (ExecutionException e) {
          throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
        }
      }
      throw new IllegalStateException(DataNodeQueryMessages.SOURCE_HANDLE_IS_ABORTED);
    } else if (closed) {
      throw new IllegalStateException(DataNodeQueryMessages.SOURCEHANDLE_IS_CLOSED);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "Query[%s]-[%s-%s-SourceHandle-%s]",
        localFragmentInstanceId.getQueryId(),
        localFragmentInstanceId.getFragmentId(),
        fullFragmentInstanceId,
        localPlanNodeId);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(threadName)
        + RamUsageEstimator.sizeOf(localPlanNodeId)
        + RamUsageEstimator.sizeOf(fullFragmentInstanceId);
  }

  @TestOnly
  public void setRetryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
  }

  /** Get data blocks from an upstream fragment instance. */
  class GetDataBlocksTask implements Runnable {
    private final int startSequenceId;
    private final int endSequenceId;
    private final long reservedBytes;
    private long deferredFailedBytes;

    GetDataBlocksTask(int startSequenceId, int endSequenceId, long reservedBytes) {
      Validate.isTrue(
          startSequenceId >= 0,
          DataNodeQueryMessages
                  .EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_DOT_START_SEQUENCE_ID__D3C0AAB7
              + startSequenceId);
      this.startSequenceId = startSequenceId;
      Validate.isTrue(
          endSequenceId > startSequenceId,
          DataNodeQueryMessages
                  .EXCEPTION_END_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_THE_START_SEQUENCE_ID_DOT_START_SEQUENCE__DF1AA2A1
              + startSequenceId
              + DataNodeQueryMessages.EXCEPTION_COMMA_END_SEQUENCE_ID_COLON_DB1AF173
              + endSequenceId);
      this.endSequenceId = endSequenceId;
      Validate.isTrue(
          reservedBytes > 0L,
          DataNodeQueryMessages.EXCEPTION_RESERVED_BYTES_SHOULD_BE_GREATER_THAN_ZERO_DOT_64086BE5);
      this.reservedBytes = reservedBytes;
    }

    @Override
    @SuppressWarnings("squid:S3776")
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled() && localResultExchange == null) {
          LOGGER.debug(
              DataNodeQueryMessages.STARTPULLTSBLOCKSFROMREMOTE_ARG_ARG_ARG_ARG,
              remoteFragmentInstanceId,
              indexOfUpstreamSinkHandle,
              startSequenceId,
              endSequenceId);
        }
        TGetDataBlockRequest req =
            new TGetDataBlockRequest(
                remoteFragmentInstanceId,
                startSequenceId,
                endSequenceId,
                indexOfUpstreamSinkHandle);
        DataBlockFetchProgress fetchProgress =
            new DataBlockFetchProgress(startSequenceId, endSequenceId);
        int attempt = 0;
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;

          long startTime = System.nanoTime();
          boolean transferAttemptRecorded = false;
          try (org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease client =
              openExchangeClient()) {
            TGetDataBlockResponse resp = getDataBlockWithFragments(client, req, fetchProgress);
            if (resp == null) {
              return;
            }
            int tsBlockNum = resp.getTsBlocks().size();
            if (tsBlockNum != endSequenceId - startSequenceId) {
              recordTransferAttempt(
                  false,
                  tsBlockNum == 0
                      ? UserDataTransferErrorCode.EMPTY_RESPONSE.name()
                      : UserDataTransferErrorCode.UNEXPECTED_RESPONSE_SIZE.name(),
                  null);
              transferAttemptRecorded = true;
              if (!closed) {
                // failed to pull TsBlocks
                LOGGER.warn(
                    DataNodeQueryMessages.FAILED_TO_PULL_TSBLOCKS,
                    localFragmentInstanceId,
                    startSequenceId,
                    endSequenceId,
                    remoteFragmentInstanceId,
                    indexOfUpstreamSinkHandle);
              }
              if (tsBlockNum == 0) {
                return;
              }
              throw new TException(
                  DataNodeQueryMessages.EXCEPTION_UNEXPECTED_DATA_BLOCK_RESPONSE_SIZE_A7DD7E33);
            }
            List<ByteBuffer> tsBlocks = new ArrayList<>(tsBlockNum);
            tsBlocks.addAll(resp.getTsBlocks());

            if (LOGGER.isDebugEnabled() && localResultExchange == null) {
              LOGGER.debug(DataNodeQueryMessages.END_PULL_TSBLOCKS_FROM_REMOTE, tsBlockNum);
            }
            DATA_EXCHANGE_COUNT_METRIC_SET.recordDataBlockNum(
                GET_DATA_BLOCK_NUM_CALLER, tsBlockNum);
            submitOwnedTask(new SendAcknowledgeDataBlockEventTask(startSequenceId, endSequenceId));
            boolean receiverClosed = false;
            synchronized (SourceHandle.this) {
              if (aborted || closed) {
                receiverClosed = true;
              } else {
                for (int i = startSequenceId; i < endSequenceId; i++) {
                  sequenceIdToTsBlock.put(i, tsBlocks.get(i - startSequenceId));
                }
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(DataNodeQueryMessages.PUT_TSBLOCKS_INTO_BUFFER);
                }
                if (!blocked.isDone()) {
                  blocked.set(null);
                }
              }
            }
            recordTransferAttempt(
                !receiverClosed,
                receiverClosed ? UserDataTransferErrorCode.RECEIVER_CLOSED.name() : null,
                null);
            transferAttemptRecorded = true;
            if (receiverClosed) {
              return;
            }
            break;
          } catch (IllegalArgumentException | IllegalStateException e) {
            if (!transferAttemptRecorded) {
              recordTransferAttempt(
                  false, UserDataTransferErrorCode.UNEXPECTED_RESPONSE_SIZE.name(), e);
            }
            fail(e);
            return;
          } catch (Error e) {
            if (!transferAttemptRecorded) {
              recordTransferAttempt(
                  false,
                  e instanceof OutOfMemoryError
                      ? UserDataTransferErrorCode.OUT_OF_MEMORY.name()
                      : null,
                  e);
            }
            fail(e);
            throw e;
          } catch (Exception e) {

            if (!transferAttemptRecorded) {
              recordTransferAttempt(false, null, e);
            }

            LOGGER.warn(
                DataNodeQueryMessages.FAILED_TO_GET_DATA_BLOCK,
                startSequenceId,
                endSequenceId,
                attempt,
                e);

            // reach retry max times
            if (attempt == MAX_ATTEMPT_TIMES) {
              fail(e);
              return;
            }

            // sleep some time before retrying
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              // if interrupted during sleeping, fast fail and don't retry any more
              fail(e);
            }
          } finally {
            DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
                GET_DATA_BLOCK_TASK_CALLER, System.nanoTime() - startTime);
          }
        }
      }
    }

    private void recordTransferAttempt(boolean success, String errorCode, Throwable error) {
      if (localResultExchange == null) {
        DataNodeUserDataTransferAuditor.record(
            localEndpoint, remoteEndpoint, localEndpoint, success, errorCode, error);
      }
    }

    private void fail(Throwable t) {
      synchronized (SourceHandle.this) {
        if (aborted || closed) {
          return;
        }
        if (localResultExchange != null) {
          deferredFailedBytes = reservedBytes;
        } else if (reservedBytes > 0) {
          bufferRetainedSizeInBytes -= reservedBytes;
          localMemoryManager
              .getQueryPool()
              .free(
                  localFragmentInstanceId.getQueryId(),
                  fullFragmentInstanceId,
                  localPlanNodeId,
                  reservedBytes);
        }
      }
      notifyFailure(t);
    }

    private void settledAfterTaskExit() {
      long bytes;
      synchronized (SourceHandle.this) {
        bytes = deferredFailedBytes;
        deferredFailedBytes = 0;
        if (terminalCleanupStarted || bytes <= 0) return;
        bufferRetainedSizeInBytes -= bytes;
      }
      try {
        localMemoryManager
            .getQueryPool()
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                bytes);
      } catch (Throwable failure) {
        recordEndpointFailure(failure);
      }
    }

    private TGetDataBlockResponse getDataBlockWithFragments(
        org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease client,
        TGetDataBlockRequest request,
        DataBlockFetchProgress fetchProgress)
        throws TException {
      while (!fetchProgress.isFinished()) {
        synchronized (SourceHandle.this) {
          if (aborted || closed) {
            fetchProgress.discard();
            return null;
          }
        }
        TGetDataBlockRequest fragmentRequest = request.deepCopy();
        fragmentRequest.setStartSequenceId(fetchProgress.nextSequenceId);
        fragmentRequest.setOffset(fetchProgress.offset);
        TGetDataBlockResponse response = client.getDataBlock(fragmentRequest);
        if (!client.isLocal())
          REMOTE_SHUFFLE_PAYLOAD_BYTE_TRACKER.recordReceived(
              localFragmentInstanceId,
              remoteFragmentInstanceId,
              indexOfUpstreamSinkHandle,
              response.getTsBlocks());
        synchronized (SourceHandle.this) {
          if (aborted || closed) {
            fetchProgress.discard();
            return null;
          }
          if (response.getTsBlocks().isEmpty()) {
            return response;
          }
          fetchProgress.addResponse(response);
        }
      }
      return new TGetDataBlockResponse(fetchProgress.tsBlocks);
    }

    private class DataBlockFetchProgress {
      private final int endSequenceId;
      private final List<ByteBuffer> tsBlocks;
      private int nextSequenceId;
      private int offset;
      private PublicBAOS partialTsBlock;
      private int partialTsBlockTotalLength;

      private DataBlockFetchProgress(int startSequenceId, int endSequenceId) {
        this.nextSequenceId = startSequenceId;
        this.endSequenceId = endSequenceId;
        this.tsBlocks = new ArrayList<>(endSequenceId - startSequenceId);
      }

      private boolean isFinished() {
        return nextSequenceId == endSequenceId && partialTsBlock == null;
      }

      private void discard() {
        tsBlocks.clear();
        partialTsBlock = null;
        partialTsBlockTotalLength = 0;
        offset = 0;
      }

      private void addResponse(TGetDataBlockResponse response) {
        List<ByteBuffer> responseBlocks = response.getTsBlocks();
        boolean lastBlockIsFragment = response.isSetOffset();
        int blockIndex = 0;

        if (partialTsBlock != null) {
          appendFragment(responseBlocks.get(blockIndex++));
          if (lastBlockIsFragment && blockIndex == responseBlocks.size()) {
            updateOffset(response.getOffset());
            return;
          }
          Preconditions.checkState(
              partialTsBlock.size() == partialTsBlockTotalLength,
              DataNodeQueryMessages
                  .EXCEPTION_ACCUMULATED_TSBLOCK_FRAGMENT_LENGTH_ARG_DOES_NOT_MATCH_TOTALLENGTH_ARG_1B784303,
              partialTsBlock.size(),
              partialTsBlockTotalLength);
          tsBlocks.add(ByteBuffer.wrap(partialTsBlock.getBuf(), 0, partialTsBlock.size()));
          partialTsBlock = null;
          partialTsBlockTotalLength = 0;
          offset = 0;
          nextSequenceId++;
        }

        int lastCompleteBlockIndex =
            lastBlockIsFragment ? responseBlocks.size() - 1 : responseBlocks.size();
        while (blockIndex < lastCompleteBlockIndex) {
          tsBlocks.add(responseBlocks.get(blockIndex++));
          nextSequenceId++;
        }

        if (lastBlockIsFragment) {
          checkArgument(
              response.isSetTotalLength(),
              DataNodeQueryMessages
                  .EXCEPTION_THE_FIRST_FRAGMENTED_DATA_BLOCK_RESPONSE_MUST_INCLUDE_TOTALLENGTH_C5C79BC2);
          partialTsBlockTotalLength = response.getTotalLength();
          partialTsBlock = new PublicBAOS(partialTsBlockTotalLength);
          appendFragment(responseBlocks.get(blockIndex));
          updateOffset(response.getOffset());
        }

        Preconditions.checkState(
            nextSequenceId <= endSequenceId,
            DataNodeQueryMessages
                .EXCEPTION_NEXT_SEQUENCE_ID_ARG_EXCEEDS_REQUESTED_END_SEQUENCE_ID_ARG_30B1726E,
            nextSequenceId,
            endSequenceId);
        Preconditions.checkState(
            lastBlockIsFragment || nextSequenceId != endSequenceId || partialTsBlock == null,
            DataNodeQueryMessages
                .EXCEPTION_A_COMPLETED_DATA_BLOCK_RESPONSE_RANGE_MUST_NOT_RETAIN_A_PARTIAL_TSBLOCK_85E5C287);
      }

      private void appendFragment(ByteBuffer fragment) {
        checkArgument(
            fragment.hasRemaining(),
            DataNodeQueryMessages.EXCEPTION_TSBLOCK_FRAGMENT_MUST_NOT_BE_EMPTY_C7D19863);
        partialTsBlock.writeBytes(fragment.array());
      }

      private void updateOffset(int nextOffset) {
        checkArgument(
            nextOffset > offset && nextOffset == partialTsBlock.size(),
            DataNodeQueryMessages
                .EXCEPTION_NEXT_FRAGMENT_OFFSET_ARG_MUST_BE_GREATER_THAN_CURRENT_OFFSET_ARG_AND_MATCH_ACCUMULATED_FRAGMENT_LENGTH_ARG_ECC31047,
            nextOffset,
            offset,
            partialTsBlock.size());
        offset = nextOffset;
      }
    }
  }

  class SendAcknowledgeDataBlockEventTask implements Runnable {

    private final int startSequenceId;
    private final int endSequenceId;

    public SendAcknowledgeDataBlockEventTask(int startSequenceId, int endSequenceId) {
      this.startSequenceId = startSequenceId;
      this.endSequenceId = endSequenceId;
    }

    @Override
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(DataNodeQueryMessages.SEND_ACK_TSBLOCK, startSequenceId, endSequenceId);
        }
        int attempt = 0;
        TAcknowledgeDataBlockEvent acknowledgeDataBlockEvent =
            new TAcknowledgeDataBlockEvent(
                remoteFragmentInstanceId,
                startSequenceId,
                endSequenceId,
                indexOfUpstreamSinkHandle);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          long startTime = System.nanoTime();
          try (org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease client =
              openExchangeClient()) {
            client.onAcknowledgeDataBlockEvent(acknowledgeDataBlockEvent);
            break;
          } catch (Throwable e) {
            LOGGER.warn(
                DataNodeQueryMessages.FAILED_TO_SEND_ACK_DATA_BLOCK_EVENT,
                startSequenceId,
                endSequenceId,
                attempt,
                e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              notifyFailure(e);
              return;
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              notifyFailure(e);
              return;
            }
          } finally {
            DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
                ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER, System.nanoTime() - startTime);
            DATA_EXCHANGE_COUNT_METRIC_SET.recordDataBlockNum(
                ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER, endSequenceId - startSequenceId);
          }
        }
      }
    }
  }

  class SendCloseSinkChannelEventTask implements Runnable {

    @Override
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.SENDCLOSESINKCHANNELEVENT_TO_SHUFFLESINKHANDLE_ARG_INDEX_ARG,
              remoteFragmentInstanceId,
              indexOfUpstreamSinkHandle);
        }
        int attempt = 0;
        TCloseSinkChannelEvent closeSinkChannelEvent =
            new TCloseSinkChannelEvent(remoteFragmentInstanceId, indexOfUpstreamSinkHandle);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease client =
              openExchangeClient()) {
            client.onCloseSinkChannelEvent(closeSinkChannelEvent);
            break;
          } catch (Throwable e) {
            LOGGER.warn(
                DataNodeQueryMessages.SEND_CLOSE_SINK_CHANNEL_EVENT_FAILED,
                remoteFragmentInstanceId,
                indexOfUpstreamSinkHandle);
            if (attempt == MAX_ATTEMPT_TIMES) {
              notifyFailure(e);
              return;
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              notifyFailure(e);
              return;
            }
          }
        }
      }
    }
  }

  private org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease
      openExchangeClient() throws org.apache.iotdb.commons.client.exception.ClientManagerException {
    return org.apache.iotdb.db.queryengine.execution.exchange.ExchangeClientLease.open(
        mppDataExchangeServiceClientManager, remoteEndpoint, localResultExchange);
  }

  public boolean isLocalResultExchange() {
    return localResultExchange != null;
  }

  @Override
  public String getTransportMode() {
    return localResultExchange == null ? "REMOTE_RPC" : "LOCAL_SERIALIZED_RESULT";
  }
}
