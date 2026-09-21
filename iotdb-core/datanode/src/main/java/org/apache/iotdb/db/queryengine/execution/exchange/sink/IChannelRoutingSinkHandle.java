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

package org.apache.iotdb.db.queryengine.execution.exchange.sink;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

/**
 * A sink handle that can route an output block to an explicitly selected downstream channel.
 *
 * <p>The ordinary {@link ISinkHandle#send(TsBlock)} operation is intentionally insufficient for a
 * hash exchange because it follows that handle's scheduling strategy. Callers that need a
 * partitioning guarantee must use this capability instead of relying on the current channel.
 */
public interface IChannelRoutingSinkHandle extends ISinkHandle {

  /** Returns the number of addressable downstream channels. */
  int getChannelCount();

  /** Sends a block to exactly {@code channelIndex}; it must not change the handle's scheduler. */
  void sendToChannel(int channelIndex, TsBlock tsBlock);

  /**
   * Returns a future that completes only after every addressable channel can accept its next block.
   * This conservative gate prevents a hash exchange from bypassing channel backpressure.
   */
  ListenableFuture<?> isAllChannelsNotFull();
}
