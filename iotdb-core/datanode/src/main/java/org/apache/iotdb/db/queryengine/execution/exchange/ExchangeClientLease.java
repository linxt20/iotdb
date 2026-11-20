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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import org.apache.thrift.TException;

/** Reuses the endpoint state machines while making transport ownership explicit. */
public final class ExchangeClientLease implements AutoCloseable {
  private final SyncDataNodeMPPDataExchangeServiceClient remote;
  private final LocalResultExchange local;

  private ExchangeClientLease(
      SyncDataNodeMPPDataExchangeServiceClient remote, LocalResultExchange local) {
    this.remote = remote;
    this.local = local;
  }

  public static ExchangeClientLease open(
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> manager,
      TEndPoint endpoint,
      LocalResultExchange local)
      throws ClientManagerException {
    return local == null
        ? new ExchangeClientLease(manager.borrowClient(endpoint), null)
        : new ExchangeClientLease(null, local);
  }

  public boolean isLocal() {
    return local != null;
  }

  public TGetDataBlockResponse getDataBlock(TGetDataBlockRequest request) throws TException {
    return local == null ? remote.getDataBlock(request) : local.getDataBlock(request);
  }

  public void onNewDataBlockEvent(TNewDataBlockEvent event) throws TException {
    if (local == null) remote.onNewDataBlockEvent(event);
    else local.onNewDataBlockEvent(event);
  }

  public void onEndOfDataBlockEvent(TEndOfDataBlockEvent event) throws TException {
    if (local == null) remote.onEndOfDataBlockEvent(event);
    else local.onEndOfDataBlockEvent(event);
  }

  public void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent event) throws TException {
    if (local == null) remote.onAcknowledgeDataBlockEvent(event);
    else local.onAcknowledgeDataBlockEvent(event);
  }

  public void onCloseSinkChannelEvent(TCloseSinkChannelEvent event) throws TException {
    if (local == null) remote.onCloseSinkChannelEvent(event);
    else local.onCloseSinkChannelEvent(event);
  }

  @Override
  public void close() {
    if (remote != null) remote.close();
  }
}
