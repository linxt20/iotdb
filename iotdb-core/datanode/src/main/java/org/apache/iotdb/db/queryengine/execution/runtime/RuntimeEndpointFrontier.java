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

package org.apache.iotdb.db.queryengine.execution.runtime;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import java.util.Objects;

/** One pre-admitted endpoint slot, bound before any handle can be published. */
public record RuntimeEndpointFrontier(
    RuntimeFrontierReservation reservation, int slot, RuntimeExchangeBlockCodec.Bounds bounds) {
  public RuntimeEndpointFrontier {
    Objects.requireNonNull(reservation);
    Objects.requireNonNull(bounds);
    if (bounds.schema() == null) {
      throw new IllegalArgumentException("Endpoint codec requires an exact admitted schema");
    }
    if (slot < 0 || slot >= reservation.getLaneCount()) {
      throw new IllegalArgumentException("Invalid endpoint slot");
    }
    long required =
        reservation.getMemorySide() == MemorySide.INPUT
            ? bounds.inputEnvelopeBytes()
            : bounds.outputEnvelopeBytes();
    if (reservation.getBytesPerLane() < required) {
      throw new IllegalArgumentException("Endpoint envelope was not fully prepaid");
    }
  }

  public void bind(
      MemoryPool pool, TFragmentInstanceId fragment, MemorySide side, Object endpoint) {
    Objects.requireNonNull(fragment);
    if (!reservation.isBackedBy(pool)
        || reservation.getMemorySide() != side
        || !reservation.getMemoryReservationKey().queryId().equals(fragment.getQueryId())
        || !reservation
            .getMemoryReservationKey()
            .fragmentInstanceId()
            .equals(FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(fragment))) {
      throw new IllegalArgumentException("Endpoint frontier belongs to another pool/FI/side");
    }
    reservation.claimEndpointSlot(slot, endpoint);
  }
}
