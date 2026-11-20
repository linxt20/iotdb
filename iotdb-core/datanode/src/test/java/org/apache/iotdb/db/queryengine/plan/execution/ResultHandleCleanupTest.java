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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ResultHandleCleanupTest {
  @Test
  public void nativeInputDeregistrationWaitsForActualEndpointReceipt() throws Exception {
    SourceHandle handle = mock(SourceHandle.class);
    SettableFuture<Void> endpoint = SettableFuture.create();
    when(handle.requestCoordinatedClose()).thenReturn(endpoint);
    AtomicInteger deregistered = new AtomicInteger();
    ListenableFuture<Void> receipt =
        QueryExecution.closeResultHandle(handle, null, deregistered::incrementAndGet);
    assertFalse(receipt.isDone());
    assertEquals(0, deregistered.get());
    endpoint.set(null);
    receipt.get(5, TimeUnit.SECONDS);
    assertEquals(1, deregistered.get());
    verify(handle, never()).close();
  }

  @Test
  public void abortPreservesOriginalCauseAndWaitsBeforeDeregistration() throws Exception {
    SourceHandle handle = mock(SourceHandle.class);
    RuntimeException expected = new RuntimeException("original cancel cause");
    SettableFuture<Void> endpoint = SettableFuture.create();
    when(handle.requestCoordinatedAbort(expected)).thenReturn(endpoint);
    AtomicInteger deregistered = new AtomicInteger();
    ListenableFuture<Void> receipt =
        QueryExecution.closeResultHandle(handle, expected, deregistered::incrementAndGet);
    assertEquals(0, deregistered.get());
    endpoint.set(null);
    receipt.get(5, TimeUnit.SECONDS);
    verify(handle).requestCoordinatedAbort(same(expected));
    assertEquals(1, deregistered.get());
  }

  @Test
  public void endpointFailureDoesNotPretendTheAccountWasDeregistered() throws Exception {
    SourceHandle handle = mock(SourceHandle.class);
    RuntimeException expected = new RuntimeException("endpoint still owns resources");
    SettableFuture<Void> endpoint = SettableFuture.create();
    when(handle.requestCoordinatedClose()).thenReturn(endpoint);
    AtomicInteger deregistered = new AtomicInteger();
    ListenableFuture<Void> receipt =
        QueryExecution.closeResultHandle(handle, null, deregistered::incrementAndGet);
    endpoint.setException(expected);
    assertSame(
        expected,
        assertThrows(ExecutionException.class, () -> receipt.get(5, TimeUnit.SECONDS)).getCause());
    assertEquals(0, deregistered.get());
  }

  @Test
  public void failedDeregistrationIsRetainedInTheCleanupReceipt() throws Exception {
    SourceHandle handle = mock(SourceHandle.class);
    SettableFuture<Void> endpoint = SettableFuture.create();
    when(handle.requestCoordinatedClose()).thenReturn(endpoint);
    RuntimeException expected = new RuntimeException("pool deregistration failed");
    ListenableFuture<Void> receipt =
        QueryExecution.closeResultHandle(
            handle,
            null,
            () -> {
              throw expected;
            });
    endpoint.set(null);
    assertSame(
        expected,
        assertThrows(ExecutionException.class, () -> receipt.get(5, TimeUnit.SECONDS)).getCause());
  }

  @Test
  public void cancellingAnObservationDoesNotCancelActualCleanup() throws Exception {
    SourceHandle handle = mock(SourceHandle.class);
    SettableFuture<Void> endpoint = SettableFuture.create();
    when(handle.requestCoordinatedClose()).thenReturn(endpoint);
    AtomicInteger deregistered = new AtomicInteger();
    ListenableFuture<Void> receipt =
        QueryExecution.closeResultHandle(handle, null, deregistered::incrementAndGet);
    receipt.cancel(true);
    assertFalse(endpoint.isCancelled());
    endpoint.set(null);
    assertEquals(1, deregistered.get());
  }

  @Test
  public void originalSynchronousLocalAndMemoryHandlesStillCloseBeforeCleanup() throws Exception {
    ISourceHandle handle = mock(ISourceHandle.class);
    AtomicInteger closed = new AtomicInteger();
    doAnswer(
            i -> {
              closed.incrementAndGet();
              return null;
            })
        .when(handle)
        .close();
    QueryExecution.closeResultHandle(handle, null, () -> assertEquals(1, closed.get()))
        .get(5, TimeUnit.SECONDS);
    verify(handle).close();
  }
}
