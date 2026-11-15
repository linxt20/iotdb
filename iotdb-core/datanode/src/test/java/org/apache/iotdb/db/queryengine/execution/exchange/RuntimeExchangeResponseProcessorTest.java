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

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class RuntimeExchangeResponseProcessorTest {
  @Test
  public void nestedScopeRestoresParentAndAttemptsEveryPinAfterFailure() {
    AtomicInteger closed = new AtomicInteger();
    RuntimeException expected = new RuntimeException("pin failure");
    try (RpcResponseLeaseScope parent = RpcResponseLeaseScope.enter()) {
      parent.retain(closed::incrementAndGet);
      RpcResponseLeaseScope child = RpcResponseLeaseScope.enter();
      child.retain(
          () -> {
            throw expected;
          });
      child.retain(closed::incrementAndGet);
      Assert.assertSame(expected, Assert.assertThrows(RuntimeException.class, child::close));
      Assert.assertEquals(1, closed.get());
      Assert.assertSame(parent, RpcResponseLeaseScope.requireCurrent());
      child.close();
      Assert.assertEquals(1, closed.get());
    }
    Assert.assertEquals(2, closed.get());
    Assert.assertThrows(IllegalStateException.class, RpcResponseLeaseScope::requireCurrent);
  }

  @Test
  public void processorPreservesDelegateFailureWhileSettlingAllResponsePins() {
    AtomicInteger closed = new AtomicInteger();
    TException expected = new TException("write or flush failed");
    RuntimeException pinFailure = new RuntimeException("pin close failed");
    TProcessor processor =
        RuntimeExchangeResponseProcessor.wrap(
            (input, output) -> {
              RpcResponseLeaseScope.requireCurrent()
                  .retain(
                      () -> {
                        throw pinFailure;
                      });
              RpcResponseLeaseScope.requireCurrent().retain(closed::incrementAndGet);
              throw expected;
            });
    Assert.assertSame(
        expected, Assert.assertThrows(TException.class, () -> processor.process(null, null)));
    Assert.assertEquals(1, closed.get());
    Assert.assertEquals(1, expected.getSuppressed().length);
    Assert.assertSame(pinFailure, expected.getSuppressed()[0]);
    Assert.assertThrows(IllegalStateException.class, RpcResponseLeaseScope::requireCurrent);
  }

  @Test
  public void processorReturnsOnlyAfterItsPinsExitAndRestoresAnOuterScope() throws Exception {
    AtomicInteger closed = new AtomicInteger();
    try (RpcResponseLeaseScope parent = RpcResponseLeaseScope.enter()) {
      TProcessor processor =
          RuntimeExchangeResponseProcessor.wrap(
              (input, output) -> {
                Assert.assertNotSame(parent, RpcResponseLeaseScope.requireCurrent());
                RpcResponseLeaseScope.requireCurrent().retain(closed::incrementAndGet);
                Assert.assertEquals(0, closed.get());
              });
      processor.process(null, null);
      Assert.assertEquals(1, closed.get());
      Assert.assertSame(parent, RpcResponseLeaseScope.requireCurrent());
    }
  }
}
