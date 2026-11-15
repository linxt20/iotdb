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
import org.apache.thrift.protocol.TProtocol;

import java.util.Objects;

/** The current synchronous processor writes and flushes the response before process returns. */
public final class RuntimeExchangeResponseProcessor implements TProcessor {
  private final TProcessor delegate;

  private RuntimeExchangeResponseProcessor(TProcessor delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  public static TProcessor wrap(TProcessor delegate) {
    return new RuntimeExchangeResponseProcessor(delegate);
  }

  @Override
  public void process(TProtocol input, TProtocol output) throws TException {
    try (RpcResponseLeaseScope ignored = RpcResponseLeaseScope.enter()) {
      delegate.process(input, output);
    }
  }
}
