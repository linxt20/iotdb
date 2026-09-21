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

import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class RemoteShufflePayloadByteTrackerTest {

  @Test
  public void testSeparatesDirectionsQueriesAndFragments() {
    RemoteShufflePayloadByteTracker tracker = new RemoteShufflePayloadByteTracker();
    TFragmentInstanceId producer = new TFragmentInstanceId("query-a", 0, "0");
    TFragmentInstanceId consumer = new TFragmentInstanceId("query-a", 1, "1");

    ByteBuffer first = ByteBuffer.wrap(new byte[8]);
    first.position(3);
    ByteBuffer second = ByteBuffer.wrap(new byte[9]);
    second.limit(4);
    tracker.recordSent(producer, 2, Arrays.asList(first, second));

    ByteBuffer retry = ByteBuffer.wrap(new byte[10]);
    retry.position(4);
    tracker.recordSent(producer, 2, Collections.singletonList(retry));

    ByteBuffer received = ByteBuffer.wrap(new byte[12]);
    received.position(5);
    tracker.recordReceived(consumer, producer, 2, Collections.singletonList(received));

    RemoteShufflePayloadByteTracker.QuerySnapshot snapshot = tracker.snapshot("query-a");
    Assert.assertNotNull(snapshot);
    // The retry is intentionally included: this measures transferred payload attempts, not unique
    // sequence IDs or SinkChannel buffers.
    Assert.assertEquals(5 + 4 + 6, snapshot.getSent().getPayloadBytes());
    Assert.assertEquals(3, snapshot.getSent().getPayloadBlocks());
    Assert.assertEquals(7, snapshot.getReceived().getPayloadBytes());
    Assert.assertEquals(1, snapshot.getReceived().getPayloadBlocks());
    Assert.assertEquals(2, snapshot.getFragments().size());
  }

  @Test
  public void testRejectsCrossQueryReceiveWithoutContamination() {
    RemoteShufflePayloadByteTracker tracker = new RemoteShufflePayloadByteTracker();
    tracker.recordReceived(
        new TFragmentInstanceId("consumer-query", 1, "0"),
        new TFragmentInstanceId("producer-query", 0, "0"),
        0,
        Collections.singletonList(ByteBuffer.wrap(new byte[3])));

    Assert.assertNull(tracker.snapshot("consumer-query"));
    Assert.assertNull(tracker.snapshot("producer-query"));
  }

  @Test
  public void testSnapshotRemovalDoesNotAffectOtherQuery() {
    RemoteShufflePayloadByteTracker tracker = new RemoteShufflePayloadByteTracker();
    TFragmentInstanceId first = new TFragmentInstanceId("first", 0, "0");
    TFragmentInstanceId second = new TFragmentInstanceId("second", 0, "0");
    tracker.recordSent(first, 0, Collections.singletonList(ByteBuffer.wrap(new byte[1])));
    tracker.recordSent(second, 0, Collections.singletonList(ByteBuffer.wrap(new byte[2])));

    tracker.remove("first");

    Assert.assertNull(tracker.snapshot("first"));
    Assert.assertEquals(2, tracker.snapshot("second").getSent().getPayloadBytes());
  }
}
