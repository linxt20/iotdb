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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.statistics.snapshot.*;
import org.apache.iotdb.mpp.rpc.thrift.*;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DynamicParallelTransportCompatibilityTest {
  static TFetchFragmentInstanceStatisticsResp response(int version) {
    TFetchFragmentInstanceStatisticsResp r = new TFetchFragmentInstanceStatisticsResp();
    r.setStatus(RpcUtils.SUCCESS_STATUS);
    r.setFragmentInstanceId(new TFragmentInstanceId("q", 0, "i"));
    r.setSchemaVersion(version);
    r.setSnapshotGeneration(1);
    r.setCapturedAtNanos(100);
    r.setSourceNodeId("0");
    r.setSourceEndpoint("127.0.0.1:10730");
    r.setState("FINISHED");
    r.setIsTerminal(true);
    TDriverRuntimeSnapshot d = new TDriverRuntimeSnapshot("q.0.i", "0", "q.0.i.0", 1);
    d.setDriverRole("input");
    d.setDriverWallNanos(10);
    d.setDriverOutputRows(2);
    d.setDriverTsBlockCount(1);
    r.setDriverSnapshots(new ArrayList<>(List.of(d)));
    return r;
  }

  static TDriverBucketAssignment assignment(String source, int bucket, String... units) {
    return new TDriverBucketAssignment(source, bucket, new ArrayList<>(List.of(units)));
  }

  static TFetchFragmentInstanceStatisticsResp signed(TFetchFragmentInstanceStatisticsResp r) {
    r.unsetSnapshotChecksum();
    r.setSnapshotChecksum(RuntimeSnapshotAdapters.fromResponse(r).getChecksum());
    return r;
  }

  static List<String> legacy(TFetchFragmentInstanceStatisticsResp r) throws Exception {
    URL jar =
        DynamicParallelTransportCompatibilityTest.class.getResource(
            "/runtime-snapshot-c-v1/legacy-c-adapter.jar");
    assertNotNull(jar);
    try (java.io.InputStream in = jar.openStream()) {
      byte[] actual = MessageDigest.getInstance("SHA-256").digest(in.readAllBytes());
      StringBuilder hex = new StringBuilder();
      for (byte b : actual) {
        hex.append(String.format("%02x", b & 255));
      }
      assertEquals(
          "e95a5a6f875cd47d727bdd78044e1f2944fdb8a27b2212a42500ba6cd7577915", hex.toString());
    }
    try (URLClassLoader loader =
        new URLClassLoader(
            new URL[] {jar}, DynamicParallelTransportCompatibilityTest.class.getClassLoader()) {
          @Override
          protected synchronized Class<?> loadClass(String name, boolean resolve)
              throws ClassNotFoundException {
            if (name.startsWith("org.apache.iotdb.db.queryengine.statistics.snapshot.")
                || name.startsWith("org.apache.iotdb.mpp.rpc.thrift.")) {
              Class<?> type = findLoadedClass(name);
              if (type == null) {
                type = findClass(name);
              }
              if (resolve) {
                resolveClass(type);
              }
              return type;
            }
            return super.loadClass(name, resolve);
          }
        }) {
      Class<?> adapter =
          loader.loadClass(
              "org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotAdapters");
      assertEquals(jar, adapter.getProtectionDomain().getCodeSource().getLocation());
      Class<?> responseType =
          loader.loadClass("org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp");
      assertEquals(jar, responseType.getProtectionDomain().getCodeSource().getLocation());
      Class<?> oldDriver =
          loader.loadClass("org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot");
      assertThrows(NoSuchMethodException.class, () -> oldDriver.getMethod("getScanAssignments"));
      org.apache.thrift.TBase<?, ?> oldResponse =
          (org.apache.thrift.TBase<?, ?>) responseType.getDeclaredConstructor().newInstance();
      new TDeserializer().deserialize(oldResponse, new TSerializer().serialize(r));
      Object snapshot = adapter.getMethod("fromResponse", responseType).invoke(null, oldResponse);
      return List.of(
          (String) snapshot.getClass().getMethod("getChecksum").invoke(snapshot),
          snapshot.getClass().getMethod("getValidity").invoke(snapshot).toString());
    }
  }

  @Test
  public void v1ChecksumRemainsExactlyEqualToActualFrozenReader() throws Exception {
    TFetchFragmentInstanceStatisticsResp r = signed(response(1));
    List<String> old = legacy(r);
    assertEquals(old.get(0), r.getSnapshotChecksum());
    assertTrue(RuntimeSnapshotAdapters.fromResponse(r).getValidity().isEmpty());
  }

  @Test
  public void v2SameDriverTwoSourcesRoundTripAndDeepCopy() throws Exception {
    TFetchFragmentInstanceStatisticsResp r = response(2);
    r.getDriverSnapshots()
        .get(0)
        .setScanAssignments(
            new ArrayList<>(List.of(assignment("a", 0, "a0"), assignment("b", 0, "b0"))));
    signed(r);
    byte[] bytes = new TSerializer().serialize(r);
    TFetchFragmentInstanceStatisticsResp copy = new TFetchFragmentInstanceStatisticsResp();
    new TDeserializer().deserialize(copy, bytes);
    FragmentRuntimeSnapshot value = RuntimeSnapshotAdapters.fromResponse(copy);
    assertEquals(Integer.valueOf(2), value.getSchemaVersion());
    assertTrue(value.getValidity().isEmpty());
    assertEquals(2, value.getDrivers().get(0).getScanAssignments().size());
    copy.getDriverSnapshots()
        .get(0)
        .getScanAssignments()
        .get(0)
        .getWorkUnitIds()
        .set(0, "tampered");
    assertEquals(
        List.of("a0"), value.getDrivers().get(0).getScanAssignments().get(0).getWorkUnitIds());
    assertTrue(
        RuntimeSnapshotAdapters.fromResponse(copy)
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  @Test
  public void canonicalAssignmentOrderAndMissingMarkerAreExplicit() {
    TFetchFragmentInstanceStatisticsResp a = response(2);
    a.getDriverSnapshots()
        .get(0)
        .setScanAssignments(List.of(assignment("b", 0, "b1", "b0"), assignment("a", 0, "a0")));
    signed(a);
    TFetchFragmentInstanceStatisticsResp b = response(2);
    b.getDriverSnapshots()
        .get(0)
        .setScanAssignments(List.of(assignment("a", 0, "a0"), assignment("b", 0, "b0", "b1")));
    signed(b);
    assertEquals(a.getSnapshotChecksum(), b.getSnapshotChecksum());
    TFetchFragmentInstanceStatisticsResp missing = signed(response(2));
    TFetchFragmentInstanceStatisticsResp empty = response(2);
    empty.getDriverSnapshots().get(0).setScanAssignments(List.of());
    signed(empty);
    assertNotEquals(missing.getSnapshotChecksum(), empty.getSnapshotChecksum());
  }

  @Test
  public void v2WithoutAssignmentsNeverDowngradesToV1() {
    FragmentRuntimeSnapshot root = RuntimeSnapshotAdapters.fromResponse(signed(response(2)));
    assertEquals(Integer.valueOf(2), root.getSchemaVersion());
    assertNull(root.getDrivers().get(0).getScanAssignments());
    assertTrue(root.getValidity().isEmpty());
    assertNotEquals(signed(response(1)).getSnapshotChecksum(), root.getChecksum());
  }

  @Test
  public void schemaAbsenceAndUnknownRemainDisplayOnly() {
    TFetchFragmentInstanceStatisticsResp r = response(1);
    r.unsetSchemaVersion();
    FragmentRuntimeSnapshot absent = RuntimeSnapshotAdapters.fromResponse(r);
    assertNull(absent.getSchemaVersion());
    assertNull(absent.getChecksum());
    assertTrue(absent.getValidity().contains(RuntimeSnapshotValidity.LEGACY_UNVERSIONED));
    FragmentRuntimeSnapshot unknown = RuntimeSnapshotAdapters.fromResponse(response(99));
    assertEquals(Integer.valueOf(99), unknown.getSchemaVersion());
    assertNull(unknown.getChecksum());
    assertTrue(unknown.getValidity().contains(RuntimeSnapshotValidity.UNSUPPORTED_SCHEMA_VERSION));
  }

  @Test
  public void claimedV1WithAssignmentsIsInconsistent() {
    TFetchFragmentInstanceStatisticsResp r = response(1);
    r.getDriverSnapshots().get(0).setScanAssignments(List.of(assignment("a", 0, "a0")));
    signed(r);
    assertTrue(
        RuntimeSnapshotAdapters.fromResponse(r)
            .getValidity()
            .contains(RuntimeSnapshotValidity.INCONSISTENT_SCHEMA_VERSION));
  }

  @Test
  public void oldReaderRejectsTheEntireV2Response() throws Exception {
    List<String> old = legacy(signed(response(2)));
    assertTrue(old.get(1).contains("CHECKSUM_MISMATCH"));
  }

  @Test
  public void duplicateSourceBucketCannotSilentlyOverwrite() {
    TFetchFragmentInstanceStatisticsResp r = response(2);
    r.getDriverSnapshots()
        .get(0)
        .setScanAssignments(List.of(assignment("a", 0, "a0"), assignment("a", 0, "a1")));
    assertThrows(IllegalArgumentException.class, () -> RuntimeSnapshotAdapters.fromResponse(r));
  }
}
