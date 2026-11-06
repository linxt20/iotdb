# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import pytest
from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TTransport

from iotdb.thrift.common.ttypes import TSStatus
from iotdb.thrift.datanode.ttypes import (
    TDriverRuntimeSnapshot,
    TFetchFragmentInstanceStatisticsResp,
    TFragmentInstanceId,
    TOperatorStatistics,
)

PROTOCOLS = [
    TBinaryProtocol.TBinaryProtocol,
    TCompactProtocol.TCompactProtocol,
    TBinaryProtocol.TBinaryProtocolAccelerated,
    TCompactProtocol.TCompactProtocolAccelerated,
]


def round_trip(value, protocol):
    transport = TTransport.TMemoryBuffer()
    value.validate()
    encoder = protocol(transport)
    if protocol.__name__.endswith("Accelerated"):
        assert encoder._fast_encode is not None
    value.write(encoder)
    wire = transport.getvalue()
    restored = type(value)()
    restored.read(protocol(TTransport.TMemoryBuffer(wire)))
    restored.validate()
    return restored


def driver_snapshot():
    return TDriverRuntimeSnapshot(
        fragmentInstanceId="q.1.0",
        driverPipelineId="2",
        driverTaskId="q.1.0.2",
        driverRole="SCAN",
        driverSampleGeneration=3,
        driverFirstProcessNanos=100,
        driverLastProcessNanos=200,
        driverCpuNanos=11,
        driverWallNanos=101,
        driverReadyNanos=13,
        driverBlockedNanos=17,
        driverOutputRows=19,
        driverOutputBytes=23,
        driverTsBlockCount=29,
        validityReasons=["VALID"],
    )


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_driver_runtime_fields_survive_wire(protocol):
    original = driver_snapshot()
    assert round_trip(original, protocol) == original


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_fragment_snapshot_preserves_driver_plan_and_resource_fields(protocol):
    original = TFetchFragmentInstanceStatisticsResp(
        status=TSStatus(code=200),
        fragmentInstanceId=TFragmentInstanceId(
            queryId="q", fragmentId=1, instanceId="0"
        ),
        schemaVersion=1,
        snapshotGeneration=3,
        capturedAtNanos=201,
        sourceNodeId="2",
        sourceEndpoint="127.0.0.1:10730",
        validityReasons=["VALID"],
        snapshotChecksum="snapshot-sha",
        isTerminal=True,
        driverSnapshots=[driver_snapshot()],
        plannedDop=1,
        actualPipelineCount=1,
        splitMode="DEVICE",
        splitReason="DOP_ONE",
        planIdentityChecksum="plan-sha",
        workUnitIds=["device:0"],
        bucketIds=[0],
        exchangeEdgeIds=["q.1.0/sink/q.2.0/receive"],
        canonicalFallbackReason="NONE",
        planGeneration=7,
        resourceObjectCount=32,
        resourceUniqueMemoryKeyCount=1,
        resourceNominalShareBytes=125703,
        admissionStatus="ADMISSION_ACCEPTED",
        admissionReason="NONE",
    )
    restored = round_trip(original, protocol)
    assert restored == original
    assert restored.resourceObjectCount == 32
    assert restored.resourceUniqueMemoryKeyCount == 1
    assert restored.resourceNominalShareBytes == 125703
    assert restored.driverSnapshots[0].driverOutputBytes == 23


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_absent_optional_fields_stay_distinct_from_explicit_zero(protocol):
    legacy = TFetchFragmentInstanceStatisticsResp(status=TSStatus(code=200))
    restored = round_trip(legacy, protocol)
    assert restored.driverSnapshots is None
    assert restored.resourceObjectCount is None
    assert restored.resourceUniqueMemoryKeyCount is None
    assert restored.resourceNominalShareBytes is None
    assert restored.isTerminal is None

    zero = TFetchFragmentInstanceStatisticsResp(
        status=TSStatus(code=200),
        resourceObjectCount=0,
        resourceUniqueMemoryKeyCount=0,
        resourceNominalShareBytes=0,
        isTerminal=False,
        driverSnapshots=[],
    )
    restored_zero = round_trip(zero, protocol)
    assert restored_zero == zero
    assert restored_zero.resourceObjectCount == 0
    assert restored_zero.isTerminal is False
    assert restored_zero.driverSnapshots == []


@pytest.mark.parametrize("protocol", PROTOCOLS)
def test_existing_operator_statistics_include_output_bytes_and_block_count(protocol):
    original = TOperatorStatistics(
        planNodeId="scan-0",
        operatorType="TableScanOperator",
        totalExecutionTimeInNanos=11,
        nextCalledCount=2,
        hasNextCalledCount=3,
        specifiedInfo={"scanSourceId": "source-0"},
        outputRows=19,
        memoryUsage=37,
        count=1,
        outputBytes=23,
        tsBlockCount=29,
    )
    assert round_trip(original, protocol) == original
