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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SymbolMapper;
import org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotHistoryStore;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;

import java.util.*;

/** Fragment preparation reads history only. Each source selects after its actual DOP estimate. */
public final class DynamicParallelPlanningService {
  private static final DynamicSnapshotHistoryStore HISTORY = new DynamicSnapshotHistoryStore();
  private final DynamicSnapshotHistoryStore history;
  private final String endpoint;
  private final Map<String, Source> sources = new LinkedHashMap<>();
  private final Map<String, Selection> selections = new HashMap<>();
  private final Map<String, DynamicSnapshotHistoryStore.AdmissionRejection> rejections =
      new HashMap<>();
  private final Map<String, List<DynamicSnapshotHistoryStore.Entry>> feedback = new HashMap<>();

  private static final class Source {
    final DeviceTableScanNode node;
    final String position;
    final List<PlanNode> ancestors;

    Source(DeviceTableScanNode node, String position, List<PlanNode> ancestors) {
      this.node = node;
      this.position = position;
      this.ancestors = new ArrayList<>(ancestors);
    }
  }

  public static final class Selection {
    private final ScanParallelPlan plan;
    private final DynamicQueryShape shape;
    private final DynamicScanGrouping grouping;
    private final DynamicParallelScheduleDecision decision;
    private long checkSequence;
    private int contextDop, previousDop;
    private boolean dopEstimationEnabled;
    private String estimateStatus = "NOT_REQUESTED_SERIAL_GUARD";
    private Integer estimatedDop;
    private List<DynamicFeedbackIdentity> inputs = Collections.emptyList();

    Selection(
        ScanParallelPlan plan,
        DynamicQueryShape shape,
        DynamicScanGrouping grouping,
        DynamicParallelScheduleDecision decision) {
      this.plan = plan;
      this.shape = shape;
      this.grouping = grouping;
      this.decision = decision;
    }

    public ScanParallelPlan getPlan() {
      return plan;
    }

    public DynamicQueryShape getShape() {
      return shape;
    }

    public DynamicParallelScheduleDecision getDecision() {
      return decision;
    }
  }

  public DynamicParallelPlanningService() {
    this(HISTORY, localEndpoint());
  }

  public DynamicParallelPlanningService(DynamicSnapshotHistoryStore history, String endpoint) {
    this.history = Objects.requireNonNull(history);
    this.endpoint = Objects.requireNonNull(endpoint);
  }

  public static DynamicSnapshotHistoryStore historyStore() {
    return HISTORY;
  }

  public static String localEndpoint() {
    return IoTDBDescriptor.getInstance().getConfig().getInternalAddress()
        + ":"
        + IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  public void prepare(PlanNode root, LocalExecutionPlanContext context) {
    requirePlanningMutable(context);
    sources.clear();
    selections.clear();
    feedback.clear();
    rejections.clear();
    if (!context.getDynamicScanAssignments().isEnabled()) return;
    walk(root, "root", Collections.emptyList());
    // History is bound to each complete semantic shape after the real source input is known.
    // Freeze arrival membership now; terminal publication during generation cannot enter this plan.
    for (Source source : sources.values()) {
      for (ScanParallelPlan.Mode mode :
          Arrays.asList(
              ScanParallelPlan.Mode.DEVICE,
              ScanParallelPlan.Mode.ORDERED,
              ScanParallelPlan.Mode.TIME_PARTITION)) {
        DynamicQueryShape shape = shape(source, mode, context);
        if (shape != null) {
          feedback.put(shape.getChecksum(), history.history(shape, endpoint));
          DynamicSnapshotHistoryStore.AdmissionRejection rejection =
              history.admissionRejection(shape, endpoint);
          if (rejection != null) rejections.put(shape.getChecksum(), rejection);
        }
      }
    }
  }

  private void walk(PlanNode node, String path, List<PlanNode> ancestors) {
    // Specialized aggregate/tree/external scans keep their existing visitors and are not one of
    // the three plain-device consumers managed by this service.
    if (node.getClass() == DeviceTableScanNode.class) {
      sources.put(
          node.getPlanNodeId().getId(), new Source((DeviceTableScanNode) node, path, ancestors));
      return;
    }
    List<PlanNode> next = new ArrayList<>(ancestors);
    next.add(node);
    List<PlanNode> children = node.getChildren();
    if (children == null) return;
    for (int i = 0; i < children.size(); i++)
      walk(children.get(i), path + "/" + node.getClass().getSimpleName() + "/" + i, next);
  }

  public DynamicQueryShape shapeFor(
      String id, ScanParallelPlan.Mode mode, LocalExecutionPlanContext context) {
    Source source = sources.get(id);
    return source == null ? null : shape(source, mode, context);
  }

  private DynamicQueryShape shape(
      Source source, ScanParallelPlan.Mode mode, LocalExecutionPlanContext context) {
    DeviceTableScanNode node = source.node;
    IDataRegionForQuery region = context.getInstanceContext().getDataRegion();
    if (region == null
        || region.getDataRegionIdString() == null
        || node.getDeviceEntryDataSetHandle().isPresent()
        || node.getQualifiedObjectName() == null
        || node.getAssignments() == null
        || node.getOutputSymbols() == null
        || node.getDeviceEntries() == null
        || node.getScanOrder() == null) return null;
    for (org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema column :
        node.getAssignments().values())
      if (column == null || column.getName() == null || column.getType() == null) return null;
    Map<Symbol, Symbol> aliases = new HashMap<>();
    Map<String, String> types = new TreeMap<>();
    for (Source bound : sources.values()) {
      if (bound.node.getQualifiedObjectName() == null || bound.node.getAssignments() == null)
        continue;
      StringBuilder sourceIdentity = new StringBuilder();
      DynamicIdentityEncoding.field(sourceIdentity, bound.position);
      DynamicIdentityEncoding.field(
          sourceIdentity, bound.node.getQualifiedObjectName().getDatabaseName());
      DynamicIdentityEncoding.field(
          sourceIdentity, bound.node.getQualifiedObjectName().getObjectName());
      bound
          .node
          .getAssignments()
          .forEach(
              (symbol, column) -> {
                if (column == null || column.getName() == null) return;
                StringBuilder columnIdentity = new StringBuilder(sourceIdentity);
                DynamicIdentityEncoding.field(columnIdentity, column.getName());
                aliases.put(symbol, new Symbol(columnIdentity.toString()));
              });
    }
    SymbolMapper mapper = new SymbolMapper(symbol -> aliases.getOrDefault(symbol, symbol));
    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
      Symbol symbol = node.getOutputSymbols().get(i);
      org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema column =
          node.getAssignments().get(symbol);
      if (column == null) return null;
      types.put(
          Integer.toString(i) + ":" + column.getName(),
          column.getType().toString() + ":" + column.getColumnCategory());
    }
    StringBuilder filters = new StringBuilder(),
        order = new StringBuilder(node.getScanOrder().name());
    DynamicIdentityEncoding.field(filters, expression(node.getPushDownPredicate(), mapper));
    DynamicIdentityEncoding.field(filters, Long.toString(node.getPushDownLimit()));
    DynamicIdentityEncoding.field(filters, Long.toString(node.getPushDownOffset()));
    DynamicIdentityEncoding.field(filters, Boolean.toString(node.isPushLimitToEachDevice()));
    for (int index = source.ancestors.size() - 1; index >= 0; index--) {
      PlanNode ancestor = source.ancestors.get(index);
      if (ancestor instanceof FilterNode)
        DynamicIdentityEncoding.field(
            filters, expression(((FilterNode) ancestor).getPredicate(), mapper));
      if (ancestor instanceof ProjectNode) {
        int i = 0;
        for (Map.Entry<Symbol, Expression> e :
            ((ProjectNode) ancestor).getAssignments().getMap().entrySet()) {
          String canonical = expression(e.getValue(), mapper);
          DynamicIdentityEncoding.field(filters, canonical);
          aliases.put(e.getKey(), new Symbol("projection_" + i++ + "_" + canonical));
        }
      }
      if (ancestor instanceof SortNode) {
        org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme scheme =
            ((SortNode) ancestor).getOrderingScheme();
        for (Symbol symbol : scheme.getOrderBy()) {
          DynamicIdentityEncoding.field(order, aliases.getOrDefault(symbol, symbol).getName());
          DynamicIdentityEncoding.field(order, scheme.getOrdering(symbol).name());
        }
      }
      if (ancestor instanceof LimitNode)
        DynamicIdentityEncoding.field(filters, "LIMIT:" + ((LimitNode) ancestor).getCount());
      if (ancestor instanceof OffsetNode)
        DynamicIdentityEncoding.field(filters, "OFFSET:" + ((OffsetNode) ancestor).getCount());
    }
    List<String> devices = new ArrayList<>();
    node.getDeviceEntries().forEach(entry -> devices.add(String.valueOf(entry.getDeviceID())));
    return new DynamicQueryShape(
        node.getQualifiedObjectName().getDatabaseName(),
        node.getQualifiedObjectName().getObjectName(),
        region.getDataRegionIdString(),
        source.position,
        devices,
        expression(node.getTimePredicate().orElse(null), mapper),
        filters.toString(),
        types,
        order.toString(),
        mode);
  }

  private static String expression(Expression expression, SymbolMapper mapper) {
    return expression == null ? null : mapper.map(expression).toString();
  }

  public Selection select(ScanParallelPlanner.Input input, LocalExecutionPlanContext context) {
    return select(
        input,
        context,
        IoTDBDescriptor.getInstance().getConfig().isEnableDopEstimation(),
        "CALLER_SUPPLIED_CAP",
        null);
  }

  public Selection select(
      ScanParallelPlanner.Input input,
      LocalExecutionPlanContext context,
      boolean dopEstimationEnabled,
      String estimateStatus,
      Integer estimatedDop) {
    requirePlanningMutable(context);
    ScanParallelPlanner planner = new ScanParallelPlanner();
    ScanParallelPlan staticPlan;
    if (context.getDynamicScanAssignments().isEnabled()) {
      ScanParallelPlan.FallbackReason qualification =
          ScanParallelPlanner.qualificationFailure(input);
      if (qualification != null)
        staticPlan = ScanParallelPlanner.serialForDynamicFallback(input, qualification);
      else if (staticMaterializationExceedsBudget(input))
        staticPlan =
            ScanParallelPlanner.serialForDynamicFallback(
                input, ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_BUDGET_EXCEEDED);
      else staticPlan = planner.plan(input);
    } else staticPlan = planner.plan(input);
    if (staticPlan.getMode() == ScanParallelPlan.Mode.SERIAL && staticPlan.getWorkUnits().isEmpty())
      staticPlan =
          ScanParallelPlanner.serialForDynamicFallback(input, staticPlan.getFallbackReason());
    ScanParallelPlan.Mode requested =
        input.isOrdered()
            ? ScanParallelPlan.Mode.ORDERED
            : input.isTimePartitionMorsel() && input.getTimePartitions().size() > 1
                ? ScanParallelPlan.Mode.TIME_PARTITION
                : ScanParallelPlan.Mode.DEVICE;
    DynamicQueryShape shape =
        context.getDynamicScanAssignments().isEnabled()
            ? shapeFor(input.getPlanNodeId(), requested, context)
            : null;
    Selection selection = new Selection(staticPlan, shape, null, null);
    if (context.getDynamicScanAssignments().isEnabled() && shape != null) {
      ScanParallelPlan.FallbackReason guard =
          staticPlan.getMode() == ScanParallelPlan.Mode.SERIAL
              ? staticPlan.getFallbackReason()
              : ScanParallelPlan.FallbackReason.NONE;
      DynamicSnapshotHistoryStore.AdmissionRejection rejection =
          rejections.get(shape.getChecksum());
      int previousDop =
          rejection == null ? staticPlan.getTargetParallelism() : rejection.getPreviousDop();
      DynamicScanGrouping grouping = null;
      // Preserve the original qualification reason. Neither grouping construction nor cost
      // projection can override a higher-priority safety rejection.
      if (guard == ScanParallelPlan.FallbackReason.NONE) {
        try {
          grouping = new DynamicScanGroupingBuilder().build(input);
        } catch (IllegalArgumentException | ArithmeticException e) {
          guard = ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_BUDGET_EXCEEDED;
        }
      }
      DynamicCostHistoryStore.Estimate estimate = null;
      if (grouping != null) {
        estimate =
            rejection == null
                ? new DynamicCostHistoryStore(history)
                    .estimateFromEntries(
                        shape,
                        feedback.getOrDefault(shape.getChecksum(), Collections.emptyList()),
                        grouping)
                : new DynamicCostHistoryStore.Estimate(
                    grouping,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList());
      }
      // Continue from the latest qualified frozen observation, rather than replacing the
      // previous execution width with this request's newly estimated static cap. A trusted
      // admission rejection retains precedence over ordinary measured history.
      if (rejection == null && estimate != null && estimate.getPreviousDop() != null) {
        previousDop = estimate.getPreviousDop();
      }
      List<DynamicParallelScheduleInput.Observation> observations =
          estimate == null ? Collections.emptyList() : estimate.getObservations();
      DynamicParallelScheduler.Result policyResult =
          history.decide(
              shape,
              endpoint,
              new DynamicParallelScheduleInput(
                  input.getDop(),
                  previousDop,
                  grouping == null ? 0 : Math.toIntExact(grouping.getWorkUnitCount()),
                  guard,
                  rejection != null,
                  observations),
              new DynamicParallelScheduler());
      DynamicParallelScheduleDecision decision = policyResult.getDecision();
      if (decision.getAction() == DynamicParallelScheduleDecision.Action.KEEP_STATIC)
        decision =
            new DynamicParallelScheduleDecision(
                decision.getAction(), staticPlan.getTargetParallelism(), decision.getReason());
      ScanParallelPlan selected = staticPlan;
      DynamicScanGrouping selectedGrouping = null;
      if (decision.getAction() == DynamicParallelScheduleDecision.Action.SERIAL_FALLBACK)
        selected =
            ScanParallelPlanner.serialForDynamicFallback(
                input,
                guard == null || guard == ScanParallelPlan.FallbackReason.NONE
                    ? ScanParallelPlan.FallbackReason.PLAN_ERROR
                    : guard);
      else if (decision.getAction() != DynamicParallelScheduleDecision.Action.KEEP_STATIC) {
        selectedGrouping = estimate.getGrouping();
        selected = planner.planWithGrouping(input, selectedGrouping, decision.getTargetDop());
        if (selected.getMode() == ScanParallelPlan.Mode.SERIAL) selectedGrouping = null;
      }
      selection = new Selection(selected, shape, selectedGrouping, decision);
      selection.checkSequence = policyResult.getState().getCheckSequence();
      selection.contextDop = context.getDegreeOfParallelism();
      selection.previousDop = previousDop;
      if (estimate != null
          && decision.getAction() != DynamicParallelScheduleDecision.Action.SERIAL_FALLBACK)
        selection.inputs = estimate.getInputs();
    }
    selection.dopEstimationEnabled = dopEstimationEnabled;
    selection.estimateStatus = estimateStatus;
    selection.estimatedDop = estimatedDop;
    selections.put(input.getPlanNodeId(), selection);
    return selection;
  }

  public boolean isDynamic(ScanParallelPlan plan) {
    if (plan.getWorkUnits().isEmpty()) return false;
    Selection selection = selections.get(plan.getWorkUnits().get(0).getSourceKey().getPlanNodeId());
    return selection != null
        && selection.grouping != null
        && selection.plan.getWorkUnits().equals(plan.getWorkUnits());
  }

  public boolean isSerialSelected(String id) {
    Selection selection = selections.get(id);
    return selection != null && selection.plan.getMode() == ScanParallelPlan.Mode.SERIAL;
  }

  public void registerFinal(ScanParallelPlan plan, LocalExecutionPlanContext context) {
    if (!context.getDynamicScanAssignments().isEnabled() || plan.getWorkUnits().isEmpty()) return;
    String id = plan.getWorkUnits().get(0).getSourceKey().getPlanNodeId();
    Selection selection = selections.get(id);
    DynamicQueryShape shape =
        selection == null ? shapeFor(id, ScanParallelPlan.Mode.DEVICE, context) : selection.shape;
    if (shape != null) {
      if (selection == null) {
        DynamicParallelScheduler.Result guarded =
            history.decide(
                shape,
                endpoint,
                new DynamicParallelScheduleInput(
                    Math.max(1, plan.getInputDop()),
                    1,
                    plan.getWorkUnits().size(),
                    plan.getFallbackReason(),
                    false,
                    Collections.emptyList()),
                new DynamicParallelScheduler());
        selection = new Selection(plan, shape, null, guarded.getDecision());
        selection.checkSequence = guarded.getState().getCheckSequence();
        selection.contextDop = context.getDegreeOfParallelism();
        selection.previousDop = 1;
        selection.dopEstimationEnabled =
            IoTDBDescriptor.getInstance().getConfig().isEnableDopEstimation();
        selections.put(id, selection);
      }
      DynamicSourcePlan source =
          new DynamicSourcePlan(shape, plan, isDynamic(plan) ? selection.grouping : null);
      context.getDynamicScanAssignments().registerSource(source);
      if (selection != null && selection.decision != null)
        context
            .getDynamicScanAssignments()
            .registerReceipt(
                new DynamicDecisionReceipt(
                        source,
                        selection.decision,
                        endpoint,
                        selection.checkSequence,
                        selection.contextDop,
                        plan.getInputDop(),
                        selection.previousDop,
                        selection.dopEstimationEnabled,
                        selection.estimateStatus,
                        selection.estimatedDop)
                    .withInputs(selection.inputs));
    }
  }

  /** Bound the cold KEEP_STATIC candidate before its repeated coverage is materialized. */
  private static boolean staticMaterializationExceedsBudget(ScanParallelPlanner.Input input) {
    long possible = input.getDevices().size();
    boolean byTime =
        input.isTimePartitionMorsel() && input.getTimePartitions().size() > 1 && !input.isOrdered();
    if (byTime) possible = Math.multiplyExact(possible, (long) input.getTimePartitions().size());
    int target = (int) Math.min(input.getDop(), possible);
    int times = byTime ? Math.min(input.getTimePartitions().size(), target) : 1;
    int devices =
        byTime
            ? Math.max(1, Math.min(input.getDevices().size(), target / times))
            : Math.min(input.getDevices().size(), target);
    long references =
        Math.addExact(
            Math.multiplyExact((long) input.getDevices().size(), times),
            Math.multiplyExact((long) input.getTimePartitions().size(), devices));
    return (long) devices * times > DynamicScanGrouping.MAX_WORK_UNITS
        || references > DynamicScanGrouping.MAX_GROUPED_REFERENCES;
  }

  private static void requirePlanningMutable(LocalExecutionPlanContext context) {
    if (context.getDynamicScanAssignments().isFrozen())
      throw new IllegalStateException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
  }

  public Collection<Selection> selections() {
    return Collections.unmodifiableList(new ArrayList<>(selections.values()));
  }
}
