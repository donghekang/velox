/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/substrait/VariantToVectorConverter.h"
#include "velox/type/Type.h"

namespace facebook::velox::substrait {
namespace {
core::AggregationNode::Step toAggregationStep(
    const ::substrait::AggregateRel& sAgg) {
  if (sAgg.measures().size() == 0) {
    // When only groupings exist, set the phase to be Single.
    return core::AggregationNode::Step::kSingle;
  }

  // Use the first measure to set aggregation phase.
  const auto& firstMeasure = sAgg.measures()[0];
  const auto& aggFunction = firstMeasure.measure();
  switch (aggFunction.phase()) {
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kPartial;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kIntermediate;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
      return core::AggregationNode::Step::kFinal;
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT:
      return core::AggregationNode::Step::kSingle;
    default:
      VELOX_FAIL("Aggregate phase is not supported.");
  }
}

core::JoinType toVeloxJoinType(::substrait::JoinRel_JoinType joinType) {
  switch (joinType) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      return core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      return core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      return core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      return core::JoinType::kRight;
      break;

    default:
      VELOX_UNSUPPORTED(
          "Velox-substrait does not support join type ", joinType);
      break;
  }
  VELOX_UNREACHABLE();
}
core::SortOrder toSortOrder(const ::substrait::SortField& sortField) {
  switch (sortField.direction()) {
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
      return core::kAscNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
      return core::kAscNullsLast;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
      return core::kDescNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
      return core::kDescNullsLast;
    default:
      VELOX_FAIL("Sort direction is not supported.");
  }
}

/// Holds the information required to create
/// a project node to simulate the emit
/// behavior in Substrait.
struct EmitInfo {
  std::vector<core::TypedExprPtr> expressions;
  std::vector<std::string> projectNames;
};

/// Helper function to extract the attributes required to create a ProjectNode
/// used for interpretting Substrait Emit.
EmitInfo getEmitInfo(
    const ::substrait::RelCommon& relCommon,
    const core::PlanNodePtr& node) {
  const auto& emit = relCommon.emit();
  int emitSize = emit.output_mapping_size();
  EmitInfo emitInfo;
  emitInfo.projectNames.reserve(emitSize);
  emitInfo.expressions.reserve(emitSize);
  const auto& outputType = node->outputType();
  for (int i = 0; i < emitSize; i++) {
    int32_t mapId = emit.output_mapping(i);
    emitInfo.projectNames[i] = outputType->nameOf(mapId);
    emitInfo.expressions[i] = std::make_shared<core::FieldAccessTypedExpr>(
        outputType->childAt(mapId), outputType->nameOf(mapId));
  }
  return emitInfo;
}

} // namespace

core::PlanNodePtr SubstraitVeloxPlanConverter::processEmit(
    const ::substrait::RelCommon& relCommon,
    const core::PlanNodePtr& noEmitNode) {
  switch (relCommon.emit_kind_case()) {
    case ::substrait::RelCommon::EmitKindCase::kDirect:
      return noEmitNode;
    case ::substrait::RelCommon::EmitKindCase::kEmit: {
      auto emitInfo = getEmitInfo(relCommon, noEmitNode);
      return std::make_shared<core::ProjectNode>(
          nextPlanNodeId(),
          std::move(emitInfo.projectNames),
          std::move(emitInfo.expressions),
          noEmitNode);
    }
    default:
      VELOX_FAIL("unrecognized emit kind");
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::AggregateRel& aggRel) {
  auto childNode = convertSingleInput<::substrait::AggregateRel>(aggRel);
  core::AggregationNode::Step aggStep = toAggregationStep(aggRel);
  const auto& inputType = childNode->outputType();
  std::vector<core::FieldAccessTypedExprPtr> veloxGroupingExprs;

  // Get the grouping expressions.
  for (const auto& grouping : aggRel.groupings()) {
    for (const auto& groupingExpr : grouping.grouping_expressions()) {
      // Velox's groupings are limited to be Field.
      veloxGroupingExprs.emplace_back(
          exprConverter_->toVeloxExpr(groupingExpr.selection(), inputType));
    }
  }

  // Parse measures and get the aggregate expressions.
  // Each measure represents one aggregate expression.
  std::vector<core::CallTypedExprPtr> aggExprs;
  aggExprs.reserve(aggRel.measures().size());
  std::vector<core::FieldAccessTypedExprPtr> aggregateMasks;
  aggregateMasks.reserve(aggRel.measures().size());

  for (const auto& measure : aggRel.measures()) {
    core::FieldAccessTypedExprPtr aggregateMask;
    ::substrait::Expression substraitAggMask = measure.filter();
    // Get Aggregation Masks.
    if (measure.has_filter()) {
      if (substraitAggMask.ByteSizeLong() == 0) {
        aggregateMask = {};
      } else {
        aggregateMask =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                exprConverter_->toVeloxExpr(substraitAggMask, inputType));
      }
      aggregateMasks.push_back(aggregateMask);
    }

    const auto& aggFunction = measure.measure();
    auto funcName = substraitParser_->findVeloxFunction(
        functionMap_, aggFunction.function_reference());
    std::vector<core::TypedExprPtr> aggParams;
    aggParams.reserve(aggFunction.arguments().size());
    for (const auto& arg : aggFunction.arguments()) {
      aggParams.emplace_back(
          exprConverter_->toVeloxExpr(arg.value(), inputType));
    }
    auto aggVeloxType = toVeloxType(
        substraitParser_->parseType(aggFunction.output_type())->type);
    auto aggExpr = std::make_shared<const core::CallTypedExpr>(
        aggVeloxType, std::move(aggParams), funcName);
    aggExprs.emplace_back(aggExpr);
  }

  bool ignoreNullKeys = false;
  std::vector<core::FieldAccessTypedExprPtr> preGroupingExprs;

  // Get the output names of Aggregation.
  std::vector<std::string> aggOutNames;
  aggOutNames.reserve(aggRel.measures().size());
  for (int idx = veloxGroupingExprs.size();
       idx < veloxGroupingExprs.size() + aggRel.measures().size();
       idx++) {
    aggOutNames.emplace_back(substraitParser_->makeNodeName(planNodeId_, idx));
  }

  auto aggregationNode = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      aggStep,
      veloxGroupingExprs,
      preGroupingExprs,
      aggOutNames,
      aggExprs,
      aggregateMasks,
      ignoreNullKeys,
      childNode);

  if (aggRel.has_common()) {
    return processEmit(aggRel.common(), std::move(aggregationNode));
  } else {
    return aggregationNode;
  }
}

void SubstraitVeloxPlanConverter::extractJoinKeys(
    const ::substrait::Expression& joinExpression,
    std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
    std::vector<const ::substrait::Expression::FieldReference*>& rightExprs) {
  std::vector<const ::substrait::Expression*> expressions;
  expressions.push_back(&joinExpression);
  while (!expressions.empty()) {
    auto visited = expressions.back();
    expressions.pop_back();
    if (visited->rex_type_case() ==
        ::substrait::Expression::RexTypeCase::kScalarFunction) {
      auto funcSpec = substraitParser_->findFunctionSpec(
          functionMap_, visited->scalar_function().function_reference());
      auto funcName = getNameBeforeDelimiter(funcSpec, ":");
      const auto& preds = visited->scalar_function();
      if (funcName == "and") {
        VELOX_CHECK_EQ(preds.arguments_size(), 2);
        for (int i = 0; i < preds.arguments_size(); i++) {
          const auto& arg = preds.arguments(i);
          if (!arg.has_value() || !arg.value().has_scalar_function())
            VELOX_FAIL(
                "Unable to parse from join expression: {}",
                joinExpression.DebugString());
          expressions.push_back(&arg.value());
        }
      } else if (funcName == "equal") {
        VELOX_CHECK_EQ(preds.arguments_size(), 2);
        for (int i = 0; i < preds.arguments_size(); i++) {
          const auto& arg = preds.arguments(i);
          if (!arg.has_value() || !arg.value().has_selection())
            VELOX_FAIL(
                "Unable to parse from join expression: {}",
                joinExpression.DebugString());
          if (i == 0)
            leftExprs.push_back(&arg.value().selection());
          else
            rightExprs.push_back(&arg.value().selection());
        }
      } else {
        VELOX_FAIL(
            "Unable to parse from join expression: {}",
            joinExpression.DebugString());
      }
    }
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::JoinRel& joinRel) {
  if (!joinRel.has_left())
    VELOX_FAIL("Left Rel is expected in JoinRel");
  if (!joinRel.has_right())
    VELOX_FAIL("Right Rel is expected in JoinRel");
  auto joinType = toVeloxJoinType(joinRel.type());

  auto leftNode = toVeloxPlan(joinRel.left());
  auto rightNode = toVeloxPlan(joinRel.right());

  auto outputSize =
      leftNode->outputType()->size() + rightNode->outputType()->size();
  std::vector<std::string> outputNames;
  std::vector<TypePtr> outputTypes;
  for (const auto& node : {leftNode, rightNode}) {
    const auto& names = node->outputType()->names();
    const auto& types = node->outputType()->children();
    outputNames.insert(outputNames.end(), names.begin(), names.end());
    outputTypes.insert(outputTypes.end(), types.begin(), types.end());
  }
  auto outputRowType = std::make_shared<const RowType>(
      std::move(outputNames), std::move(outputTypes));

  // Extract join keys from join expression
  std::vector<const ::substrait::Expression::FieldReference*> leftExprs,
      rightExprs;
  extractJoinKeys(joinRel.expression(), leftExprs, rightExprs);
  VELOX_CHECK_EQ(leftExprs.size(), rightExprs.size());
  size_t numKeys = leftExprs.size();

  std::vector<core::FieldAccessTypedExprPtr> leftKeys, rightKeys;
  for (size_t i = 0; i < numKeys; i++) {
    leftKeys.emplace_back(
        exprConverter_->toVeloxExpr(*leftExprs[i], outputRowType));
    rightKeys.emplace_back(
        exprConverter_->toVeloxExpr(*rightExprs[i], outputRowType));
  }
  core::TypedExprPtr filter;
  if (joinRel.has_post_join_filter())
    filter =
        exprConverter_->toVeloxExpr(joinRel.post_join_filter(), outputRowType);

  return std::make_shared<core::HashJoinNode>(
      nextPlanNodeId(),
      joinType,
      false, /* nullAware, only be used in the semi and anti joins */
      leftKeys,
      rightKeys,
      filter,
      leftNode,
      rightNode,
      outputRowType);
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::SetRel& setRel) {
  if (setRel.op() != ::substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL) {
    VELOX_UNSUPPORTED(
        "Substrait converter does not support {} SetRel", setRel.op());
  }
  std::vector<core::PlanNodePtr> children;
  std::string nodeId = nextPlanNodeId();
  int nodeId_int = atoi(nodeId.c_str());

  for (int i = 0; i < setRel.inputs_size(); i++) {
    auto child = toVeloxPlan(setRel.inputs(i));
    const auto& inputType = child->outputType();

    // convert the column names using ProjectNode
    std::vector<std::string> projectNames;
    std::vector<core::TypedExprPtr> expressions;
    for (int colIdx = 0; colIdx < inputType->size(); colIdx++) {
      ::substrait::Expression exp;
      auto field = exp.mutable_selection();
      field->mutable_direct_reference()->mutable_struct_field()->set_field(
          colIdx);
      field->mutable_root_reference();
      expressions.emplace_back(exprConverter_->toVeloxExpr(exp, inputType));
      projectNames.emplace_back(
          substraitParser_->makeNodeName(nodeId_int, colIdx));
    }
    children.emplace_back(std::make_shared<core::ProjectNode>(
        nextPlanNodeId(),
        std::move(projectNames),
        std::move(expressions),
        child));
  }
  return core::LocalPartitionNode::gather(nodeId, children);
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ExchangeRel& exchangeRel) {
  core::PlanNodePtr childNode;
  if (exchangeRel.has_input())
    childNode = toVeloxPlan(exchangeRel.input());
  else
    VELOX_FAIL("Child Rel is expected in ExchangeRel.");

  core::PartitionFunctionSpecPtr factory;
  if (exchangeRel.exchange_kind_case() ==
      ::substrait::ExchangeRel::kScatterByFields) {
    const auto& inputType = childNode->outputType();
    std::vector<column_index_t> keyChannels;
    for (const auto& field : exchangeRel.scatter_by_fields().fields()) {
      const auto expr = exprConverter_->toVeloxExpr(field, inputType);
      keyChannels.push_back(inputType->getChildIdx(expr->name()));
    }

    factory = std::make_shared<exec::HashPartitionFunctionSpec>(
        inputType, std::move(keyChannels));
  } else if (
      exchangeRel.exchange_kind_case() ==
      ::substrait::ExchangeRel::kRoundRobin) {
    factory = std::make_shared<exec::RoundRobinPartitionFunctionSpec>();
  } else
    VELOX_UNSUPPORTED(
        "Substrait convert does not support {} ExchangeRel",
        exchangeRel.exchange_kind_case());

  return std::make_shared<core::LocalPartitionNode>(
      nextPlanNodeId(),
      core::LocalPartitionNode::Type::kRepartition,
      factory,
      std::vector<core::PlanNodePtr>{childNode});
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ProjectRel& projectRel) {
  auto childNode = convertSingleInput<::substrait::ProjectRel>(projectRel);

  // Construct Velox Expressions.
  auto projectExprs = projectRel.expressions();
  std::vector<std::string> projectNames;
  std::vector<core::TypedExprPtr> expressions;
  projectNames.reserve(projectExprs.size());
  expressions.reserve(projectExprs.size());

  const auto& inputType = childNode->outputType();
  int colIdx = 0;
  // Note that Substrait projection adds the project expressions on top of the
  // input to the projection node. Thus we need to add the input columns first
  // and then add the projection expressions.

  // First, adding the project names and expressions from the input to
  // the project node.
  for (uint32_t idx = 0; idx < inputType->size(); idx++) {
    const auto& fieldName = inputType->nameOf(idx);
    projectNames.emplace_back(fieldName);
    expressions.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
        inputType->childAt(idx), fieldName));
    colIdx += 1;
  }

  // Then, adding project expression related project names and expressions.
  for (const auto& expr : projectExprs) {
    expressions.emplace_back(exprConverter_->toVeloxExpr(expr, inputType));
    projectNames.emplace_back(
        substraitParser_->makeNodeName(planNodeId_, colIdx));
    colIdx += 1;
  }

  if (projectRel.has_common()) {
    auto relCommon = projectRel.common();
    const auto& emit = relCommon.emit();
    int emitSize = emit.output_mapping_size();
    std::vector<std::string> emitProjectNames(emitSize);
    std::vector<core::TypedExprPtr> emitExpressions(emitSize);
    for (int i = 0; i < emitSize; i++) {
      int32_t mapId = emit.output_mapping(i);
      emitProjectNames[i] = projectNames[mapId];
      emitExpressions[i] = expressions[mapId];
    }
    return std::make_shared<core::ProjectNode>(
        nextPlanNodeId(),
        std::move(emitProjectNames),
        std::move(emitExpressions),
        std::move(childNode));
  } else {
    return std::make_shared<core::ProjectNode>(
        nextPlanNodeId(),
        std::move(projectNames),
        std::move(expressions),
        std::move(childNode));
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::SortRel& sortRel) {
  auto childNode = convertSingleInput<::substrait::SortRel>(sortRel);

  auto [sortingKeys, sortingOrders] =
      processSortField(sortRel.sorts(), childNode->outputType());

  return std::make_shared<core::OrderByNode>(
      nextPlanNodeId(),
      sortingKeys,
      sortingOrders,
      false /*isPartial*/,
      childNode);
}

std::pair<
    std::vector<core::FieldAccessTypedExprPtr>,
    std::vector<core::SortOrder>>
SubstraitVeloxPlanConverter::processSortField(
    const ::google::protobuf::RepeatedPtrField<::substrait::SortField>&
        sortFields,
    const RowTypePtr& inputType) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  sortingKeys.reserve(sortFields.size());
  sortingOrders.reserve(sortFields.size());

  for (const auto& sort : sortFields) {
    sortingOrders.emplace_back(toSortOrder(sort));

    if (sort.has_expr()) {
      auto expression = exprConverter_->toVeloxExpr(sort.expr(), inputType);
      auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              expression);
      VELOX_CHECK_NOT_NULL(
          fieldExpr, " the sorting key in Sort Operator only support field");
      sortingKeys.emplace_back(fieldExpr);
    }
  }
  return {sortingKeys, sortingOrders};
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::FilterRel& filterRel) {
  auto childNode = convertSingleInput<::substrait::FilterRel>(filterRel);

  auto filterNode = std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      exprConverter_->toVeloxExpr(
          filterRel.condition(), childNode->outputType()),
      childNode);

  if (filterRel.has_common()) {
    return processEmit(filterRel.common(), std::move(filterNode));
  } else {
    return filterNode;
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::FetchRel& fetchRel) {
  core::PlanNodePtr childNode;
  // Check the input of fetchRel, if it's sortRel, convert them into
  // topNNode. otherwise, to limitNode.
  ::substrait::SortRel sortRel;
  bool topNFlag;
  if (fetchRel.has_input()) {
    topNFlag = fetchRel.input().has_sort();
    if (topNFlag) {
      sortRel = fetchRel.input().sort();
      childNode = toVeloxPlan(sortRel.input());
    } else {
      childNode = toVeloxPlan(fetchRel.input());
    }
  } else {
    VELOX_FAIL("Child Rel is expected in FetchRel.");
  }

  if (topNFlag) {
    auto [sortingKeys, sortingOrders] =
        processSortField(sortRel.sorts(), childNode->outputType());

    VELOX_CHECK_EQ(fetchRel.offset(), 0);

    return std::make_shared<core::TopNNode>(
        nextPlanNodeId(),
        sortingKeys,
        sortingOrders,
        (int32_t)fetchRel.count(),
        false /*isPartial*/,
        childNode);

  } else {
    return std::make_shared<core::LimitNode>(
        nextPlanNodeId(),
        (int32_t)fetchRel.offset(),
        (int32_t)fetchRel.count(),
        false /*isPartial*/,
        childNode);
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& readRel,
    std::shared_ptr<SplitInfo>& splitInfo) {
  // emit is not allowed in TableScanNode and ValuesNode related
  // outputs
  if (readRel.has_common()) {
    VELOX_USER_CHECK(
        !readRel.common().has_emit(),
        "Emit not supported for ValuesNode and TableScanNode related Substrait plans.");
  }
  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> veloxTypeList;
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      colNameList.emplace_back(name);
    }
    auto substraitTypeList = substraitParser_->parseNamedStruct(baseSchema);
    veloxTypeList.reserve(substraitTypeList.size());
    for (const auto& substraitType : substraitTypeList) {
      veloxTypeList.emplace_back(toVeloxType(substraitType->type));
    }
  }

  // Parse local files
  if (readRel.has_local_files()) {
    using SubstraitFileFormatCase =
        ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;
    const auto& fileList = readRel.local_files().items();
    splitInfo->paths.reserve(fileList.size());
    splitInfo->starts.reserve(fileList.size());
    splitInfo->lengths.reserve(fileList.size());
    for (const auto& file : fileList) {
      // Expect all files to share the same index.
      splitInfo->partitionIndex = file.partition_index();
      splitInfo->starts.emplace_back(file.start());
      splitInfo->lengths.emplace_back(file.length());
      if (file.has_uri_file())
        splitInfo->paths.emplace_back(file.uri_file());
      else if (file.has_uri_folder()) {
        auto folder_uri = file.uri_folder();
        auto fs = velox::filesystems::getFileSystem(folder_uri, nullptr);
        auto file_names = fs->list(folder_uri);
        splitInfo->paths.insert(
            splitInfo->paths.end(), file_names.begin(), file_names.end());
      }

      switch (file.file_format_case()) {
        case SubstraitFileFormatCase::kOrc:
          splitInfo->format = dwio::common::FileFormat::DWRF;
          break;
        case SubstraitFileFormatCase::kParquet:
          splitInfo->format = dwio::common::FileFormat::PARQUET;
          break;
        default:
          splitInfo->format = dwio::common::FileFormat::UNKNOWN;
      }
    }
  }

  // Do not hard-code connector ID and allow for connectors other than Hive.
  static const std::string kHiveConnectorId = "test-hive";

  // Velox requires Filter Pushdown must being enabled.
  bool filterPushdownEnabled = true;
  std::shared_ptr<connector::hive::HiveTableHandle> tableHandle;
  if (!readRel.has_filter()) {
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        filterPushdownEnabled,
        connector::hive::SubfieldFilters{},
        nullptr);
  } else {
    connector::hive::SubfieldFilters filters =
        toVeloxFilter(colNameList, veloxTypeList, readRel.filter());
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        filterPushdownEnabled,
        std::move(filters),
        nullptr);
  }

  // Get assignments and out names.
  std::vector<std::string> outNames;
  outNames.reserve(colNameList.size());
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = substraitParser_->makeNodeName(planNodeId_, idx);
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx],
        connector::hive::HiveColumnHandle::ColumnType::kRegular,
        veloxTypeList[idx]);
    outNames.emplace_back(outName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  if (readRel.has_virtual_table()) {
    return toVeloxPlan(readRel, outputType);
  } else {
    return std::make_shared<core::TableScanNode>(
        nextPlanNodeId(),
        std::move(outputType),
        std::move(tableHandle),
        std::move(assignments));
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& readRel,
    const RowTypePtr& type) {
  ::substrait::ReadRel_VirtualTable readVirtualTable = readRel.virtual_table();
  int64_t numVectors = readVirtualTable.values_size();
  int64_t numColumns = type->size();
  int64_t valueFieldNums =
      readVirtualTable.values(numVectors - 1).fields_size();
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(numVectors);

  int64_t batchSize;
  // For the empty vectors, eg,vectors = makeRowVector(ROW({}, {}), 1).
  if (numColumns == 0) {
    batchSize = 1;
  } else {
    batchSize = valueFieldNums / numColumns;
  }

  for (int64_t index = 0; index < numVectors; ++index) {
    std::vector<VectorPtr> children;
    ::substrait::Expression_Literal_Struct rowValue =
        readRel.virtual_table().values(index);
    auto fieldSize = rowValue.fields_size();
    VELOX_CHECK_EQ(fieldSize, batchSize * numColumns);

    for (int64_t col = 0; col < numColumns; ++col) {
      const TypePtr& outputChildType = type->childAt(col);
      std::vector<variant> batchChild;
      batchChild.reserve(batchSize);
      for (int64_t batchId = 0; batchId < batchSize; batchId++) {
        // each value in the batch
        auto fieldIdx = col * batchSize + batchId;
        ::substrait::Expression_Literal field = rowValue.fields(fieldIdx);

        auto expr = exprConverter_->toVeloxExpr(field);
        if (auto constantExpr =
                std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                    expr)) {
          if (!constantExpr->hasValueVector()) {
            batchChild.emplace_back(constantExpr->value());
          } else {
            VELOX_UNSUPPORTED(
                "Values node with complex type values is not supported yet");
          }
        } else {
          VELOX_FAIL("Expected constant expression");
        }
      }
      children.emplace_back(
          setVectorFromVariants(outputChildType, batchChild, pool_));
    }

    vectors.emplace_back(
        std::make_shared<RowVector>(pool_, type, nullptr, batchSize, children));
  }

  return std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::move(vectors));
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::WriteRel& writeRel) {
  auto childNode = convertSingleInput<::substrait::WriteRel>(writeRel);
  auto inputType = childNode->outputType();

  // convert the output location
  VELOX_CHECK_EQ(
      writeRel.write_type_case(),
      ::substrait::WriteRel::WriteTypeCase::kNamedTable,
      "Substrait WriteRel can only write to a NamedObject");
  std::string targetPath;
  dwio::common::FileFormat out_format = dwio::common::FileFormat::PARQUET;
  VELOX_CHECK_LE(writeRel.named_table().names_size(), 2);
  VELOX_CHECK_GT(writeRel.named_table().names_size(), 0);
  targetPath = writeRel.named_table().names(0);
  if (writeRel.named_table().names_size() == 2) {
    std::string f = writeRel.named_table().names(1);
    for (char& c : f)
      c = tolower(c);
    VELOX_CHECK(f == "parquet" || f == "dwrf");
    if (f == "parquet")
      out_format = dwio::common::FileFormat::PARQUET;
    else
      out_format = dwio::common::FileFormat::DWRF;
  }

  std::shared_ptr<connector::hive::LocationHandle> locationHandle;
  switch (writeRel.op()) {
    case ::substrait::WriteRel::WRITE_OP_CTAS:
      locationHandle = std::make_shared<connector::hive::LocationHandle>(
          targetPath,
          targetPath,
          connector::hive::LocationHandle::TableType::kNew);
      break;
    case ::substrait::WriteRel::WRITE_OP_INSERT:
      locationHandle = std::make_shared<connector::hive::LocationHandle>(
          targetPath,
          targetPath,
          connector::hive::LocationHandle::TableType::kExisting);
      break;
    default:
      VELOX_FAIL("Unsupported WriteOp ", writeRel.op());
      break;
  }

  // create table handle
  std::vector<std::string> writeColumnNames;
  for (auto n : writeRel.table_schema().names())
    writeColumnNames.push_back(n);
  VELOX_CHECK_EQ(
      inputType->size(),
      writeColumnNames.size(),
      "Input stream and the write schema must have the same number of attributes");
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
      columnHandles;
  for (int i = 0; i < inputType->size(); i++)
    columnHandles.push_back(std::make_shared<connector::hive::HiveColumnHandle>(
        writeColumnNames[i],
        connector::hive::HiveColumnHandle::ColumnType::kRegular,
        inputType->childAt(i)));

  static const std::string kHiveConnectorId = "test-hive";
  auto insertTableHandel = std::make_shared<core::InsertTableHandle>(
      kHiveConnectorId,
      std::make_shared<connector::hive::HiveInsertTableHandle>(
          columnHandles, locationHandle, out_format));

  auto outputType = ROW({"rowCount"}, {BIGINT()});
  return std::make_shared<core::TableWriteNode>(
      nextPlanNodeId(),
      inputType,
      writeColumnNames,
      insertTableHandel,
      outputType,
      connector::CommitStrategy::kNoCommit,
      std::move(childNode));
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Rel& rel) {
  if (rel.has_aggregate()) {
    return toVeloxPlan(rel.aggregate());
  }
  if (rel.has_project()) {
    return toVeloxPlan(rel.project());
  }
  if (rel.has_filter()) {
    return toVeloxPlan(rel.filter());
  }
  if (rel.has_read()) {
    auto splitInfo = std::make_shared<SplitInfo>();

    auto planNode = toVeloxPlan(rel.read(), splitInfo);
    splitInfoMap_[planNode->id()] = splitInfo;
    return planNode;
  }
  if (rel.has_set()) {
    return toVeloxPlan(rel.set());
  }
  if (rel.has_exchange()) {
    return toVeloxPlan(rel.exchange());
  }
  if (rel.has_join()) {
    return toVeloxPlan(rel.join());
  }
  if (rel.has_fetch()) {
    return toVeloxPlan(rel.fetch());
  }
  if (rel.has_sort()) {
    return toVeloxPlan(rel.sort());
  }
  if (rel.has_write()) {
    return toVeloxPlan(rel.write());
  }
  VELOX_NYI("Substrait conversion not supported for Rel.");
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::RelRoot& root) {
  // TODO: Use the names as the output names for the whole computing.
  const auto& names = root.names();
  if (root.has_input()) {
    const auto& rel = root.input();
    return toVeloxPlan(rel);
  }
  VELOX_FAIL("Input is expected in RelRoot.");
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan) {
  VELOX_CHECK(
      checkTypeExtension(substraitPlan),
      "The type extension only have unknown type.")
  // Construct the function map based on the Substrait representation.
  constructFunctionMap(substraitPlan);

  // Construct the expression converter.
  exprConverter_ =
      std::make_shared<SubstraitVeloxExprConverter>(pool_, functionMap_);

  // In fact, only one RelRoot or Rel is expected here.
  VELOX_CHECK_EQ(substraitPlan.relations_size(), 1);
  const auto& rel = substraitPlan.relations(0);
  if (rel.has_root()) {
    return toVeloxPlan(rel.root());
  }
  if (rel.has_rel()) {
    return toVeloxPlan(rel.rel());
  }

  VELOX_FAIL("RelRoot or Rel is expected in Plan.");
}

std::string SubstraitVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

// This class contains the needed infos for Filter Pushdown.
// TODO: Support different types here.
class FilterInfo {
 public:
  // Used to set the left bound.
  void setLeft(double left, bool isExclusive) {
    int_left_ = std::nullopt;
    int_right_ = std::nullopt;

    double_left_ = left;
    leftExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Used to set the left bound.
  void setLeft(int64_t left, bool isExclusive) {
    double_left_ = std::nullopt;
    double_right_ = std::nullopt;

    if (isExclusive)
      left++;
    int_left_ = left;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Used to set the right bound.
  void setRight(double right, bool isExclusive) {
    int_left_ = std::nullopt;
    int_right_ = std::nullopt;

    double_right_ = right;
    rightExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Used to set the right bound.
  void setRight(int64_t right, bool isExclusive) {
    double_left_ = std::nullopt;
    double_right_ = std::nullopt;

    if (isExclusive)
      right--;
    int_right_ = right;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Will fordis Null value if called once.
  void forbidsNull() {
    nullAllowed_ = false;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Return the initialization status.
  bool isInitialized() {
    return isInitialized_ ? true : false;
  }

  std::unique_ptr<common::Filter> makeFilter() {
    if (!this->isInitialized())
      VELOX_NYI(
          "substrait conversion cannot make filter from an unintialized FilterInfo");
    if (this->double_left_ || this->double_right_) {
      return std::move(this->makeFilterDouble());
    } else {
      return std::move(this->makeFilterInt());
    }
  }

  std::unique_ptr<common::DoubleRange> makeFilterDouble() {
    double leftBound;
    double rightBound;
    bool leftUnbounded = true;
    bool rightUnbounded = true;

    if (this->double_left_) {
      leftUnbounded = false;
      leftBound = double_left_.value();
    }
    if (this->double_right_) {
      rightUnbounded = false;
      rightBound = double_right_.value();
    }
    return std::move(std::make_unique<common::DoubleRange>(
        leftBound,
        leftUnbounded,
        leftExclusive_,
        rightBound,
        rightUnbounded,
        rightExclusive_,
        nullAllowed_));
  }

  std::unique_ptr<common::BigintRange> makeFilterInt() {
    int64_t leftBound = std::numeric_limits<int64_t>::min();
    int64_t rightBound = std::numeric_limits<int64_t>::max();
    if (int_left_)
      leftBound = int_left_.value();
    if (int_right_)
      rightBound = int_right_.value();
    return std::move(std::make_unique<common::BigintRange>(
        leftBound, rightBound, nullAllowed_));
  }

  // The left bound.
  std::optional<double> double_left_ = std::nullopt;
  std::optional<int64_t> int_left_ = std::nullopt;
  // The right bound.
  std::optional<double> double_right_ = std::nullopt;
  std::optional<int64_t> int_right_ = std::nullopt;
  // The Null allowing.
  bool nullAllowed_ = true;
  // If true, left double bound will be exclusive.
  bool leftExclusive_ = false;
  // If true, right double bound will be exclusive.
  bool rightExclusive_ = false;

 private:
  bool isInitialized_ = false;
};

connector::hive::SubfieldFilters SubstraitVeloxPlanConverter::toVeloxFilter(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    const ::substrait::Expression& substraitFilter) {
  connector::hive::SubfieldFilters filters;
  // A map between the column index and the FilterInfo for that column.
  std::unordered_map<int, std::shared_ptr<FilterInfo>> colInfoMap;
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    colInfoMap[idx] = std::make_shared<FilterInfo>();
  }

  std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
  flattenConditions(substraitFilter, scalarFunctions);
  // Construct the FilterInfo for the related column.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = substraitParser_->findFunctionSpec(
        functionMap_, scalarFunction.function_reference());
    auto filterName = getNameBeforeDelimiter(filterNameSpec, ":");
    int32_t colIdx;
    // TODO: Add different types' support here.
    std::optional<double> double_value_ = std::nullopt;
    std::optional<int64_t> int_value_ = std::nullopt;
    VELOX_CHECK_LE(
        scalarFunction.arguments_size(),
        2,
        "Substrait Read filter cannot take more than two arguments");
    int colIdxPos = -1;
    // for (auto& arg : scalarFunction.arguments()) {
    for (int i = 0; i < scalarFunction.arguments_size(); i++) {
      auto& arg = scalarFunction.arguments(i);
      auto argExpr = arg.value();
      auto typeCase = argExpr.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection: {
          auto sel = argExpr.selection();
          // TODO: Only direct reference is considered here.
          auto dRef = sel.direct_reference();
          colIdx = substraitParser_->parseReferenceSegment(dRef);
          colIdxPos = i;
          break;
        }
        case ::substrait::Expression::RexTypeCase::kLiteral: {
          auto sLit = argExpr.literal();
          // TODO: Only double and int64_t is considered here.
          switch (sLit.literal_type_case()) {
            case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
              double_value_ = sLit.fp32();
              break;
            case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
              double_value_ = sLit.fp64();
              break;
            case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
              int_value_ = sLit.i16();
              break;
            case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
              int_value_ = sLit.i32();
              break;
            case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
              int_value_ = sLit.i64();
              break;
            default:
              VELOX_NYI(
                  "substrait conversion not support for literal type '{}'",
                  sLit.literal_type_case());
              break;
          }
          break;
        }
        default:
          VELOX_NYI(
              "Substrait conversion not supported for arg type '{}'", typeCase);
      }
    }

    if (colIdxPos != 0) {
      if (filterName == "gte")
        filterName = "lte";
      else if (filterName == "gt")
        filterName = "lt";
      else if (filterName == "lte")
        filterName = "gte";
      else if (filterName == "lt")
        filterName = "gt";
    }

    if (filterName == "is_not_null") {
      colInfoMap[colIdx]->forbidsNull();
    } else if (filterName == "gte" && double_value_)
      colInfoMap[colIdx]->setLeft(double_value_.value(), false);
    else if (filterName == "gte")
      colInfoMap[colIdx]->setLeft(int_value_.value(), false);
    else if (filterName == "gt" && double_value_)
      colInfoMap[colIdx]->setLeft(double_value_.value(), true);
    else if (filterName == "gt")
      colInfoMap[colIdx]->setLeft(int_value_.value(), true);
    else if (filterName == "lte" && double_value_)
      colInfoMap[colIdx]->setRight(double_value_.value(), false);
    else if (filterName == "lte")
      colInfoMap[colIdx]->setRight(int_value_.value(), false);
    else if (filterName == "lt" && double_value_)
      colInfoMap[colIdx]->setRight(double_value_.value(), true);
    else if (filterName == "lt")
      colInfoMap[colIdx]->setRight(int_value_.value(), true);
    else if (filterName == "equal" && double_value_) {
      colInfoMap[colIdx]->setLeft(double_value_.value(), false);
      colInfoMap[colIdx]->setRight(double_value_.value(), false);
    } else if (filterName == "equal") {
      colInfoMap[colIdx]->setLeft(int_value_.value(), false);
      colInfoMap[colIdx]->setRight(int_value_.value(), false);
    } else {
      VELOX_NYI(
          "Substrait conversion not supported for filter name '{}'",
          filterName);
    }
  }

  // Construct the Filters.
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    auto filterInfo = colInfoMap[idx];
    if (filterInfo->isInitialized()) {
      filters[common::Subfield(inputNameList[idx])] =
          std::move(filterInfo->makeFilter());
    }

    //   double leftBound;
    //   double rightBound;
    //   bool leftUnbounded = true;
    //   bool rightUnbounded = true;
    //   bool leftExclusive = false;
    //   bool rightExclusive = false;
    //   if (filterInfo->isInitialized()) {
    //     if (filterInfo->left_) {
    //       leftUnbounded = false;
    //       leftBound = filterInfo->left_.value();
    //       leftExclusive = filterInfo->leftExclusive_;
    //     }
    //     if (filterInfo->right_) {
    //       rightUnbounded = false;
    //       rightBound = filterInfo->right_.value();
    //       rightExclusive = filterInfo->rightExclusive_;
    //     }
    //     bool nullAllowed = filterInfo->nullAllowed_;
    //     filters[common::Subfield(inputNameList[idx])] =
    //         std::make_unique<common::DoubleRange>(
    //             leftBound,
    //             leftUnbounded,
    //             leftExclusive,
    //             rightBound,
    //             rightUnbounded,
    //             rightExclusive,
    //             nullAllowed);
    // }
  }
  return filters;
}

void SubstraitVeloxPlanConverter::flattenConditions(
    const ::substrait::Expression& substraitFilter,
    std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions) {
  auto typeCase = substraitFilter.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sFunc = substraitFilter.scalar_function();
      auto filterNameSpec = substraitParser_->findFunctionSpec(
          functionMap_, sFunc.function_reference());
      // TODO: Only and relation is supported here.
      if (getNameBeforeDelimiter(filterNameSpec, ":") == "and") {
        for (const auto& sCondition : sFunc.arguments()) {
          flattenConditions(sCondition.value(), scalarFunctions);
        }
      } else {
        scalarFunctions.emplace_back(sFunc);
      }
      break;
    }
    default:
      VELOX_NYI("GetFlatConditions not supported for type '{}'", typeCase);
  }
}

void SubstraitVeloxPlanConverter::constructFunctionMap(
    const ::substrait::Plan& substraitPlan) {
  // Construct the function map based on the Substrait representation.
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (!sExtension.has_extension_function()) {
      continue;
    }
    const auto& sFmap = sExtension.extension_function();
    auto id = sFmap.function_anchor();
    auto name = sFmap.name();
    functionMap_[id] = name;
  }
}

bool SubstraitVeloxPlanConverter::checkTypeExtension(
    const ::substrait::Plan& substraitPlan) {
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (!sExtension.has_extension_type()) {
      continue;
    }

    // Only support UNKNOWN type in UserDefined type extension.
    if (sExtension.extension_type().name() != "UNKNOWN") {
      return false;
    }
  }
  return true;
}

const std::string& SubstraitVeloxPlanConverter::findFunction(
    uint64_t id) const {
  return substraitParser_->findFunctionSpec(functionMap_, id);
}

} // namespace facebook::velox::substrait
