#include <folly/init/Init.h>
#include <sys/time.h>
#include <cstring>
#include <initializer_list>
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

using namespace facebook::velox;

RowTypePtr createRowType(std::string prefix, int attribute_num) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < attribute_num; i++) {
    names.push_back(prefix + std::to_string(i));
    types.push_back(BIGINT());
  }
  return ROW(std::move(names), std::move(types));
}

RowVectorPtr
createAllData(RowTypePtr type, int64_t tnum, memory::MemoryPool* pool) {
  std::vector<VectorPtr> vectors;
  uint32_t anum = type->size();
  std::random_device rd;
  for (uint32_t i = 0; i < anum; i++) {
    auto v = BaseVector::create(BIGINT(), tnum, pool);
    auto raw_values = v->values()->asMutable<int64_t>();
    std::iota(raw_values, raw_values + tnum, 0);
    std::mt19937 g(rd());
    std::shuffle(raw_values, raw_values + tnum, g);
    vectors.push_back(v);
  }
  return std::make_shared<RowVector>(
      pool, type, BufferPtr(nullptr), tnum, vectors);
}

std::vector<RowVectorPtr> createBatchedData(
    RowTypePtr type,
    int64_t tnum,
    int64_t batch_size,
    memory::MemoryPool* pool,
    RowVectorPtr (*createAllData)(RowTypePtr, int64_t, memory::MemoryPool*)) {
  uint32_t anum = type->size();

  auto data = createAllData(type, tnum, pool);
  int64_t* raw_data[anum];
  for (uint32_t i = 0; i < anum; i++)
    raw_data[i] = data->childAt(i)->values()->asMutable<int64_t>();

  std::vector<RowVectorPtr> ans;
  for (int64_t i = 0; i < tnum; i += batch_size) {
    int64_t size = (i + batch_size) > tnum ? tnum - i : batch_size;
    std::vector<VectorPtr> vectors;
    for (uint32_t a = 0; a < anum; a++) {
      auto v = BaseVector::create(BIGINT(), size, pool);
      auto raw_value = v->values()->asMutable<int64_t>();
      memcpy(raw_value, raw_data[a] + i, size * sizeof(int64_t));
      vectors.push_back(v);
    }
    ans.push_back(std::make_shared<RowVector>(
        pool, type, BufferPtr(nullptr), size, vectors));
  }
  return ans;
}

void makeBuildDeepJoin(
    const std::vector<std::vector<RowVectorPtr>>& tables,
    const std::vector<RowTypePtr>& data_types,
    int index,
    std::shared_ptr<core::PlanNodeIdGenerator> id_generator,
    exec::test::PlanBuilder& plan_builder,
    std::vector<std::string>& output_layout) {
  core::PlanNodePtr build_node;
  std::vector<std::string> build_layout;
  if (index == 1) {
    build_node =
        exec::test::PlanBuilder(id_generator).values(tables[0]).planNode();
    build_layout = data_types[0]->names();
  } else {
    exec::test::PlanBuilder builder(id_generator);
    makeBuildDeepJoin(
        tables, data_types, index - 1, id_generator, builder, build_layout);
    build_node = builder.planNode();
  }

  output_layout.clear();
  output_layout.insert(
      output_layout.end(),
      data_types[index]->names().begin(),
      data_types[index]->names().end());
  output_layout.insert(
      output_layout.end(), build_layout.begin(), build_layout.end());
  std::string probe_key = data_types[index]->names()[0];
  std::string build_key = build_layout[0];
  plan_builder.values(tables[index])
      .hashJoin(
          {probe_key},
          {build_key},
          build_node,
          "",
          output_layout,
          core::JoinType::kInner);
}

void makeProbeDeepJoin(
    const std::vector<std::vector<RowVectorPtr>>& tables,
    const std::vector<RowTypePtr>& data_types,
    int index,
    std::shared_ptr<core::PlanNodeIdGenerator> id_generator,
    exec::test::PlanBuilder& plan_builder,
    std::vector<std::string>& output_layout) {
  std::vector<std::string> probe_layout;
  if (index == 1) {
    plan_builder.values(tables[0]);
    probe_layout = data_types[0]->names();
  } else {
    makeProbeDeepJoin(
        tables,
        data_types,
        index - 1,
        id_generator,
        plan_builder,
        probe_layout);
  }

  auto build_node =
      exec::test::PlanBuilder(id_generator).values(tables[index]).planNode();

  output_layout.clear();
  output_layout.insert(
      output_layout.end(), probe_layout.begin(), probe_layout.end());
  output_layout.insert(
      output_layout.end(),
      data_types[index]->names().begin(),
      data_types[index]->names().end());
  std::string probe_key = probe_layout[0];
  std::string build_key = data_types[index]->names()[0];

  plan_builder.hashJoin(
      {probe_key},
      {build_key},
      build_node,
      "",
      output_layout,
      core::JoinType::kInner);
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  // 0 is left(build) deep and 1 is right (probe) deep
  const int join_type = atoi(argv[1]);
  const int table_num = atoi(argv[2]);
  const int attribute_num = atoi(argv[3]);
  const int64_t tuple_num = atoi(argv[4]);
  const int64_t batch_size = 1L * 1024 * 1024;

  auto pool = memory::getDefaultMemoryPool();
  std::vector<RowTypePtr> types;
  std::vector<std::vector<RowVectorPtr>> data;
  for (int i = 0; i < table_num; i++) {
    types.push_back(
        createRowType("t" + std::to_string(i) + "_a", attribute_num));
    data.push_back(createBatchedData(
        types.back(), tuple_num, batch_size, pool.get(), createAllData));
  }

  aggregate::prestosql::registerAllAggregateFunctions();

  timeval start, end;
  gettimeofday(&start, NULL);

  auto id_generator = std::make_shared<core::PlanNodeIdGenerator>();
  exec::test::PlanBuilder plan_builder(id_generator);
  std::vector<std::string> output_layout;
  if (join_type == 0)
    makeBuildDeepJoin(
        data,
        types,
        data.size() - 1,
        id_generator,
        plan_builder,
        output_layout);
  else
    makeProbeDeepJoin(
        data,
        types,
        data.size() - 1,
        id_generator,
        plan_builder,
        output_layout);

  std::vector<std::string> aggregates;
  aggregates.push_back("count(" + output_layout[0] + ")");
  for (const auto& a : output_layout)
    aggregates.push_back("avg(" + a + " )");
  auto plan_node = plan_builder.partialAggregation({}, aggregates)
                       .finalAggregation()
                       .planNode();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));
  exec::test::AssertQueryBuilder builder(plan_node);
  builder.queryCtx(std::make_shared<core::QueryCtx>(executor.get()));
  builder.maxDrivers(1);
  builder.config(core::QueryConfig::kSpillEnabled, "false");

  RowVectorPtr result = builder.copyResults(pool.get(), true);
  printf("Query result: %d rows\n", result->size());
  for (vector_size_t i = 0; i < result->size(); i++) {
    printf("\t%s\n", result->toString(i).c_str());
  }

  gettimeofday(&end, NULL);
  double time_value =
      ((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec)) /
      1000000.0;
  printf("Total time: %f seconds\n", time_value);
  fflush(stdout);

  return 0;
}
