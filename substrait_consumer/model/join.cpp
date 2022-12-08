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

RowVectorPtr createBuildData(
    RowTypePtr type,
    int64_t tnum,
    memory::MemoryPool* pool,
    const int64_t* value_range = NULL) {
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

RowVectorPtr createProbeData(
    RowTypePtr type,
    int64_t tnum,
    memory::MemoryPool* pool,
    const int64_t* value_range) {
  std::vector<VectorPtr> vectors;
  uint32_t anum = type->size();
  std::srand(std::time(nullptr));

  for (uint32_t i = 0; i < anum; i++) {
    auto v = BaseVector::create(BIGINT(), tnum, pool);
    auto raw_values = v->values()->asMutable<int64_t>();
    for (int64_t t = 0; t < tnum; t++)
      raw_values[t] = (std::rand() / (double)RAND_MAX) * (*value_range);
    vectors.push_back(v);
  }
  return std::make_shared<RowVector>(
      pool, type, BufferPtr(nullptr), tnum, vectors);
}

std::vector<RowVectorPtr> createData(
    RowTypePtr type,
    int64_t tnum,
    int64_t batch_size,
    memory::MemoryPool* pool,
    RowVectorPtr (*createAllData)(
        RowTypePtr,
        int64_t,
        memory::MemoryPool*,
        const int64_t*),
    const int64_t* value_range = nullptr) {
  uint32_t anum = type->size();

  auto data = createAllData(type, tnum, pool, value_range);
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

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  // number of attributes in the build and probe tables
  const int attribute_num[2] = {atoi(argv[1]), atoi(argv[2])};
  // number of tuples in the build and probe tables
  const int64_t tuple_num[2] = {atoll(argv[3]), atoll(argv[4])};
  // const int thread_num = atoi(argv[5]);
  const int64_t batch_size = 1L * 1024 * 1024;

  auto pool = memory::getDefaultMemoryPool();
  auto build_type = createRowType("build", attribute_num[0]);
  auto probe_type = createRowType("probe", attribute_num[1]);
  auto build_data = createData(
      build_type, tuple_num[0], batch_size, pool.get(), createBuildData);
  auto probe_data = createData(
      probe_type,
      tuple_num[1],
      batch_size,
      pool.get(),
      createProbeData,
      &(tuple_num[0]));
  aggregate::prestosql::registerAllAggregateFunctions();

  timeval start, end;
  gettimeofday(&start, NULL);

  auto plan_node_id_generator = std::make_shared<core::PlanNodeIdGenerator>();
  auto build_table_node = exec::test::PlanBuilder(plan_node_id_generator)
                              .values(build_data)
                              .planNode();
  std::string join_filter;
  std::vector<std::string> join_output_layout;
  join_output_layout.insert(
      join_output_layout.end(),
      probe_type->names().begin(),
      probe_type->names().end());
  join_output_layout.insert(
      join_output_layout.end(),
      build_type->names().begin(),
      build_type->names().end());

  std::vector<std::string> aggregates;
  aggregates.push_back("count(" + join_output_layout[0] + ")");
  for (const auto& a : join_output_layout)
    aggregates.push_back("avg(" + a + ")");

  auto plan_node = exec::test::PlanBuilder(plan_node_id_generator)
                       .values(probe_data)
                       .hashJoin(
                           {"probe0"},
                           {"build0"},
                           build_table_node,
                           join_filter,
                           join_output_layout,
                           core::JoinType::kInner)
                       .partialAggregation({}, aggregates)
                       .finalAggregation()
                       .planNode();
  exec::test::AssertQueryBuilder builder(plan_node);
  // std::shared_ptr<folly::Executor> executors(
  //     std::make_shared<folly::CPUThreadPoolExecutor>(thread_num));
  // builder.queryCtx(std::make_shared<core::QueryCtx>(executors.get()));
  builder.config(core::QueryConfig::kSpillEnabled, "false");
  // builder.maxDrivers(thread_num);

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
