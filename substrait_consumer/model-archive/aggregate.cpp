#include <folly/init/Init.h>
#include <sys/time.h>
#include <cstring>
#include "substrait_consumer/aggregation_functions/registerAggregate.h"
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
createUniqueData(RowTypePtr type, int64_t tnum, memory::MemoryPool* pool) {
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

RowVectorPtr createRandomData(
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

void fillZeros(
    RowVectorPtr data,
    int attribute,
    int64_t start_pos,
    int64_t end_pos) {
  int64_t* raw_data = data->childAt(attribute)->values()->asMutable<int64_t>();
  memset(raw_data + start_pos, 0, (end_pos - start_pos) * sizeof(int64_t));
}

// std::vector<RowVectorPtr> createData(
//     RowTypePtr type,
//     int64_t tnum,
//     int64_t batch_size,
//     memory::MemoryPool* pool,
//     RowVectorPtr (*createAllData)(
//         RowTypePtr,
//         int64_t,
//         memory::MemoryPool*,
//         const int64_t*),
//     const int64_t* value_range = nullptr) {
//   uint32_t anum = type->size();

//   auto data = createAllData(type, tnum, pool, value_range);
//   int64_t* raw_data[anum];
//   for (uint32_t i = 0; i < anum; i++)
//     raw_data[i] = data->childAt(i)->values()->asMutable<int64_t>();

//   std::vector<RowVectorPtr> ans;
//   for (int64_t i = 0; i < tnum; i += batch_size) {
//     int64_t size = (i + batch_size) > tnum ? tnum - i : batch_size;
//     std::vector<VectorPtr> vectors;
//     for (uint32_t a = 0; a < anum; a++) {
//       auto v = BaseVector::create(BIGINT(), size, pool);
//       auto raw_value = v->values()->asMutable<int64_t>();
//       memcpy(raw_value, raw_data[a] + i, size * sizeof(int64_t));
//       vectors.push_back(v);
//     }
//     ans.push_back(std::make_shared<RowVector>(
//         pool, type, BufferPtr(nullptr), size, vectors));
//   }
//   return ans;
// }

std::vector<RowVectorPtr> chunkData(
    RowTypePtr type,
    RowVectorPtr data,
    int64_t batch_size,
    memory::MemoryPool* pool) {
  auto tnum = data->size();
  uint32_t anum = type->size();
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

void printVector(RowVectorPtr data) {
  printf("Query result: %d rows\n", data->size());
  for (vector_size_t i = 0; i < data->size(); i++) {
    printf("\t%s\n", data->toString(i).c_str());
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  // number of attributes in the table
  const int attribute_num = atoi(argv[1]);
  // number of unique and random tuples
  const int64_t tuple_num[2] = {atoll(argv[2]), atoll(argv[3])};
  const int64_t batch_size = 1L * 1024 * 1024;
  // Whether to mimic the actual data by filling 0
  bool mimic = false;
  if (argc >= 5 && atoi(argv[4]) == 1)
    mimic = true;

  auto pool = memory::getDefaultMemoryPool();
  auto type = createRowType("attribute", attribute_num);
  auto attribute_names = type->names();
  std::vector<RowVectorPtr> data;

  {
    int64_t tuples_per_table = tuple_num[0];
    int table_num = tuple_num[1] / tuples_per_table + 1;
    int attribute_per_table = attribute_num / table_num;

    auto all_data = createUniqueData(type, tuple_num[0], pool.get());
    if (mimic)
      for (int i = attribute_per_table; i < attribute_num; i++)
        fillZeros(all_data, i, 0, tuple_num[0]);
    // printVector(all_data);
    data = chunkData(type, all_data, batch_size, pool.get());

    if (tuple_num[1] > 0) {
      auto random_data =
          createRandomData(type, tuple_num[1], pool.get(), &(tuple_num[0]));
      if (mimic) {
        for (int i = 0; i < table_num - 1; i++) {
          int64_t start_pos = i * tuples_per_table;
          int64_t end_pos = (i + 1) * tuples_per_table;
          // the first attribute is the tuple id that cannot be 0
          for (int a = 1; a < attribute_num; a++) {
            if (a >= (i + 1) * attribute_per_table &&
                a < (i + 2) * attribute_per_table)
              continue;
            fillZeros(random_data, a, start_pos, end_pos);
          }
        }
      }
      // printVector(random_data);
      auto chunked_random =
          chunkData(type, random_data, batch_size, pool.get());
      data.insert(data.end(), chunked_random.begin(), chunked_random.end());
    }
  }

  // auto data =
  //     createData(type, tuple_num[0], batch_size, pool.get(),
  //     createUniqueData);
  // if (tuple_num[1] > 0) {
  //   auto random_data = createData(
  //       type,
  //       tuple_num[1],
  //       batch_size,
  //       pool.get(),
  //       createRandomData,
  //       &(tuple_num[0]));
  //   data.insert(data.end(), random_data.begin(), random_data.end());
  // }
  aggregate::prestosql::registerAllAggregateFunctions();
  registerReconstructAggregates();

  timeval start, end;
  gettimeofday(&start, NULL);

  std::vector<std::string> reconstruct_aggregates;
  for (int i = 1; i < attribute_num; i++)
    reconstruct_aggregates.push_back("reconstruct(" + attribute_names[i] + ")");

  std::vector<std::string> aggregates = {
      "count(" + attribute_names[0] + ")", "avg(" + attribute_names[0] + ")"};
  for (int i = 0; i < attribute_num - 1; i++)
    aggregates.push_back("avg(a" + std::to_string(i) + ")");

  auto plan_node_id_generator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan_node =
      exec::test::PlanBuilder(plan_node_id_generator)
          .values(data)
          .singleAggregation({attribute_names[0]}, reconstruct_aggregates)
          .partialAggregation({}, aggregates)
          .finalAggregation()
          .planNode();

  exec::test::AssertQueryBuilder builder(plan_node);
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
