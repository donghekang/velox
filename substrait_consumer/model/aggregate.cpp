#include <folly/init/Init.h>
#include <math.h>
#include <sys/time.h>
#include <cstring>
#include "substrait_consumer/aggregation_functions/registerAggregate.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

using namespace facebook::velox;

RowTypePtr createRowType(int attribute_num, int bitmap_num) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.push_back("tid");
  types.push_back(INTEGER());
  for (int i = 0; i < bitmap_num; i++) {
    names.push_back("b" + std::to_string(i));
    types.push_back(VARCHAR());
  }
  for (int i = 0; i < attribute_num; i++) {
    names.push_back("a" + std::to_string(i));
    types.push_back(INTEGER());
  }
  return ROW(std::move(names), std::move(types));
}

void setBit(char* const bits, int pos) {
  int idx = pos / 8;
  int off = pos % 8;
  char mask = 1 << off;
  bits[idx] |= mask;
}

std::vector<RowVectorPtr> createData(
    RowTypePtr type,
    int tnum,
    int attribute_num,
    int bitmap_num,
    int start_attr,
    int end_attr,
    memory::MemoryPool* pool,
    int batch_size = 1 * 1024 * 1024) {
  std::vector<std::vector<VectorPtr>> vectors;
  int batch_num = tnum / batch_size;
  assert(batch_num > 0 && tnum % batch_size == 0);

  // create tid
  {
    auto v = BaseVector::create(INTEGER(), tnum, pool);
    auto raw_values = v->values()->asMutable<int>();
    std::iota(raw_values, raw_values + tnum, 0);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(raw_values, raw_values + tnum, g);

    for (int i = 0; i < tnum; i += batch_size) {
      auto v_batch = BaseVector::create(INTEGER(), batch_size, pool);
      auto raw_batch = v_batch->values()->asMutable<int>();
      memcpy(raw_batch, raw_values + i, batch_size * sizeof(int));
      vectors.push_back({v_batch});
    }
  }

  // create bitmap
  {
    int byte_size = ceil(attribute_num / 8.0);
    char c_data[byte_size];
    for (int i = start_attr; i < end_attr; i++)
      setBit(c_data, i);
    StringView data(c_data, byte_size);

    for (int k = 0; k < batch_num; k++) {
      for (int i = 0; i < bitmap_num; i++) {
        auto v = BaseVector::createConstant(variant(data), batch_size, pool);
        // auto v = BaseVector::create(VARCHAR(), batch_size, pool);
        // auto flat = v->as<FlatVector<StringView>>();
        // for (int j = 0; j < batch_size; j++)
        //   flat->set(j, data);
        vectors[k].push_back(v);
      }
    }
  }

  // create data
  {
    for (int k = 0; k < batch_num; k++) {
      for (int i = 0; i < attribute_num; i++) {
        if (i >= start_attr && i < end_attr) {
          auto v = BaseVector::create(INTEGER(), batch_size, pool);
          auto raw_values = v->values()->asMutable<int>();
          for (int j = 0; j < batch_size; j++)
            raw_values[j] = rand() % 1024;
          vectors[k].push_back(v);
        } else {
          auto v =
              BaseVector::createConstant(variant((int)0), batch_size, pool);
          vectors[k].push_back(v);
        }
      }
    }
  }

  std::vector<RowVectorPtr> ans;
  for (int i = 0; i < batch_num; i++) {
    ans.push_back(std::make_shared<RowVector>(
        pool, type, BufferPtr(nullptr), batch_size, vectors[i]));
  }
  return ans;
}

std::vector<RowVectorPtr> createData(
    RowTypePtr type,
    int tnum,
    int attribute_num,
    int bitmap_num,
    int group_num,
    memory::MemoryPool* pool,
    int batch_size = 1 * 1024 * 1024) {
  std::vector<RowVectorPtr> ans;
  assert(attribute_num % group_num == 0);
  int group_size = attribute_num / group_num;
  for (int i = 0; i < attribute_num; i += group_size) {
    auto t = createData(
        type,
        tnum,
        attribute_num,
        bitmap_num,
        i,
        i + group_size,
        pool,
        batch_size);
    ans.insert(ans.end(), t.begin(), t.end());
  }
  return ans;
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  const int tnum = atoi(argv[1]);
  const int bitmap_num = atoi(argv[2]);
  const int attribute_num = atoi(argv[3]);
  const int group_num = atoi(argv[4]);
  const int batch_size = 1 * 1024 * 1024;
  int thread_num = 6;
  if(argc >= 6)
       thread_num = atoi(argv[5]);

  auto pool = memory::getDefaultMemoryPool();
  auto type = createRowType(attribute_num, bitmap_num);
  auto attribute_names = type->names();
  auto data = createData(
      type, tnum, attribute_num, bitmap_num, group_num, pool.get(), batch_size);

  aggregate::prestosql::registerAllAggregateFunctions();
  registerReconstructAggregates();

  timeval start, end;
  gettimeofday(&start, NULL);

  std::vector<std::string> reconstruct_aggregates;
  std::vector<std::string> sum_aggregates;
  for (int i = 0; i < bitmap_num; i++) {
    reconstruct_aggregates.push_back(
        "reconstruct(" + attribute_names[1 + i] + ")");
  }
  for (int i = 0; i < attribute_num; i++) {
    reconstruct_aggregates.push_back(
        "reconstruct(" + attribute_names[1 + bitmap_num + i] + ")");
  }

  auto plan_node_id_generator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan_builder =
      exec::test::PlanBuilder(plan_node_id_generator)
          .values(data)
          .localPartition({"tid"})
          .singleAggregation({attribute_names[0]}, reconstruct_aggregates);

  {
    auto out_names = plan_builder.planNode()->outputType()->names();
    for (int i = 0; i < attribute_num; i++) {
      // sum_aggregates.push_back
      sum_aggregates.push_back("sum(" + out_names[1 + bitmap_num + i] + ")");
    }
  }

  auto plan_node = plan_builder.partialAggregation({}, sum_aggregates)
                       .finalAggregation()
                       .planNode();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(thread_num));
  exec::test::AssertQueryBuilder query_builder(plan_node);
  query_builder.queryCtx(std::make_shared<core::QueryCtx>(executor.get()));
  query_builder.maxDrivers(thread_num);
  query_builder.config(core::QueryConfig::kSpillEnabled, "false");

  RowVectorPtr result = query_builder.copyResults(pool.get(), true);
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
