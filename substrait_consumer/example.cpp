#include <folly/init/Init.h>
#include <google/protobuf/util/json_util.h>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include "stdio.h"
#include "stdlib.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/substrait/proto/substrait/plan.pb.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;

// void readSubstraitPlan(const char* path, ::substrait::Plan& plan) {
//   std::ifstream ifile(path);
//   std::stringstream buffer;
//   buffer << ifile.rdbuf();
//   ifile.close();

//   std::string substrait_json = buffer.str();
//   auto status =
//       google::protobuf::util::JsonStringToMessage(substrait_json, &plan);
//   VELOX_CHECK(
//       status.ok(),
//       "Failed to parse Substrait Json: {} {}",
//       status.code(),
//       status.message());
// }

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  auto pool = memory::addDefaultLeafMemoryPool();

  auto build_type = ROW({{"build_0", BIGINT()}, {"build_1", BIGINT()}});
  auto probe_type = ROW({{"probe_0", BIGINT()}, {"probe_1", BIGINT()}});

  RowVectorPtr build_vector, probe_vector;
  {
    const size_t build_size = 5;
    auto v0 = BaseVector::create(BIGINT(), build_size, pool.get());
    auto v1 = BaseVector::create(BIGINT(), build_size, pool.get());
    auto v0_raw = v0->values()->asMutable<int64_t>();
    auto v1_raw = v1->values()->asMutable<int64_t>();
    for (size_t i = 0; i < build_size; i++) {
      v0_raw[i] = i;
      v1_raw[i] = i + build_size;
    }
    build_vector = std::make_shared<RowVector>(
        pool.get(),
        build_type,
        BufferPtr(nullptr),
        build_size,
        std::vector<VectorPtr>{v0, v1});
  }
  {
    const size_t probe_size = 10;
    auto v0 = BaseVector::create(BIGINT(), probe_size, pool.get());
    auto v1 = BaseVector::create(BIGINT(), probe_size, pool.get());
    auto v0_raw = v0->values()->asMutable<int64_t>();
    auto v1_raw = v1->values()->asMutable<int64_t>();
    for (size_t i = 0; i < probe_size; i++) {
      v0_raw[i] = i;
      v1_raw[i] = i + probe_size;
    }
    probe_vector = std::make_shared<RowVector>(
        pool.get(),
        probe_type,
        BufferPtr(nullptr),
        probe_size,
        std::vector<VectorPtr>{v0, v1});
  }

  // For fun, let's print the shuffled data to stdout.
  std::cout << "Build vector generated:" << std::endl;
  for (vector_size_t i = 0; i < build_vector->size(); ++i) {
    std::cout << build_vector->toString(i) << std::endl;
  }
  std::cout << "Probe vector generated:" << std::endl;
  for (vector_size_t i = 0; i < probe_vector->size(); ++i) {
    std::cout << probe_vector->toString(i) << std::endl;
  }

  auto plan_node_id_generator = std::make_shared<core::PlanNodeIdGenerator>();
  auto build_table_node = exec::test::PlanBuilder(plan_node_id_generator)
                              .values({build_vector})
                              .planNode();
  std::vector<std::string> join_out_layout;
  join_out_layout.insert(
      join_out_layout.end(),
      probe_type->names().begin(),
      probe_type->names().end());
  join_out_layout.insert(
      join_out_layout.end(),
      build_type->names().begin(),
      build_type->names().end());
  auto join_node = exec::test::PlanBuilder(plan_node_id_generator)
                       .values({probe_vector})
                       .hashJoin(
                           {"probe_0"},
                           {"build_0"},
                           build_table_node,
                           "",
                           join_out_layout,
                           core::JoinType::kLeft)
                       .planNode();

  exec::test::AssertQueryBuilder builder(join_node);
  RowVectorPtr result = builder.copyResults(pool.get(), true);
  printf("Query result: %d rows\n", result->size());
  for (vector_size_t i = 0; i < result->size(); i++) {
    printf("\t%s\n", result->toString(i).c_str());
  }
  return 0;
}
