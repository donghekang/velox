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
#include "velox/exec/tests/utils/PlanBuilder.h"
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
  auto pool = memory::getDefaultScopedMemoryPool();

  auto inputRowType = ROW({{"my_col", BIGINT()}});
  const size_t vectorSize = 10;
  auto vector = BaseVector::create(BIGINT(), vectorSize, pool.get());
  auto rawValues = vector->values()->asMutable<int64_t>();

  std::iota(rawValues, rawValues + vectorSize, 0); // 0, 1, 2, 3, ...
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rawValues, rawValues + vectorSize, g);

  auto rowVector = std::make_shared<RowVector>(
      pool.get(), // pool where allocations will be made.
      inputRowType, // input row type (defined above).
      BufferPtr(nullptr), // no nulls on this example.
      vectorSize, // length of the vectors.
      std::vector<VectorPtr>{vector}); // the input vector data.

  // For fun, let's print the shuffled data to stdout.
  std::cout << "Input vector generated:" << std::endl;
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    std::cout << rowVector->toString(i) << std::endl;
  }

  std::string file_path(argv[1]);
  const std::string kHiveConnectorId = "test-hive";
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();

  auto writePlanFragment =
      exec::test::PlanBuilder()
          .values({rowVector})
          .tableWrite(
              inputRowType->names(),
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  std::make_shared<connector::hive::HiveInsertTableHandle>(
                      file_path)))
          .planFragment();
  auto writeTask = std::make_shared<exec::Task>(
      "my_write_task", writePlanFragment, 0, core::QueryCtx::createForTest());
  while (auto result = writeTask->next())
    ;

  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(inputRowType)
                              .capturePlanNodeId(scanNodeId)
                              .orderBy({"my_col"}, false)
                              .planFragment();
  auto readTask = std::make_shared<exec::Task>(
      "my_read_task", readPlanFragment, 0, core::QueryCtx::createForTest());
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, "file://" + file_path, dwio::common::FileFormat::DWRF);
  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  readTask->noMoreSplits(scanNodeId);

  while (auto result = readTask->next()) {
    printf("Vector available after processing (scan + sort):\n");
    for (vector_size_t i = 0; i < result->size(); i++)
      printf("%s\n", result->toString(i).c_str());
  }
  //   if (argc < 2) {
  //     printf("Please specify a Substrait plan\n");
  //     return EXIT_FAILURE;
  //   }

  //   ::substrait::Plan plan;
  //   readSubstraitPlan(argv[1], plan);
  return 0;
}
