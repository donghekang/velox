#include <folly/init/Init.h>
#include <google/protobuf/util/json_util.h>
#include <sys/time.h>
#include <fstream>
#include <sstream>
#include "stdio.h"
#include "stdlib.h"
#include "substrait_consumer/aggregation_functions/registerAggregate.h"
#include "substrait_consumer/scalar_functions/bitmap_scalar.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/PlanFragment.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/proto/substrait/plan.pb.h"

namespace velox = facebook::velox;
using namespace facebook::velox;

#define VERBOSE

const std::string kHiveConnectorId = "test-hive";

void readSubstraitPlan(const char* path, ::substrait::Plan& plan) {
  std::ifstream ifile(path, std::ios::binary);
  if (!ifile) {
    VELOX_FAIL("Cannot open file: ", path);
  } else if (!plan.ParseFromIstream(&ifile)) {
    VELOX_FAIL("Failed to parse plan");
  }
  ifile.close();
}

void registerFunctions() {
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  registerMyBitmapScalarFunction();
  registerBitmapORAggregates();
  registerReconstructAggregates();
}

void registerConnector() {
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();
  parquet::registerParquetReaderFactory(::parquet::ParquetReaderType::NATIVE);
}

void addSplits(
    const std::unordered_map<
        core::PlanNodeId,
        std::shared_ptr<
            velox::substrait::SubstraitVeloxPlanConverter::SplitInfo>>&
        split_infos,
    std::shared_ptr<exec::Task> task) {
  for (auto it = split_infos.begin(); it != split_infos.end(); it++) {
    const auto& paths = it->second->paths;
    const auto format = it->second->format;
    for (auto p : paths) {
      auto s = std::make_shared<connector::hive::HiveConnectorSplit>(
          kHiveConnectorId, p, dwio::common::FileFormat::PARQUET);
      task->addSplit(it->first, exec::Split{s});
    }
    task->noMoreSplits(it->first);
  }
}

void addSplits(
    const std::unordered_map<
        core::PlanNodeId,
        std::shared_ptr<
            velox::substrait::SubstraitVeloxPlanConverter::SplitInfo>>&
        split_infos,
    exec::test::AssertQueryBuilder& query_builder) {
  for (auto it = split_infos.begin(); it != split_infos.end(); it++) {
    const auto& paths = it->second->paths;
    const auto format = it->second->format;
    for (auto p : paths) {
      auto s = std::make_shared<connector::hive::HiveConnectorSplit>(
          kHiveConnectorId, p, dwio::common::FileFormat::PARQUET);
      query_builder.split(it->first, std::move(exec::Split{s}));
    }
  }
}

int main(int argc, char** argv) {
  if (argc < 2) {
    printf("Please specify a Substrait plan\n");
    return EXIT_FAILURE;
  }

  folly::init(&argc, &argv);
  auto pool = memory::getDefaultMemoryPool();
  registerConnector();
  registerFunctions();

  timeval start, end;
  gettimeofday(&start, NULL);

  ::substrait::Plan substriat_plan;
  readSubstraitPlan(argv[1], substriat_plan);
  velox::substrait::SubstraitVeloxPlanConverter plan_converter(pool.get());
  auto plan_node = plan_converter.toVeloxPlan(substriat_plan);

  exec::test::AssertQueryBuilder query_builder(plan_node);
  addSplits(plan_converter.splitInfos(), query_builder);

  int thread_num = 12;
  if (argc == 3)
    thread_num = atoi(argv[2]);
  // query_builder.maxDrivers(driver_per_pipeline);
  // printf("Thread per pipeline: %d\n", driver_per_pipeline);
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(thread_num));
  query_builder.queryCtx(std::make_shared<core::QueryCtx>(executor.get()));
  query_builder.maxDrivers(thread_num);
  printf("Number of threads: %d\n", thread_num);

  int printStats = false;
#ifdef VERBOSE
  printStats = true;
#endif

  RowVectorPtr result = query_builder.copyResults(pool.get(), printStats);
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
