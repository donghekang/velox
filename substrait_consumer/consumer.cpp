#include <folly/init/Init.h>
#include <google/protobuf/util/json_util.h>
#include <fstream>
#include <sstream>
#include "stdio.h"
#include "stdlib.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/PlanFragment.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/Task.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/proto/substrait/plan.pb.h"

namespace velox = facebook::velox;
using namespace facebook::velox;

const std::string kHiveConnectorId = "test-hive";

void readSubstraitPlan(const char* path, ::substrait::Plan& plan) {
  std::ifstream ifile(path);
  std::stringstream buffer;
  buffer << ifile.rdbuf();
  ifile.close();

  std::string substrait_json = buffer.str();
  auto status =
      google::protobuf::util::JsonStringToMessage(substrait_json, &plan);
  VELOX_CHECK(
      status.ok(),
      "Failed to parse Substrait Json: {} {}",
      status.code(),
      status.message());
}

void registerConnector() {
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();
  parquet::registerParquetReaderFactory();
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

int main(int argc, char** argv) {
  if (argc < 2) {
    printf("Please specify a Substrait plan\n");
    return EXIT_FAILURE;
  }

  folly::init(&argc, &argv);
  auto pool = memory::getDefaultScopedMemoryPool();
  registerConnector();

  ::substrait::Plan substriat_plan;
  readSubstraitPlan(argv[1], substriat_plan);
  velox::substrait::SubstraitVeloxPlanConverter plan_converter(pool.get());
  auto plan_node = plan_converter.toVeloxPlan(substriat_plan);
  core::PlanFragment plan_fragment(plan_node);
  auto task = std::make_shared<exec::Task>(
      "my_task", plan_fragment, 0, core::QueryCtx::createForTest());

  addSplits(plan_converter.splitInfos(), task);
  printf("%s\n", plan_node->toString(true, true).c_str());

  while (auto result = task->next()) {
    printf("Query result:\n");
    for (vector_size_t i = 0; i < result->size(); i++) {
      printf("\t%s\n", result->toString(i).c_str());
    }
  }

  return 0;
}
