#include <folly/init/Init.h>
#include <cstring>
#include "substrait_consumer/aggregation_functions/registerAggregate.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

using namespace facebook::velox;

const int ANUMS[2] = {1, 10};

const std::string kHiveConnectorId = "test-hive";

RowTypePtr createRowType(std::vector<std::string> names) {
  std::vector<TypePtr> types;
  types.resize(names.size(), INTEGER());
  return ROW(std::move(names), std::move(types));
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
  parquet::registerParquetWriterFactory();
}

void BuildJoin(
    const std::string& path,
    std::shared_ptr<core::PlanNodeIdGenerator> id_generator,
    exec::test::PlanBuilder& builder,
    std::vector<std::string>& output_layout) {
  std::vector<std::string> build_names = {"id"}, probe_names = {"id"};
  for (int i = 0; i < ANUMS[0]; i++)
    build_names.push_back("a" + std::to_string(i));
  for (int i = 0; i < ANUMS[1]; i++)
    probe_names.push_back("b" + std::to_string(i));
  // read the build table
  core::PlanNodeId buildScanNodeId, probeScanNodeId;
  auto buildNode = exec::test::PlanBuilder(id_generator)
                       .tableScan(createRowType(build_names))
                       .capturePlanNodeId(buildScanNodeId)
                       .planNode();
  // read the probe table and join
  builder.tableScan(createRowType(probe_names))
      .capturePlanNodeId(probeScanNodeId)
      .hashJoin(
          {"id"}, {"id"}, buildNode, "", output_layout, core::JoinType::kInner);
}

int main(int argc, char** argv) {
  const double selectivity = atof(argv[1]);
  const std::string path = argv[2];
  int thread_num = 1;
  if (argc >= 4)
    thread_num = atoi(argv[3]);

  folly::init(&argc, &argv);
  auto pool = memory::addDefaultLeafMemoryPool();

  registerConnector();

  core::PlanNodeId scanNodeId;
  auto planNode =
      exec::test::PlanBuilder()
          .tableScan(createRowType({"id", "a1"}))
          .capturePlanNodeId(scanNodeId)
          .tableWrite(
              {"a0", "a1"},
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  exec::test::HiveConnectorTestBase::makeHiveInsertTableHandle(
                      {"a0, a1"},
                      {INTEGER(), INTEGER()},
                      {},
                      exec::test::HiveConnectorTestBase::makeLocationHandle(
                          path + "/temp"),
                      dwio::common::FileFormat::PARQUET)),
              connector::CommitStrategy::kNoCommit)
          .planNode();

  exec::test::AssertQueryBuilder query_builder(planNode);
  query_builder.split(
      scanNodeId,
      std::make_shared<connector::hive::HiveConnectorSplit>(
          kHiveConnectorId,
          "file://" + path + "/a.parquet",
          dwio::common::FileFormat::PARQUET));

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(thread_num));
  query_builder.queryCtx(std::make_shared<core::QueryCtx>(executor.get()));
  query_builder.maxDrivers(thread_num);

  RowVectorPtr result = query_builder.copyResults(pool.get(), true);
  printf("Query result: %d rows\n", result->size());
  for (vector_size_t i = 0; i < result->size(); i++) {
    printf("\t%s\n", result->toString(i).c_str());
  }

  return 0;
}
