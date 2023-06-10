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

#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "velox/common/base/Fs.h"
#include "velox/exec/tests/AggregationFuzzerRunner.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

DEFINE_string(
    aggregation_fuzzer_repro_path,
    "",
    "Directory path where all plans generated by AggregationFuzzer are "
    "expected to reside. Any plan paths "
    "already specified via a startup flag will take precedence.");

DEFINE_string(
    plan_nodes_path,
    "",
    "Path for plan nodes to be restored from disk. This will enable single run "
    "of the fuzzer with the on-disk persisted plan information.");

static std::string checkAndReturnFilePath(
    const std::string_view& fileName,
    const std::string& flagName) {
  auto path =
      fmt::format("{}/{}", FLAGS_aggregation_fuzzer_repro_path, fileName);
  if (fs::exists(path)) {
    LOG(INFO) << "Using " << flagName << " = " << path;
    return path;
  } else {
    LOG(INFO) << "File for " << flagName << " not found.";
  }
  return "";
}

static void checkDirForExpectedFiles() {
  LOG(INFO) << "Searching input directory for expected files at "
            << FLAGS_aggregation_fuzzer_repro_path;

  FLAGS_plan_nodes_path = FLAGS_plan_nodes_path.empty()
      ? checkAndReturnFilePath(exec::test::kPlanNodeFileName, "plan_nodes")
      : FLAGS_plan_nodes_path;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  folly::init(&argc, &argv);

  if (!FLAGS_aggregation_fuzzer_repro_path.empty()) {
    checkDirForExpectedFiles();
  }

  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions();
  facebook::velox::functions::prestosql::registerAllScalarFunctions();

  return exec::test::AggregationFuzzerRunner::run(FLAGS_plan_nodes_path);
}
