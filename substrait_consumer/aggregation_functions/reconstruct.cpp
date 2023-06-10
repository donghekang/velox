#include "substrait_consumer/aggregation_functions/registerAggregate.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
// #include "velox/functions/prestosql/aggregates/SimpleNumericAggregate.h"
// #include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"

using namespace facebook::velox;

template <typename T>
struct InitTrait {
  static constexpr T initValue() {
    return T(0);
  }
};

template <>
struct InitTrait<Timestamp> {
  static constexpr Timestamp initValue() {
    return Timestamp(0, 0);
  }
};

template <>
struct InitTrait<Date> {
  static constexpr Date initValue() {
    return Date(0);
  }
};

template <typename T>
class ReconstructHook final : public aggregate::AggregationHook {
 public:
  ReconstructHook(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      char** groups,
      uint64_t* numNulls)
      : aggregate::AggregationHook(
            offset,
            nullByte,
            nullMask,
            groups,
            numNulls) {}

  void addValue(vector_size_t row, const void* value) override {
    auto group = findGroup(row);
    const T* casted_value = reinterpret_cast<const T*>(value);
    if (*casted_value == kInitialValue_)
      return;

    clearNull(group);
    *reinterpret_cast<T*>(group + offset_) = *casted_value;
  }

 private:
  static constexpr T kInitialValue_{InitTrait<T>::initValue()};
};

template <typename T>
class NumericReconstructAggregate
    : public functions::aggregate::SimpleNumericAggregate<T, T, T> {
  using BaseAggregate = functions::aggregate::SimpleNumericAggregate<T, T, T>;

 public:
  explicit NumericReconstructAggregate(TypePtr resultType)
      : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices)
      *exec::Aggregate::value<T>(groups[i]) = kInitialValue_;
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && args[0]->isLazy()) {
      BaseAggregate::template pushdown<ReconstructHook<T>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, T>(
        groups,
        rows,
        args[0],
        [](T& result, T value) {
          if (value != kInitialValue_)
            result = value;
        },
        mayPushdown);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) {
          if (value != kInitialValue_)
            result = value;
        },
        [](T& result, T value, int) {
          if (value != kInitialValue_)
            result = value;
        },
        mayPushdown,
        kInitialValue_);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<T>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<T>(group);
        });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<T>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<T>(group);
        });
  }

 private:
  static constexpr T kInitialValue_{InitTrait<T>::initValue()};
};

template <>
void NumericReconstructAggregate<Timestamp>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<Timestamp>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return Timestamp::fromMillis(ts.toMillis());
      });
}

class NonNumericReconstructAggregate : public exec::Aggregate {
 public:
  explicit NonNumericReconstructAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(functions::aggregate::SingleValueAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      new (groups[i] + offset_) functions::aggregate::SingleValueAccumulator();
    }
  }

  // void finalize(char** /* groups */, int32_t /* numGroups */) override {
  //   // Nothing to do
  // }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdate(groups, rows, args[0]);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdateSingleGroup(group, rows, args[0]);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    (*result)->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if ((*result)->mayHaveNulls()) {
      BufferPtr nulls = (*result)->mutableNulls((*result)->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator =
          value<functions::aggregate::SingleValueAccumulator>(group);
      if (!accumulator->hasValue())
        (*result)->setNull(i, true);
      else {
        if (rawNulls)
          bits::clearBit(rawNulls, i);
        accumulator->read(*result, i);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    // partial and final aggregations are the same
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<functions::aggregate::SingleValueAccumulator>(group)->destroy(
          allocator_);
    }
  }

 protected:
  void
  doUpdate(char** groups, const SelectivityVector& rows, const VectorPtr& arg) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping() && decoded.isNullAt(0))
      return;
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i) || decoded.valueAt<StringView>(i).size() == 0)
        return;
      auto accumulator =
          value<functions::aggregate::SingleValueAccumulator>(groups[i]);
      accumulator->write(baseVector, indices[i], allocator_);
    });
  }

  void doUpdateSingleGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0) || decoded.valueAt<StringView>(0).size() == 0)
        return;
      auto accumulator =
          value<functions::aggregate::SingleValueAccumulator>(group);
      accumulator->write(baseVector, indices[0], allocator_);
      return;
    }

    auto accumulator =
        value<functions::aggregate::SingleValueAccumulator>(group);
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i) || decoded.valueAt<StringView>(i).size() == 0)
        return;
      accumulator->write(baseVector, indices[i], allocator_);
    });
  }
};

void registerReconstructAggregates() {
  const std::string kReconstruct = "reconstruct";
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("T")
                           .returnType("T")
                           .intermediateType("T")
                           .argumentType("T")
                           .build());
  exec::registerAggregateFunction(
      kReconstruct,
      std::move(signatures),
      [kReconstruct](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes only one argument", kReconstruct);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<NumericReconstructAggregate<int8_t>>(
                resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<NumericReconstructAggregate<int16_t>>(
                resultType);
          case TypeKind::INTEGER:
            return std::make_unique<NumericReconstructAggregate<int32_t>>(
                resultType);
          case TypeKind::BIGINT:
            return std::make_unique<NumericReconstructAggregate<int64_t>>(
                resultType);
          case TypeKind::REAL:
            return std::make_unique<NumericReconstructAggregate<float>>(
                resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<NumericReconstructAggregate<double>>(
                resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<NumericReconstructAggregate<Timestamp>>(
                resultType);
          case TypeKind::DATE:
            return std::make_unique<NumericReconstructAggregate<Date>>(
                resultType);
          case TypeKind::VARCHAR:
          case TypeKind::VARBINARY:
            return std::make_unique<NonNumericReconstructAggregate>(inputType);
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                kReconstruct,
                inputType->kindName());
            break;
        }
      });
}