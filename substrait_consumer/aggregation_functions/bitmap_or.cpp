#include "substrait_consumer/aggregation_functions/registerAggregate.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

struct BitmapOrAccumlator {
  bool hasValue() const {
    return data_ != nullptr;
  }

  void write(const StringView& data) {
    VELOX_CHECK(!hasValue());
    size_ = data.size();
    data_ = new char[size_];
    memcpy(data_, data.data(), size_);
  }

  void read(const VectorPtr& vector, vector_size_t index) const {
    VELOX_CHECK(hasValue());

    auto result = vector->asUnchecked<FlatVector<StringView>>();
    result->set(index, StringView(data_, size_));
  }

  void bitmapOR(const StringView& data) {
    VELOX_CHECK(hasValue());
    VELOX_CHECK_EQ(data.size(), size_);
    for (int i = 0; i < size_; i++)
      data_[i] |= data.data()[i];
  }

  void destroy() {
    if (hasValue())
      delete[] data_;
    size_ = 0;
    data_ = nullptr;
  }

 private:
  char* data_{nullptr};
  int size_ = 0;
};

class BitmapOrAggregate : public exec::Aggregate {
 public:
  explicit BitmapOrAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(BitmapOrAccumlator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices)
      new (groups[i] + offset_) BitmapOrAccumlator();
  };

  // void finalize(char**, int32_t) override {
  //   // Nothing to do
  // }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool) override {
    doUpdate(groups, rows, args[0]);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushDown) override {
    addRawInput(groups, rows, args, mayPushDown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) {
    doUpdateSingleGroups(group, rows, args[0]);
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

    for (int i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<BitmapOrAccumlator>(group);
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
      value<BitmapOrAccumlator>(group)->destroy();
    }
  }

 private:
  void
  doUpdate(char** groups, const SelectivityVector& rows, const VectorPtr& arg) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping() && decoded.isNullAt(0))
      return;

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i))
        return;
      StringView data = decoded.valueAt<StringView>(i);
      auto accumlator = value<BitmapOrAccumlator>(groups[i]);
      if (!accumlator->hasValue()) {
        accumlator->write(data);
      } else {
        accumlator->bitmapOR(data);
      }
    });
  }

  void doUpdateSingleGroups(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0))
        return;
      StringView data = decoded.valueAt<StringView>(0);
      auto accumulator = value<BitmapOrAccumlator>(group);
      if (!accumulator->hasValue())
        accumulator->write(data);
      else
        accumulator->bitmapOR(data);
      return;
    }

    auto accumulator = value<BitmapOrAccumlator>(group);
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i))
        return;
      StringView data = decoded.valueAt<StringView>(i);
      if (!accumulator->hasValue())
        accumulator->write(data);
      else
        accumulator->bitmapOR(data);
    });
  }
};

void registerBitmapORAggregates() {
  const std::string kBitmapOr = "bitmap_or";
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("T")
                           .returnType("T")
                           .intermediateType("T")
                           .argumentType("T")
                           .build());
  exec::registerAggregateFunction(
      kBitmapOr,
      std::move(signatures),
      [kBitmapOr](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes only one argument", kBitmapOr);
        auto inputType = argTypes[0];
        VELOX_CHECK_EQ(
            inputType->kind(),
            TypeKind::VARBINARY,
            "Unknown input type for {} aggregation {}",
            kBitmapOr,
            inputType->kindName());
        return std::make_unique<BitmapOrAggregate>(inputType);
      });
}