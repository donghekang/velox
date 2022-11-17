#include "velox/functions/Udf.h"
#include "velox/functions/lib/RegistrationHelpers.h"
using namespace facebook::velox;

template <typename T>
struct MyBitmapGetFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput1>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Varbinary>& arg0, const TInput1& arg1) {
    uint64_t off = arg1;
    const uint8_t* bits = reinterpret_cast<const uint8_t*>(arg0.data());
    result = (bits[off >> 3] >> (off & 0x07)) & 1;
    return true;
  }
};

template <typename T>
struct MyBitmapCountFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TOut>
  FOLLY_ALWAYS_INLINE bool call(TOut& result, const arg_type<Varbinary>& arg0) {
    result = 0;

    auto count = [&](uint8_t value) -> int {
      int ans = 0;
      while (value != 0) {
        if (value & 0x01)
          ans++;
        value >>= 1;
      }
      return ans;
    };

    const uint8_t* bits = reinterpret_cast<const uint8_t*>(arg0.data());
    for (size_t i = 0; i < arg0.size(); i++) {
      result += count(bits[i]);
    }
    return true;
  }
};

template <typename T>
struct MyBitmapOrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Varbinary>& result,
      const arg_type<Varbinary>* arg0,
      const arg_type<Varbinary>* arg1) {
    VELOX_CHECK(arg0 != nullptr || arg1 != nullptr);
    size_t size = arg0 ? arg0->size() : arg1->size();

    result.resize(size);
    uint8_t* out_data = reinterpret_cast<uint8_t*>(result.data());
    const uint8_t* arg0_data =
        arg0 ? reinterpret_cast<const uint8_t*>(arg0->data()) : nullptr;
    const uint8_t* arg1_data =
        arg1 ? reinterpret_cast<const uint8_t*>(arg1->data()) : nullptr;

    if (arg0_data == nullptr)
      memcpy(out_data, arg1->data(), size);
    else if (arg1_data == nullptr)
      memcpy(out_data, arg0->data(), size);
    else {
      VELOX_CHECK_EQ(arg0->size(), arg1->size());
      for (size_t i = 0; i < size; i++)
        out_data[i] = arg0_data[i] | arg1_data[i];
    }
  }
};

template <typename T>
struct MyBitmapAndFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Varbinary>& result,
      const arg_type<Varbinary>* arg0,
      const arg_type<Varbinary>* arg1) {
    VELOX_CHECK(arg0 != nullptr || arg1 != nullptr);
    size_t size = arg0 ? arg0->size() : arg1->size();

    result.resize(size);
    uint8_t* out_data = reinterpret_cast<uint8_t*>(result.data());
    const uint8_t* arg0_data =
        arg0 ? reinterpret_cast<const uint8_t*>(arg0->data()) : nullptr;
    const uint8_t* arg1_data =
        arg1 ? reinterpret_cast<const uint8_t*>(arg1->data()) : nullptr;

    if (arg0_data == nullptr)
      memcpy(out_data, arg1->data(), size);
    else if (arg1_data == nullptr)
      memcpy(out_data, arg0->data(), size);
    else {
      VELOX_CHECK_EQ(arg0->size(), arg1->size());
      for (size_t i = 0; i < size; i++)
        out_data[i] = arg0_data[i] & arg1_data[i];
    }
  }
};

void registerMyBitmapScalarFunction() {
  registerFunction<MyBitmapGetFunction, bool, Varbinary, int8_t>(
      {"bitmap_get"});
  registerFunction<MyBitmapGetFunction, bool, Varbinary, int16_t>(
      {"bitmap_get"});
  registerFunction<MyBitmapGetFunction, bool, Varbinary, int32_t>(
      {"bitmap_get"});
  registerFunction<MyBitmapGetFunction, bool, Varbinary, int64_t>(
      {"bitmap_get"});

  registerFunction<MyBitmapCountFunction, int32_t, Varbinary>({"bitmap_count"});

  registerFunction<MyBitmapAndFunction, Varbinary, Varbinary, Varbinary>(
      {"bitmap_and_scalar"});
  registerFunction<MyBitmapOrFunction, Varbinary, Varbinary, Varbinary>(
      {"bitmap_or_scalar"});
}