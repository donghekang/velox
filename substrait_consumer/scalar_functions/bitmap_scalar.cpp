#include "velox/functions/Udf.h"
#include "velox/functions/lib/RegistrationHelpers.h"
using namespace facebook::velox;

template <typename T>
struct MyBitmapGetFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput1>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Varchar>& arg0, const TInput1& arg1) {
    uint64_t off = arg1;
    const uint8_t* bits = reinterpret_cast<const uint8_t*>(arg0.data());
    result = (bits[off >> 3] >> (off & 0x07)) & 1;
    return true;
  }
};

void registerMyBitmapGetFunction() {
  registerFunction<MyBitmapGetFunction, bool, Varchar, int8_t>({"bitmap_get"});
  registerFunction<MyBitmapGetFunction, bool, Varchar, int16_t>({"bitmap_get"});
  registerFunction<MyBitmapGetFunction, bool, Varchar, int32_t>({"bitmap_get"});
  registerFunction<MyBitmapGetFunction, bool, Varchar, int64_t>({"bitmap_get"});
}