include_directories(../velox/external/xxhash)

add_executable(example example.cpp)
target_link_libraries(
    example
    velox_type
    velox_vector
    velox_exec
    velox_exec_test_lib
    velox_hive_connector
    velox_memory
)

add_executable(consumer
    consumer.cpp
    scalar_functions/bitmap_scalar.cpp
)
add_dependencies(consumer 
    velox_substrait_plan_converter
    velox_functions_prestosql
)

target_link_libraries(
    consumer
    velox_substrait_plan_converter
    velox_exec
    velox_hive_connector
    velox_memory
    velox_dwio_parquet_reader
    velox_functions_prestosql
    velox_exec_test_lib
)