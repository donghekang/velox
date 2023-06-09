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
    velox_window
)

add_executable(consumer
    consumer.cpp
    scalar_functions/bitmap_scalar.cpp
    aggregation_functions/bitmap_or.cpp
    aggregation_functions/reconstruct.cpp
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

# add_executable(join 
#     model/join.cpp)

# target_link_libraries(join 
#     velox_type
#     velox_vector
#     velox_exec
#     velox_exec_test_lib
#     velox_hive_connector
#     velox_memory
#     velox_functions_prestosql
# ) 

add_executable(aggregate 
    model/aggregate.cpp 
    aggregation_functions/bitmap_or.cpp
    aggregation_functions/reconstruct.cpp)


target_link_libraries(aggregate 
    velox_type
    velox_vector
    velox_exec
    velox_exec_test_lib
    velox_hive_connector
    velox_memory
    velox_functions_prestosql
) 


add_executable(single 
    split_engine_motivation/single.cpp)


target_link_libraries(single 
    velox_type
    velox_vector
    velox_exec
    velox_exec_test_lib
    velox_vector_test_lib
    velox_hive_connector
    velox_memory
    velox_dwio_parquet_reader
    velox_functions_prestosql
) 

# add_executable(nway_join 
#     model/n-way-join.cpp)

# target_link_libraries(nway_join 
#     velox_type
#     velox_vector
#     velox_exec
#     velox_exec_test_lib
#     velox_hive_connector
#     velox_memory
#     velox_functions_prestosql
# ) 