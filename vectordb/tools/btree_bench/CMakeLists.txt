set(BTREE_BENCH_SOURCES btree_bench.cpp)
add_executable(btree-bench ${BTREE_BENCH_SOURCES})

target_link_libraries(btree-bench vdbms)
set_target_properties(btree-bench PROPERTIES OUTPUT_NAME vdbms-btree-bench)
