set(TERRIER_BENCH_SOURCES terrier.cpp)
add_executable(terrier-bench ${TERRIER_BENCH_SOURCES})

target_link_libraries(terrier-bench vdbms)
set_target_properties(terrier-bench PROPERTIES OUTPUT_NAME vdbms-terrier-bench)

