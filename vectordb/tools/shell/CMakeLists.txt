set(SHELL_SOURCES shell.cpp)
find_package(OpenMP REQUIRED)
add_executable(shell ${SHELL_SOURCES})

target_link_libraries(shell vdbms vdbms_linenoise argparse utf8proc OpenMP::OpenMP_CXX)
set_target_properties(shell PROPERTIES OUTPUT_NAME vdbms-shell)
