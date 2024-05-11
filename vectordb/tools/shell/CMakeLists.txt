set(SHELL_SOURCES shell.cpp)
add_executable(shell ${SHELL_SOURCES})

target_link_libraries(shell vdbms vdbms_linenoise argparse utf8proc)
set_target_properties(shell PROPERTIES OUTPUT_NAME vdbms-shell)
