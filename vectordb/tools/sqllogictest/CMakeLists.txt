set(SQLLOGICTEST_SOURCES sqllogictest.cpp parser.cpp)
add_executable(sqllogictest ${SQLLOGICTEST_SOURCES})

target_link_libraries(sqllogictest vdbms)
set_target_properties(sqllogictest PROPERTIES OUTPUT_NAME vdbms-sqllogictest)
