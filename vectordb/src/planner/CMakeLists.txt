add_library(
  vdbms_planner
  OBJECT
  expression_factory.cpp
  plan_aggregation.cpp
  plan_func_call.cpp
  plan_expression.cpp
  plan_insert.cpp
  plan_table_ref.cpp
  plan_select.cpp
  plan_window_function.cpp
  planner.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_planner>
  PARENT_SCOPE)
