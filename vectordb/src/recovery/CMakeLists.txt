add_library(
  vdbms_recovery
  OBJECT
  log_manager.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_recovery>
  PARENT_SCOPE)
