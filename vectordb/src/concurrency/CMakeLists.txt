add_library(
  vdbms_concurrency
  OBJECT
  transaction_manager.cpp
  transaction_manager_impl.cpp
  watermark.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_concurrency>
  PARENT_SCOPE)
