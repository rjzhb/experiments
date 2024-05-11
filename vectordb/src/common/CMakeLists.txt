add_library(
  vdbms_common
  OBJECT
  vdbms_instance.cpp
  vdbms_ddl.cpp
  config.cpp
  util/string_util.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_common>
  PARENT_SCOPE)
