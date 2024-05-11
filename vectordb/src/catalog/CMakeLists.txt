add_library(
  vdbms_catalog
  OBJECT
  column.cpp
  table_generator.cpp
  schema.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_catalog>
    PARENT_SCOPE)
