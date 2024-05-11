add_library(
  vdbms_container_disk_hash
  OBJECT
        disk_extendible_hash_table.cpp
        linear_probe_hash_table.cpp
        disk_extendible_hash_table_utils.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_container_disk_hash>
    PARENT_SCOPE)
