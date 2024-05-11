add_library(
    vdbms_storage_disk
    OBJECT
    disk_manager.cpp
    disk_manager_memory.cpp
    disk_scheduler.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_storage_disk>
    PARENT_SCOPE)
