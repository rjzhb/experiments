add_library(
    vdbms_storage_index
    OBJECT
    b_plus_tree_index.cpp
    b_plus_tree.cpp
    extendible_hash_table_index.cpp
    index_iterator.cpp
    linear_probe_hash_table_index.cpp
    ivfflat_index.cpp
    hnsw_index.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_storage_disk>
    PARENT_SCOPE)
