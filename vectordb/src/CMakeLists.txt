add_subdirectory(binder)
add_subdirectory(buffer)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(concurrency)
add_subdirectory(container)
add_subdirectory(execution)
add_subdirectory(recovery)
add_subdirectory(storage)
add_subdirectory(type)
add_subdirectory(planner)
add_subdirectory(primer)
add_subdirectory(optimizer)

add_library(vdbms STATIC ${ALL_OBJECT_FILES})

set(vdbms_LIBS
        vdbms_binder
        vdbms_buffer
        vdbms_catalog
        vdbms_common
        vdbms_concurrency
        vdbms_execution
        vdbms_recovery
        vdbms_type
        vdbms_container_disk_hash
        vdbms_storage_disk
        vdbms_storage_index
        vdbms_storage_page
        vdbms_storage_table
        vdbms_planner
        vdbms_primer
        vdbms_optimizer
        )

find_package(Threads REQUIRED)

set(vdbms_THIRDPARTY_LIBS
        vdbms_murmur3
        duckdb_pg_query
        fmt
        libfort::fort
        Threads::Threads
        )

target_link_libraries(
        vdbms
        ${vdbms_LIBS}
        ${vdbms_THIRDPARTY_LIBS})

target_include_directories(
        vdbms PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
