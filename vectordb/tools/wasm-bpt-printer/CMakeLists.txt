set(WASM_SHELL_SOURCES wasm-bpt-printer.cpp)

if(EMSCRIPTEN)
    add_executable(wasm-bpt-printer ${WASM_SHELL_SOURCES})
    target_link_libraries(wasm-bpt-printer vdbms)
    set_target_properties(wasm-bpt-printer PROPERTIES OUTPUT_NAME vdbms-wasm-bpt-printer)
    target_link_options(wasm-bpt-printer PRIVATE -sEXPORTED_FUNCTIONS=['_vdbmsInit','_vdbmsApplyCommand','_free'] -sEXPORTED_RUNTIME_METHODS=['ccall','cwrap','allocateUTF8','UTF8ToString'])
endif()
