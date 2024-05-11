set(WASM_SHELL_SOURCES wasm-shell.cpp)

if(EMSCRIPTEN)
    add_executable(wasm-shell ${WASM_SHELL_SOURCES})
    target_link_libraries(wasm-shell vdbms)
    set_target_properties(wasm-shell PROPERTIES OUTPUT_NAME vdbms-wasm-shell)
    target_link_options(wasm-shell PRIVATE -sEXPORTED_FUNCTIONS=['_vdbmsInit','_vdbmsExecuteQuery','_free'] -sEXPORTED_RUNTIME_METHODS=['ccall','cwrap','allocateUTF8','UTF8ToString'])
endif()
