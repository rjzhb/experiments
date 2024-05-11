add_library(
  vdbms_primer
  OBJECT
  orset.cpp
  orset_driver.cpp
  trie.cpp
  trie_store.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:vdbms_primer>
  PARENT_SCOPE)
