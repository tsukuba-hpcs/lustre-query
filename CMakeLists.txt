cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME lustre_query)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

set(EXTENSION_SOURCES
    src/lustre_query_extension.cpp
    src/lustre_scan_state.cpp
    src/lustre_string_filter.cpp
    src/lustre_inodes.cpp
    src/lustre_inode_links.cpp
    src/lustre_inode_objects.cpp
    src/lustre_inode_layouts.cpp
    src/lustre_link_objects.cpp
    src/lustre_link_layouts.cpp
    src/lustre_object_layouts.cpp
    src/lustre_links.cpp
    src/lustre_layouts.cpp
    src/lustre_objects.cpp
    src/lustre_fid_filter.cpp
    src/lustre_fid2path.cpp
    src/lustre_path2fid.cpp
    src/lustre_optimizer.cpp
    src/lustre_filter.cpp
    src/lustre_table_filter_eval.cpp
    src/mdt_scanner.cpp
    src/oi_lookup.c
)

# Find libext2fs
find_path(EXT2FS_INCLUDE_DIR ext2fs/ext2fs.h
    PATHS /usr/include /usr/local/include
)

find_library(EXT2FS_LIBRARY ext2fs
    PATHS /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu
)

find_library(COM_ERR_LIBRARY com_err
    PATHS /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu
)

if(NOT EXT2FS_INCLUDE_DIR OR NOT EXT2FS_LIBRARY)
    message(FATAL_ERROR "libext2fs not found. Please install e2fsprogs development package.")
endif()

message(STATUS "Found ext2fs: ${EXT2FS_LIBRARY}")
message(STATUS "Found ext2fs include: ${EXT2FS_INCLUDE_DIR}")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_include_directories(${EXTENSION_NAME} PRIVATE ${EXT2FS_INCLUDE_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${EXT2FS_INCLUDE_DIR})

target_link_libraries(${EXTENSION_NAME} ${EXT2FS_LIBRARY} ${COM_ERR_LIBRARY})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${EXT2FS_LIBRARY} ${COM_ERR_LIBRARY})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
