cmake_minimum_required(VERSION 3.20)
project(MyDB VERSION 1.0 LANGUAGES CXX)

# ============================================================================
# C++ Standard Configuration
# ============================================================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============================================================================
# Build Options
# ============================================================================
option(MYDB_BUILD_TESTS "Build unit tests" ON)
option(MYDB_BUILD_TOOLS "Build CLI tools" ON)
option(MYDB_ENABLE_PYTHON "Enable Python scripting support" ON)

# ============================================================================
# Platform Detection
# ============================================================================
if(UNIX AND NOT APPLE)
    set(MYDB_LINUX TRUE)
    message(STATUS "Building for Linux - io_uring support enabled")
else()
    set(MYDB_LINUX FALSE)
    message(STATUS "Building for non-Linux - using thread-pool I/O fallback")
endif()

# ============================================================================
# Compiler Warnings
# ============================================================================
if(MSVC)
    add_compile_options(/W4 /WX- /permissive-)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ============================================================================
# External Dependencies (FetchContent)
# ============================================================================
include(FetchContent)

# spdlog - High-performance logging
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.12.0
)

# GoogleTest - Unit testing
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)

# pybind11 - Python embedding (optional)
if(MYDB_ENABLE_PYTHON)
    FetchContent_Declare(
        pybind11
        GIT_REPOSITORY https://github.com/pybind/pybind11.git
        GIT_TAG        v2.11.1
    )
endif()

# replxx - Modern readline alternative
FetchContent_Declare(
    replxx
    GIT_REPOSITORY https://github.com/AmokHuginnsson/replxx.git
    GIT_TAG        release-0.0.4
)

# FTXUI - Terminal UI
FetchContent_Declare(
    ftxui
    GIT_REPOSITORY https://github.com/ArthurSonzogni/FTXUI.git
    GIT_TAG        v5.0.0
)

# Fetch dependencies
FetchContent_MakeAvailable(spdlog)

if(MYDB_BUILD_TESTS)
    FetchContent_MakeAvailable(googletest)
endif()

if(MYDB_ENABLE_PYTHON)
    find_package(Python3 COMPONENTS Interpreter Development QUIET)
    if(Python3_FOUND)
        FetchContent_MakeAvailable(pybind11)
        message(STATUS "Python ${Python3_VERSION} found - scripting enabled")
    else()
        message(WARNING "Python not found - disabling scripting support")
        set(MYDB_ENABLE_PYTHON OFF)
    endif()
endif()

FetchContent_MakeAvailable(replxx)
FetchContent_MakeAvailable(ftxui)

# ============================================================================
# liburing (Linux only)
# ============================================================================
if(MYDB_LINUX)
    find_library(LIBURING_LIBRARY NAMES uring)
    if(LIBURING_LIBRARY)
        message(STATUS "Found liburing: ${LIBURING_LIBRARY}")
    else()
        message(WARNING "liburing not found - using fallback I/O")
        set(MYDB_LINUX FALSE)
    endif()
endif()

# ============================================================================
# MyDB Core Library
# ============================================================================
add_library(mydb_core STATIC
    src/db.cpp
    src/engine/memtable.cpp
    src/engine/skiplist.cpp
    src/engine/wal.cpp
    src/engine/sstable_builder.cpp
    src/engine/sstable_reader.cpp
    src/engine/bloom_filter.cpp
    src/engine/compactor.cpp
    src/network/server.cpp
    src/network/protocol.cpp
    src/network/io_context.cpp
)

target_include_directories(mydb_core 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(mydb_core 
    PUBLIC 
        spdlog::spdlog
)

# Platform-specific configuration
if(MYDB_LINUX AND LIBURING_LIBRARY)
    target_compile_definitions(mydb_core PUBLIC MYDB_USE_IO_URING)
    target_link_libraries(mydb_core PUBLIC ${LIBURING_LIBRARY})
endif()

# Python scripting support
if(MYDB_ENABLE_PYTHON)
    target_sources(mydb_core PRIVATE src/scripting/python_vm.cpp)
    target_link_libraries(mydb_core PUBLIC pybind11::embed)
    target_compile_definitions(mydb_core PUBLIC MYDB_ENABLE_PYTHON)
endif()

# ============================================================================
# MyDB Server Executable
# ============================================================================
add_executable(mydb_server src/main.cpp)
target_link_libraries(mydb_server PRIVATE mydb_core)

# ============================================================================
# CLI Tools
# ============================================================================
if(MYDB_BUILD_TOOLS)
    add_executable(mydb_cli tools/mydb-cli.cpp)
    target_link_libraries(mydb_cli PRIVATE mydb_core)
endif()

# ============================================================================
# MyDB Shell (Standard Interactive Shell)
# ============================================================================
add_executable(mydb_shell 
    src/cli/shell.cpp
    src/cli/shell_main.cpp
)

target_link_libraries(mydb_shell 
    PRIVATE 
        mydb_core 
        replxx
)

# ============================================================================
# TUI Dashboard
# ============================================================================
add_executable(mydb_dashboard
    src/tools/dashboard.cpp
)

target_link_libraries(mydb_dashboard
    PRIVATE
        mydb_core
        ftxui::screen
        ftxui::dom
        ftxui::component
)

# ============================================================================
# Unit Tests
# ============================================================================
if(MYDB_BUILD_TESTS)
    enable_testing()
    
    add_executable(mydb_tests
        tests/test_skiplist.cpp
        tests/test_wal_recovery.cpp
        tests/test_sstable.cpp
        tests/test_bloom_filter.cpp
    )
    
    target_link_libraries(mydb_tests 
        PRIVATE 
            mydb_core
            GTest::gtest_main
    )
    
    include(GoogleTest)
    gtest_discover_tests(mydb_tests)
endif()
