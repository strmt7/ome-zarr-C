cmake_minimum_required(VERSION 4.3)

project(ome_zarr_c_native VERSION 0.0.1 LANGUAGES CXX)

option(OME_ZARR_C_NATIVE_ENABLE_LTO "Enable link-time optimization for native targets." OFF)
option(OME_ZARR_C_NATIVE_MARCH_NATIVE "Enable host-specific -march=native tuning for local native builds." OFF)
option(OME_ZARR_C_NATIVE_BUILD_BENCHMARKS "Build native benchmark tools." ON)
option(OME_ZARR_C_NATIVE_BUILD_TESTS "Build native self-tests." ON)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(OME_ZARR_C_NATIVE_REQUIRED_ZSTD_VERSION "1.5.7")
set(OME_ZARR_C_NATIVE_REQUIRED_BLOSC_VERSION "1.21.6")

function(ome_zarr_c_read_define_int header_path macro_name out_var)
    file(
        STRINGS
        "${header_path}"
        matched_line
        REGEX "^#define[ \t]+${macro_name}[ \t]+[0-9]+")
    if(NOT matched_line)
        message(FATAL_ERROR "Failed to read ${macro_name} from ${header_path}")
    endif()
    string(
        REGEX REPLACE
        "^#define[ \t]+${macro_name}[ \t]+([0-9]+).*"
        "\\1"
        matched_value
        "${matched_line}")
    set("${out_var}" "${matched_value}" PARENT_SCOPE)
endfunction()

function(ome_zarr_c_assert_header_version header_path version_prefix required_version)
    ome_zarr_c_read_define_int("${header_path}" "${version_prefix}_VERSION_MAJOR" version_major)
    ome_zarr_c_read_define_int("${header_path}" "${version_prefix}_VERSION_MINOR" version_minor)
    ome_zarr_c_read_define_int("${header_path}" "${version_prefix}_VERSION_RELEASE" version_patch)
    set(found_version "${version_major}.${version_minor}.${version_patch}")
    if(found_version VERSION_LESS required_version)
        message(
            FATAL_ERROR
            "${version_prefix} header at ${header_path} is ${found_version}, "
            "but ome-zarr-C native builds require ${required_version} or newer. "
            "Install the pinned latest native host toolchain before building.")
    endif()
endfunction()

set(OME_ZARR_C_NATIVE_SOURCES
    cpp/native/axes.cpp
    cpp/native/cli.cpp
    cpp/native/conversions.cpp
    cpp/native/create_assets.cpp
    cpp/native/create_runtime.cpp
    cpp/native/csv.cpp
    cpp/native/dask_utils.cpp
    cpp/native/data.cpp
    cpp/native/format.cpp
    cpp/native/io.cpp
    cpp/native/local_runtime.cpp
    cpp/native/python_random.cpp
    cpp/native/reader.cpp
    cpp/native/reader_oracle.cpp
    cpp/native/scale.cpp
    cpp/native/scale_runtime.cpp
    cpp/native/utils.cpp
    cpp/native/writer.cpp
    third_party/tinyxml2/tinyxml2.cpp
)

add_library(ome_zarr_native STATIC ${OME_ZARR_C_NATIVE_SOURCES})
target_include_directories(ome_zarr_native PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpp/native)
target_compile_definitions(
    ome_zarr_native
    PUBLIC
        OME_ZARR_C_SOURCE_ROOT="${CMAKE_CURRENT_SOURCE_DIR}"
        OME_ZARR_C_NATIVE_PROJECT_VERSION="${PROJECT_VERSION}")

find_path(BLOSC_INCLUDE_DIR NAMES blosc.h REQUIRED)
find_library(BLOSC_LIBRARY NAMES blosc REQUIRED)
find_path(ZSTD_INCLUDE_DIR NAMES zstd.h REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd REQUIRED)

ome_zarr_c_assert_header_version(
    "${BLOSC_INCLUDE_DIR}/blosc.h"
    "BLOSC"
    "${OME_ZARR_C_NATIVE_REQUIRED_BLOSC_VERSION}")
ome_zarr_c_assert_header_version(
    "${ZSTD_INCLUDE_DIR}/zstd.h"
    "ZSTD"
    "${OME_ZARR_C_NATIVE_REQUIRED_ZSTD_VERSION}")

target_include_directories(
    ome_zarr_native
    PUBLIC
        ${BLOSC_INCLUDE_DIR}
        ${ZSTD_INCLUDE_DIR})
target_link_libraries(ome_zarr_native PUBLIC ${BLOSC_LIBRARY} ${ZSTD_LIBRARY})

add_library(
    ome_zarr_native_api
    SHARED
    cpp/api/ome_zarr_native_api.cpp
)
target_link_libraries(ome_zarr_native_api PRIVATE ome_zarr_native)
target_include_directories(
    ome_zarr_native_api
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/cpp/api)
target_compile_definitions(
    ome_zarr_native_api
    PRIVATE
        OME_ZARR_NATIVE_API_BUILD)

if(MSVC)
    target_compile_options(ome_zarr_native PRIVATE /O2 /permissive-)
else()
    target_compile_options(ome_zarr_native PRIVATE -O3 -fvisibility=hidden)
    if(OME_ZARR_C_NATIVE_MARCH_NATIVE)
        target_compile_options(ome_zarr_native PRIVATE -march=native)
    endif()
endif()

if(OME_ZARR_C_NATIVE_ENABLE_LTO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT ipo_supported OUTPUT ipo_error)
    if(NOT ipo_supported)
        message(FATAL_ERROR "LTO requested but not supported: ${ipo_error}")
    endif()
    set_property(TARGET ome_zarr_native PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    set_property(TARGET ome_zarr_native_api PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

if(OME_ZARR_C_NATIVE_BUILD_BENCHMARKS)
    add_executable(
        ome_zarr_native_bench_format
        cpp/tools/native_bench_format.cpp
    )
    target_link_libraries(ome_zarr_native_bench_format PRIVATE ome_zarr_native)

    add_executable(
        ome_zarr_native_bench_core
        cpp/tools/native_bench_core.cpp
    )
    target_link_libraries(ome_zarr_native_bench_core PRIVATE ome_zarr_native)
endif()

if(OME_ZARR_C_NATIVE_BUILD_TESTS)
    enable_testing()
    add_executable(
        ome_zarr_native_selftest
        cpp/tools/native_selftest.cpp
    )
    target_link_libraries(ome_zarr_native_selftest PRIVATE ome_zarr_native)
    add_test(NAME ome_zarr_native_selftest COMMAND ome_zarr_native_selftest)

    add_executable(
        ome_zarr_native_api_selftest
        cpp/tools/native_api_selftest.cpp
    )
    target_link_libraries(ome_zarr_native_api_selftest PRIVATE ome_zarr_native_api)
    add_test(NAME ome_zarr_native_api_selftest COMMAND ome_zarr_native_api_selftest)
endif()

add_executable(
    ome_zarr_native_cli
    cpp/tools/native_cli.cpp
)
target_link_libraries(ome_zarr_native_cli PRIVATE ome_zarr_native)

add_executable(
    ome_zarr_native_probe
    cpp/tools/native_oracle_probe.cpp
)
target_link_libraries(ome_zarr_native_probe PRIVATE ome_zarr_native)

if(OME_ZARR_C_NATIVE_BUILD_TESTS)
    add_test(
        NAME ome_zarr_native_cli_help
        COMMAND ome_zarr_native_cli --help
    )
    add_test(
        NAME ome_zarr_native_probe_help
        COMMAND ome_zarr_native_probe --help
    )
endif()
