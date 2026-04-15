cmake_minimum_required(VERSION 3.20)

project(ome_zarr_c_native LANGUAGES CXX)

option(OME_ZARR_C_NATIVE_ENABLE_LTO "Enable link-time optimization for native targets." OFF)
option(OME_ZARR_C_NATIVE_MARCH_NATIVE "Enable host-specific -march=native tuning for local native builds." OFF)
option(OME_ZARR_C_NATIVE_BUILD_BENCHMARKS "Build native benchmark tools." ON)
option(OME_ZARR_C_NATIVE_BUILD_TESTS "Build native self-tests." ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(OME_ZARR_C_NATIVE_SOURCES
    cpp/native/axes.cpp
    cpp/native/cli.cpp
    cpp/native/conversions.cpp
    cpp/native/csv.cpp
    cpp/native/dask_utils.cpp
    cpp/native/data.cpp
    cpp/native/format.cpp
    cpp/native/io.cpp
    cpp/native/reader.cpp
    cpp/native/scale.cpp
    cpp/native/utils.cpp
    cpp/native/writer.cpp
)

add_library(ome_zarr_native STATIC ${OME_ZARR_C_NATIVE_SOURCES})
target_include_directories(ome_zarr_native PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cpp/native)

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
endif()

add_executable(
    ome_zarr_native_cli
    cpp/tools/native_cli.cpp
)
target_link_libraries(ome_zarr_native_cli PRIVATE ome_zarr_native)

if(OME_ZARR_C_NATIVE_BUILD_TESTS)
    add_test(
        NAME ome_zarr_native_cli_help
        COMMAND ome_zarr_native_cli --help
    )
    add_test(
        NAME ome_zarr_native_cli_format_matches
        COMMAND ome_zarr_native_cli format matches --version 0.5 --multiscales-version 0.5
    )
    add_test(
        NAME ome_zarr_native_cli_create_plan
        COMMAND ome_zarr_native_cli cli create-plan --method astronaut
    )
    add_test(
        NAME ome_zarr_native_cli_scale_factors
        COMMAND ome_zarr_native_cli cli scale-factors --downscale 2 --max-layer 4
    )
    add_test(
        NAME ome_zarr_native_cli_generate_well
        COMMAND ome_zarr_native_cli format generate-well --path B/3 --rows A,B,C --columns 1,2,3
    )
    add_test(
        NAME ome_zarr_native_cli_view_plan
        COMMAND ome_zarr_native_cli utils view-plan --path /tmp/demo/image.zarr --port 8013 --discovered-count 1
    )
    add_test(
        NAME ome_zarr_native_cli_create_zarr_plan
        COMMAND ome_zarr_native_cli data create-plan --version 0.5 --base-shape 1,3,512,512 --smallest-shape 1,3,64,64
    )
    add_test(
        NAME ome_zarr_native_cli_io_subpath
        COMMAND ome_zarr_native_cli io subpath --path /tmp/demo.zarr --subpath labels/0 --file
    )
    add_test(
        NAME ome_zarr_native_cli_finder_plan
        COMMAND ome_zarr_native_cli utils finder-plan --path /tmp/demo/images --port 8012
    )
    add_test(
        NAME ome_zarr_native_cli_writer_image_plan
        COMMAND ome_zarr_native_cli writer image-plan --axes t,c,y,x --scaler-present --scaler-max-layer 4 --scaler-method local_mean
    )
endif()
