#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#  if defined(OME_ZARR_NATIVE_API_BUILD)
#    define OME_ZARR_NATIVE_API_EXPORT __declspec(dllexport)
#  else
#    define OME_ZARR_NATIVE_API_EXPORT __declspec(dllimport)
#  endif
#else
#  define OME_ZARR_NATIVE_API_EXPORT __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OmeZarrNativeApiResult {
    int ok;
    char* json;
    char* error_type;
    char* error_message;
} OmeZarrNativeApiResult;

typedef struct OmeZarrNativeApiU8ArrayResult {
    int ok;
    uint8_t* data;
    size_t data_len;
    size_t* shape;
    size_t ndim;
    char* error_type;
    char* error_message;
} OmeZarrNativeApiU8ArrayResult;

OME_ZARR_NATIVE_API_EXPORT const char* ome_zarr_native_api_abi_version(void);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiResult
ome_zarr_native_api_project_metadata(void);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiResult ome_zarr_native_api_call_json(
    const char* operation,
    const char* request_json);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiResult
ome_zarr_native_api_int_to_rgba(int32_t value);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiResult
ome_zarr_native_api_int_to_rgba_255(int32_t value);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiResult ome_zarr_native_api_rgba_to_int(
    uint8_t r,
    uint8_t g,
    uint8_t b,
    uint8_t a);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiResult
ome_zarr_native_api_parse_csv_value(const char* value, const char* col_type);

OME_ZARR_NATIVE_API_EXPORT OmeZarrNativeApiU8ArrayResult
ome_zarr_native_api_rgb_to_5d_u8(
    const uint8_t* data,
    size_t ndim,
    const size_t* shape);

OME_ZARR_NATIVE_API_EXPORT void
ome_zarr_native_api_free_result(OmeZarrNativeApiResult result);

OME_ZARR_NATIVE_API_EXPORT void
ome_zarr_native_api_free_u8_array_result(OmeZarrNativeApiU8ArrayResult result);

#ifdef __cplusplus
}
#endif
