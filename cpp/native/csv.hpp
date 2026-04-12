#pragma once

#include <cstdint>
#include <string>
#include <variant>

namespace ome_zarr_c::native_code {

using CsvValue = std::variant<std::string, double, std::int64_t, bool>;

CsvValue parse_csv_value(const std::string& value, const std::string& col_type);

}  // namespace ome_zarr_c::native_code
