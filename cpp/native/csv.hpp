#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <variant>
#include <vector>

namespace ome_zarr_c::native_code {

using CsvValue = std::variant<std::string, double, std::int64_t, bool>;

struct CsvColumnSpec {
    std::string name;
    std::string type;
};

using CsvProps = std::vector<std::pair<std::string, CsvValue>>;
using CsvPropsById = std::map<std::string, CsvProps>;

CsvValue parse_csv_value(const std::string& value, const std::string& col_type);

std::vector<CsvColumnSpec> parse_csv_key_specs(const std::string& csv_keys);

CsvPropsById csv_props_by_id(
    const std::vector<std::vector<std::string>>& rows,
    const std::string& csv_id,
    const std::vector<CsvColumnSpec>& specs);

std::vector<std::string> csv_label_paths(
    bool has_plate,
    bool has_multiscales,
    const std::string& zarr_path,
    const std::vector<std::string>& well_paths);

}  // namespace ome_zarr_c::native_code
