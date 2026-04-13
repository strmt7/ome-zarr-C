#include "csv.hpp"

#include <cmath>
#include <sstream>
#include <stdexcept>

namespace ome_zarr_c::native_code {

CsvValue parse_csv_value(const std::string& value, const std::string& col_type) {
    try {
        if (col_type == "d") {
            return std::stod(value);
        }
        if (col_type == "l") {
            const double parsed = std::stod(value);
            if (std::isnan(parsed)) {
                return value;
            }
            if (std::isinf(parsed)) {
                throw std::overflow_error("cannot convert float infinity to integer");
            }
            return static_cast<std::int64_t>(std::nearbyint(parsed));
        }
        if (col_type == "b") {
            return !value.empty();
        }
    } catch (const std::invalid_argument&) {
        return value;
    } catch (const std::out_of_range&) {
        return value;
    }

    return value;
}

std::vector<CsvColumnSpec> parse_csv_key_specs(const std::string& csv_keys) {
    std::vector<CsvColumnSpec> specs;
    std::stringstream stream(csv_keys);
    std::string item;
    while (std::getline(stream, item, ',')) {
        const std::size_t split_at = item.rfind('#');
        if (split_at == std::string::npos) {
            specs.push_back(CsvColumnSpec{item, "s"});
            continue;
        }

        std::string type = item.substr(split_at + 1);
        if (type != "d" && type != "l" && type != "s" && type != "b") {
            type = "s";
        }
        specs.push_back(CsvColumnSpec{item.substr(0, split_at), type});
    }
    return specs;
}

CsvPropsById csv_props_by_id(
    const std::vector<std::vector<std::string>>& rows,
    const std::string& csv_id,
    const std::vector<CsvColumnSpec>& specs) {
    CsvPropsById props_by_id;
    if (rows.empty()) {
        return props_by_id;
    }

    const auto& header = rows.front();
    std::ptrdiff_t id_column = -1;
    for (std::size_t index = 0; index < header.size(); ++index) {
        if (header[index] == csv_id) {
            id_column = static_cast<std::ptrdiff_t>(index);
            break;
        }
    }
    if (id_column < 0) {
        throw std::invalid_argument("missing csv id column");
    }

    for (std::size_t row_index = 1; row_index < rows.size(); ++row_index) {
        const auto& row = rows[row_index];
        if (static_cast<std::size_t>(id_column) >= row.size()) {
            throw std::out_of_range("list index out of range");
        }

        CsvProps row_props;
        for (const auto& spec : specs) {
            for (std::size_t column_index = 0; column_index < header.size(); ++column_index) {
                if (header[column_index] != spec.name) {
                    continue;
                }
                if (column_index >= row.size()) {
                    break;
                }
                row_props.emplace_back(
                    spec.name,
                    parse_csv_value(row[column_index], spec.type));
                break;
            }
        }
        props_by_id[row[static_cast<std::size_t>(id_column)]] = std::move(row_props);
    }

    return props_by_id;
}

std::vector<std::string> csv_label_paths(
    bool has_plate,
    bool has_multiscales,
    const std::string& zarr_path,
    const std::vector<std::string>& well_paths) {
    if (!has_plate && !has_multiscales) {
        throw std::runtime_error("zarr_path must be to plate.zarr or image.zarr");
    }

    std::vector<std::string> paths;
    if (has_plate) {
        for (const auto& well_path : well_paths) {
            paths.push_back(zarr_path + "/" + well_path + "/0/labels/0");
        }
        return paths;
    }

    paths.push_back(zarr_path + "/labels/0");
    return paths;
}

}  // namespace ome_zarr_c::native_code
