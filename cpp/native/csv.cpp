#include "csv.hpp"

#include <cmath>
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

}  // namespace ome_zarr_c::native_code
