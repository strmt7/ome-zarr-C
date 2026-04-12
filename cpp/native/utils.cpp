#include "utils.hpp"

#include <algorithm>

namespace ome_zarr_c::native_code {

namespace {

std::pair<std::string, std::string> posix_split(const std::string& path) {
    const auto slash = path.find_last_of('/');
    const std::size_t split_index =
        slash == std::string::npos ? 0 : static_cast<std::size_t>(slash + 1);

    std::string head = path.substr(0, split_index);
    const std::string tail = path.substr(split_index);

    if (!head.empty()) {
        const bool all_separators =
            std::all_of(head.begin(), head.end(), [](char value) { return value == '/'; });
        if (!all_separators) {
            while (!head.empty() && head.back() == '/') {
                head.pop_back();
            }
        }
    }

    return {head, tail};
}

}  // namespace

std::string strip_common_prefix(std::vector<std::vector<std::string>>& parts) {
    if (parts.empty()) {
        throw std::runtime_error("No common prefix:\n");
    }

    std::size_t min_length = parts.front().size();
    for (const auto& part : parts) {
        min_length = std::min(min_length, part.size());
    }

    std::size_t first_mismatch = 0;
    for (std::size_t index = 0; index < min_length; ++index) {
        const std::string& candidate = parts.front()[index];
        const bool all_equal = std::all_of(
            parts.begin() + 1,
            parts.end(),
            [&](const auto& part) { return part[index] == candidate; });
        if (!all_equal) {
            break;
        }
        first_mismatch += 1;
    }

    if (first_mismatch == 0) {
        throw std::runtime_error("");
    }

    const std::string common = parts.front()[first_mismatch - 1];
    for (auto& part : parts) {
        part.erase(part.begin(), part.begin() + static_cast<std::ptrdiff_t>(first_mismatch - 1));
    }
    return common;
}

std::vector<std::string> splitall(const std::string& path) {
    std::vector<std::string> parts;
    std::string current = path;

    while (true) {
        const auto [head, tail] = posix_split(current);

        if (head == current) {
            parts.insert(parts.begin(), head);
            break;
        }
        if (tail == current) {
            parts.insert(parts.begin(), tail);
            break;
        }

        current = head;
        parts.insert(parts.begin(), tail);
    }

    return parts;
}

}  // namespace ome_zarr_c::native_code
