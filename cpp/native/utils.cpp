#include "utils.hpp"

#include <algorithm>
#include <sstream>

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

std::pair<std::string, std::string> split_head_tail(const std::string& path) {
    if (path.empty()) {
        return {"", ""};
    }

    auto split_index = path.find_last_of('/');
    if (split_index == std::string::npos) {
        return {"", path};
    }
    return {
        path.substr(0, split_index),
        path.substr(split_index + 1),
    };
}

std::pair<std::string, std::string> split_like_os_path(const std::string& path) {
    if (path.empty()) {
        return {"", ""};
    }
    if (path == "/") {
        return {"/", ""};
    }
    if (path.back() == '/') {
        return {path.substr(0, path.size() - 1), ""};
    }
    return split_head_tail(path);
}

std::string join_path(const std::string& left, const std::string& right) {
    if (left.empty()) {
        return right;
    }
    if (!left.empty() && left.back() == '/') {
        return left + right;
    }
    return left + "/" + right;
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

std::string utils_missing_metadata_message() {
    return "No .zattrs or zarr.json found in {path_to_zattrs}";
}

std::vector<UtilsDiscoveredImage> utils_plate_images(
    const std::string& path_to_zattrs,
    const std::string& basename,
    const std::string& dirname,
    const std::vector<std::string>& wells) {
    if (wells.empty()) {
        return {};
    }
    return {
        UtilsDiscoveredImage{
            join_path(join_path(path_to_zattrs, wells.front()), "0"),
            basename,
            dirname,
        }
    };
}

std::vector<UtilsDiscoveredImage> utils_bioformats_images(
    const std::string& path_to_zattrs,
    const std::string& basename,
    const std::string& dirname,
    const std::vector<std::optional<std::string>>& image_names) {
    std::vector<UtilsDiscoveredImage> images;
    images.reserve(image_names.size());
    for (std::size_t index = 0; index < image_names.size(); ++index) {
        const auto& maybe_name = image_names[index];
        images.push_back(
            UtilsDiscoveredImage{
                join_path(path_to_zattrs, std::to_string(index)),
                maybe_name.value_or(basename + " Series:" + std::to_string(index)),
                dirname,
            });
    }
    return images;
}

std::vector<UtilsDiscoveredImage> utils_single_multiscales_image(
    const std::string& path_to_zattrs,
    const std::string& basename,
    const std::string& dirname) {
    return {UtilsDiscoveredImage{path_to_zattrs, basename, dirname}};
}

std::string utils_info_not_ome_zarr_line(const std::string& node_repr) {
    return "not an ome-zarr node: " + node_repr;
}

std::vector<std::string> utils_info_header_lines(
    const std::string& node_repr,
    const std::string& version,
    const std::vector<std::string>& spec_names) {
    std::vector<std::string> lines;
    lines.reserve(spec_names.size() + 4);
    lines.push_back(node_repr);
    lines.push_back(" - version: " + version);
    lines.push_back(" - metadata");
    for (const auto& spec_name : spec_names) {
        lines.push_back("   - " + spec_name);
    }
    lines.push_back(" - data");
    return lines;
}

std::string utils_info_data_line(
    const std::string& shape_repr,
    const std::optional<std::string>& minmax_repr) {
    std::string line = "   - " + shape_repr;
    if (minmax_repr.has_value()) {
        line += " minmax=" + minmax_repr.value();
    }
    return line;
}

UtilsDownloadPlan utils_download_plan(std::vector<std::vector<std::string>> parts) {
    return UtilsDownloadPlan{strip_common_prefix(parts), std::move(parts)};
}

UtilsDownloadNodePlan utils_download_node_plan(int zarr_format, bool has_axes) {
    UtilsDownloadNodePlan plan{};
    plan.wrap_ome_metadata = zarr_format == 3;
    plan.use_v2_chunk_key_encoding = zarr_format == 2;
    plan.use_dimension_names = zarr_format != 2 && has_axes;
    return plan;
}

UtilsViewPlan utils_view_plan(
    const std::string& input_path,
    int port,
    bool force,
    std::size_t discovered_count) {
    UtilsViewPlan plan{};
    if (!force && discovered_count == 0) {
        plan.should_warn = true;
        plan.warning_message =
            "No OME-Zarr images found in " + input_path + ". "
            "Try $ ome_zarr finder " + input_path +
            " or use -f to force open in browser.";
        return plan;
    }

    auto [parent_dir, image_name] = split_like_os_path(input_path);
    if (image_name.empty()) {
        const auto split_parent = split_like_os_path(parent_dir);
        parent_dir = split_parent.first;
        image_name = split_parent.second;
    }
    plan.parent_dir = parent_dir;
    plan.image_name = image_name;
    plan.url =
        "https://ome.github.io/ome-ngff-validator/?source=http://localhost:" +
        std::to_string(port) + "/" + image_name;
    return plan;
}

UtilsFinderPlan utils_finder_plan(const std::string& input_path, int port) {
    auto [parent_path, server_dir] = split_like_os_path(input_path);
    if (server_dir.empty()) {
        const auto split_parent = split_like_os_path(parent_path);
        parent_path = split_parent.first;
        server_dir = split_parent.second;
    }

    const std::string csv_path = join_path(input_path, "biofile_finder.csv");
    const std::string source_uri =
        "http://localhost:" + std::to_string(port) + "/" + server_dir +
        "/biofile_finder.csv";

    return UtilsFinderPlan{
        parent_path,
        server_dir,
        csv_path,
        source_uri,
        "https://bff.allencell.org/app",
    };
}

UtilsFinderRow utils_finder_row(
    int port,
    const std::string& server_dir,
    const std::string& relpath,
    const std::string& name,
    const std::string& folders_path,
    const std::string& uploaded) {
    std::string rel_url;
    const auto rel_parts = splitall(relpath);
    for (std::size_t index = 0; index < rel_parts.size(); ++index) {
        if (index > 0) {
            rel_url += "/";
        }
        rel_url += rel_parts[index];
    }

    std::string folders;
    const auto folder_parts = splitall(folders_path);
    for (std::size_t index = 0; index < folder_parts.size(); ++index) {
        if (index > 0) {
            folders += ",";
        }
        folders += folder_parts[index];
    }

    return UtilsFinderRow{
        "http://localhost:" + std::to_string(port) + "/" + server_dir + "/" + rel_url,
        name,
        folders,
        uploaded,
    };
}

}  // namespace ome_zarr_c::native_code
