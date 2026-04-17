#include "reader_oracle.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "reader.hpp"

namespace ome_zarr_c::native_code {

namespace {

using ordered_json = nlohmann::ordered_json;

enum class ProbeSpecKind {
    labels,
    label,
    multiscales,
    omero,
    plate,
    well,
};

std::string spec_name(const ProbeSpecKind kind) {
    switch (kind) {
        case ProbeSpecKind::labels:
            return "Labels";
        case ProbeSpecKind::label:
            return "Label";
        case ProbeSpecKind::multiscales:
            return "Multiscales";
        case ProbeSpecKind::omero:
            return "OMERO";
        case ProbeSpecKind::plate:
            return "Plate";
        case ProbeSpecKind::well:
            return "Well";
    }
    throw std::runtime_error("Unknown spec kind");
}

ProbeSpecKind spec_kind_from_name(const std::string& name) {
    if (name == "Labels") {
        return ProbeSpecKind::labels;
    }
    if (name == "Label") {
        return ProbeSpecKind::label;
    }
    if (name == "Multiscales") {
        return ProbeSpecKind::multiscales;
    }
    if (name == "OMERO") {
        return ProbeSpecKind::omero;
    }
    if (name == "Plate") {
        return ProbeSpecKind::plate;
    }
    if (name == "Well") {
        return ProbeSpecKind::well;
    }
    throw std::runtime_error("Unknown spec name: " + name);
}

std::string normalize_path(std::string_view path) {
    std::vector<std::string> parts;
    bool absolute = false;
    if (!path.empty() && path.front() == '/') {
        absolute = true;
    }

    std::string token;
    for (const char raw_char : path) {
        const char ch = raw_char == '\\' ? '/' : raw_char;
        if (ch == '/') {
            if (!token.empty()) {
                if (token == "..") {
                    if (!parts.empty()) {
                        parts.pop_back();
                    }
                } else if (token != ".") {
                    parts.push_back(token);
                }
                token.clear();
            }
            continue;
        }
        token.push_back(ch);
    }
    if (!token.empty()) {
        if (token == "..") {
            if (!parts.empty()) {
                parts.pop_back();
            }
        } else if (token != ".") {
            parts.push_back(token);
        }
    }

    std::string normalized = absolute ? "/" : "";
    for (std::size_t index = 0; index < parts.size(); ++index) {
        if (index != 0U) {
            normalized += "/";
        }
        normalized += parts[index];
    }
    if (absolute && normalized == "/") {
        return normalized;
    }
    if (normalized.empty()) {
        return absolute ? "/" : ".";
    }
    return normalized;
}

std::string join_path(const std::string& left, const std::string& right) {
    if (right.empty()) {
        return normalize_path(left);
    }
    if (!right.empty() && right.front() == '/') {
        return normalize_path(right);
    }
    if (left == "/") {
        return normalize_path("/" + right);
    }
    return normalize_path(left + "/" + right);
}

std::string basename_path(const std::string& path) {
    const auto normalized = normalize_path(path);
    if (normalized == "/") {
        return "/";
    }
    const auto pos = normalized.find_last_of('/');
    if (pos == std::string::npos) {
        return normalized;
    }
    return normalized.substr(pos + 1U);
}

bool json_contains(const ordered_json& value, const char* key) {
    return value.is_object() && value.contains(key);
}

const ordered_json& json_at_or_empty(const ordered_json& value, const char* key) {
    static const ordered_json empty_object = ordered_json::object();
    if (!value.is_object()) {
        return empty_object;
    }
    const auto iter = value.find(key);
    if (iter == value.end()) {
        return empty_object;
    }
    return *iter;
}

struct ProbeArray {
    std::vector<std::size_t> shape;
    std::vector<std::vector<std::size_t>> chunks;
    std::string dtype;
    std::vector<std::int64_t> values;
};

std::size_t shape_product(const std::vector<std::size_t>& shape) {
    return std::accumulate(
        shape.begin(),
        shape.end(),
        std::size_t{1},
        [](const std::size_t left, const std::size_t right) { return left * right; });
}

std::vector<std::size_t> row_major_strides(const std::vector<std::size_t>& shape) {
    std::vector<std::size_t> strides(shape.size(), 1U);
    if (shape.empty()) {
        return strides;
    }
    for (std::size_t index = shape.size(); index-- > 1U;) {
        strides[index - 1U] = strides[index] * shape[index];
    }
    return strides;
}

std::size_t linear_index(
    const std::vector<std::size_t>& coords,
    const std::vector<std::size_t>& strides) {
    std::size_t index = 0U;
    for (std::size_t axis = 0; axis < coords.size(); ++axis) {
        index += coords[axis] * strides[axis];
    }
    return index;
}

void iterate_coords(
    const std::vector<std::size_t>& shape,
    const std::function<void(const std::vector<std::size_t>&)>& visitor) {
    if (shape.empty()) {
        visitor({});
        return;
    }
    std::vector<std::size_t> coords(shape.size(), 0U);
    while (true) {
        visitor(coords);
        std::size_t axis = shape.size();
        while (axis != 0U) {
            --axis;
            ++coords[axis];
            if (coords[axis] < shape[axis]) {
                break;
            }
            coords[axis] = 0U;
            if (axis == 0U) {
                return;
            }
        }
    }
}

ordered_json nested_values_json(
    const std::vector<std::int64_t>& flat,
    const std::vector<std::size_t>& shape,
    std::size_t offset = 0U,
    std::size_t axis = 0U) {
    if (axis >= shape.size()) {
        return flat[offset];
    }
    const auto strides = row_major_strides(shape);
    const auto step = strides[axis];
    ordered_json result = ordered_json::array();
    for (std::size_t index = 0; index < shape[axis]; ++index) {
        result.push_back(nested_values_json(flat, shape, offset + (index * step), axis + 1U));
    }
    return result;
}

ordered_json array_signature(const ProbeArray& array) {
    ordered_json chunks_json = ordered_json::array();
    for (const auto& axis_chunks : array.chunks) {
        ordered_json axis = ordered_json::array();
        for (const auto chunk : axis_chunks) {
            axis.push_back(chunk);
        }
        chunks_json.push_back(std::move(axis));
    }

    ordered_json shape_json = ordered_json::array();
    for (const auto dim : array.shape) {
        shape_json.push_back(dim);
    }

    return ordered_json{
        {"shape", std::move(shape_json)},
        {"dtype", array.dtype},
        {"chunks", std::move(chunks_json)},
        {"values", nested_values_json(array.values, array.shape)},
    };
}

ProbeArray zeros_array(const std::vector<std::size_t>& shape, const std::string& dtype) {
    ProbeArray array{};
    array.shape = shape;
    array.dtype = dtype;
    array.values.assign(shape_product(shape), 0);
    array.chunks.reserve(shape.size());
    for (const auto dim : shape) {
        array.chunks.push_back({dim});
    }
    return array;
}

ProbeArray concatenate_arrays(const std::vector<ProbeArray>& arrays, const std::size_t axis) {
    if (arrays.empty()) {
        throw std::runtime_error("cannot concatenate zero arrays");
    }
    const auto rank = arrays.front().shape.size();
    if (axis >= rank) {
        throw std::runtime_error("concatenate axis out of range");
    }

    ProbeArray result{};
    result.shape = arrays.front().shape;
    result.dtype = arrays.front().dtype;
    result.chunks = arrays.front().chunks;
    result.shape[axis] = 0U;
    result.chunks[axis].clear();

    for (const auto& array : arrays) {
        if (array.shape.size() != rank || array.dtype != result.dtype) {
            throw std::runtime_error("concatenate requires matching rank and dtype");
        }
        for (std::size_t check_axis = 0; check_axis < rank; ++check_axis) {
            if (check_axis == axis) {
                continue;
            }
            if (array.shape[check_axis] != result.shape[check_axis]) {
                throw std::runtime_error("concatenate requires matching non-axis shapes");
            }
        }
        result.shape[axis] += array.shape[axis];
        result.chunks[axis].insert(
            result.chunks[axis].end(),
            array.chunks[axis].begin(),
            array.chunks[axis].end());
    }

    result.values.assign(shape_product(result.shape), 0);
    const auto result_strides = row_major_strides(result.shape);

    std::vector<std::size_t> axis_offsets;
    axis_offsets.reserve(arrays.size());
    std::size_t running_offset = 0U;
    for (const auto& array : arrays) {
        axis_offsets.push_back(running_offset);
        running_offset += array.shape[axis];
    }

    iterate_coords(result.shape, [&](const std::vector<std::size_t>& coords) {
        std::size_t array_index = 0U;
        while (
            array_index + 1U < arrays.size() &&
            coords[axis] >= axis_offsets[array_index + 1U]) {
            ++array_index;
        }
        const auto& source = arrays[array_index];
        auto source_coords = coords;
        source_coords[axis] -= axis_offsets[array_index];
        const auto source_index = linear_index(source_coords, row_major_strides(source.shape));
        const auto target_index = linear_index(coords, result_strides);
        result.values[target_index] = source.values[source_index];
    });

    return result;
}

struct ProbeStoreNode {
    std::string path;
    ordered_json root_attrs = ordered_json::object();
    ordered_json zgroup = ordered_json::object();
    ordered_json zarray = ordered_json::object();
    bool exists = true;
};

struct ProbeHierarchy {
    std::unordered_map<std::string, ProbeStoreNode> nodes;
    std::unordered_map<std::string, ProbeArray> arrays;

    ProbeStoreNode create_child(const ProbeStoreNode& parent, const std::string& child) const {
        const auto full_path = join_path(parent.path, child);
        const auto iter = nodes.find(full_path);
        if (iter != nodes.end()) {
            return iter->second;
        }
        ProbeStoreNode missing{};
        missing.path = full_path;
        missing.exists = false;
        return missing;
    }

    const ProbeArray& load(const ProbeStoreNode& node, const std::string& subpath = "") const {
        const auto full_path = subpath.empty() ? node.path : join_path(node.path, subpath);
        const auto iter = arrays.find(full_path);
        if (iter == arrays.end()) {
            throw std::runtime_error(full_path);
        }
        return iter->second;
    }
};

ProbeArray make_array(
    const std::vector<std::size_t>& shape,
    const std::vector<std::vector<std::size_t>>& chunks,
    const std::string& dtype,
    const std::vector<std::int64_t>& values) {
    ProbeArray array{};
    array.shape = shape;
    array.chunks = chunks;
    array.dtype = dtype;
    array.values = values;
    return array;
}

struct Scenario {
    ProbeHierarchy hierarchy;
    std::string root_path;
};

ProbeStoreNode& add_node(
    ProbeHierarchy& hierarchy,
    const std::string& path,
    ordered_json root_attrs = ordered_json::object(),
    ordered_json zgroup = ordered_json::object(),
    ordered_json zarray = ordered_json::object()) {
    ProbeStoreNode node{};
    node.path = normalize_path(path);
    node.root_attrs = std::move(root_attrs);
    node.zgroup = std::move(zgroup);
    node.zarray = std::move(zarray);
    auto [iter, _inserted] = hierarchy.nodes.emplace(node.path, std::move(node));
    return iter->second;
}

void add_arrays(
    ProbeHierarchy& hierarchy,
    const ProbeStoreNode& node,
    const std::vector<std::pair<std::string, ProbeArray>>& entries) {
    for (const auto& [relative_path, array] : entries) {
        const auto full_path = relative_path.empty() ? node.path : join_path(node.path, relative_path);
        hierarchy.arrays[full_path] = array;
    }
}

Scenario build_image_scenario() {
    Scenario scenario{};
    auto& dataset = add_node(
        scenario.hierarchy,
        "/dataset",
        ordered_json{
            {"multiscales",
             ordered_json::array({
                 ordered_json{
                     {"version", "0.5"},
                     {"name", "main-image"},
                     {"axes",
                      ordered_json::array({
                          ordered_json{{"name", "c"}, {"type", "channel"}},
                          ordered_json{{"name", "y"}, {"type", "space"}},
                          ordered_json{{"name", "x"}, {"type", "space"}},
                      })},
                     {"datasets",
                      ordered_json::array({
                          ordered_json{
                              {"path", "0"},
                              {"coordinateTransformations",
                               ordered_json::array({
                                   ordered_json{{"type", "scale"}, {"scale", ordered_json::array({1, 1, 1})}},
                               })},
                          },
                          ordered_json{
                              {"path", "1"},
                              {"coordinateTransformations",
                               ordered_json::array({
                                   ordered_json{{"type", "scale"}, {"scale", ordered_json::array({1, 2, 2})}},
                               })},
                          },
                      })},
                 },
             })},
            {"omero",
             ordered_json{
                 {"rdefs", ordered_json{{"model", "greyscale"}}},
                 {"channels",
                  ordered_json::array({
                      ordered_json{
                          {"color", "FF0000"},
                          {"label", "red"},
                          {"active", 1},
                          {"window", ordered_json{{"start", 0}, {"end", 255}}},
                      },
                      ordered_json{
                          {"color", "00FF00"},
                          {"label", "green"},
                          {"active", 0},
                          {"window", ordered_json{{"start", 5}, {"end", 10}}},
                      },
                  })},
             }},
        },
        ordered_json{{"version", "0.5"}});
    add_arrays(
        scenario.hierarchy,
        dataset,
        {
            {"0",
             make_array(
                 {3U, 2U, 2U},
                 {{1U, 1U, 1U}, {2U}, {2U}},
                 "uint16",
                 {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})},
            {"1",
             make_array(
                 {3U, 1U, 1U},
                 {{1U, 1U, 1U}, {1U}, {1U}},
                 "uint16",
                 {0, 1, 2})},
        });

    add_node(
        scenario.hierarchy,
        "/dataset/labels",
        ordered_json{{"labels", ordered_json::array({"coins"})}},
        ordered_json{{"version", "0.5"}});

    auto& label = add_node(
        scenario.hierarchy,
        "/dataset/labels/coins",
        ordered_json{
            {"image-label",
             ordered_json{
                 {"source", ordered_json{{"image", "../../"}}},
                 {"colors",
                  ordered_json::array({
                      ordered_json{{"label-value", 1}, {"rgba", ordered_json::array({255, 0, 0, 255})}},
                      ordered_json{{"label-value", true}, {"rgba", ordered_json::array({0, 255, 0, 255})}},
                      ordered_json{{"label-value", "bad"}, {"rgba", ordered_json::array({0, 0, 0, 0})}},
                  })},
                 {"properties",
                  ordered_json::array({
                      ordered_json{{"label-value", 1}, {"name", "cell"}},
                      ordered_json{{"label-value", 2}, {"name", "background"}},
                  })},
             }},
            {"image", ordered_json{{"source", "synthetic"}}},
            {"multiscales",
             ordered_json::array({
                 ordered_json{
                     {"version", "0.5"},
                     {"name", "coins"},
                     {"axes", ordered_json::array({"y", "x"})},
                     {"datasets",
                      ordered_json::array({
                          ordered_json{{"path", "0"}},
                          ordered_json{{"path", "1"}},
                      })},
                 },
             })},
        },
        ordered_json{{"version", "0.5"}});
    add_arrays(
        scenario.hierarchy,
        label,
        {
            {"0",
             make_array(
                 {2U, 2U},
                 {{2U}, {2U}},
                 "uint8",
                 {0, 1, 2, 3})},
            {"1",
             make_array(
                 {1U, 1U},
                 {{1U}, {1U}},
                 "uint8",
                 {3})},
        });

    scenario.root_path = "/dataset";
    return scenario;
}

Scenario build_hcs_scenario() {
    Scenario scenario{};
    add_node(
        scenario.hierarchy,
        "/plate",
        ordered_json{
            {"plate",
             ordered_json{
                 {"rows", ordered_json::array({ordered_json{{"name", "A"}}})},
                 {"columns", ordered_json::array({ordered_json{{"name", "1"}}})},
                 {"wells", ordered_json::array({ordered_json{{"path", "A/1"}}})},
             }},
        },
        ordered_json{{"version", "0.4"}});
    add_node(
        scenario.hierarchy,
        "/plate/A/1",
        ordered_json{
            {"well",
             ordered_json{
                 {"images", ordered_json::array({ordered_json{{"path", "0"}}})},
             }},
        },
        ordered_json{{"version", "0.4"}});
    auto& image = add_node(
        scenario.hierarchy,
        "/plate/A/1/0",
        ordered_json{
            {"multiscales",
             ordered_json::array({
                 ordered_json{
                     {"version", "0.4"},
                     {"name", "field-0"},
                     {"axes", ordered_json::array({"y", "x"})},
                     {"datasets",
                      ordered_json::array({
                          ordered_json{{"path", "0"}},
                          ordered_json{{"path", "1"}},
                      })},
                 },
             })},
        },
        ordered_json{{"version", "0.4"}});
    add_arrays(
        scenario.hierarchy,
        image,
        {
            {"0",
             make_array(
                 {2U, 3U},
                 {{2U}, {3U}},
                 "uint16",
                 {0, 1, 2, 3, 4, 5})},
            {"1",
             make_array(
                 {1U, 2U},
                 {{1U}, {2U}},
                 "uint16",
                 {0, 1})},
        });
    scenario.root_path = "/plate";
    return scenario;
}

Scenario build_units_scenario() {
    Scenario scenario{};
    auto& image = add_node(
        scenario.hierarchy,
        "/units-image",
        ordered_json{
            {"multiscales",
             ordered_json::array({
                 ordered_json{
                     {"version", "0.4"},
                     {"axes",
                      ordered_json::array({
                          ordered_json{{"name", "z"}, {"type", "space"}, {"unit", "micrometer"}},
                          ordered_json{{"name", "y"}, {"type", "space"}, {"unit", "micrometer"}},
                          ordered_json{{"name", "x"}, {"type", "space"}, {"unit", "micrometer"}},
                      })},
                     {"datasets",
                      ordered_json::array({
                          ordered_json{
                              {"path", "0"},
                              {"coordinateTransformations",
                               ordered_json::array({
                                   ordered_json{{"type", "scale"}, {"scale", ordered_json::array({1.0, 1.0, 1.0})}},
                               })},
                          },
                      })},
                 },
             })},
        },
        ordered_json{{"version", "0.4"}});
    add_arrays(
        scenario.hierarchy,
        image,
        {
            {"0",
             make_array(
                 {2U, 2U, 2U},
                 {{1U, 1U}, {2U}, {2U}},
                 "uint16",
                 {0, 1, 2, 3, 4, 5, 6, 7})},
        });
    scenario.root_path = "/units-image";
    return scenario;
}

Scenario build_raw_scenario() {
    Scenario scenario{};
    auto& raw = add_node(
        scenario.hierarchy,
        "/raw-array",
        ordered_json::object(),
        ordered_json::object(),
        ordered_json{{"shape", ordered_json::array({2, 2})}});
    add_arrays(
        scenario.hierarchy,
        raw,
        {
            {"",
             make_array(
                 {2U, 2U},
                 {{2U}, {2U}},
                 "uint8",
                 {0, 1, 2, 3})},
        });
    scenario.root_path = "/raw-array";
    return scenario;
}

Scenario build_ignored_scenario() {
    Scenario scenario{};
    add_node(
        scenario.hierarchy,
        "/ignored",
        ordered_json::object(),
        ordered_json{{"version", "0.5"}},
        ordered_json::object());
    scenario.root_path = "/ignored";
    return scenario;
}

Scenario build_omero_edge_scenario(const std::string& scenario_name) {
    Scenario scenario{};
    ordered_json root_attrs = ordered_json::object();
    if (scenario_name == "omero-edge-0") {
        root_attrs["omero"] = ordered_json{{"channels", nullptr}};
    } else if (scenario_name == "omero-edge-1") {
        root_attrs["omero"] = ordered_json{{"__opaque_channels_object__", true}};
    } else if (scenario_name == "omero-edge-2") {
        root_attrs["omero"] = ordered_json{
            {"channels", ordered_json::array({ordered_json{{"color", "GG0000"}}})},
        };
    } else if (scenario_name == "omero-edge-3") {
        root_attrs["omero"] = ordered_json{
            {"rdefs", ordered_json{{"model", "color"}}},
            {"channels",
             ordered_json::array({
                 ordered_json{{"active", ""}, {"window", ordered_json{{"start", 1}, {"end", 2}}}},
                 ordered_json{{"active", 0}, {"window", ordered_json{{"start", 3}}}},
             })},
        };
    } else {
        throw std::runtime_error("Unknown reader edge scenario: " + scenario_name);
    }
    const auto path =
        scenario_name == "omero-edge-0"
            ? "/edge-0"
            : scenario_name == "omero-edge-1"
                ? "/edge-1"
                : scenario_name == "omero-edge-2"
                    ? "/edge-2"
                    : "/edge-3";
    add_node(
        scenario.hierarchy,
        path,
        std::move(root_attrs),
        ordered_json{{"version", "0.5"}});
    scenario.root_path = path;
    return scenario;
}

Scenario build_scenario(const std::string& scenario_name) {
    if (scenario_name == "image") {
        return build_image_scenario();
    }
    if (scenario_name == "plate" || scenario_name == "well") {
        return build_hcs_scenario();
    }
    if (scenario_name == "units") {
        return build_units_scenario();
    }
    if (scenario_name == "raw") {
        return build_raw_scenario();
    }
    if (scenario_name == "ignored") {
        return build_ignored_scenario();
    }
    if (scenario_name.rfind("omero-edge-", 0U) == 0U) {
        return build_omero_edge_scenario(scenario_name);
    }
    throw std::runtime_error("Unknown reader scenario: " + scenario_name);
}

const Scenario& cached_image_scenario() {
    static const Scenario scenario = build_image_scenario();
    return scenario;
}

const Scenario& cached_hcs_scenario() {
    static const Scenario scenario = build_hcs_scenario();
    return scenario;
}

struct CollisionKey {
    enum class Kind { string, integer, boolean };

    Kind kind = Kind::string;
    std::string string_value;
    std::int64_t integer_value = 0;
    bool boolean_value = false;
};

bool collision_key_equal(const CollisionKey& left, const CollisionKey& right) {
    if (
        left.kind == CollisionKey::Kind::string ||
        right.kind == CollisionKey::Kind::string) {
        return left.kind == right.kind && left.string_value == right.string_value;
    }
    const auto left_numeric = left.kind == CollisionKey::Kind::boolean
        ? static_cast<std::int64_t>(left.boolean_value)
        : left.integer_value;
    const auto right_numeric = right.kind == CollisionKey::Kind::boolean
        ? static_cast<std::int64_t>(right.boolean_value)
        : right.integer_value;
    return left_numeric == right_numeric;
}

ordered_json encode_collision_key(const CollisionKey& key) {
    if (key.kind == CollisionKey::Kind::string) {
        return ordered_json{{"kind", "str"}, {"value", key.string_value}};
    }
    if (key.kind == CollisionKey::Kind::boolean) {
        return ordered_json{{"kind", "bool"}, {"value", key.boolean_value}};
    }
    return ordered_json{{"kind", "int"}, {"value", key.integer_value}};
}

class CollisionMapping {
public:
    void insert_or_assign(CollisionKey key, ordered_json value) {
        for (auto& [existing_key, existing_value] : items_) {
            if (collision_key_equal(existing_key, key)) {
                existing_value = std::move(value);
                return;
            }
        }
        items_.push_back(std::make_pair(std::move(key), std::move(value)));
    }

    [[nodiscard]] bool empty() const {
        return items_.empty();
    }

    [[nodiscard]] ordered_json to_json() const {
        ordered_json result = ordered_json::object();
        ordered_json encoded = ordered_json::array();
        for (const auto& [key, value] : items_) {
            encoded.push_back(
                ordered_json{{"key", encode_collision_key(key)}, {"value", value}});
        }
        result["__mapping__"] = std::move(encoded);
        return result;
    }

private:
    std::vector<std::pair<CollisionKey, ordered_json>> items_;
};

struct ProbeContext {
    const ProbeHierarchy* hierarchy = nullptr;
    std::vector<std::string> seen;
};

struct ProbeSpec {
    ProbeSpecKind kind;
    ordered_json signature;
};

struct ProbeNode {
    ProbeContext& context;
    ProbeStoreNode zarr;
    bool visible = true;
    ordered_json metadata = ordered_json::object();
    std::vector<ProbeArray> data;
    std::vector<ProbeSpec> specs;
    std::vector<std::unique_ptr<ProbeNode>> pre_nodes;
    std::vector<std::unique_ptr<ProbeNode>> post_nodes;

    ProbeNode(
        ProbeContext& context_in,
        ProbeStoreNode zarr_in,
        bool visibility,
        bool plate_labels = false)
        : context(context_in), zarr(std::move(zarr_in)), visible(visibility) {
        initialise_specs(plate_labels);
    }

    ProbeSpec* first(const ProbeSpecKind kind) {
        for (auto& spec : specs) {
            if (spec.kind == kind) {
                return &spec;
            }
        }
        return nullptr;
    }

    ProbeSpec* load(const ProbeSpecKind kind) {
        return first(kind);
    }

    ProbeNode* add(
        const ProbeStoreNode& child,
        const bool prepend = false,
        const std::optional<bool> visibility_override = std::nullopt,
        const bool plate_labels = false) {
        const bool already_seen = std::find(
            context.seen.begin(),
            context.seen.end(),
            child.path) != context.seen.end();
        const auto plan = reader_node_add_plan(
            already_seen,
            plate_labels,
            visibility_override.has_value(),
            visibility_override.value_or(false),
            visible);
        if (!plan.should_add) {
            return nullptr;
        }

        context.seen.push_back(child.path);
        auto node = std::make_unique<ProbeNode>(context, child, plan.visibility, plate_labels);
        auto* node_ptr = node.get();
        if (prepend) {
            pre_nodes.push_back(std::move(node));
        } else {
            post_nodes.push_back(std::move(node));
        }
        return node_ptr;
    }

    void set_visible(const bool new_visible) {
        const bool old_visible = visible;
        if (old_visible == new_visible) {
            return;
        }
        visible = new_visible;
        for (auto& child : pre_nodes) {
            child->set_visible(new_visible);
        }
        for (auto& child : post_nodes) {
            child->set_visible(new_visible);
        }
    }

    void write_metadata(ordered_json& output) const {
        if (reader_should_write_metadata(specs.size())) {
            for (const auto& item : zarr.root_attrs.items()) {
                output[item.key()] = item.value();
            }
        }
    }

    [[nodiscard]] std::string repr() const {
        return reader_node_repr(zarr.path + repr_suffix(), visible);
    }

    [[nodiscard]] ordered_json signature() const {
        ordered_json firsts = ordered_json::object();
        for (const ProbeSpecKind kind :
             {ProbeSpecKind::labels,
              ProbeSpecKind::label,
              ProbeSpecKind::multiscales,
              ProbeSpecKind::omero,
              ProbeSpecKind::plate,
              ProbeSpecKind::well}) {
            const auto* match = find_spec(kind);
            ordered_json pair = ordered_json::array();
            if (match == nullptr) {
                pair.push_back(nullptr);
                pair.push_back(nullptr);
            } else {
                const auto name = spec_name(kind);
                pair.push_back(name);
                pair.push_back(name);
            }
            firsts[spec_name(kind)] = std::move(pair);
        }

        ordered_json spec_list = ordered_json::array();
        for (const auto& spec : specs) {
            spec_list.push_back(spec.signature);
        }

        ordered_json data_list = ordered_json::array();
        for (const auto& array : data) {
            data_list.push_back(array_signature(array));
        }

        ordered_json pre_list = ordered_json::array();
        for (const auto& child : pre_nodes) {
            pre_list.push_back(child->repr());
        }

        ordered_json post_list = ordered_json::array();
        for (const auto& child : post_nodes) {
            post_list.push_back(child->repr());
        }

        return ordered_json{
            {"repr", repr()},
            {"visible", visible},
            {"specs", std::move(spec_list)},
            {"firsts", std::move(firsts)},
            {"metadata", metadata},
            {"data", std::move(data_list)},
            {"pre_nodes", std::move(pre_list)},
            {"post_nodes", std::move(post_list)},
        };
    }

private:
    [[nodiscard]] std::string repr_suffix() const {
        std::string suffix;
        if (!zarr.zgroup.empty()) {
            suffix += " [zgroup]";
        }
        if (!zarr.zarray.empty()) {
            suffix += " [zarray]";
        }
        return suffix;
    }

    const ProbeSpec* find_spec(const ProbeSpecKind kind) const {
        for (const auto& spec : specs) {
            if (spec.kind == kind) {
                return &spec;
            }
        }
        return nullptr;
    }

    void initialise_specs(bool plate_labels);
};

ordered_json normalized_multiscale_axes(
    const ordered_json& axes,
    const std::string& version) {
    ordered_json normalized = ordered_json::array();
    if (axes.is_null()) {
        if (version == "0.1" || version == "0.2") {
            normalized = ordered_json::array({"t", "c", "z", "y", "x"});
        } else {
            throw std::runtime_error("'Axes' object has no attribute 'axes'");
        }
    } else {
        for (const auto& axis : axes) {
            if (axis.is_string()) {
                ordered_json axis_object = ordered_json::object();
                const auto name = axis.get<std::string>();
                axis_object["name"] = name;
                if (name == "x" || name == "y" || name == "z") {
                    axis_object["type"] = "space";
                } else if (name == "c") {
                    axis_object["type"] = "channel";
                } else if (name == "t") {
                    axis_object["type"] = "time";
                }
                normalized.push_back(std::move(axis_object));
            } else {
                normalized.push_back(axis);
            }
        }
    }
    if (version == "0.3") {
        ordered_json names = ordered_json::array();
        for (const auto& axis : normalized) {
            names.push_back(axis.at("name"));
        }
        return names;
    }
    return normalized;
}

CollisionKey collision_key_from_json(const ordered_json& value) {
    CollisionKey key{};
    if (value.is_boolean()) {
        key.kind = CollisionKey::Kind::boolean;
        key.boolean_value = value.get<bool>();
        return key;
    }
    if (value.is_number_integer() || value.is_number_unsigned()) {
        key.kind = CollisionKey::Kind::integer;
        key.integer_value = value.get<std::int64_t>();
        return key;
    }
    if (value.is_string()) {
        key.kind = CollisionKey::Kind::string;
        key.string_value = value.get<std::string>();
        return key;
    }
    throw std::runtime_error("Unsupported label key type");
}

ordered_json label_metadata_payload(
    const ProbeStoreNode& zarr,
    const bool visible) {
    ordered_json metadata = ordered_json::object();
    ordered_json image_label = json_at_or_empty(zarr.root_attrs, "image-label");
    ordered_json source = json_at_or_empty(image_label, "source");

    metadata["visible"] = visible;
    metadata["name"] = basename_path(zarr.path);

    CollisionMapping colors;
    if (
        image_label.is_object() &&
        image_label.contains("colors") &&
        image_label["colors"].is_array()) {
        for (const auto& color : image_label["colors"]) {
            try {
                const auto label_value = color.at("label-value");
                ReaderLabelColorInput input{};
                input.label_is_bool = label_value.is_boolean();
                input.label_bool = input.label_is_bool && label_value.get<bool>();
                input.label_is_int = label_value.is_number_integer() || label_value.is_number_unsigned();
                input.label_int = input.label_is_int ? label_value.get<std::int64_t>() : 0;
                if (color.contains("rgba") && color["rgba"].is_array()) {
                    input.has_rgba = true;
                    for (const auto& entry : color["rgba"]) {
                        input.rgba.push_back(entry.get<std::int64_t>());
                    }
                }
                const auto plan = reader_label_color_plan(input);
                if (!plan.keep) {
                    continue;
                }
                ordered_json rgba = ordered_json::array();
                for (const auto component : plan.rgba) {
                    rgba.push_back(component);
                }
                CollisionKey key{};
                if (plan.label_is_bool) {
                    key.kind = CollisionKey::Kind::boolean;
                    key.boolean_value = plan.label_bool;
                } else {
                    key.kind = CollisionKey::Kind::integer;
                    key.integer_value = plan.label_int;
                }
                colors.insert_or_assign(std::move(key), std::move(rgba));
            } catch (...) {
            }
        }
    }
    metadata["color"] = colors.empty() ? ordered_json::object() : colors.to_json();

    ordered_json nested_metadata = ordered_json::object();
    nested_metadata["image"] = zarr.root_attrs.contains("image")
        ? zarr.root_attrs["image"]
        : ordered_json::object();
    nested_metadata["path"] = basename_path(zarr.path);
    metadata["metadata"] = std::move(nested_metadata);

    CollisionMapping properties;
    if (
        image_label.is_object() &&
        image_label.contains("properties") &&
        image_label["properties"].is_array()) {
        for (const auto& props : image_label["properties"]) {
            try {
                const auto label_value = props.at("label-value");
                const auto plan = reader_label_property_plan(ReaderLabelPropertyInput{
                    label_value.is_boolean(),
                    label_value.is_boolean() && label_value.get<bool>(),
                    label_value.is_number_integer() || label_value.is_number_unsigned(),
                    (label_value.is_number_integer() || label_value.is_number_unsigned())
                        ? label_value.get<std::int64_t>()
                        : 0,
                });
                if (!plan.keep) {
                    continue;
                }
                ordered_json props_copy = props;
                props_copy.erase("label-value");
                CollisionKey key{};
                if (plan.label_is_bool) {
                    key.kind = CollisionKey::Kind::boolean;
                    key.boolean_value = plan.label_bool;
                } else {
                    key.kind = CollisionKey::Kind::integer;
                    key.integer_value = plan.label_int;
                }
                properties.insert_or_assign(std::move(key), std::move(props_copy));
            } catch (...) {
            }
        }
    }
    if (!properties.empty()) {
        metadata["properties"] = properties.to_json();
    }
    metadata["parent_image"] = source.contains("image") ? source["image"] : nullptr;
    return metadata;
}

ordered_json omero_metadata_payload(const ordered_json& image_data, const bool node_visible) {
    ordered_json metadata = ordered_json::object();
    const auto rdefs = image_data.contains("rdefs") ? image_data["rdefs"] : ordered_json::object();
    const auto model =
        rdefs.is_object() && rdefs.contains("model") && rdefs["model"].is_string()
            ? rdefs["model"].get<std::string>()
            : "unknown";

    if (image_data.contains("__opaque_channels_object__") && image_data["__opaque_channels_object__"] == true) {
        return ordered_json::object();
    }

    const auto channels_iter = image_data.find("channels");
    if (channels_iter == image_data.end() || channels_iter->is_null()) {
        return ordered_json::object();
    }
    if (!channels_iter->is_array()) {
        return ordered_json::object();
    }

    std::vector<ReaderOmeroChannelInput> inputs;
    inputs.reserve(channels_iter->size());
    for (const auto& channel : *channels_iter) {
        ReaderOmeroChannelInput input{};
        if (channel.is_object() && channel.contains("color") && channel["color"].is_string()) {
            input.has_color = true;
            input.color = channel["color"].get<std::string>();
        }
        if (channel.is_object() && channel.contains("label") && channel["label"].is_string()) {
            input.has_label = true;
            input.label = channel["label"].get<std::string>();
        }
        if (channel.is_object() && channel.contains("active")) {
            input.has_active = true;
            input.active_truthy = !channel["active"].is_null() &&
                !(channel["active"].is_boolean() && !channel["active"].get<bool>()) &&
                !(channel["active"].is_number_integer() && channel["active"].get<std::int64_t>() == 0) &&
                !(channel["active"].is_string() && channel["active"].get<std::string>().empty());
        }
        if (channel.is_object() && channel.contains("window") && channel["window"].is_object()) {
            input.has_window = true;
            input.has_window_start = channel["window"].contains("start");
            input.has_window_end = channel["window"].contains("end");
        }
        inputs.push_back(std::move(input));
    }

    try {
        const auto plan = reader_omero_plan(model, inputs);
        ordered_json channel_names = ordered_json::array();
        ordered_json visibles = ordered_json::array();
        ordered_json contrast_limits = ordered_json::array();
        ordered_json colormaps = ordered_json::array();
        bool any_incomplete_window = false;

        for (std::size_t index = 0; index < channels_iter->size(); ++index) {
            const auto& channel = (*channels_iter)[index];
            const auto& channel_plan = plan.channels[index];

            channel_names.push_back("channel_" + std::to_string(index));
            visibles.push_back(true);
            contrast_limits.push_back(nullptr);

            if (channel_plan.has_label && channel.contains("label")) {
                channel_names[index] = channel["label"];
            }
            if (channel.contains("active")) {
                if (channel_plan.visible_mode == ReaderVisibleMode::node_visible_if_active) {
                    visibles[index] = node_visible;
                } else if (channel_plan.visible_mode == ReaderVisibleMode::keep_raw_active) {
                    visibles[index] = channel["active"];
                }
            }
            if (channel_plan.has_color) {
                ordered_json rgb = ordered_json::array();
                if (channel_plan.force_greyscale_rgb) {
                    rgb = ordered_json::array({1, 1, 1});
                } else {
                    for (const auto component : channel_plan.rgb) {
                        rgb.push_back(component);
                    }
                }
                colormaps.push_back(
                    ordered_json::array({ordered_json::array({0, 0, 0}), rgb}));
            }
            if (channel.contains("window")) {
                if (!channel_plan.has_complete_window) {
                    any_incomplete_window = true;
                } else {
                    contrast_limits[index] = ordered_json::array({
                        channel["window"]["start"],
                        channel["window"]["end"],
                    });
                }
            }
        }

        metadata["channel_names"] = std::move(channel_names);
        metadata["visible"] = std::move(visibles);
        metadata["contrast_limits"] = any_incomplete_window ? ordered_json(nullptr) : std::move(contrast_limits);
        metadata["colormap"] = std::move(colormaps);
        return metadata;
    } catch (...) {
        return ordered_json::object();
    }
}

struct WellRuntimeInfo {
    ordered_json well_data;
    ordered_json img_metadata;
    std::vector<ProbeArray> img_pyramid;
    std::string numpy_type;
    std::vector<std::string> dataset_paths;
    std::size_t row_count = 0U;
    std::size_t column_count = 0U;
};

ProbeNode make_node(ProbeContext& context, const ProbeStoreNode& zarr, const bool visible, const bool plate_labels = false);

WellRuntimeInfo build_well_runtime(
    ProbeContext& context,
    const ProbeStoreNode& well_zarr,
    ProbeNode& node) {
    const auto well_data =
        well_zarr.root_attrs.contains("well")
            ? well_zarr.root_attrs["well"]
            : ordered_json::object();
    std::vector<std::string> image_paths;
    if (well_data.contains("images") && well_data["images"].is_array()) {
        for (const auto& image : well_data["images"]) {
            image_paths.push_back(image.at("path").get<std::string>());
        }
    }
    const auto plan = reader_well_plan(image_paths);

    const auto image_zarr = context.hierarchy->create_child(well_zarr, image_paths.at(0));
    auto image_node = make_node(context, image_zarr, node.visible);
    const auto datasets =
        image_zarr.root_attrs["multiscales"][0]["datasets"];

    std::vector<std::string> dataset_paths;
    dataset_paths.reserve(datasets.size());
    for (const auto& dataset : datasets) {
        dataset_paths.push_back(dataset.at("path").get<std::string>());
    }
    const auto level_plans = reader_well_level_plans(
        image_paths,
        dataset_paths,
        plan.row_count,
        plan.column_count);

    const auto x_index = image_node.metadata["axes"].size() - 1U;
    const auto y_index = image_node.metadata["axes"].size() - 2U;
    std::vector<ProbeArray> pyramid;
    pyramid.reserve(image_node.data.size());

    for (std::size_t level = 0; level < image_node.data.size(); ++level) {
        const auto& tile_shape = image_node.data[level].shape;
        const auto& level_plan = level_plans[level];
        std::vector<ProbeArray> row_arrays;
        row_arrays.reserve(plan.row_count);
        for (std::size_t row = 0; row < plan.row_count; ++row) {
            std::vector<ProbeArray> tiles;
            tiles.reserve(plan.column_count);
            for (std::size_t col = 0; col < plan.column_count; ++col) {
                const auto field_index = (plan.column_count * row) + col;
                ProbeArray tile{};
                if (level_plan.has_tile[field_index]) {
                    try {
                        tile = context.hierarchy->load(well_zarr, level_plan.tile_paths[field_index]);
                    } catch (...) {
                        tile = zeros_array(tile_shape, image_node.data.front().dtype);
                    }
                } else {
                    tile = zeros_array(tile_shape, image_node.data.front().dtype);
                }
                tiles.push_back(std::move(tile));
            }
            row_arrays.push_back(concatenate_arrays(tiles, x_index));
        }
        pyramid.push_back(concatenate_arrays(row_arrays, y_index));
    }

    node.data = pyramid;
    node.metadata = image_node.metadata;

    WellRuntimeInfo info{};
    info.well_data = well_data;
    info.img_metadata = image_node.metadata;
    info.img_pyramid = pyramid;
    info.numpy_type = image_node.data.front().dtype;
    info.dataset_paths = dataset_paths;
    info.row_count = plan.row_count;
    info.column_count = plan.column_count;
    return info;
}

struct PlateRuntimeInfo {
    ordered_json plate_data;
    std::vector<std::string> row_names;
    std::vector<std::string> col_names;
    std::vector<std::string> well_paths;
    std::size_t row_count = 0U;
    std::size_t column_count = 0U;
    std::string first_field_path;
    std::vector<std::string> dataset_paths;
    std::string numpy_type;
    ordered_json axes;
};

PlateRuntimeInfo build_plate_runtime(
    ProbeContext& context,
    const ProbeStoreNode& plate_zarr,
    ProbeNode& node) {
    const auto plate_data =
        plate_zarr.root_attrs.contains("plate")
            ? plate_zarr.root_attrs["plate"]
            : ordered_json::object();

    std::vector<std::string> row_names;
    std::vector<std::string> col_names;
    std::vector<std::string> well_paths;
    for (const auto& row : plate_data["rows"]) {
        row_names.push_back(row.at("name").get<std::string>());
    }
    for (const auto& column : plate_data["columns"]) {
        col_names.push_back(column.at("name").get<std::string>());
    }
    for (const auto& well : plate_data["wells"]) {
        well_paths.push_back(well.at("path").get<std::string>());
    }
    const auto plan = reader_plate_plan(row_names, col_names, well_paths);

    const auto first_well = context.hierarchy->create_child(plate_zarr, plan.well_paths.at(0));
    auto well_node = make_node(context, first_well, node.visible);
    const auto* well_spec = well_node.first(ProbeSpecKind::well);
    if (well_spec == nullptr) {
        throw std::runtime_error("Could not find first well");
    }
    const auto well_info = build_well_runtime(context, first_well, well_node);
    const auto first_field_path =
        first_well.root_attrs["well"]["images"][0]["path"].get<std::string>();
    const auto image_zarr = context.hierarchy->create_child(first_well, first_field_path);
    const auto datasets = image_zarr.root_attrs["multiscales"][0]["datasets"];
    std::vector<std::string> dataset_paths;
    for (const auto& dataset : datasets) {
        dataset_paths.push_back(dataset.at("path").get<std::string>());
    }
    const auto level_plans = reader_plate_level_plans(
        plan.row_names,
        plan.col_names,
        plan.well_paths,
        first_field_path,
        dataset_paths);

    std::vector<ProbeArray> pyramid;
    pyramid.reserve(well_info.img_pyramid.size());
    for (std::size_t level = 0; level < well_info.img_pyramid.size(); ++level) {
        const auto tile_shape = well_info.img_pyramid[level].shape;
        const auto& level_plan = level_plans[level];
        std::vector<ProbeArray> row_arrays;
        row_arrays.reserve(plan.row_count);
        for (std::size_t row = 0; row < plan.row_count; ++row) {
            std::vector<ProbeArray> tiles;
            tiles.reserve(plan.column_count);
            for (std::size_t col = 0; col < plan.column_count; ++col) {
                const auto index = (row * plan.column_count) + col;
                ProbeArray tile{};
                if (!level_plan.has_tile[index]) {
                    tile = zeros_array(tile_shape, well_info.numpy_type);
                } else {
                    try {
                        tile = context.hierarchy->load(
                            plate_zarr,
                            level_plan.tile_paths[index]);
                    } catch (...) {
                        tile = zeros_array(tile_shape, well_info.numpy_type);
                    }
                }
                tiles.push_back(std::move(tile));
            }
            row_arrays.push_back(concatenate_arrays(tiles, well_info.img_metadata["axes"].size() - 1U));
        }
        pyramid.push_back(concatenate_arrays(row_arrays, well_info.img_metadata["axes"].size() - 2U));
    }

    node.data = pyramid;
    node.metadata = well_info.img_metadata;
    node.metadata["metadata"] = ordered_json{{"plate", plate_data}};

    PlateRuntimeInfo info{};
    info.plate_data = plate_data;
    info.row_names = plan.row_names;
    info.col_names = plan.col_names;
    info.well_paths = plan.well_paths;
    info.row_count = plan.row_count;
    info.column_count = plan.column_count;
    info.first_field_path = first_field_path;
    info.dataset_paths = dataset_paths;
    info.numpy_type = well_info.numpy_type;
    info.axes = well_info.img_metadata["axes"];
    return info;
}

ProbeNode make_node(
    ProbeContext& context,
    const ProbeStoreNode& zarr,
    const bool visible,
    const bool plate_labels) {
    return ProbeNode(context, zarr, visible, plate_labels);
}

void ProbeNode::initialise_specs(const bool plate_labels) {
    ReaderSpecFlags flags{};
    flags.has_labels = json_contains(zarr.root_attrs, "labels");
    flags.has_image_label = json_contains(zarr.root_attrs, "image-label");
    flags.has_zgroup = !zarr.zgroup.empty();
    flags.has_multiscales = json_contains(zarr.root_attrs, "multiscales");
    flags.has_omero = json_contains(zarr.root_attrs, "omero");
    flags.has_plate = json_contains(zarr.root_attrs, "plate");
    flags.has_well = json_contains(zarr.root_attrs, "well");

    for (const auto& name : reader_matching_specs(flags)) {
        const auto kind = spec_kind_from_name(name);
        ProbeSpec spec{};
        spec.kind = kind;
        spec.signature = ordered_json{{"type", name}};

        if (kind == ProbeSpecKind::labels) {
            if (zarr.root_attrs.contains("labels") && zarr.root_attrs["labels"].is_array()) {
                std::vector<std::string> labels;
                for (const auto& label : zarr.root_attrs["labels"]) {
                    labels.push_back(label.get<std::string>());
                }
                for (const auto& label_name : reader_labels_names(labels)) {
                    const auto child = context.hierarchy->create_child(zarr, label_name);
                    if (child.exists) {
                        add(child, false, std::nullopt, plate_labels);
                    }
                }
            }
        } else if (kind == ProbeSpecKind::label) {
            const auto payload = label_metadata_payload(zarr, visible);
            if (!payload["parent_image"].is_null()) {
                const auto parent = context.hierarchy->create_child(
                    zarr,
                    payload["parent_image"].get<std::string>());
                if (parent.exists) {
                    add(parent, true, false, plate_labels);
                }
            }
            metadata["visible"] = payload["visible"];
            metadata["name"] = payload["name"];
            metadata["color"] = payload["color"];
            metadata["metadata"] = payload["metadata"];
            if (payload.contains("properties")) {
                metadata["properties"] = payload["properties"];
            }
        } else if (kind == ProbeSpecKind::multiscales) {
            const auto& first = zarr.root_attrs["multiscales"][0];
            const auto version =
                first.contains("version") ? first["version"].get<std::string>() : "0.1";
            const auto axes = first.contains("axes") ? first["axes"] : ordered_json(nullptr);
            metadata["axes"] = normalized_multiscale_axes(axes, version);
            metadata["name"] = first.contains("name") ? first["name"] : ordered_json(nullptr);

            std::vector<ReaderMultiscalesDatasetInput> datasets;
            ordered_json dataset_paths = ordered_json::array();
            ordered_json transformations = ordered_json::array();
            for (const auto& dataset : first["datasets"]) {
                ReaderMultiscalesDatasetInput input{};
                input.path = dataset.at("path").get<std::string>();
                input.has_coordinate_transformations =
                    dataset.contains("coordinateTransformations");
                datasets.push_back(std::move(input));
                dataset_paths.push_back(dataset.at("path"));
                transformations.push_back(
                    dataset.contains("coordinateTransformations")
                        ? dataset["coordinateTransformations"]
                        : ordered_json(nullptr));
            }
            const auto summary = reader_multiscales_summary(ReaderMultiscalesInput{
                first.contains("version"),
                version,
                first.contains("name"),
                first.contains("name") && first["name"].is_string()
                    ? first["name"].get<std::string>()
                    : "",
                datasets,
            });
            spec.signature["datasets"] = ordered_json::array();
            for (const auto& path : summary.paths) {
                spec.signature["datasets"].push_back(path);
                data.push_back(context.hierarchy->load(zarr, path));
            }
            if (summary.any_coordinate_transformations) {
                metadata["coordinateTransformations"] = std::move(transformations);
            }
            const auto labels = context.hierarchy->create_child(zarr, "labels");
            if (labels.exists) {
                add(labels, false, false, plate_labels);
            }
        } else if (kind == ProbeSpecKind::omero) {
            const auto image_data = zarr.root_attrs["omero"];
            metadata.update(omero_metadata_payload(image_data, visible));
        } else if (kind == ProbeSpecKind::well) {
            const auto info = build_well_runtime(context, zarr, *this);
            (void)info;
        } else if (kind == ProbeSpecKind::plate) {
            const auto info = build_plate_runtime(context, zarr, *this);
            spec.signature["row_names"] = info.row_names;
            spec.signature["col_names"] = info.col_names;
            spec.signature["well_paths"] = info.well_paths;
            spec.signature["row_count"] = info.row_count;
            spec.signature["column_count"] = info.column_count;
            spec.signature["first_field_path"] = info.first_field_path;
            spec.signature["img_paths"] = info.dataset_paths;
        }

        specs.push_back(std::move(spec));
    }
}

ordered_json reader_signature_from_root(const Scenario& scenario, const ProbeStoreNode& root) {
    ProbeContext context{};
    context.hierarchy = &scenario.hierarchy;
    context.seen.push_back(root.path);

    auto node = make_node(context, root, true);
    ordered_json result = ordered_json::array();
    if (!node.specs.empty()) {
        std::function<void(const ProbeNode&)> descend = [&](const ProbeNode& current) {
            for (const auto& child : current.pre_nodes) {
                descend(*child);
            }
            result.push_back(current.signature());
            for (const auto& child : current.post_nodes) {
                descend(*child);
            }
        };
        descend(node);
        return result;
    }
    if (!root.zarray.empty()) {
        node.data.push_back(context.hierarchy->load(root));
        result.push_back(node.signature());
        return result;
    }
    return ordered_json::array();
}

ordered_json reader_matches_payload(const Scenario& scenario, const std::string& scenario_name) {
    ordered_json payload = ordered_json::object();
    if (scenario_name == "image") {
        const auto& image = scenario.hierarchy.nodes.at("/dataset");
        const auto& labels = scenario.hierarchy.nodes.at("/dataset/labels");
        const auto& label = scenario.hierarchy.nodes.at("/dataset/labels/coins");
        ReaderSpecFlags image_flags{};
        image_flags.has_labels = json_contains(image.root_attrs, "labels");
        image_flags.has_image_label = json_contains(image.root_attrs, "image-label");
        image_flags.has_zgroup = !image.zgroup.empty();
        image_flags.has_multiscales = json_contains(image.root_attrs, "multiscales");
        image_flags.has_omero = json_contains(image.root_attrs, "omero");
        image_flags.has_plate = json_contains(image.root_attrs, "plate");
        image_flags.has_well = json_contains(image.root_attrs, "well");
        payload["Labels.matches"] = json_contains(labels.root_attrs, "labels");
        payload["Label.matches"] = json_contains(label.root_attrs, "image-label");
        payload["Multiscales.matches"] = image_flags.has_zgroup && image_flags.has_multiscales;
        payload["OMERO.matches"] = json_contains(image.root_attrs, "omero");
    } else {
        const auto& plate = scenario.hierarchy.nodes.at("/plate");
        const auto& well = scenario.hierarchy.nodes.at("/plate/A/1");
        payload["Plate.matches"] = json_contains(plate.root_attrs, "plate");
        payload["Well.matches"] = json_contains(well.root_attrs, "well");
    }
    return payload;
}

ordered_json reader_node_ops_payload() {
    const auto& scenario = cached_image_scenario();
    ProbeContext context{};
    context.hierarchy = &scenario.hierarchy;
    context.seen.push_back("/dataset");
    auto node = make_node(context, scenario.hierarchy.nodes.at("/dataset"), true);

    const auto before = node.signature();
    node.set_visible(false);
    const auto hidden = node.signature();
    node.set_visible(true);
    const auto shown = node.signature();
    ordered_json metadata = ordered_json::object();
    node.write_metadata(metadata);
    const auto duplicate = node.add(scenario.hierarchy.nodes.at("/dataset/labels"));

    ordered_json payload = ordered_json::object();
    payload["before"] = before;
    payload["hidden"] = hidden;
    payload["shown"] = shown;
    payload["first_labels"] = node.first(ProbeSpecKind::labels) == nullptr
        ? ordered_json(nullptr)
        : ordered_json("Labels");
    payload["load_multiscales"] = node.load(ProbeSpecKind::multiscales) == nullptr
        ? ordered_json(nullptr)
        : ordered_json("Multiscales");
    payload["metadata"] = metadata;
    payload["duplicate_add"] = duplicate == nullptr
        ? ordered_json(nullptr)
        : ordered_json(duplicate->repr());
    return payload;
}

ordered_json reader_image_surface_payload() {
    const auto& scenario = cached_image_scenario();
    ProbeContext context{};
    context.hierarchy = &scenario.hierarchy;
    const auto& root = scenario.hierarchy.nodes.at("/dataset");
    auto node = make_node(context, root, true);

    ordered_json descend = ordered_json::array();
    std::function<void(const ProbeNode&)> visit = [&](const ProbeNode& current) {
        for (const auto& child : current.pre_nodes) {
            visit(*child);
        }
        descend.push_back(current.signature());
        for (const auto& child : current.post_nodes) {
            visit(*child);
        }
    };
    visit(node);

    ordered_json payload = ordered_json::object();
    payload["array"] = array_signature(context.hierarchy->load(root, "0"));
    payload["descend"] = std::move(descend);
    payload["lookup"] = root.root_attrs["multiscales"];
    return payload;
}

ordered_json reader_plate_surface_payload() {
    const auto& scenario = cached_hcs_scenario();
    ProbeContext context{};
    context.hierarchy = &scenario.hierarchy;
    context.seen.push_back("/plate");
    auto plate_node = make_node(context, scenario.hierarchy.nodes.at("/plate"), true);
    auto image_node = make_node(context, scenario.hierarchy.nodes.at("/plate/A/1/0"), true);

    const auto runtime = build_plate_runtime(context, scenario.hierarchy.nodes.at("/plate"), plate_node);
    const auto level_plan = reader_plate_level_plans(
        runtime.row_names,
        runtime.col_names,
        runtime.well_paths,
        runtime.first_field_path,
        runtime.dataset_paths);
    const auto& stitched = context.hierarchy->load(
        scenario.hierarchy.nodes.at("/plate"),
        level_plan[0].tile_paths[0]);

    ordered_json payload = ordered_json::object();
    payload["get_numpy_type"] = image_node.data.front().dtype;
    payload["get_tile_path"] = reader_plate_tile_path(
        runtime.row_names[0],
        runtime.col_names[0],
        runtime.first_field_path,
        runtime.dataset_paths[0]);
    payload["get_stitched_grid"] = array_signature(stitched);
    payload["after_get_pyramid_lazy"] = ordered_json{
        {"metadata", plate_node.metadata},
        {"data", ordered_json::array()},
    };
    for (const auto& array : plate_node.data) {
        payload["after_get_pyramid_lazy"]["data"].push_back(array_signature(array));
    }
    payload["reader_signature"] = reader_signature_from_root(
        scenario,
        scenario.hierarchy.nodes.at("/plate"));
    return payload;
}

const ProbeStoreNode& root_for_signature(const Scenario& scenario, const std::string& name) {
    if (name == "well") {
        return scenario.hierarchy.nodes.at("/plate/A/1");
    }
    if (name == "plate") {
        return scenario.hierarchy.nodes.at("/plate");
    }
    return scenario.hierarchy.nodes.at(scenario.root_path);
}

}  // namespace

ordered_json reader_probe_matches(const std::string& scenario) {
    return reader_matches_payload(
        scenario == "image" ? cached_image_scenario() : cached_hcs_scenario(),
        scenario);
}

ordered_json reader_probe_node_ops(const std::string& scenario) {
    if (scenario != "image") {
        throw std::runtime_error("reader node-ops only supports the image scenario");
    }
    return reader_node_ops_payload();
}

ordered_json reader_probe_signature(const std::string& scenario) {
    if (scenario == "image") {
        const auto& built = cached_image_scenario();
        return reader_signature_from_root(built, root_for_signature(built, scenario));
    }
    if (scenario == "plate" || scenario == "well") {
        const auto& built = cached_hcs_scenario();
        return reader_signature_from_root(built, root_for_signature(built, scenario));
    }
    const auto built = build_scenario(scenario);
    return reader_signature_from_root(built, root_for_signature(built, scenario));
}

ordered_json reader_probe_image_surface() {
    return reader_image_surface_payload();
}

ordered_json reader_probe_plate_surface() {
    return reader_plate_surface_payload();
}

}  // namespace ome_zarr_c::native_code
