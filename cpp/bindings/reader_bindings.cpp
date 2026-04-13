#include <pybind11/pybind11.h>

#include <string>
#include <vector>

#include "../native/reader.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

py::list reader_matching_specs(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    ome_zarr_c::native_code::ReaderSpecFlags flags{};
    flags.has_labels =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("labels")));
    flags.has_image_label =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("image-label")));
    flags.has_zgroup = ome_zarr_c::bindings::object_truthy(zarr.attr("zgroup"));
    flags.has_multiscales =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("multiscales")));
    flags.has_omero =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("omero")));
    flags.has_plate =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("plate")));
    flags.has_well =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("well")));
    const auto native_matches = ome_zarr_c::native_code::reader_matching_specs(flags);

    py::list matches;
    for (const auto& match : native_matches) {
        matches.append(py::str(match));
    }
    return matches;
}

bool reader_matches_labels(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    return py::cast<bool>(root_attrs.attr("__contains__")(py::str("labels")));
}

bool reader_matches_label(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    return py::cast<bool>(root_attrs.attr("__contains__")(py::str("image-label")));
}

bool reader_matches_multiscales(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    return ome_zarr_c::bindings::object_truthy(zarr.attr("zgroup")) &&
           py::cast<bool>(root_attrs.attr("__contains__")(py::str("multiscales")));
}

bool reader_matches_omero(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    return py::cast<bool>(root_attrs.attr("__contains__")(py::str("omero")));
}

bool reader_matches_plate(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    return py::cast<bool>(root_attrs.attr("__contains__")(py::str("plate")));
}

bool reader_matches_well(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    return py::cast<bool>(root_attrs.attr("__contains__")(py::str("well")));
}

py::object reader_labels_names(const py::dict& root_attrs) {
    std::vector<std::string> labels;
    py::object raw = root_attrs.attr("get")(py::str("labels"), py::list());
    if (!raw.is_none()) {
        for (const py::handle& label_handle : py::iterable(raw)) {
            labels.push_back(py::cast<std::string>(label_handle));
        }
    }
    py::list result;
    for (const auto& label : ome_zarr_c::native_code::reader_labels_names(labels)) {
        result.append(py::str(label));
    }
    return result;
}

py::str reader_node_repr(const py::object& zarr, bool visible) {
    return py::str(ome_zarr_c::native_code::reader_node_repr(
        py::cast<std::string>(py::str(zarr)),
        visible));
}

py::dict reader_node_add_payload(
    bool already_seen,
    bool plate_labels,
    py::object visibility,
    bool current_visibility) {
    const auto plan = ome_zarr_c::native_code::reader_node_add_plan(
        already_seen,
        plate_labels,
        !visibility.is_none(),
        !visibility.is_none() && py::cast<bool>(visibility),
        current_visibility);
    py::dict payload;
    payload["should_add"] = py::bool_(plan.should_add);
    payload["visibility"] = py::bool_(plan.visibility);
    return payload;
}

py::dict reader_label_payload(
    const py::dict& root_attrs,
    const py::object& name,
    bool visible) {
    py::dict payload;
    py::object image_label = root_attrs.attr("get")(py::str("image-label"), py::dict());
    py::object source = image_label.attr("get")(py::str("source"), py::dict());
    payload["parent_image"] = source.attr("get")(py::str("image"), py::none());

    py::dict colors;
    py::object color_list = image_label.attr("get")(py::str("colors"), py::list());
    if (ome_zarr_c::bindings::object_truthy(color_list)) {
        for (const py::handle& color_handle : color_list) {
            py::object color = py::reinterpret_borrow<py::object>(color_handle);
            try {
                py::object label_value = color.attr("__getitem__")(py::str("label-value"));
                py::object rgba = color.attr("get")(py::str("rgba"), py::none());
                ome_zarr_c::native_code::ReaderLabelColorInput input{};
                input.label_is_bool = PyBool_Check(label_value.ptr()) != 0;
                input.label_bool = input.label_is_bool &&
                    py::cast<bool>(label_value);
                input.label_is_int = PyLong_Check(label_value.ptr()) != 0;
                input.label_int = input.label_is_int
                    ? py::cast<std::int64_t>(label_value)
                    : 0;
                input.has_rgba = !rgba.is_none() &&
                    ome_zarr_c::bindings::object_truthy(rgba);
                if (input.has_rgba) {
                    for (const py::handle& entry : rgba) {
                        input.rgba.push_back(py::cast<std::int64_t>(entry));
                    }
                }

                const auto plan =
                    ome_zarr_c::native_code::reader_label_color_plan(input);
                if (!plan.keep) {
                    continue;
                }

                py::object key;
                if (plan.label_is_bool) {
                    key = py::bool_(plan.label_bool);
                } else {
                    key = py::int_(plan.label_int);
                }
                py::list normalized;
                for (const double entry : plan.rgba) {
                    normalized.append(py::float_(entry));
                }
                colors[key] = normalized;
            } catch (...) {
            }
        }
    }

    py::dict properties;
    py::object props_list = image_label.attr("get")(py::str("properties"), py::list());
    if (ome_zarr_c::bindings::object_truthy(props_list)) {
        for (const py::handle& props_handle : props_list) {
            py::object props = py::reinterpret_borrow<py::object>(props_handle);
            py::object label_value = props.attr("__getitem__")(py::str("label-value"));
            const auto plan = ome_zarr_c::native_code::reader_label_property_plan(
                ome_zarr_c::native_code::ReaderLabelPropertyInput{
                    PyBool_Check(label_value.ptr()) != 0,
                    PyBool_Check(label_value.ptr()) != 0 &&
                        py::cast<bool>(label_value),
                    PyLong_Check(label_value.ptr()) != 0,
                    PyLong_Check(label_value.ptr()) != 0
                        ? py::cast<std::int64_t>(label_value)
                        : 0,
                });
            if (!plan.keep) {
                continue;
            }
            py::dict props_copy = py::dict(props);
            props_copy.attr("pop")(py::str("label-value"));
            py::object key;
            if (plan.label_is_bool) {
                key = py::bool_(plan.label_bool);
            } else {
                key = py::int_(plan.label_int);
            }
            properties[key] = props_copy;
        }
    }

    py::dict metadata;
    metadata["visible"] = py::bool_(visible);
    metadata["name"] = name;
    metadata["color"] = colors;

    py::dict nested_metadata;
    nested_metadata["image"] = root_attrs.attr("get")(py::str("image"), py::dict());
    nested_metadata["path"] = name;
    metadata["metadata"] = nested_metadata;

    payload["metadata"] = metadata;
    payload["properties"] = properties;
    return payload;
}

py::dict reader_multiscales_payload(const py::dict& root_attrs) {
    py::dict payload;
    py::list multiscales =
        py::cast<py::list>(root_attrs.attr("get")(py::str("multiscales"), py::list()));
    py::object first = multiscales[0];
    py::object datasets = first.attr("__getitem__")(py::str("datasets"));
    py::object axes = first.attr("get")(py::str("axes"), py::none());

    std::vector<ome_zarr_c::native_code::ReaderMultiscalesDatasetInput> native_datasets;
    native_datasets.reserve(py::len(datasets));
    for (const py::handle& dataset_handle : datasets) {
        py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
        py::object transform =
            dataset.attr("get")(py::str("coordinateTransformations"), py::none());
        ome_zarr_c::native_code::ReaderMultiscalesDatasetInput input{};
        input.path = py::cast<std::string>(dataset.attr("__getitem__")(py::str("path")));
        input.has_coordinate_transformations = !transform.is_none();
        native_datasets.push_back(std::move(input));
    }

    const auto plan = ome_zarr_c::native_code::reader_multiscales_summary(
        ome_zarr_c::native_code::ReaderMultiscalesInput{
            !first.attr("get")(py::str("version"), py::none()).is_none(),
            py::cast<std::string>(first.attr("get")(py::str("version"), py::str("0.1"))),
            !first.attr("get")(py::str("name"), py::none()).is_none(),
            py::cast<std::string>(py::str(first.attr("get")(py::str("name"), py::none()))),
            native_datasets,
        });
    payload["version"] = py::str(plan.version);
    payload["datasets"] = datasets;
    payload["axes"] = axes;
    payload["name"] = first.attr("get")(py::str("name"), py::none());
    py::list paths;
    py::list transformations;
    for (const py::handle& dataset_handle : datasets) {
        py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
        transformations.append(
            dataset.attr("get")(py::str("coordinateTransformations"), py::none()));
    }
    for (const auto& path : plan.paths) {
        paths.append(py::str(path));
    }
    payload["paths"] = paths;
    if (plan.any_coordinate_transformations) {
        payload["coordinateTransformations"] = transformations;
    }

    return payload;
}

py::dict reader_omero_payload(const py::dict& image_data, bool node_visible) {
    std::string model = "unknown";
    py::object rdefs = image_data.attr("get")(py::str("rdefs"), py::dict());
    if (ome_zarr_c::bindings::object_truthy(rdefs)) {
        model = py::cast<std::string>(
            rdefs.attr("get")(py::str("model"), py::str("unset")));
    }

    py::object channels = image_data.attr("get")(py::str("channels"), py::none());
    const py::size_t channel_count = py::len(channels);

    py::list colormaps;
    py::list contrast_limits;
    py::list names;
    py::list visibles;
    std::vector<ome_zarr_c::native_code::ReaderOmeroChannelInput> native_channels;
    native_channels.reserve(channel_count);
    for (py::size_t idx = 0; idx < channel_count; ++idx) {
        contrast_limits.append(py::none());
        names.append(py::str("channel_" + std::to_string(idx)));
        visibles.append(py::bool_(true));
        py::object channel = channels.attr("__getitem__")(py::int_(idx));

        py::object color = channel.attr("get")(py::str("color"), py::none());
        py::object label = channel.attr("get")(py::str("label"), py::none());
        py::object active = channel.attr("get")(py::str("active"), py::none());
        py::object window = channel.attr("get")(py::str("window"), py::none());
        py::object start = py::none();
        py::object end = py::none();
        if (!window.is_none()) {
            start = window.attr("get")(py::str("start"), py::none());
            end = window.attr("get")(py::str("end"), py::none());
        }
        native_channels.push_back(ome_zarr_c::native_code::ReaderOmeroChannelInput{
            !color.is_none(),
            color.is_none() ? "" : py::cast<std::string>(color),
            !label.is_none(),
            label.is_none() ? "" : py::cast<std::string>(label),
            !active.is_none(),
            !active.is_none() && ome_zarr_c::bindings::object_truthy(active),
            !window.is_none(),
            !start.is_none(),
            !end.is_none(),
        });
    }

    py::object contrast_limits_value = contrast_limits;
    const auto plan =
        ome_zarr_c::native_code::reader_omero_plan(model, native_channels);
    for (py::size_t idx = 0; idx < channel_count; ++idx) {
        py::object channel = channels.attr("__getitem__")(py::int_(idx));
        py::object label = channel.attr("get")(py::str("label"), py::none());
        py::object active = channel.attr("get")(py::str("active"), py::none());
        py::object window = channel.attr("get")(py::str("window"), py::none());
        py::object start = py::none();
        py::object end = py::none();
        if (!window.is_none()) {
            start = window.attr("get")(py::str("start"), py::none());
            end = window.attr("get")(py::str("end"), py::none());
        }
        const auto& channel_plan = plan.channels[idx];

        if (channel_plan.has_color) {
            py::list rgb;
            if (channel_plan.force_greyscale_rgb) {
                rgb.append(py::int_(1));
                rgb.append(py::int_(1));
                rgb.append(py::int_(1));
            } else {
                for (const double component : channel_plan.rgb) {
                    rgb.append(py::float_(component));
                }
            }
            py::list colormap;
            py::list zero_rgb;
            zero_rgb.append(py::int_(0));
            zero_rgb.append(py::int_(0));
            zero_rgb.append(py::int_(0));
            colormap.append(zero_rgb);
            colormap.append(rgb);
            colormaps.append(colormap);
        }

        if (channel_plan.has_label) {
            names[idx] = label;
        }

        if (!active.is_none()) {
            if (channel_plan.visible_mode ==
                ome_zarr_c::native_code::ReaderVisibleMode::node_visible_if_active) {
                visibles[idx] = py::bool_(node_visible);
            } else if (
                channel_plan.visible_mode ==
                ome_zarr_c::native_code::ReaderVisibleMode::keep_raw_active) {
                visibles[idx] = active;
            }
        }

        if (!window.is_none()) {
            if (!channel_plan.has_complete_window) {
                contrast_limits_value = py::none();
            } else if (!contrast_limits_value.is_none()) {
                py::list limits;
                limits.append(start);
                limits.append(end);
                contrast_limits[idx] = limits;
            }
        }
    }

    py::dict metadata;
    metadata["channel_names"] = names;
    metadata["visible"] = visibles;
    metadata["contrast_limits"] = contrast_limits_value;
    metadata["colormap"] = colormaps;
    return metadata;
}

py::dict reader_well_payload(const py::dict& root_attrs) {
    py::dict payload;
    py::object well_data = root_attrs.attr("get")(py::str("well"), py::dict());
    payload["well_data"] = well_data;

    py::list images =
        py::cast<py::list>(well_data.attr("get")(py::str("images"), py::list()));
    std::vector<std::string> image_paths;
    image_paths.reserve(py::len(images));
    for (const py::handle& image_handle : images) {
        py::object image = py::reinterpret_borrow<py::object>(image_handle);
        image_paths.push_back(
            py::cast<std::string>(image.attr("__getitem__")(py::str("path"))));
    }

    const auto plan = ome_zarr_c::native_code::reader_well_plan(image_paths);
    py::list image_paths_list;
    for (const auto& path : plan.image_paths) {
        image_paths_list.append(py::str(path));
    }
    payload["image_paths"] = image_paths_list;
    payload["row_count"] = py::int_(plan.row_count);
    payload["column_count"] = py::int_(plan.column_count);
    return payload;
}

py::dict reader_plate_payload(const py::dict& root_attrs) {
    py::dict payload;
    py::object plate_data = root_attrs.attr("get")(py::str("plate"), py::dict());
    payload["plate_data"] = plate_data;

    py::list rows =
        py::cast<py::list>(plate_data.attr("get")(py::str("rows"), py::list()));
    py::list columns =
        py::cast<py::list>(plate_data.attr("get")(py::str("columns"), py::list()));
    py::list wells =
        py::cast<py::list>(plate_data.attr("get")(py::str("wells"), py::list()));

    std::vector<std::string> row_names;
    std::vector<std::string> col_names;
    std::vector<std::string> well_paths;
    row_names.reserve(py::len(rows));
    col_names.reserve(py::len(columns));
    well_paths.reserve(py::len(wells));

    for (const py::handle& row_handle : rows) {
        py::object row = py::reinterpret_borrow<py::object>(row_handle);
        row_names.push_back(
            py::cast<std::string>(row.attr("__getitem__")(py::str("name"))));
    }
    for (const py::handle& column_handle : columns) {
        py::object column = py::reinterpret_borrow<py::object>(column_handle);
        col_names.push_back(
            py::cast<std::string>(column.attr("__getitem__")(py::str("name"))));
    }
    for (const py::handle& well_handle : wells) {
        py::object well = py::reinterpret_borrow<py::object>(well_handle);
        well_paths.push_back(
            py::cast<std::string>(well.attr("__getitem__")(py::str("path"))));
    }

    const auto plan =
        ome_zarr_c::native_code::reader_plate_plan(row_names, col_names, well_paths);
    py::list row_names_list;
    py::list col_names_list;
    py::list well_paths_list;
    for (const auto& name : plan.row_names) {
        row_names_list.append(py::str(name));
    }
    for (const auto& name : plan.col_names) {
        col_names_list.append(py::str(name));
    }
    for (const auto& path : plan.well_paths) {
        well_paths_list.append(py::str(path));
    }
    payload["row_names"] = row_names_list;
    payload["col_names"] = col_names_list;
    payload["well_paths"] = well_paths_list;
    payload["row_count"] = py::int_(plan.row_count);
    payload["column_count"] = py::int_(plan.column_count);
    return payload;
}

py::str reader_plate_tile_path(
    const py::str& row_name,
    const py::str& col_name,
    const py::str& first_field_path,
    const py::str& dataset_path) {
    return py::str(ome_zarr_c::native_code::reader_plate_tile_path(
        py::cast<std::string>(row_name),
        py::cast<std::string>(col_name),
        py::cast<std::string>(first_field_path),
        py::cast<std::string>(dataset_path)));
}

}  // namespace

void register_reader_bindings(py::module_& m) {
    m.def("reader_matching_specs", &reader_matching_specs, py::arg("zarr"));
    m.def("reader_matches_labels", &reader_matches_labels, py::arg("zarr"));
    m.def("reader_matches_label", &reader_matches_label, py::arg("zarr"));
    m.def("reader_matches_multiscales", &reader_matches_multiscales, py::arg("zarr"));
    m.def("reader_matches_omero", &reader_matches_omero, py::arg("zarr"));
    m.def("reader_matches_plate", &reader_matches_plate, py::arg("zarr"));
    m.def("reader_matches_well", &reader_matches_well, py::arg("zarr"));
    m.def("reader_labels_names", &reader_labels_names, py::arg("root_attrs"));
    m.def("reader_node_repr", &reader_node_repr, py::arg("zarr"), py::arg("visible"));
    m.def("reader_node_add_payload",
          &reader_node_add_payload,
          py::arg("already_seen"),
          py::arg("plate_labels"),
          py::arg("visibility"),
          py::arg("current_visibility"));
    m.def("reader_label_payload",
          &reader_label_payload,
          py::arg("root_attrs"),
          py::arg("name"),
          py::arg("visible"));
    m.def("reader_multiscales_payload",
          &reader_multiscales_payload,
          py::arg("root_attrs"));
    m.def("reader_omero_payload",
          &reader_omero_payload,
          py::arg("image_data"),
          py::arg("node_visible"));
    m.def("reader_well_payload", &reader_well_payload, py::arg("root_attrs"));
    m.def("reader_plate_payload", &reader_plate_payload, py::arg("root_attrs"));
    m.def("reader_plate_tile_path",
          &reader_plate_tile_path,
          py::arg("row_name"),
          py::arg("col_name"),
          py::arg("first_field_path"),
          py::arg("dataset_path"));
}
