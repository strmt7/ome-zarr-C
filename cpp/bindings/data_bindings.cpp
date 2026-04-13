#include <pybind11/pybind11.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "../native/data.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

void data_make_circle(
    py::int_ height,
    py::int_ width,
    py::object value,
    py::object target) {
    const auto points = ome_zarr_c::native_code::circle_points(
        py::cast<std::size_t>(height),
        py::cast<std::size_t>(width));
    for (const auto& point : points) {
        ome_zarr_c::bindings::set_item(target, py::make_tuple(point.y, point.x), value);
    }
}

py::object data_rgb_to_5d(py::object pixels) {
    py::object numpy = py::module_::import("numpy");
    py::tuple pixel_shape = py::cast<py::tuple>(pixels.attr("shape"));
    std::vector<std::size_t> native_shape;
    native_shape.reserve(py::len(pixel_shape));
    for (const py::handle& dim : pixel_shape) {
        native_shape.push_back(py::cast<std::size_t>(dim));
    }

    try {
        const auto channel_order = ome_zarr_c::native_code::rgb_channel_order(native_shape);
        if (native_shape.size() == 2) {
            py::object stack = numpy.attr("array")(py::make_tuple(pixels));
            py::object channels = numpy.attr("array")(py::make_tuple(stack));
            return numpy.attr("array")(py::make_tuple(channels));
        }

        py::list channels;
        for (const auto channel_index : channel_order) {
            py::object channel = pixels.attr("__getitem__")(
                py::make_tuple(
                    py::slice(py::none(), py::none(), py::none()),
                    py::slice(py::none(), py::none(), py::none()),
                    py::int_(channel_index)));
            channels.append(numpy.attr("array")(py::make_tuple(channel)));
        }
        return numpy.attr("array")(py::make_tuple(channels));
    } catch (const std::invalid_argument&) {
        PyErr_SetString(
            PyExc_AssertionError,
            ("expecting 2 or 3d: (" +
             py::cast<std::string>(py::str(pixels.attr("shape"))) + ")")
                .c_str());
        throw py::error_already_set();
    }
}

py::tuple data_coins() {
    py::object scipy_zoom = py::module_::import("scipy.ndimage").attr("zoom");
    py::object skimage_data = py::module_::import("skimage.data");
    py::object threshold_otsu =
        py::module_::import("skimage.filters").attr("threshold_otsu");
    py::object label = py::module_::import("skimage.measure").attr("label");
    py::object clear_border =
        py::module_::import("skimage.segmentation").attr("clear_border");
    py::object morphology = py::module_::import("skimage.morphology");
    py::object closing = morphology.attr("closing");
    py::object footprint_rectangle = morphology.attr("footprint_rectangle");
    py::object remove_small_objects = morphology.attr("remove_small_objects");
    const auto plan = ome_zarr_c::native_code::coins_plan();

    py::object image = skimage_data.attr("coins")().attr("__getitem__")(
        py::make_tuple(
            py::slice(
                static_cast<py::ssize_t>(plan.crop_margin),
                -static_cast<py::ssize_t>(plan.crop_margin),
                1),
            py::slice(
                static_cast<py::ssize_t>(plan.crop_margin),
                -static_cast<py::ssize_t>(plan.crop_margin),
                1)));
    py::object thresh = threshold_otsu(image);
    py::object bw = closing(
        py::reinterpret_borrow<py::object>(PyObject_RichCompare(
            image.ptr(), thresh.ptr(), Py_GT)),
        footprint_rectangle(
            py::make_tuple(plan.footprint_rows, plan.footprint_cols)));
    py::object cleared = remove_small_objects(
        clear_border(bw),
        py::arg("max_size") = plan.clear_border_max_size);
    py::object label_image = label(cleared);

    py::list pyramid;
    py::list labels;
    for (const long scale : plan.scales) {
        pyramid.append(
            scipy_zoom(image, py::int_(scale), py::arg("order") = plan.image_order));
        labels.append(
            scipy_zoom(
                label_image,
                py::int_(scale),
                py::arg("order") = plan.label_order));
    }

    return py::make_tuple(pyramid, labels);
}

py::tuple data_astronaut() {
    py::object numpy = py::module_::import("numpy");
    py::object skimage_data = py::module_::import("skimage.data");
    py::object core = py::module_::import("ome_zarr_c._core");
    const auto plan = ome_zarr_c::native_code::astronaut_plan();

    py::object astro = skimage_data.attr("astronaut")();
    py::list channels;
    for (const auto channel_index : plan.channel_indices) {
        channels.append(astro.attr("__getitem__")(
            py::make_tuple(
                py::slice(py::none(), py::none(), py::none()),
                py::slice(py::none(), py::none(), py::none()),
                py::int_(channel_index))));
    }
    astro = numpy.attr("array")(py::tuple(channels));
    py::tuple tile_repetitions(plan.tile_repetitions.size());
    for (py::size_t index = 0; index < plan.tile_repetitions.size(); ++index) {
        tile_repetitions[index] = py::int_(plan.tile_repetitions[index]);
    }
    py::object pixels = numpy.attr("tile")(astro, tile_repetitions);
    py::list pyramid = py::cast<py::list>(core.attr("scaler_nearest")(pixels));

    py::list shape = py::cast<py::list>(pyramid[0].attr("shape"));
    py::ssize_t y = py::cast<py::ssize_t>(shape[1]);
    py::ssize_t x = py::cast<py::ssize_t>(shape[2]);
    py::object label = numpy.attr("zeros")(
        py::make_tuple(y, x), py::arg("dtype") = numpy.attr("int8"));

    for (const auto& circle : plan.circles) {
        py::object target = label.attr("__getitem__")(
            py::make_tuple(
                py::slice(
                    static_cast<py::ssize_t>(circle.offset_y),
                    static_cast<py::ssize_t>(circle.offset_y + circle.height),
                    1),
                py::slice(
                    static_cast<py::ssize_t>(circle.offset_x),
                    static_cast<py::ssize_t>(circle.offset_x + circle.width),
                    1)));
        data_make_circle(
            py::int_(circle.height),
            py::int_(circle.width),
            py::int_(circle.value),
            target);
    }

    py::list labels = py::cast<py::list>(core.attr("scaler_nearest")(label));
    return py::make_tuple(pyramid, labels);
}

std::vector<std::size_t> native_shape_from_object(py::object array) {
    py::tuple shape = py::cast<py::tuple>(array.attr("shape"));
    std::vector<std::size_t> native_shape;
    native_shape.reserve(py::len(shape));
    for (const py::handle& dim : shape) {
        native_shape.push_back(py::cast<std::size_t>(dim));
    }
    return native_shape;
}

std::vector<std::size_t> native_chunks_from_object(py::object chunks) {
    std::vector<std::size_t> native_chunks;
    if (chunks.is_none()) {
        return native_chunks;
    }
    for (const py::handle& dim : py::iterable(chunks)) {
        native_chunks.push_back(py::cast<std::size_t>(dim));
    }
    return native_chunks;
}

py::object data_create_zarr(
    const std::string& zarr_directory,
    py::object method = py::none(),
    const std::string& label_name = "coins",
    py::object fmt = py::none(),
    py::object chunks = py::none()) {
    py::object data_module = py::module_::import("ome_zarr_c.data");
    py::object format_module = py::module_::import("ome_zarr_c.format");
    py::object io_module = py::module_::import("ome_zarr_c.io");
    py::object writer_module = py::module_::import("ome_zarr_c.writer");
    py::object zarr = py::module_::import("zarr");
    py::object random = py::module_::import("random");

    if (method.is_none()) {
        method = data_module.attr("coins");
    }
    if (fmt.is_none()) {
        fmt = format_module.attr("CurrentFormat")();
    }

    py::tuple generated = py::cast<py::tuple>(method());
    py::list pyramid = py::cast<py::list>(generated[0]);
    py::list labels = py::cast<py::list>(generated[1]);

    const std::string version = py::cast<std::string>(fmt.attr("version"));
    if (version == "0.1" || version == "0.2") {
        py::list converted_pyramid;
        for (const py::handle& level : pyramid) {
            converted_pyramid.append(data_rgb_to_5d(py::reinterpret_borrow<py::object>(level)));
        }
        pyramid = converted_pyramid;
        if (py::len(labels) > 0) {
            py::list converted_labels;
            for (const py::handle& level : labels) {
                converted_labels.append(data_rgb_to_5d(py::reinterpret_borrow<py::object>(level)));
            }
            labels = converted_labels;
        }
    }

    const auto plan = ome_zarr_c::native_code::create_zarr_plan(
        version,
        native_shape_from_object(pyramid[0]),
        native_shape_from_object(pyramid[py::len(pyramid) - 1]),
        native_chunks_from_object(chunks));

    py::object loc = io_module.attr("parse_url")(
        py::str(zarr_directory),
        py::arg("mode") = "w",
        py::arg("fmt") = fmt);
    if (loc.is_none()) {
        PyErr_SetString(PyExc_AssertionError, "not a zarr: None");
        throw py::error_already_set();
    }
    py::object grp = zarr.attr("group")(loc.attr("store"));

    py::tuple chunk_tuple(plan.chunks.size());
    for (py::size_t index = 0; index < plan.chunks.size(); ++index) {
        chunk_tuple[index] = py::int_(plan.chunks[index]);
    }
    py::dict storage_options;
    storage_options["chunks"] = chunk_tuple;

    py::dict image_data;
    py::list channels;
    for (const auto& channel_plan : plan.channels) {
        py::dict channel;
        py::dict window;
        window["start"] = py::int_(0);
        window["end"] = py::int_(255);
        window["min"] = py::int_(0);
        window["max"] = py::int_(255);
        channel["window"] = window;
        channel["color"] = py::str(channel_plan.color);
        if (channel_plan.has_label) {
            channel["label"] = py::str(channel_plan.label);
        }
        channel["active"] = py::bool_(true);
        channels.append(channel);
    }
    py::dict rdefs;
    rdefs["model"] = py::str(plan.channel_model);
    image_data["rdefs"] = rdefs;
    image_data["channels"] = channels;

    py::object axes = plan.axes_is_none
        ? py::object(py::none())
        : py::object(py::str(plan.axes));
    py::dict metadata;
    metadata["omero"] = image_data;
    writer_module.attr("write_multiscale")(
        pyramid,
        grp,
        py::arg("axes") = axes,
        py::arg("storage_options") = storage_options,
        py::arg("metadata") = metadata,
        py::arg("fmt") = fmt);

    if (py::len(labels) > 0) {
        py::object labels_grp = grp.attr("create_group")(py::str("labels"));
        py::dict labels_metadata;
        py::list label_names;
        label_names.append(py::str(label_name));
        labels_metadata["labels"] = label_names;
        writer_module.attr("add_metadata")(labels_grp, labels_metadata);

        py::object label_grp = labels_grp.attr("create_group")(py::str(label_name));
        py::object label_axes = plan.labels_axes_is_none
            ? py::object(py::none())
            : py::object(py::str(plan.labels_axes));
        writer_module.attr("write_multiscale")(
            labels,
            label_grp,
            py::arg("axes") = label_axes,
            py::arg("fmt") = fmt);

        py::list colors;
        py::list properties;
        for (std::size_t label_value = 1; label_value <= plan.random_label_count; ++label_value) {
            py::list rgba;
            for (int component = 0; component < 4; ++component) {
                rgba.append(random.attr("randrange")(py::int_(0), py::int_(256)));
            }
            py::dict color_entry;
            color_entry["label-value"] = py::int_(label_value);
            color_entry["rgba"] = rgba;
            colors.append(color_entry);

            py::dict property_entry;
            property_entry["label-value"] = py::int_(label_value);
            property_entry["class"] =
                py::str("class " + std::to_string(label_value));
            properties.append(property_entry);
        }

        py::dict source;
        source["image"] = py::str(plan.source_image);
        py::dict image_label;
        image_label["version"] = py::str(version);
        image_label["colors"] = colors;
        image_label["properties"] = properties;
        image_label["source"] = source;
        py::dict label_metadata;
        label_metadata["image-label"] = image_label;
        writer_module.attr("add_metadata")(label_grp, label_metadata);
    }

    return grp;
}

}  // namespace

void register_data_bindings(py::module_& m) {
    m.def("data_make_circle", &data_make_circle, py::arg("h"), py::arg("w"), py::arg("value"), py::arg("target"));
    m.def("data_rgb_to_5d", &data_rgb_to_5d, py::arg("pixels"));
    m.def("data_coins", &data_coins);
    m.def("data_astronaut", &data_astronaut);
    m.def(
        "data_create_zarr",
        &data_create_zarr,
        py::arg("zarr_directory"),
        py::arg("method") = py::none(),
        py::arg("label_name") = "coins",
        py::arg("fmt") = py::none(),
        py::arg("chunks") = py::none());
}
