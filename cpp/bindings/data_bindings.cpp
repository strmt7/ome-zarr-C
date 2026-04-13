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

}  // namespace

void register_data_bindings(py::module_& m) {
    m.def("data_make_circle", &data_make_circle, py::arg("h"), py::arg("w"), py::arg("value"), py::arg("target"));
    m.def("data_rgb_to_5d", &data_rgb_to_5d, py::arg("pixels"));
    m.def("data_coins", &data_coins);
    m.def("data_astronaut", &data_astronaut);
}
