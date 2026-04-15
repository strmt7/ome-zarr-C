#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include <cmath>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "../native/data.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

py::object numpy_module() {
    static py::object module = py::module_::import("numpy");
    return module;
}

void raise_boolean_mask_shape_error(
    const py::buffer_info& info,
    py::ssize_t expected_height,
    py::ssize_t expected_width) {
    if (info.ndim < 1 || info.shape[0] != expected_height) {
        throw py::index_error(
            "boolean index did not match indexed array along axis 0; size of axis is " +
            std::to_string(info.ndim < 1 ? 0 : info.shape[0]) +
            " but size of corresponding boolean axis is " +
            std::to_string(expected_height));
    }
    if (info.ndim < 2 || info.shape[1] != expected_width) {
        throw py::index_error(
            "boolean index did not match indexed array along axis 1; size of axis is " +
            std::to_string(info.ndim < 2 ? 0 : info.shape[1]) +
            " but size of corresponding boolean axis is " +
            std::to_string(expected_width));
    }
}

void data_make_circle(
    py::int_ height,
    py::int_ width,
    py::object value,
    py::array target) {
    py::buffer_info target_info = target.request();
    const py::ssize_t native_height = py::cast<py::ssize_t>(height);
    const py::ssize_t native_width = py::cast<py::ssize_t>(width);
    raise_boolean_mask_shape_error(target_info, native_height, native_width);

    py::array scalar = numpy_module().attr("array")(
        py::make_tuple(value),
        py::arg("dtype") = target.attr("dtype"));
    py::buffer_info scalar_info = scalar.request();

    const auto points = ome_zarr_c::native_code::circle_points(
        py::cast<std::size_t>(height),
        py::cast<std::size_t>(width));
    auto* base = static_cast<char*>(target_info.ptr);
    for (const auto& point : points) {
        char* destination = base +
            static_cast<py::ssize_t>(point.y) * target_info.strides[0] +
            static_cast<py::ssize_t>(point.x) * target_info.strides[1];
        std::memcpy(destination, scalar_info.ptr, target_info.itemsize);
    }
}

py::array data_rgb_to_5d(py::array pixels) {
    py::buffer_info input = pixels.request();
    std::vector<std::size_t> native_shape;
    native_shape.reserve(input.shape.size());
    for (const py::ssize_t dim : input.shape) {
        native_shape.push_back(static_cast<std::size_t>(dim));
    }

    try {
        const auto channel_order = ome_zarr_c::native_code::rgb_channel_order(native_shape);
        if (native_shape.size() == 2) {
            py::array output(
                pixels.dtype(),
                std::vector<py::ssize_t>{1, 1, 1, input.shape[0], input.shape[1]});
            py::buffer_info out = output.request();
            auto* in_base = static_cast<const char*>(input.ptr);
            auto* out_base = static_cast<char*>(out.ptr);
            for (py::ssize_t y = 0; y < input.shape[0]; ++y) {
                for (py::ssize_t x = 0; x < input.shape[1]; ++x) {
                    const char* source =
                        in_base + y * input.strides[0] + x * input.strides[1];
                    char* destination =
                        out_base + y * out.strides[3] + x * out.strides[4];
                    std::memcpy(destination, source, input.itemsize);
                }
            }
            return output;
        }

        py::array output(
            pixels.dtype(),
            std::vector<py::ssize_t>{
                1,
                static_cast<py::ssize_t>(channel_order.size()),
                1,
                input.shape[0],
                input.shape[1],
            });
        py::buffer_info out = output.request();
        auto* in_base = static_cast<const char*>(input.ptr);
        auto* out_base = static_cast<char*>(out.ptr);
        for (const auto channel_index : channel_order) {
            for (py::ssize_t y = 0; y < input.shape[0]; ++y) {
                for (py::ssize_t x = 0; x < input.shape[1]; ++x) {
                    const char* source = in_base + y * input.strides[0] +
                        x * input.strides[1] +
                        static_cast<py::ssize_t>(channel_index) * input.strides[2];
                    char* destination = out_base +
                        static_cast<py::ssize_t>(channel_index) * out.strides[1] +
                        y * out.strides[3] +
                        x * out.strides[4];
                    std::memcpy(destination, source, input.itemsize);
                }
            }
        }
        return output;
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
