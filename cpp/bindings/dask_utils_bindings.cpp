#include <pybind11/pybind11.h>

#include <cstdint>
#include <vector>

#include "../native/dask_utils.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

std::vector<std::int64_t> int64_vector_from_sequence(const py::handle& values) {
    std::vector<std::int64_t> native_values;
    const py::sequence sequence = py::cast<py::sequence>(values);
    native_values.reserve(py::len(sequence));
    for (const py::handle& value : sequence) {
        native_values.push_back(py::cast<std::int64_t>(value));
    }
    return native_values;
}

std::vector<double> double_vector_from_sequence(const py::handle& values) {
    std::vector<double> native_values;
    const py::sequence sequence = py::cast<py::sequence>(values);
    native_values.reserve(py::len(sequence));
    for (const py::handle& value : sequence) {
        native_values.push_back(py::cast<double>(value));
    }
    return native_values;
}

py::tuple numpy_int64_tuple(const std::vector<std::int64_t>& values) {
    py::object numpy_int64 = py::module_::import("numpy").attr("int64");
    py::tuple tuple(values.size());
    for (py::size_t index = 0; index < values.size(); ++index) {
        tuple[index] = numpy_int64(values[index]);
    }
    return tuple;
}

py::tuple int_tuple(const std::vector<std::int64_t>& values) {
    py::tuple tuple(values.size());
    for (py::size_t index = 0; index < values.size(); ++index) {
        tuple[index] = py::int_(values[index]);
    }
    return tuple;
}

py::tuple double_tuple(const std::vector<double>& values) {
    py::tuple tuple(values.size());
    for (py::size_t index = 0; index < values.size(); ++index) {
        tuple[index] = py::float_(values[index]);
    }
    return tuple;
}

py::tuple better_chunksize(py::object image, py::object factors) {
    const auto plan = ome_zarr_c::native_code::better_chunksize(
        int64_vector_from_sequence(image.attr("chunksize")),
        double_vector_from_sequence(factors));
    return py::make_tuple(
        numpy_int64_tuple(plan.better_chunks),
        numpy_int64_tuple(plan.block_output_shape));
}

py::object dask_resize(
    py::object image,
    py::object output_shape,
    py::args args,
    py::kwargs kwargs) {
    py::object dask_array = py::module_::import("dask.array");
    py::object skimage_resize =
        py::module_::import("skimage.transform").attr("resize");
    const auto plan = ome_zarr_c::native_code::resize_plan(
        int64_vector_from_sequence(image.attr("shape")),
        int64_vector_from_sequence(image.attr("chunksize")),
        int64_vector_from_sequence(output_shape));
    py::object image_prepared =
        image.attr("rechunk")(int_tuple(plan.chunk_plan.better_chunks));
    py::tuple extra_args = py::reinterpret_borrow<py::tuple>(args);
    py::dict call_kwargs = py::reinterpret_borrow<py::dict>(kwargs);

    py::object resize_block = py::cpp_function(
        [factors = plan.factors, skimage_resize, extra_args, call_kwargs](
            py::object image_block,
            py::object block_info = py::none()) {
            static_cast<void>(block_info);
            py::object resized = ome_zarr_c::bindings::call_callable(
                skimage_resize,
                {
                    image_block,
                    int_tuple(ome_zarr_c::native_code::block_output_shape(
                        int64_vector_from_sequence(image_block.attr("shape")),
                        factors)),
                },
                extra_args,
                call_kwargs);
            return resized.attr("astype")(image_block.attr("dtype"));
        },
        py::arg("image_block"),
        py::arg("block_info") = py::none());

    py::object output = dask_array
                            .attr("map_blocks")(
                                resize_block,
                                image_prepared,
                                py::arg("dtype") = image.attr("dtype"),
                                py::arg("chunks") =
                                    int_tuple(plan.chunk_plan.block_output_shape))
                            .attr("__getitem__")(
                                ome_zarr_c::bindings::output_slices_for_shape(
                                    output_shape));
    return output.attr("rechunk")(image.attr("chunksize"))
        .attr("astype")(image.attr("dtype"));
}

py::object dask_local_mean(
    py::object image,
    py::object output_shape,
    py::args args,
    py::kwargs kwargs) {
    py::object dask_array = py::module_::import("dask.array");
    py::object downscale_local_mean =
        py::module_::import("skimage.transform").attr("downscale_local_mean");
    const auto plan = ome_zarr_c::native_code::local_mean_plan(
        int64_vector_from_sequence(image.attr("shape")),
        int64_vector_from_sequence(image.attr("chunksize")),
        int64_vector_from_sequence(output_shape));
    py::object image_prepared =
        image.attr("rechunk")(int_tuple(plan.chunk_plan.better_chunks));
    py::tuple extra_args = py::reinterpret_borrow<py::tuple>(args);
    py::dict call_kwargs = py::reinterpret_borrow<py::dict>(kwargs);
    py::tuple factor_tuple = int_tuple(plan.int_factors);

    py::object local_mean_block = py::cpp_function(
        [downscale_local_mean, factor_tuple, extra_args, call_kwargs](
            py::object image_block) {
            py::object reduced = ome_zarr_c::bindings::call_callable(
                downscale_local_mean,
                {image_block, factor_tuple},
                extra_args,
                call_kwargs);
            return reduced.attr("astype")(image_block.attr("dtype"));
        },
        py::arg("image_block"));

    py::object output = dask_array
                            .attr("map_blocks")(
                                local_mean_block,
                                image_prepared,
                                py::arg("dtype") = image.attr("dtype"),
                                py::arg("chunks") =
                                    int_tuple(plan.chunk_plan.block_output_shape))
                            .attr("__getitem__")(
                                ome_zarr_c::bindings::output_slices_for_shape(
                                    output_shape));
    return output.attr("rechunk")(image.attr("chunksize"))
        .attr("astype")(image.attr("dtype"));
}

py::object dask_zoom(
    py::object image,
    py::object output_shape,
    py::args args,
    py::kwargs kwargs) {
    static_cast<void>(args);
    static_cast<void>(kwargs);
    py::object dask_array = py::module_::import("dask.array");
    py::object scipy_zoom = py::module_::import("scipy.ndimage").attr("zoom");
    const auto plan = ome_zarr_c::native_code::zoom_plan(
        int64_vector_from_sequence(image.attr("shape")),
        int64_vector_from_sequence(image.attr("chunksize")),
        int64_vector_from_sequence(output_shape));
    py::object image_prepared =
        image.attr("rechunk")(int_tuple(plan.chunk_plan.better_chunks));

    py::object zoom_block = py::cpp_function(
        [inverse_factors = plan.inverse_factors, scipy_zoom](py::object image_block) {
            py::object zoomed = scipy_zoom(
                image_block,
                double_tuple(inverse_factors),
                py::arg("order") = 1);
            return zoomed.attr("astype")(image_block.attr("dtype"));
        },
        py::arg("image_block"));

    py::object output = dask_array
                            .attr("map_blocks")(
                                zoom_block,
                                image_prepared,
                                py::arg("dtype") = image.attr("dtype"),
                                py::arg("chunks") =
                                    int_tuple(plan.chunk_plan.block_output_shape))
                            .attr("__getitem__")(
                                ome_zarr_c::bindings::output_slices_for_shape(
                                    int_tuple(plan.resized_output_shape)));
    return output.attr("rechunk")(image.attr("chunksize"))
        .attr("astype")(image.attr("dtype"));
}

py::object downscale_nearest(py::object image, py::object factors) {
    py::tuple factor_tuple = py::tuple(factors);
    py::tuple shape = py::cast<py::tuple>(image.attr("shape"));
    const py::ssize_t factor_count = py::len(factor_tuple);
    const py::ssize_t ndim = py::cast<py::ssize_t>(image.attr("ndim"));

    if (factor_count != ndim) {
        throw py::value_error(
            "Dimension mismatch: " +
            py::cast<std::string>(py::str(image.attr("ndim"))) +
            " image dimensions, " + std::to_string(factor_count) +
            " scale factors");
    }

    std::vector<std::int64_t> native_shape;
    std::vector<std::int64_t> native_factors;
    native_shape.reserve(static_cast<std::size_t>(py::len(shape)));
    native_factors.reserve(static_cast<std::size_t>(factor_count));
    bool valid_types = true;
    for (py::size_t index = 0; index < static_cast<py::size_t>(py::len(shape)); ++index) {
        native_shape.push_back(py::cast<std::int64_t>(shape[index]));
    }
    for (py::size_t index = 0; index < static_cast<py::size_t>(factor_count); ++index) {
        const py::handle factor = factor_tuple[index];
        if (!PyLong_Check(factor.ptr())) {
            valid_types = false;
            break;
        }
        native_factors.push_back(py::cast<std::int64_t>(factor));
    }

    if (!valid_types) {
        throw py::value_error(
            "All scale factors must not be greater than the dimension length: (" +
            py::cast<std::string>(py::str(factor_tuple)) + ") <= (" +
            py::cast<std::string>(py::str(shape)) + ")");
    }

    try {
        ome_zarr_c::native_code::validate_downscale_nearest(
            native_shape,
            native_factors);
    } catch (const std::invalid_argument&) {
        throw py::value_error(
            "All scale factors must not be greater than the dimension length: (" +
            py::cast<std::string>(py::str(factor_tuple)) + ") <= (" +
            py::cast<std::string>(py::str(shape)) + ")");
    }

    py::tuple slices(static_cast<py::size_t>(factor_count));
    for (py::size_t index = 0; index < static_cast<py::size_t>(factor_count); ++index) {
        slices[index] = py::slice(py::none(), py::none(), factor_tuple[index]);
    }
    return image.attr("__getitem__")(slices);
}

}  // namespace

void register_dask_utils_bindings(py::module_& m) {
    m.def("_better_chunksize", &better_chunksize, py::arg("image"), py::arg("factors"));
    m.def("resize", &dask_resize, py::arg("image"), py::arg("output_shape"));
    m.def("local_mean", &dask_local_mean, py::arg("image"), py::arg("output_shape"));
    m.def("zoom", &dask_zoom, py::arg("image"), py::arg("output_shape"));
    m.def("downscale_nearest", &downscale_nearest, py::arg("image"), py::arg("factors"));
}
