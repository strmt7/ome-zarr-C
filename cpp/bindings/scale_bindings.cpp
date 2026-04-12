#include <pybind11/pybind11.h>

#include <array>
#include <cstdint>
#include <functional>
#include <map>

#include "../native/scale.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

py::list scaler_methods() {
    py::list methods;
    for (const auto& method : ome_zarr_c::native_code::scaler_methods()) {
        methods.append(py::str(method));
    }
    return methods;
}

py::list build_pyramid(
    py::object image,
    py::object scale_factors,
    py::object dims,
    py::object method = py::str("nearest"),
    py::object chunks = py::none()) {
    py::object numpy = py::module_::import("numpy");
    py::object dask_array = py::module_::import("dask.array");
    py::object builtins = py::module_::import("builtins");
    py::object warnings = py::module_::import("warnings");
    py::object core = py::module_::import("ome_zarr_c._core");
    py::tuple dims_tuple = py::tuple(dims);
    std::vector<std::string> native_dims;
    native_dims.reserve(py::len(dims_tuple));
    for (const py::handle& dim_handle : dims_tuple) {
        native_dims.push_back(py::cast<std::string>(dim_handle));
    }

    if (py::isinstance(image, numpy.attr("ndarray"))) {
        if (!chunks.is_none()) {
            image = dask_array.attr("from_array")(image, py::arg("chunks") = chunks);
        } else {
            image = dask_array.attr("from_array")(image);
        }
    }

    std::string method_key;
    if (py::isinstance<py::str>(method)) {
        method_key = py::cast<std::string>(method);
    } else if (py::hasattr(method, "value") &&
               py::isinstance<py::str>(method.attr("value"))) {
        method_key = py::cast<std::string>(method.attr("value"));
    }

    bool all_int_scale_factors =
        py::isinstance<py::list>(scale_factors) || py::isinstance<py::tuple>(scale_factors);
    if (all_int_scale_factors) {
        for (const py::handle& scale_factor : py::iterable(scale_factors)) {
            if (!PyLong_Check(scale_factor.ptr())) {
                all_int_scale_factors = false;
                break;
            }
        }
    }

    std::vector<ome_zarr_c::native_code::ScaleLevel> native_scale_levels;
    if (all_int_scale_factors) {
        native_scale_levels = ome_zarr_c::native_code::scale_levels_from_ints(
            native_dims,
            static_cast<std::size_t>(py::len(scale_factors)));
    } else {
        std::vector<std::map<std::string, double>> input_levels;
        input_levels.reserve(py::len(scale_factors));
        for (py::ssize_t index = 0; index < py::len(scale_factors); ++index) {
            py::object level = scale_factors.attr("__getitem__")(index);
            py::dict reordered_level;
            std::map<std::string, double> native_level;
            for (const py::handle& dim_handle : dims_tuple) {
                py::object value = level.attr("get")(dim_handle, py::int_(1));
                reordered_level[dim_handle] = value;
                native_level[py::cast<std::string>(dim_handle)] = py::cast<double>(value);
            }
            ome_zarr_c::bindings::set_item(scale_factors, py::int_(index), reordered_level);
            input_levels.push_back(std::move(native_level));
        }
        native_scale_levels = ome_zarr_c::native_code::reorder_scale_levels(
            native_dims,
            input_levels);
    }

    std::vector<std::int64_t> base_shape;
    py::tuple image_shape = py::cast<py::tuple>(image.attr("shape"));
    base_shape.reserve(py::len(image_shape));
    for (const py::handle& dim : image_shape) {
        base_shape.push_back(py::cast<std::int64_t>(dim));
    }
    const auto pyramid_plan = ome_zarr_c::native_code::build_pyramid_plan(
        base_shape,
        native_dims,
        native_scale_levels);

    py::list images;
    images.append(image);

    for (const auto& plan : pyramid_plan) {
        for (const auto& warning_dim : plan.warning_dims) {
            warnings.attr("warn")(
                "Dimension " + warning_dim + " is too small to downsample further.",
                builtins.attr("UserWarning"),
                py::arg("stacklevel") = 3);
        }

        py::tuple target_shape(plan.target_shape.size());
        for (py::size_t dim_index = 0; dim_index < plan.target_shape.size(); ++dim_index) {
            target_shape[dim_index] = py::int_(plan.target_shape[dim_index]);
        }

        py::object current_image = images[py::len(images) - 1];
        py::object new_image;
        if (method_key == "resize") {
            new_image = core.attr("resize")(
                current_image,
                target_shape,
                py::arg("order") = 1,
                py::arg("mode") = "reflect",
                py::arg("anti_aliasing") = true,
                py::arg("preserve_range") = true);
        } else if (method_key == "nearest") {
            new_image = core.attr("resize")(
                current_image,
                target_shape,
                py::arg("order") = 0,
                py::arg("mode") = "reflect",
                py::arg("anti_aliasing") = false,
                py::arg("preserve_range") = true);
        } else if (method_key == "local_mean") {
            new_image = core.attr("local_mean")(current_image, target_shape);
        } else if (method_key == "zoom") {
            new_image = core.attr("zoom")(current_image, target_shape);
        } else {
            throw py::value_error(
                "Unknown downsampling method: " +
                py::cast<std::string>(py::str(method)));
        }

        images.append(new_image);
    }

    return images;
}

py::object scaler_resize_image(
    py::object image,
    py::int_ downscale = py::int_(2),
    py::int_ order = py::int_(1)) {
    py::object dask_array = py::module_::import("dask.array");
    py::object skimage_transform = py::module_::import("skimage.transform");
    py::object builtins = py::module_::import("builtins");

    py::object resize_func = skimage_transform.attr("resize");
    if (py::isinstance(image, dask_array.attr("Array"))) {
        resize_func = py::module_::import("ome_zarr_c._core").attr("resize");
    }

    py::tuple image_shape = py::cast<py::tuple>(image.attr("shape"));
    std::vector<std::int64_t> native_shape;
    native_shape.reserve(py::len(image_shape));
    for (const py::handle& dim : image_shape) {
        native_shape.push_back(py::cast<std::int64_t>(dim));
    }
    const auto output_shape = ome_zarr_c::native_code::scaler_resize_image_shape(
        native_shape,
        py::cast<std::int64_t>(downscale));
    py::tuple out_shape(output_shape.size());
    for (py::size_t index = 0; index < output_shape.size(); ++index) {
        out_shape[index] = py::int_(output_shape[index]);
    }

    py::object dtype = image.attr("dtype");
    py::object resized = resize_func(
        image.attr("astype")(builtins.attr("float")),
        out_shape,
        py::arg("order") = order,
        py::arg("mode") = "reflect",
        py::arg("anti_aliasing") = false);
    return resized.attr("astype")(dtype);
}

py::list scaler_by_plane(
    py::object base,
    const std::function<py::object(py::object, py::ssize_t, py::ssize_t)>& transform,
    py::int_ max_layer = py::int_(4)) {
    py::object numpy = py::module_::import("numpy");
    py::list rv;
    rv.append(base);

    for (py::ssize_t level_index = 0; level_index < max_layer; ++level_index) {
        py::object stack_to_scale = rv[py::len(rv) - 1];
        const py::ssize_t stack_ndim = py::cast<py::ssize_t>(stack_to_scale.attr("ndim"));
        py::tuple stack_shape = py::cast<py::tuple>(stack_to_scale.attr("shape"));

        std::array<py::ssize_t, 5> shape_5d = {1, 1, 1, 1, 1};
        for (py::ssize_t dim_index = 0; dim_index < stack_ndim; ++dim_index) {
            shape_5d[5 - stack_ndim + dim_index] = py::cast<py::ssize_t>(stack_shape[dim_index]);
        }

        const py::ssize_t T = shape_5d[0];
        const py::ssize_t C = shape_5d[1];
        const py::ssize_t Z = shape_5d[2];
        const py::ssize_t Y = shape_5d[3];
        const py::ssize_t X = shape_5d[4];

        if (stack_ndim == 2) {
            rv.append(transform(stack_to_scale, Y, X));
            continue;
        }

        const py::ssize_t stack_dims = stack_ndim - 2;
        py::object new_stack = py::none();

        for (py::ssize_t t = 0; t < T; ++t) {
            for (py::ssize_t c = 0; c < C; ++c) {
                for (py::ssize_t z = 0; z < Z; ++z) {
                    const std::array<py::ssize_t, 3> indices = {t, c, z};
                    py::tuple dims_to_slice(stack_dims);
                    for (py::ssize_t dim_index = 0; dim_index < stack_dims; ++dim_index) {
                        dims_to_slice[dim_index] =
                            py::int_(indices[3 - stack_dims + dim_index]);
                    }

                    py::object plane = stack_to_scale.attr("__getitem__")(dims_to_slice);
                    py::object out = transform(plane, Y, X);

                    if (new_stack.is_none()) {
                        py::tuple out_shape = py::cast<py::tuple>(out.attr("shape"));
                        py::tuple new_shape(stack_dims + 2);
                        for (py::ssize_t dim_index = 0; dim_index < stack_dims; ++dim_index) {
                            new_shape[dim_index] =
                                py::int_(shape_5d[3 - stack_dims + dim_index]);
                        }
                        new_shape[stack_dims] = out_shape[0];
                        new_shape[stack_dims + 1] = out_shape[1];
                        new_stack = numpy.attr("zeros")(
                            new_shape, py::arg("dtype") = base.attr("dtype"));
                    }

                    ome_zarr_c::bindings::set_item(new_stack, dims_to_slice, out);
                }
            }
        }

        rv.append(new_stack);
    }

    return rv;
}

py::object scaler_nearest_plane(
    py::object plane,
    py::ssize_t size_y,
    py::ssize_t size_x,
    py::int_ downscale = py::int_(2)) {
    py::object dask_array = py::module_::import("dask.array");
    py::object resize_func = py::module_::import("skimage.transform").attr("resize");
    if (py::isinstance(plane, dask_array.attr("Array"))) {
        resize_func = py::module_::import("ome_zarr_c._core").attr("resize");
    }

    const auto native_shape = ome_zarr_c::native_code::scaler_nearest_plane_shape(
        size_y,
        size_x,
        py::cast<std::int64_t>(downscale));
    py::tuple output_shape(native_shape.size());
    for (py::size_t index = 0; index < native_shape.size(); ++index) {
        output_shape[index] = py::int_(native_shape[index]);
    }

    return resize_func(
               plane,
               output_shape,
               py::arg("order") = 0,
               py::arg("preserve_range") = true,
               py::arg("anti_aliasing") = false)
        .attr("astype")(plane.attr("dtype"));
}

py::list scaler_nearest(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    return scaler_by_plane(
        base,
        [downscale](py::object plane, py::ssize_t size_y, py::ssize_t size_x) {
            return scaler_nearest_plane(plane, size_y, size_x, downscale);
        },
        max_layer);
}

py::list scaler_local_mean(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    py::object downscale_local_mean =
        py::module_::import("skimage.transform").attr("downscale_local_mean");
    py::list rv;
    rv.append(base);

    const auto factor_values = ome_zarr_c::native_code::scaler_local_mean_factors(
        py::cast<std::size_t>(base.attr("ndim")),
        py::cast<std::int64_t>(downscale));
    py::tuple factors(factor_values.size());
    for (py::size_t index = 0; index < factor_values.size(); ++index) {
        factors[index] = py::int_(factor_values[index]);
    }

    for (py::ssize_t level_index = 0; level_index < max_layer; ++level_index) {
        py::object next_level =
            downscale_local_mean(rv[py::len(rv) - 1], py::arg("factors") = factors)
                .attr("astype")(base.attr("dtype"));
        rv.append(next_level);
    }

    return rv;
}

py::list scaler_zoom(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    py::object scipy_zoom = py::module_::import("scipy.ndimage").attr("zoom");
    py::list rv;
    rv.append(base);
    py::print(base.attr("shape"));
    const auto zoom_factors = ome_zarr_c::native_code::scaler_zoom_factors(
        py::cast<long>(downscale),
        py::cast<long>(max_layer));
    for (py::size_t level_index = 0; level_index < zoom_factors.size(); ++level_index) {
        py::print(py::int_(level_index), downscale);
        const long zoom_factor = zoom_factors[level_index];
        rv.append(scipy_zoom(base, py::int_(zoom_factor)));
        py::print(rv[py::len(rv) - 1].attr("shape"));
    }

    py::list reversed_result;
    for (py::ssize_t index = py::len(rv) - 1; index >= 0; --index) {
        reversed_result.append(rv[index]);
        if (index == 0) {
            break;
        }
    }
    return reversed_result;
}

}  // namespace

void register_scale_bindings(py::module_& m) {
    m.def("scaler_methods", &scaler_methods);
    m.def("_build_pyramid",
          &build_pyramid,
          py::arg("image"),
          py::arg("scale_factors"),
          py::arg("dims"),
          py::arg("method") = py::str("nearest"),
          py::arg("chunks") = py::none());
    m.def("scaler_resize_image",
          &scaler_resize_image,
          py::arg("image"),
          py::arg("downscale") = 2,
          py::arg("order") = 1);
    m.def("scaler_nearest",
          &scaler_nearest,
          py::arg("base"),
          py::arg("downscale") = 2,
          py::arg("max_layer") = 4);
    m.def("scaler_local_mean",
          &scaler_local_mean,
          py::arg("base"),
          py::arg("downscale") = 2,
          py::arg("max_layer") = 4);
    m.def("scaler_zoom",
          &scaler_zoom,
          py::arg("base"),
          py::arg("downscale") = 2,
          py::arg("max_layer") = 4);
}
