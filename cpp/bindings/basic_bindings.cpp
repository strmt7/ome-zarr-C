#include <pybind11/pybind11.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "../native/axes.hpp"
#include "../native/csv.hpp"
#include "../native/format.hpp"
#include "../native/utils.hpp"
#include "../native/writer.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

bool is_number_like(const py::handle& value) {
    return PyFloat_Check(value.ptr()) || PyLong_Check(value.ptr());
}

std::string format_version_string(const py::object& fmt_or_version) {
    if (py::isinstance<py::str>(fmt_or_version)) {
        return py::cast<std::string>(fmt_or_version);
    }
    return py::cast<std::string>(fmt_or_version.attr("version"));
}

[[noreturn]] void raise_coordinate_transformations_validation_error(
    const ome_zarr_c::native_code::CoordinateTransformationsValidationError& error,
    int ndim,
    int nlevels,
    const py::list& coordinate_transformations) {
    switch (error.code()) {
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::count_mismatch:
            throw py::value_error(
                "coordinate_transformations count: " +
                std::to_string(error.actual_count()) +
                " must match datasets " +
                std::to_string(nlevels));
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::missing_type:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
            throw py::value_error(
                "Missing type in: " + ome_zarr_c::bindings::repr_object(group));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::invalid_scale_count:
            throw py::value_error(
                "Must supply 1 'scale' item in coordinate_transformations");
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::first_not_scale:
            throw py::value_error(
                "First coordinate_transformations must be 'scale'");
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::missing_scale_argument:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
                py::object first = py::cast<py::list>(group)[py::int_(0)];
            throw py::value_error(
                "Missing scale argument in: " +
                ome_zarr_c::bindings::repr_object(first));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::scale_length_mismatch:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
                py::object first = py::cast<py::list>(group)[py::int_(0)];
            throw py::value_error(
                "'scale' list " +
                ome_zarr_c::bindings::repr_object(
                    first.attr("__getitem__")(py::str("scale"))) +
                " must match number of image dimensions: " +
                std::to_string(ndim));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::scale_non_numeric:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
                py::object first = py::cast<py::list>(group)[py::int_(0)];
            throw py::value_error(
                "'scale' values must all be numbers: " +
                ome_zarr_c::bindings::repr_object(
                    first.attr("__getitem__")(py::str("scale"))));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::invalid_translation_count:
            throw py::value_error(
                "Must supply 0 or 1 'translation' item incoordinate_transformations");
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::missing_translation_argument:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
                py::object first = py::cast<py::list>(group)[py::int_(0)];
            throw py::value_error(
                "Missing scale argument in: " +
                ome_zarr_c::bindings::repr_object(first));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::translation_length_mismatch:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
            throw py::value_error(
                "'translation' list " +
                ome_zarr_c::bindings::repr_object(
                    py::cast<py::list>(group)[py::int_(error.transformation_index())]
                        .attr("__getitem__")(py::str("translation"))) +
                " must match image dimensions count: " +
                std::to_string(ndim));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::translation_non_numeric:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
            throw py::value_error(
                "'translation' values must all be numbers: " +
                ome_zarr_c::bindings::repr_object(
                    py::cast<py::list>(group)[py::int_(error.transformation_index())]
                        .attr("__getitem__")(py::str("translation"))));
            }
    }

    throw py::value_error("Unknown coordinate transformation validation error");
}

[[noreturn]] void raise_dataset_validation_error(
    const ome_zarr_c::native_code::DatasetValidationError& error,
    const py::object& datasets) {
    switch (error.code()) {
        case ome_zarr_c::native_code::DatasetValidationErrorCode::empty_datasets:
            throw py::value_error("Empty datasets list");
        case ome_zarr_c::native_code::DatasetValidationErrorCode::unrecognized_type:
            {
                py::sequence sequence = py::cast<py::sequence>(datasets);
                py::object dataset = sequence[py::int_(error.dataset_index())];
                throw py::value_error(
                    "Unrecognized type for " +
                    py::cast<std::string>(py::str(dataset)));
            }
        case ome_zarr_c::native_code::DatasetValidationErrorCode::missing_path:
            throw py::value_error("no 'path' in dataset");
    }

    throw py::value_error("Unknown dataset validation error");
}

ome_zarr_c::native_code::CoordinateTransformationsValidationInput
coordinate_transformations_validation_input(py::handle transformations_handle) {
    py::list transformations = py::cast<py::list>(transformations_handle);
    ome_zarr_c::native_code::CoordinateTransformationsValidationInput native_group;
    native_group.transformations.reserve(py::len(transformations));
    py::str scale_text("scale");
    py::str translation_text("translation");

    for (const py::handle& transformation_handle : transformations) {
        py::dict transformation = py::cast<py::dict>(transformation_handle);
        ome_zarr_c::native_code::CoordinateTransformationValidationInput
            native_transformation;
        native_transformation.has_type = false;
        native_transformation.kind =
            ome_zarr_c::native_code::CoordinateTransformationKind::other;
        native_transformation.has_scale = false;
        native_transformation.scale_length = 0;
        native_transformation.scale_all_numeric = true;
        native_transformation.has_translation = false;
        native_transformation.translation_length = 0;
        native_transformation.translation_all_numeric = true;

        py::object type = transformation.attr("get")("type", py::none());
        if (!type.is_none()) {
            native_transformation.has_type = true;
            native_transformation.kind = ome_zarr_c::native_code::CoordinateTransformationKind::other;
            if (ome_zarr_c::bindings::objects_equal(type, scale_text)) {
                native_transformation.kind =
                    ome_zarr_c::native_code::CoordinateTransformationKind::scale;
            } else if (ome_zarr_c::bindings::objects_equal(type, translation_text)) {
                native_transformation.kind =
                    ome_zarr_c::native_code::CoordinateTransformationKind::translation;
            }
        }

        if (transformation.contains("scale")) {
            native_transformation.has_scale = true;
            py::sequence scale_values = py::cast<py::sequence>(transformation["scale"]);
            native_transformation.scale_length = py::len(scale_values);
            for (const py::handle& value : scale_values) {
                native_transformation.scale_all_numeric =
                    native_transformation.scale_all_numeric && is_number_like(value);
            }
        }

        if (transformation.contains("translation")) {
            native_transformation.has_translation = true;
            py::sequence translation_values =
                py::cast<py::sequence>(transformation["translation"]);
            native_transformation.translation_length = py::len(translation_values);
            for (const py::handle& value : translation_values) {
                native_transformation.translation_all_numeric =
                    native_transformation.translation_all_numeric &&
                    is_number_like(value);
            }
        }

        native_group.transformations.push_back(std::move(native_transformation));
    }

    return native_group;
}

py::object get_valid_axes(
    py::object ndim = py::none(),
    py::object axes = py::none(),
    py::object fmt = py::none()) {
    static py::object logger =
        py::module_::import("logging").attr("getLogger")(py::str("ome_zarr.writer"));

    const std::string version = format_version_string(fmt);
    std::optional<std::int64_t> native_ndim;
    if (!ndim.is_none()) {
        native_ndim = py::cast<std::int64_t>(ndim);
    }

    try {
        const auto plan = ome_zarr_c::native_code::get_valid_axes_plan(
            version,
            !axes.is_none(),
            native_ndim);
        if (plan.log_ignored_axes) {
            logger.attr("info")("axes ignored for version 0.1 or 0.2");
        }
        if (plan.return_none) {
            return py::none();
        }
        if (axes.is_none()) {
            py::list guessed_axes;
            for (const auto& axis_name : plan.axes) {
                guessed_axes.append(py::str(axis_name));
            }
            axes = guessed_axes;
        }
        if (plan.log_auto_axes) {
            logger.attr("info")(
                "Auto using axes %s for " + plan.auto_label + " data",
                axes);
        }
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }

    std::vector<ome_zarr_c::native_code::AxisRecord> records;
    if (PyUnicode_Check(axes.ptr())) {
        const std::string axis_string = py::cast<std::string>(axes);
        records.reserve(axis_string.size());
        for (const char axis : axis_string) {
            ome_zarr_c::native_code::AxisRecord record{};
            record.has_name = true;
            record.name = std::string(1, axis);
            record.has_type = false;
            record.type = "";
            record.axis_repr = "";
            record.type_repr = "None";
            records.push_back(std::move(record));
        }
    } else {
        records = ome_zarr_c::bindings::axis_records_from_sequence(
            py::cast<py::sequence>(axes));
    }

    if (native_ndim.has_value()) {
        try {
            ome_zarr_c::native_code::validate_axes_length(records.size(), native_ndim.value());
        } catch (const std::invalid_argument& exc) {
            throw py::value_error(exc.what());
        }
    }

    try {
        if (version == "0.3") {
            const auto names = ome_zarr_c::native_code::get_names(records);
            ome_zarr_c::native_code::validate_03(names);
            py::list name_list;
            for (const auto& name : names) {
                name_list.append(py::str(name));
            }
            return name_list;
        }

        const auto normalized_records =
            ome_zarr_c::native_code::axes_to_dicts(records);
        ome_zarr_c::native_code::validate_axes_types(normalized_records);
        return ome_zarr_c::bindings::axis_records_to_dict_list_preserving_original_dicts(
            normalized_records,
            py::cast<py::sequence>(axes));
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

py::tuple extract_dims_from_axes(py::object axes = py::none()) {
    if (axes.is_none()) {
        return py::make_tuple("t", "c", "z", "y", "x");
    }

    bool all_strings = true;
    for (const py::handle& axis_handle : py::iterable(axes)) {
        if (!py::isinstance<py::str>(axis_handle)) {
            all_strings = false;
            break;
        }
    }
    if (all_strings) {
        py::list names;
        for (const py::handle& axis_handle : py::iterable(axes)) {
            names.append(py::str(axis_handle));
        }
        return py::tuple(names);
    }

    bool all_named_dicts = true;
    for (const py::handle& axis_handle : py::iterable(axes)) {
        if (!py::isinstance<py::dict>(axis_handle)) {
            all_named_dicts = false;
            break;
        }
        py::dict axis = py::cast<py::dict>(axis_handle);
        if (!axis.contains("name")) {
            all_named_dicts = false;
            break;
        }
    }
    if (all_named_dicts) {
        py::list names;
        const auto native_names = ome_zarr_c::native_code::extract_dims_from_axes(
            ome_zarr_c::bindings::axis_records_from_sequence(
                py::cast<py::sequence>(axes)));
        for (const auto& name : native_names) {
            names.append(py::str(name));
        }
        return py::tuple(names);
    }

    throw py::type_error(
        "`axes` must be a list of strings or a list of dicts containing 'name'");
}

py::tuple retuple(py::object chunks, py::object shape) {
    if (PyLong_Check(chunks.ptr())) {
        const py::size_t shape_len = py::len(shape);
        py::tuple result(shape_len);
        for (py::size_t index = 0; index < shape_len; ++index) {
            result[index] = chunks;
        }
        return result;
    }

    py::tuple shape_tuple = py::cast<py::tuple>(shape);
    py::tuple chunk_tuple =
        py::tuple(py::module_::import("builtins").attr("tuple")(chunks));
    const auto prefix = ome_zarr_c::native_code::retuple_prefix(
        static_cast<std::size_t>(py::len(shape_tuple)),
        static_cast<std::size_t>(py::len(chunk_tuple)));

    py::tuple result(prefix.size() + py::len(chunk_tuple));
    py::size_t index = 0;
    for (const auto prefix_index : prefix) {
        result[index++] = shape_tuple[prefix_index];
    }
    for (const py::handle& value : chunk_tuple) {
        result[index++] = py::reinterpret_borrow<py::object>(value);
    }
    return result;
}

py::list validate_well_images(py::object images, py::object fmt = py::none()) {
    static_cast<void>(fmt);
    py::object logger = py::module_::import("logging").attr("getLogger")(
        py::str("ome_zarr.writer"));
    py::list validated_images;

    for (const py::handle& image_handle : py::iterable(images)) {
        py::object image = py::reinterpret_borrow<py::object>(image_handle);
        ome_zarr_c::native_code::WellImageInput input{};
        input.is_string = py::isinstance<py::str>(image);
        input.is_dict = py::isinstance<py::dict>(image);
        input.repr = py::cast<std::string>(py::str(image));
        if (input.is_string) {
            input.path = input.repr;
        } else if (input.is_dict) {
            py::dict image_dict = py::cast<py::dict>(image);
            input.has_path = image_dict.contains("path");
            if (input.has_path) {
                input.path_is_string = py::isinstance<py::str>(image_dict["path"]);
                if (input.path_is_string) {
                    input.path = py::cast<std::string>(image_dict["path"]);
                }
            }
            input.has_acquisition = image_dict.contains("acquisition");
            if (input.has_acquisition) {
                input.acquisition_is_int = PyLong_Check(image_dict["acquisition"].ptr());
            }
            for (const auto& key_value : image_dict) {
                const py::handle key_handle = key_value.first;
                if (!ome_zarr_c::bindings::objects_equal(key_handle, py::str("acquisition")) &&
                    !ome_zarr_c::bindings::objects_equal(key_handle, py::str("path"))) {
                    input.has_unexpected_key = true;
                    break;
                }
            }
        }

        try {
            const auto validated = ome_zarr_c::native_code::validate_well_image(input);
            if (validated.has_unexpected_key) {
                logger.attr("debug")("%s contains unspecified keys", image);
            }
            if (validated.materialize) {
                py::dict validated_image;
                validated_image["path"] = py::str(validated.path);
                validated_images.append(validated_image);
            } else {
                validated_images.append(image);
            }
        } catch (const std::invalid_argument& exc) {
            throw py::value_error(exc.what());
        }
    }

    return validated_images;
}

py::object validate_plate_acquisitions(
    py::object acquisitions,
    py::object fmt = py::none()) {
    static_cast<void>(fmt);
    py::object logger = py::module_::import("logging").attr("getLogger")(
        py::str("ome_zarr.writer"));

    for (const py::handle& acquisition_handle : py::iterable(acquisitions)) {
        py::object acquisition = py::reinterpret_borrow<py::object>(acquisition_handle);
        ome_zarr_c::native_code::PlateAcquisitionInput input{};
        input.is_dict = py::isinstance<py::dict>(acquisition);
        input.repr = py::cast<std::string>(py::str(acquisition));
        if (input.is_dict) {
            py::dict acquisition_dict = py::cast<py::dict>(acquisition);
            input.has_id = acquisition_dict.contains("id");
            if (input.has_id) {
                input.id_is_int = PyLong_Check(acquisition_dict["id"].ptr());
            }
            for (const auto& key_value : acquisition_dict) {
                const py::handle key_handle = key_value.first;
                if (!ome_zarr_c::bindings::objects_equal(key_handle, py::str("id")) &&
                    !ome_zarr_c::bindings::objects_equal(key_handle, py::str("name")) &&
                    !ome_zarr_c::bindings::objects_equal(
                        key_handle, py::str("maximumfieldcount")) &&
                    !ome_zarr_c::bindings::objects_equal(
                        key_handle, py::str("description")) &&
                    !ome_zarr_c::bindings::objects_equal(
                        key_handle, py::str("starttime")) &&
                    !ome_zarr_c::bindings::objects_equal(
                        key_handle, py::str("endtime"))) {
                    input.has_unexpected_key = true;
                    break;
                }
            }
        }

        try {
            ome_zarr_c::native_code::validate_plate_acquisition(input);
            if (input.has_unexpected_key) {
                logger.attr("debug")("%s contains unspecified keys", acquisition);
            }
        } catch (const std::invalid_argument& exc) {
            throw py::value_error(exc.what());
        }
    }

    return acquisitions;
}

py::list validate_plate_rows_columns(
    py::object rows_or_columns,
    py::object fmt = py::none()) {
    static_cast<void>(fmt);
    std::vector<std::string> native_values;
    native_values.reserve(py::len(rows_or_columns));
    for (const py::handle& element_handle : py::iterable(rows_or_columns)) {
        py::object element = py::reinterpret_borrow<py::object>(element_handle);
        if (!py::isinstance<py::str>(element)) {
            static_cast<void>(element.attr("isalnum")());
        }
        native_values.push_back(py::cast<std::string>(element));
    }

    try {
        const auto validated = ome_zarr_c::native_code::validate_plate_rows_columns(
            native_values,
            py::cast<std::string>(py::str(rows_or_columns)));
        py::list validated_list;
        for (const auto& element : validated) {
            py::dict validated_element;
            validated_element["name"] = py::str(element);
            validated_list.append(validated_element);
        }
        return validated_list;
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

py::object validate_datasets(
    py::object datasets,
    py::object dims,
    py::object fmt = py::none()) {
    std::vector<ome_zarr_c::native_code::DatasetInput> native_datasets;
    py::list transformations;
    std::vector<ome_zarr_c::native_code::CoordinateTransformationsValidationInput>
        native_groups;
    const std::string version = format_version_string(fmt);
    const bool validate_transformations = version == "0.4" || version == "0.5";
    if (!datasets.is_none()) {
        native_datasets.reserve(py::len(datasets));
        if (validate_transformations) {
            native_groups.reserve(py::len(datasets));
        }
        for (const py::handle& dataset_handle : py::iterable(datasets)) {
            py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
            ome_zarr_c::native_code::DatasetInput input{};
            input.is_dict = py::isinstance<py::dict>(dataset);
            if (input.is_dict) {
                py::dict dataset_dict = py::cast<py::dict>(dataset);
                py::object path = dataset_dict.attr("get")("path");
                input.path_truthy = ome_zarr_c::bindings::object_truthy(path);
                py::object transformation =
                    dataset_dict.attr("get")("coordinateTransformations");
                input.has_transformation = !transformation.is_none();
                if (validate_transformations && input.has_transformation) {
                    transformations.append(transformation);
                    native_groups.push_back(
                        coordinate_transformations_validation_input(transformation));
                }
            }
            native_datasets.push_back(std::move(input));
        }
    }

    try {
        ome_zarr_c::native_code::validate_datasets(native_datasets);
        if (validate_transformations) {
            ome_zarr_c::native_code::validate_coordinate_transformations(
                py::cast<int>(dims), py::len(datasets), native_groups);
        }
        return datasets;
    } catch (const ome_zarr_c::native_code::DatasetValidationError& exc) {
        raise_dataset_validation_error(exc, datasets);
    } catch (const ome_zarr_c::native_code::CoordinateTransformationsValidationError& exc) {
        raise_coordinate_transformations_validation_error(
            exc, py::cast<int>(dims), py::len(datasets), transformations);
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

}  // namespace

void register_basic_bindings(py::module_& m) {
    m.def("_get_valid_axes",
          &get_valid_axes,
          py::arg("ndim") = py::none(),
          py::arg("axes") = py::none(),
          py::arg("fmt"));
    m.def("_extract_dims_from_axes", &extract_dims_from_axes, py::arg("axes") = py::none());
    m.def("_retuple", &retuple, py::arg("chunks"), py::arg("shape"));
    m.def("_validate_well_images", &validate_well_images, py::arg("images"), py::arg("fmt"));
    m.def("_validate_plate_acquisitions",
          &validate_plate_acquisitions,
          py::arg("acquisitions"),
          py::arg("fmt"));
    m.def("_validate_plate_rows_columns",
          &validate_plate_rows_columns,
          py::arg("rows_or_columns"),
          py::arg("fmt"));
    m.def("_validate_datasets",
          &validate_datasets,
          py::arg("datasets"),
          py::arg("dims"),
          py::arg("fmt"));
}
