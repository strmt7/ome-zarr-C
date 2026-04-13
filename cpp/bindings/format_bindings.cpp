#include <pybind11/pybind11.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "../native/format.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

std::string repr_object(const py::handle& obj) {
    return py::cast<std::string>(py::repr(obj));
}

py::object dict_get_item_or_none(const py::dict& mapping, const char* key) {
    PyObject* value = PyDict_GetItemString(mapping.ptr(), key);
    if (value == nullptr) {
        return py::none();
    }
    return py::reinterpret_borrow<py::object>(value);
}

py::object mapping_version_or_none(const py::object& mapping_like) {
    if (mapping_like.is_none() || !ome_zarr_c::bindings::object_truthy(mapping_like)) {
        return py::none();
    }
    if (PyDict_Check(mapping_like.ptr())) {
        PyObject* value = PyDict_GetItemString(mapping_like.ptr(), "version");
        if (value == nullptr) {
            return py::none();
        }
        return py::reinterpret_borrow<py::object>(value);
    }
    return mapping_like.attr("get")(py::str("version"), py::none());
}

py::object get_metadata_version_object(py::dict metadata) {
    py::object multiscales = dict_get_item_or_none(metadata, "multiscales");
    if (!multiscales.is_none() && ome_zarr_c::bindings::object_truthy(multiscales)) {
        PyObject* first = PySequence_GetItem(multiscales.ptr(), 0);
        if (first == nullptr) {
            throw py::error_already_set();
        }
        py::object dataset = py::reinterpret_steal<py::object>(first);
        return mapping_version_or_none(dataset);
    }

    for (const char* key : {"plate", "well", "image-label"}) {
        py::object version = mapping_version_or_none(dict_get_item_or_none(metadata, key));
        if (!version.is_none()) {
            return version;
        }
    }
    return py::none();
}

[[noreturn]] void raise_value_error_args(const py::tuple& args) {
    PyErr_SetObject(PyExc_ValueError, args.ptr());
    throw py::error_already_set();
}

std::string metadata_version_from_key(const py::dict& metadata, const char* key) {
    py::object obj = metadata.attr("get")(py::str(key));
    if (obj.is_none()) {
        return "";
    }
    py::dict value = py::cast<py::dict>(obj);
    py::object version = value.attr("get")(py::str("version"), py::none());
    if (version.is_none()) {
        return "";
    }
    return py::cast<std::string>(py::str(version));
}

ome_zarr_c::native_code::MetadataSummary metadata_summary_from_dict(
    const py::dict& metadata) {
    ome_zarr_c::native_code::MetadataSummary summary{};
    summary.is_empty = py::len(metadata) == 0;

    py::list multiscales =
        py::cast<py::list>(metadata.attr("get")("multiscales", py::list()));
    if (py::len(multiscales) > 0) {
        py::dict dataset = py::cast<py::dict>(multiscales[0]);
        py::object version = dataset.attr("get")("version", py::none());
        if (!version.is_none()) {
            summary.has_multiscales_version = true;
            summary.multiscales_version_is_string = py::isinstance<py::str>(version);
            summary.multiscales_version = py::cast<std::string>(py::str(version));
        }
    }

    const std::string plate_version = metadata_version_from_key(metadata, "plate");
    if (!plate_version.empty()) {
        summary.has_plate_version = true;
        summary.plate_version_is_string =
            py::isinstance<py::str>(
                py::cast<py::dict>(metadata.attr("get")("plate")).attr("get")("version"));
        summary.plate_version = plate_version;
    }
    const std::string well_version = metadata_version_from_key(metadata, "well");
    if (!well_version.empty()) {
        summary.has_well_version = true;
        summary.well_version_is_string =
            py::isinstance<py::str>(
                py::cast<py::dict>(metadata.attr("get")("well")).attr("get")("version"));
        summary.well_version = well_version;
    }
    const std::string image_label_version =
        metadata_version_from_key(metadata, "image-label");
    if (!image_label_version.empty()) {
        summary.has_image_label_version = true;
        summary.image_label_version_is_string =
            py::isinstance<py::str>(
                py::cast<py::dict>(metadata.attr("get")("image-label"))
                    .attr("get")("version"));
        summary.image_label_version = image_label_version;
    }

    return summary;
}

bool is_number_like(const py::handle& value) {
    return PyFloat_Check(value.ptr()) || PyLong_Check(value.ptr());
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
                "Missing type in: " + repr_object(group));
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
                "Missing scale argument in: " + repr_object(first));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::scale_length_mismatch:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
                py::object first = py::cast<py::list>(group)[py::int_(0)];
            throw py::value_error(
                "'scale' list " +
                repr_object(first.attr("__getitem__")(py::str("scale"))) +
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
                repr_object(first.attr("__getitem__")(py::str("scale"))));
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
                "Missing scale argument in: " + repr_object(first));
            }
        case ome_zarr_c::native_code::CoordinateTransformationsValidationErrorCode::translation_length_mismatch:
            {
                py::object group =
                    coordinate_transformations[py::int_(error.group_index())];
            throw py::value_error(
                "'translation' list " +
                repr_object(
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
                repr_object(
                    py::cast<py::list>(group)[py::int_(error.transformation_index())]
                        .attr("__getitem__")(py::str("translation"))));
            }
    }

    throw py::value_error("Unknown coordinate transformation validation error");
}

std::string normalize_known_format_version(const py::handle& version) {
    try {
        return ome_zarr_c::native_code::normalize_known_format_version(
            py::cast<std::string>(py::str(version)));
    } catch (const std::invalid_argument&) {
        throw py::value_error(
            "Version " + py::cast<std::string>(py::str(version)) + " not recognized");
    }
}

py::list format_versions() {
    py::list versions;
    for (const auto& version : ome_zarr_c::native_code::format_versions()) {
        versions.append(py::str(version));
    }
    return versions;
}

py::str resolve_format_version(const py::handle& version) {
    return py::str(normalize_known_format_version(version));
}

py::object get_metadata_version(py::dict metadata) {
    return get_metadata_version_object(metadata);
}

py::object detect_format_version(py::dict metadata) {
    py::object version = get_metadata_version_object(metadata);
    if (version.is_none() || !py::isinstance<py::str>(version)) {
        return py::none();
    }
    const std::string version_string = py::cast<std::string>(version);
    if (!ome_zarr_c::native_code::is_known_format_version_string(version_string)) {
        return py::none();
    }
    return version;
}

bool format_matches(const std::string& version, py::dict metadata) {
    py::object metadata_version = get_metadata_version_object(metadata);
    if (metadata_version.is_none()) {
        return false;
    }
    return ome_zarr_c::bindings::objects_equal(metadata_version, py::str(version));
}

int format_zarr_format(const std::string& version) {
    return ome_zarr_c::native_code::format_zarr_format(version);
}

py::dict format_chunk_key_encoding(const std::string& version) {
    const auto encoding_native =
        ome_zarr_c::native_code::format_chunk_key_encoding(version);
    py::dict encoding;
    encoding["name"] = py::str(encoding_native.name);
    encoding["separator"] = py::str(encoding_native.separator);
    return encoding;
}

py::object format_init_store(const std::string& path, const std::string& mode = "r") {
    const auto plan = ome_zarr_c::native_code::format_init_store_plan(path, mode);
    if (plan.use_fsspec) {
        py::object fsspec_store =
            py::module_::import("zarr.storage").attr("FsspecStore");
        return fsspec_store.attr("from_url")(
            py::str(path),
            py::arg("storage_options") = py::none(),
            py::arg("read_only") = py::bool_(plan.read_only));
    }
    py::object local_store =
        py::module_::import("zarr.storage").attr("LocalStore");
    return local_store(py::str(path), py::arg("read_only") = py::bool_(plan.read_only));
}

void validate_well_dict_v01(py::dict well) {
    if (!well.contains("path")) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("path");
        args[3] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }
    if (!PyUnicode_Check(well["path"].ptr())) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }
}

py::dict generate_well_dict_v04(
    const std::string& well,
    const py::sequence& rows,
    const py::sequence& columns) {
    std::vector<std::string> native_rows;
    std::vector<std::string> native_columns;
    native_rows.reserve(py::len(rows));
    native_columns.reserve(py::len(columns));
    for (const py::handle& row : rows) {
        native_rows.push_back(py::cast<std::string>(row));
    }
    for (const py::handle& column : columns) {
        native_columns.push_back(py::cast<std::string>(column));
    }

    try {
        const auto generated = ome_zarr_c::native_code::generate_well_v04(
            well, native_rows, native_columns);
        py::dict result;
        result["path"] = py::str(generated.path);
        result["rowIndex"] = py::int_(generated.row_index);
        result["columnIndex"] = py::int_(generated.column_index);
        return result;
    } catch (const ome_zarr_c::native_code::WellGenerationError& error) {
        switch (error.code()) {
            case ome_zarr_c::native_code::WellGenerationErrorCode::path_not_enough_groups:
                throw py::value_error("not enough values to unpack (expected 2, got 1)");
            case ome_zarr_c::native_code::WellGenerationErrorCode::path_too_many_groups: {
#if PY_VERSION_HEX >= 0x030e0000
                const auto group_count = static_cast<int>(
                    std::count(well.begin(), well.end(), '/')) + 1;
                throw py::value_error(
                    "too many values to unpack (expected 2, got " +
                    std::to_string(group_count) + ")");
#else
                throw py::value_error("too many values to unpack (expected 2)");
#endif
            }
            case ome_zarr_c::native_code::WellGenerationErrorCode::row_missing: {
                py::tuple args(2);
                args[0] = py::str("%s is not defined in the list of rows");
                args[1] = py::str(error.detail());
                raise_value_error_args(args);
            }
            case ome_zarr_c::native_code::WellGenerationErrorCode::column_missing: {
                py::tuple args(2);
                args[0] = py::str("%s is not defined in the list of columns");
                args[1] = py::str(error.detail());
                raise_value_error_args(args);
            }
        }
    }
    throw py::value_error("unhandled well generation error");
}

void validate_well_dict_v04(py::dict well,
                            const py::sequence& rows,
                            const py::sequence& columns) {
    validate_well_dict_v01(well);

    if (!well.contains("rowIndex")) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("rowIndex");
        args[3] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!well.contains("columnIndex")) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("columnIndex");
        args[3] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!PyLong_Check(well["rowIndex"].ptr())) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!PyLong_Check(well["columnIndex"].ptr())) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }

    const std::string path = py::cast<std::string>(well["path"]);
    std::vector<std::string> native_rows;
    std::vector<std::string> native_columns;
    native_rows.reserve(py::len(rows));
    native_columns.reserve(py::len(columns));
    for (const py::handle& row : rows) {
        native_rows.push_back(py::cast<std::string>(row));
    }
    for (const py::handle& column : columns) {
        native_columns.push_back(py::cast<std::string>(column));
    }

    try {
        ome_zarr_c::native_code::validate_well_v04(
            path,
            py::cast<std::int64_t>(well["rowIndex"]),
            py::cast<std::int64_t>(well["columnIndex"]),
            native_rows,
            native_columns);
    } catch (const ome_zarr_c::native_code::WellValidationError& error) {
        py::tuple args(2);
        switch (error.code()) {
            case ome_zarr_c::native_code::WellValidationErrorCode::path_group_count:
                args[0] = py::str("%s path must exactly be composed of 2 groups");
                args[1] = well;
                break;
            case ome_zarr_c::native_code::WellValidationErrorCode::row_missing:
                args[0] = py::str("%s is not defined in the plate rows");
                args[1] = py::str(error.detail());
                break;
            case ome_zarr_c::native_code::WellValidationErrorCode::row_index_mismatch:
                args[0] = py::str("Mismatching row index for %s");
                args[1] = well;
                break;
            case ome_zarr_c::native_code::WellValidationErrorCode::column_missing:
                args[0] = py::str("%s is not defined in the plate columns");
                args[1] = py::str(error.detail());
                break;
            case ome_zarr_c::native_code::WellValidationErrorCode::column_index_mismatch:
                args[0] = py::str("Mismatching column index for %s");
                args[1] = well;
                break;
        }
        raise_value_error_args(args);
    }
}

py::list generate_coordinate_transformations(py::sequence shapes) {
    py::list shapes_list = py::list(shapes);
    std::vector<std::vector<double>> native_shapes;
    native_shapes.reserve(py::len(shapes_list));
    for (const py::handle& shape_handle : shapes_list) {
        py::sequence shape = py::cast<py::sequence>(shape_handle);
        std::vector<double> native_shape;
        native_shape.reserve(py::len(shape));
        for (const py::handle& value : shape) {
            native_shape.push_back(py::cast<double>(value));
        }
        native_shapes.push_back(std::move(native_shape));
    }

    py::list coordinate_transformations;
    try {
        const auto native_transformations =
            ome_zarr_c::native_code::generate_coordinate_transformations(native_shapes);
        for (const auto& transformations : native_transformations) {
            py::list level_transforms;
            for (const auto& transformation : transformations) {
                py::dict transform;
                transform["type"] = py::str(transformation.type);
                py::list values;
                for (const double value : transformation.values) {
                    values.append(py::float_(value));
                }
                transform[py::str(transformation.type)] = values;
                level_transforms.append(transform);
            }
            coordinate_transformations.append(level_transforms);
        }
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }

    return coordinate_transformations;
}

void validate_coordinate_transformations(
    int ndim,
    int nlevels,
    py::object coordinate_transformations_obj = py::none()) {
    if (coordinate_transformations_obj.is_none()) {
        throw py::value_error("coordinate_transformations must be provided");
    }

    py::list coordinate_transformations = py::cast<py::list>(coordinate_transformations_obj);
    std::vector<ome_zarr_c::native_code::CoordinateTransformationsValidationInput>
        native_groups;
    native_groups.reserve(py::len(coordinate_transformations));

    for (const py::handle& transformations_handle : coordinate_transformations) {
        py::list transformations = py::cast<py::list>(transformations_handle);
        ome_zarr_c::native_code::CoordinateTransformationsValidationInput native_group;
        native_group.transformations.reserve(py::len(transformations));

        for (const py::handle& transformation_handle : transformations) {
            py::dict transformation = py::cast<py::dict>(transformation_handle);
            ome_zarr_c::native_code::CoordinateTransformationValidationInput
                native_transformation;
            native_transformation.has_type = false;
            native_transformation.has_scale = false;
            native_transformation.scale_length = 0;
            native_transformation.has_translation = false;
            native_transformation.translation_length = 0;

            py::object type = transformation.attr("get")("type", py::none());
            if (!type.is_none()) {
                native_transformation.has_type = true;
                native_transformation.type = py::cast<std::string>(type);
            }

            if (transformation.contains("scale")) {
                native_transformation.has_scale = true;
                py::sequence scale_values = py::cast<py::sequence>(transformation["scale"]);
                native_transformation.scale_length = py::len(scale_values);
                native_transformation.scale_numeric.reserve(native_transformation.scale_length);
                for (const py::handle& value : scale_values) {
                    native_transformation.scale_numeric.push_back(is_number_like(value));
                }
            }

            if (transformation.contains("translation")) {
                native_transformation.has_translation = true;
                py::sequence translation_values =
                    py::cast<py::sequence>(transformation["translation"]);
                native_transformation.translation_length = py::len(translation_values);
                native_transformation.translation_numeric.reserve(
                    native_transformation.translation_length);
                for (const py::handle& value : translation_values) {
                    native_transformation.translation_numeric.push_back(
                        is_number_like(value));
                }
            }

            native_group.transformations.push_back(std::move(native_transformation));
        }

        native_groups.push_back(std::move(native_group));
    }

    try {
        ome_zarr_c::native_code::validate_coordinate_transformations(
            ndim, nlevels, native_groups);
    } catch (const ome_zarr_c::native_code::CoordinateTransformationsValidationError& exc) {
        raise_coordinate_transformations_validation_error(
            exc, ndim, nlevels, coordinate_transformations);
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

}  // namespace

void register_format_bindings(py::module_& m) {
    m.def("format_versions", &format_versions);
    m.def("resolve_format_version", &resolve_format_version, py::arg("version"));
    m.def("get_metadata_version", &get_metadata_version);
    m.def("detect_format_version", &detect_format_version);
    m.def("format_matches", &format_matches, py::arg("version"), py::arg("metadata"));
    m.def("format_zarr_format", &format_zarr_format, py::arg("version"));
    m.def("format_chunk_key_encoding", &format_chunk_key_encoding, py::arg("version"));
    m.def("format_init_store", &format_init_store, py::arg("path"), py::arg("mode") = "r");
    m.def("generate_well_dict_v04", &generate_well_dict_v04);
    m.def("validate_well_dict_v01", &validate_well_dict_v01);
    m.def("validate_well_dict_v04", &validate_well_dict_v04);
    m.def("generate_coordinate_transformations", &generate_coordinate_transformations);
    m.def("validate_coordinate_transformations", &validate_coordinate_transformations);
}
