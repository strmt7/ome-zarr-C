#include <pybind11/pybind11.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
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

PyObject* dict_get_item_string_borrowed(PyObject* mapping, const char* key) {
    if (!PyDict_Check(mapping)) {
        return nullptr;
    }
    return PyDict_GetItemString(mapping, key);
}

bool object_truthy_fast(PyObject* obj) {
    if (obj == Py_None) {
        return false;
    }
    if (PyDict_CheckExact(obj)) {
        return PyDict_GET_SIZE(obj) != 0;
    }
    if (PyList_CheckExact(obj)) {
        return PyList_GET_SIZE(obj) != 0;
    }
    if (PyTuple_CheckExact(obj)) {
        return PyTuple_GET_SIZE(obj) != 0;
    }
    const int truth = PyObject_IsTrue(obj);
    if (truth < 0) {
        throw py::error_already_set();
    }
    return truth == 1;
}

std::optional<std::size_t> sequence_index_of_string(
    const py::sequence& values,
    std::string_view target) {
    PyObject* fast = PySequence_Fast(values.ptr(), "expected a sequence");
    if (fast == nullptr) {
        throw py::error_already_set();
    }
    py::object fast_holder = py::reinterpret_steal<py::object>(fast);
    const auto size = static_cast<std::size_t>(PySequence_Fast_GET_SIZE(fast_holder.ptr()));
    PyObject** items = PySequence_Fast_ITEMS(fast_holder.ptr());

    py::object target_obj = py::none();
    for (std::size_t index = 0; index < size; ++index) {
        PyObject* item = items[index];
        if (PyUnicode_Check(item)) {
            Py_ssize_t item_size = 0;
            const char* item_text = PyUnicode_AsUTF8AndSize(item, &item_size);
            if (item_text == nullptr) {
                throw py::error_already_set();
            }
            if (item_size == static_cast<Py_ssize_t>(target.size()) &&
                std::string_view(item_text, static_cast<std::size_t>(item_size)) == target) {
                return index;
            }
            continue;
        }

        if (target_obj.is_none()) {
            PyObject* target_unicode = PyUnicode_FromStringAndSize(
                target.data(),
                static_cast<Py_ssize_t>(target.size()));
            if (target_unicode == nullptr) {
                throw py::error_already_set();
            }
            target_obj = py::reinterpret_steal<py::object>(target_unicode);
        }

        const int equal = PyObject_RichCompareBool(item, target_obj.ptr(), Py_EQ);
        if (equal < 0) {
            throw py::error_already_set();
        }
        if (equal == 1) {
            return index;
        }
    }

    return std::nullopt;
}

std::vector<std::vector<double>> shape_matrix_from_sequence(const py::sequence& shapes) {
    PyObject* shapes_fast = PySequence_Fast(shapes.ptr(), "expected a sequence");
    if (shapes_fast == nullptr) {
        throw py::error_already_set();
    }
    py::object shapes_holder = py::reinterpret_steal<py::object>(shapes_fast);
    const auto shape_count =
        static_cast<std::size_t>(PySequence_Fast_GET_SIZE(shapes_holder.ptr()));
    PyObject** shape_items = PySequence_Fast_ITEMS(shapes_holder.ptr());

    std::vector<std::vector<double>> native_shapes;
    native_shapes.reserve(shape_count);
    for (std::size_t shape_index = 0; shape_index < shape_count; ++shape_index) {
        PyObject* shape_fast =
            PySequence_Fast(shape_items[shape_index], "expected a sequence");
        if (shape_fast == nullptr) {
            throw py::error_already_set();
        }
        py::object shape_holder = py::reinterpret_steal<py::object>(shape_fast);
        const auto dimension_count =
            static_cast<std::size_t>(PySequence_Fast_GET_SIZE(shape_holder.ptr()));
        PyObject** dimension_items = PySequence_Fast_ITEMS(shape_holder.ptr());

        std::vector<double> native_shape(dimension_count);
        for (std::size_t dimension_index = 0; dimension_index < dimension_count; ++dimension_index) {
            native_shape[dimension_index] = PyFloat_AsDouble(dimension_items[dimension_index]);
            if (PyErr_Occurred() != nullptr) {
                throw py::error_already_set();
            }
        }
        native_shapes.push_back(std::move(native_shape));
    }

    return native_shapes;
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

bool is_number_like(const py::handle& value) {
    return PyFloat_Check(value.ptr()) || PyLong_Check(value.ptr());
}

ome_zarr_c::native_code::CoordinateTransformationKind classify_transformation_kind(
    PyObject* type_obj,
    PyObject* scale_text,
    PyObject* translation_text) {
    if (type_obj == nullptr || type_obj == Py_None) {
        return ome_zarr_c::native_code::CoordinateTransformationKind::other;
    }

    const int is_scale = PyObject_RichCompareBool(type_obj, scale_text, Py_EQ);
    if (is_scale < 0) {
        throw py::error_already_set();
    }
    if (is_scale == 1) {
        return ome_zarr_c::native_code::CoordinateTransformationKind::scale;
    }

    const int is_translation =
        PyObject_RichCompareBool(type_obj, translation_text, Py_EQ);
    if (is_translation < 0) {
        throw py::error_already_set();
    }
    if (is_translation == 1) {
        return ome_zarr_c::native_code::CoordinateTransformationKind::translation;
    }

    return ome_zarr_c::native_code::CoordinateTransformationKind::other;
}

py::object get_metadata_version_object_fast(py::dict metadata) {
    PyObject* metadata_obj = metadata.ptr();

    PyObject* multiscales = dict_get_item_string_borrowed(metadata_obj, "multiscales");
    if (multiscales != nullptr && object_truthy_fast(multiscales)) {
        if (PyList_CheckExact(multiscales) && PyList_GET_SIZE(multiscales) > 0) {
            PyObject* dataset = PyList_GET_ITEM(multiscales, 0);
            PyObject* version = dict_get_item_string_borrowed(dataset, "version");
            if (version != nullptr) {
                return py::reinterpret_borrow<py::object>(version);
            }
            return mapping_version_or_none(py::reinterpret_borrow<py::object>(dataset));
        }
        if (PyTuple_CheckExact(multiscales) && PyTuple_GET_SIZE(multiscales) > 0) {
            PyObject* dataset = PyTuple_GET_ITEM(multiscales, 0);
            PyObject* version = dict_get_item_string_borrowed(dataset, "version");
            if (version != nullptr) {
                return py::reinterpret_borrow<py::object>(version);
            }
            return mapping_version_or_none(py::reinterpret_borrow<py::object>(dataset));
        }
        PyObject* first = PySequence_GetItem(multiscales, 0);
        if (first == nullptr) {
            throw py::error_already_set();
        }
        py::object dataset = py::reinterpret_steal<py::object>(first);
        return mapping_version_or_none(dataset);
    }

    for (const char* key : {"plate", "well", "image-label"}) {
        PyObject* obj = dict_get_item_string_borrowed(metadata_obj, key);
        if (obj == nullptr || !object_truthy_fast(obj)) {
            continue;
        }
        PyObject* version = dict_get_item_string_borrowed(obj, "version");
        if (version != nullptr) {
            return py::reinterpret_borrow<py::object>(version);
        }
        return mapping_version_or_none(py::reinterpret_borrow<py::object>(obj));
    }

    return py::none();
}

std::vector<ome_zarr_c::native_code::CoordinateTransformationsValidationInput>
build_validation_inputs_slow(const py::list& coordinate_transformations) {
    std::vector<ome_zarr_c::native_code::CoordinateTransformationsValidationInput>
        native_groups;
    native_groups.reserve(py::len(coordinate_transformations));
    py::str scale_text("scale");
    py::str translation_text("translation");

    for (const py::handle& transformations_handle : coordinate_transformations) {
        py::list transformations = py::cast<py::list>(transformations_handle);
        ome_zarr_c::native_code::CoordinateTransformationsValidationInput native_group;
        native_group.transformations.reserve(py::len(transformations));

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

            py::object type = dict_get_item_or_none(transformation, "type");
            if (!type.is_none()) {
                native_transformation.has_type = true;
                native_transformation.kind = classify_transformation_kind(
                    type.ptr(), scale_text.ptr(), translation_text.ptr());
            }

            PyObject* scale_obj = PyDict_GetItemString(transformation.ptr(), "scale");
            if (scale_obj != nullptr) {
                native_transformation.has_scale = true;
                py::object scale_holder =
                    py::reinterpret_borrow<py::object>(scale_obj);
                py::sequence scale_values = py::cast<py::sequence>(scale_holder);
                native_transformation.scale_length = py::len(scale_values);
                for (const py::handle& value : scale_values) {
                    native_transformation.scale_all_numeric =
                        native_transformation.scale_all_numeric &&
                        is_number_like(value);
                }
            }

            PyObject* translation_obj =
                PyDict_GetItemString(transformation.ptr(), "translation");
            if (translation_obj != nullptr) {
                native_transformation.has_translation = true;
                py::object translation_holder =
                    py::reinterpret_borrow<py::object>(translation_obj);
                py::sequence translation_values =
                    py::cast<py::sequence>(translation_holder);
                native_transformation.translation_length = py::len(translation_values);
                for (const py::handle& value : translation_values) {
                    native_transformation.translation_all_numeric =
                        native_transformation.translation_all_numeric &&
                        is_number_like(value);
                }
            }

            native_group.transformations.push_back(std::move(native_transformation));
        }

        native_groups.push_back(std::move(native_group));
    }

    return native_groups;
}

std::optional<
    std::vector<ome_zarr_c::native_code::CoordinateTransformationsValidationInput>>
build_validation_inputs_fast(const py::list& coordinate_transformations) {
    std::vector<ome_zarr_c::native_code::CoordinateTransformationsValidationInput>
        native_groups;
    native_groups.reserve(static_cast<std::size_t>(PyList_GET_SIZE(coordinate_transformations.ptr())));
    py::str scale_text("scale");
    py::str translation_text("translation");

    PyObject* groups_obj = coordinate_transformations.ptr();
    const auto group_count = static_cast<std::size_t>(PyList_GET_SIZE(groups_obj));
    for (std::size_t group_index = 0; group_index < group_count; ++group_index) {
        PyObject* transformations_obj =
            PyList_GET_ITEM(groups_obj, static_cast<Py_ssize_t>(group_index));
        if (!PyList_Check(transformations_obj)) {
            return std::nullopt;
        }

        ome_zarr_c::native_code::CoordinateTransformationsValidationInput native_group;
        const auto transformation_count =
            static_cast<std::size_t>(PyList_GET_SIZE(transformations_obj));
        native_group.transformations.reserve(transformation_count);

        for (std::size_t index = 0; index < transformation_count; ++index) {
            PyObject* transformation_obj =
                PyList_GET_ITEM(transformations_obj, static_cast<Py_ssize_t>(index));
            if (!PyDict_Check(transformation_obj)) {
                return std::nullopt;
            }

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

            PyObject* type_obj = PyDict_GetItemString(transformation_obj, "type");
            if (type_obj != nullptr && type_obj != Py_None) {
                native_transformation.has_type = true;
                native_transformation.kind = classify_transformation_kind(
                    type_obj, scale_text.ptr(), translation_text.ptr());
            }

            for (const auto& [key, has_field, length_field, numeric_field] :
                 std::initializer_list<std::tuple<const char*,
                                                  bool&,
                                                  std::size_t&,
                                                  bool&>>{
                     {"scale",
                      native_transformation.has_scale,
                      native_transformation.scale_length,
                      native_transformation.scale_all_numeric},
                     {"translation",
                      native_transformation.has_translation,
                      native_transformation.translation_length,
                      native_transformation.translation_all_numeric}}) {
                PyObject* value_obj = PyDict_GetItemString(transformation_obj, key);
                if (value_obj == nullptr) {
                    continue;
                }
                PyObject* fast = PySequence_Fast(value_obj, "expected a sequence");
                if (fast == nullptr) {
                    PyErr_Clear();
                    return std::nullopt;
                }
                py::object fast_holder = py::reinterpret_steal<py::object>(fast);
                has_field = true;
                length_field = static_cast<std::size_t>(PySequence_Fast_GET_SIZE(fast_holder.ptr()));
                PyObject** items = PySequence_Fast_ITEMS(fast_holder.ptr());
                for (std::size_t value_index = 0; value_index < length_field; ++value_index) {
                    numeric_field = numeric_field &&
                        is_number_like(py::handle(items[value_index]));
                }
            }

            native_group.transformations.push_back(std::move(native_transformation));
        }

        native_groups.push_back(std::move(native_group));
    }

    return native_groups;
}

py::object get_metadata_version_object(py::dict metadata) {
    return get_metadata_version_object_fast(metadata);
}

[[noreturn]] void raise_value_error_args(const py::tuple& args) {
    PyErr_SetObject(PyExc_ValueError, args.ptr());
    throw py::error_already_set();
}

std::string metadata_version_from_key(const py::dict& metadata, const char* key) {
    py::object obj = dict_get_item_or_none(metadata, key);
    if (obj.is_none()) {
        return "";
    }
    py::dict value = py::cast<py::dict>(obj);
    py::object version = dict_get_item_or_none(value, "version");
    if (version.is_none()) {
        return "";
    }
    return py::cast<std::string>(py::str(version));
}

ome_zarr_c::native_code::MetadataSummary metadata_summary_from_dict(
    const py::dict& metadata) {
    ome_zarr_c::native_code::MetadataSummary summary{};
    summary.is_empty = py::len(metadata) == 0;

    PyObject* metadata_obj = metadata.ptr();
    PyObject* multiscales = dict_get_item_string_borrowed(metadata_obj, "multiscales");
    if (multiscales != nullptr && object_truthy_fast(multiscales)) {
        PyObject* dataset = nullptr;
        if (PyList_CheckExact(multiscales) && PyList_GET_SIZE(multiscales) > 0) {
            dataset = PyList_GET_ITEM(multiscales, 0);
        } else if (PyTuple_CheckExact(multiscales) && PyTuple_GET_SIZE(multiscales) > 0) {
            dataset = PyTuple_GET_ITEM(multiscales, 0);
        } else {
            PyObject* first = PySequence_GetItem(multiscales, 0);
            if (first == nullptr) {
                throw py::error_already_set();
            }
            py::object dataset_holder = py::reinterpret_steal<py::object>(first);
            PyObject* version = dict_get_item_string_borrowed(dataset_holder.ptr(), "version");
            if (version != nullptr) {
                summary.has_multiscales_version = true;
                summary.multiscales_version_is_string = PyUnicode_Check(version);
                summary.multiscales_version = py::cast<std::string>(
                    py::str(py::reinterpret_borrow<py::object>(version)));
            }
            return summary;
        }
        PyObject* version = dict_get_item_string_borrowed(dataset, "version");
        if (version != nullptr) {
            summary.has_multiscales_version = true;
            summary.multiscales_version_is_string = PyUnicode_Check(version);
            summary.multiscales_version = py::cast<std::string>(
                py::str(py::reinterpret_borrow<py::object>(version)));
        }
    }

    for (const auto& [key, has_flag, is_string_flag, value_out] :
         std::initializer_list<std::tuple<const char*,
                                          bool&,
                                          bool&,
                                          std::string&>>{
             {"plate",
              summary.has_plate_version,
              summary.plate_version_is_string,
              summary.plate_version},
             {"well",
              summary.has_well_version,
              summary.well_version_is_string,
              summary.well_version},
             {"image-label",
              summary.has_image_label_version,
              summary.image_label_version_is_string,
              summary.image_label_version}}) {
        PyObject* entry = dict_get_item_string_borrowed(metadata_obj, key);
        if (entry == nullptr || !object_truthy_fast(entry)) {
            continue;
        }
        PyObject* version = dict_get_item_string_borrowed(entry, "version");
        if (version == nullptr) {
            continue;
        }
        has_flag = true;
        is_string_flag = PyUnicode_Check(version);
        value_out = py::cast<std::string>(
            py::str(py::reinterpret_borrow<py::object>(version)));
    }

    return summary;
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
    const auto detected =
        ome_zarr_c::native_code::detect_format_version(metadata_summary_from_dict(metadata));
    if (!detected.has_value()) {
        return py::none();
    }
    return py::str(detected.value());
}

py::tuple format_match_details(const std::string& version, py::dict metadata) {
    py::object metadata_version = get_metadata_version_object(metadata);
    py::bool_ matched = py::bool_(false);
    if (!metadata_version.is_none()) {
        matched = py::bool_(
            ome_zarr_c::bindings::objects_equal(metadata_version, py::str(version)));
    }
    return py::make_tuple(std::move(metadata_version), matched);
}

bool format_matches(const std::string& version, py::dict metadata) {
    return ome_zarr_c::native_code::format_matches(
        version,
        metadata_summary_from_dict(metadata));
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

py::str format_class_name(const std::string& version) {
    return py::str(ome_zarr_c::native_code::format_class_name(version));
}

bool format_class_matches(
    const std::string& version,
    const std::string& self_module,
    const std::string& other_module,
    const std::string& other_name) {
    return ome_zarr_c::native_code::format_class_matches(
        version,
        self_module,
        other_module,
        other_name);
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
    PyObject* well_obj = well.ptr();
    PyObject* path_obj = PyDict_GetItemString(well_obj, "path");
    if (path_obj == nullptr) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("path");
        args[3] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }
    if (!PyUnicode_Check(path_obj)) {
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
    try {
        const auto parts =
            ome_zarr_c::native_code::split_well_path_for_generation(well);
        const auto row_index = sequence_index_of_string(rows, parts.row);
        if (!row_index.has_value()) {
            py::tuple args(2);
            args[0] = py::str("%s is not defined in the list of rows");
            args[1] = py::str(parts.row);
            raise_value_error_args(args);
        }
        const auto column_index = sequence_index_of_string(columns, parts.column);
        if (!column_index.has_value()) {
            py::tuple args(2);
            args[0] = py::str("%s is not defined in the list of columns");
            args[1] = py::str(parts.column);
            raise_value_error_args(args);
        }
        py::dict result;
        result["path"] = py::str(well);
        result["rowIndex"] = py::int_(row_index.value());
        result["columnIndex"] = py::int_(column_index.value());
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
            case ome_zarr_c::native_code::WellGenerationErrorCode::row_missing:
            case ome_zarr_c::native_code::WellGenerationErrorCode::column_missing:
                break;
        }
    }
    throw py::value_error("unhandled well generation error");
}

void validate_well_dict_v04(py::dict well,
                            const py::sequence& rows,
                            const py::sequence& columns) {
    PyObject* well_obj = well.ptr();
    PyObject* path_obj = PyDict_GetItemString(well_obj, "path");
    if (path_obj == nullptr) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("path");
        args[3] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }
    if (!PyUnicode_Check(path_obj)) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }

    PyObject* row_index_obj = PyDict_GetItemString(well_obj, "rowIndex");
    if (row_index_obj == nullptr) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("rowIndex");
        args[3] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    PyObject* column_index_obj = PyDict_GetItemString(well_obj, "columnIndex");
    if (column_index_obj == nullptr) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("columnIndex");
        args[3] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!PyLong_Check(row_index_obj)) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!PyLong_Check(column_index_obj)) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }

    const std::string path =
        py::cast<std::string>(py::reinterpret_borrow<py::object>(path_obj));
    const std::int64_t row_index =
        py::cast<std::int64_t>(py::reinterpret_borrow<py::object>(row_index_obj));
    const std::int64_t column_index =
        py::cast<std::int64_t>(py::reinterpret_borrow<py::object>(column_index_obj));

    try {
        const auto parts =
            ome_zarr_c::native_code::split_well_path_for_validation(path);
        const auto actual_row_index = sequence_index_of_string(rows, parts.row);
        if (!actual_row_index.has_value()) {
            py::tuple args(2);
            args[0] = py::str("%s is not defined in the plate rows");
            args[1] = py::str(parts.row);
            raise_value_error_args(args);
        }
        if (row_index != static_cast<std::int64_t>(actual_row_index.value())) {
            py::tuple args(2);
            args[0] = py::str("Mismatching row index for %s");
            args[1] = well;
            raise_value_error_args(args);
        }

        const auto actual_column_index = sequence_index_of_string(columns, parts.column);
        if (!actual_column_index.has_value()) {
            py::tuple args(2);
            args[0] = py::str("%s is not defined in the plate columns");
            args[1] = py::str(parts.column);
            raise_value_error_args(args);
        }
        if (column_index != static_cast<std::int64_t>(actual_column_index.value())) {
            py::tuple args(2);
            args[0] = py::str("Mismatching column index for %s");
            args[1] = well;
            raise_value_error_args(args);
        }
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
    py::list coordinate_transformations;
    try {
        const auto native_transformations = ome_zarr_c::native_code::generate_coordinate_transformations(
            shape_matrix_from_sequence(shapes));
        coordinate_transformations = py::list(native_transformations.size());
        py::size_t group_index = 0;
        for (const auto& transformations : native_transformations) {
            py::list level_transforms(transformations.size());
            py::size_t transform_index = 0;
            for (const auto& transformation : transformations) {
                py::dict transform;
                transform["type"] = py::str(transformation.type);
                py::list values(transformation.values.size());
                py::size_t value_index = 0;
                for (const double value : transformation.values) {
                    values[value_index++] = py::float_(value);
                }
                transform[py::str(transformation.type)] = values;
                level_transforms[transform_index++] = std::move(transform);
            }
            coordinate_transformations[group_index++] = std::move(level_transforms);
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
    auto native_groups =
        build_validation_inputs_fast(coordinate_transformations).value_or(
            build_validation_inputs_slow(coordinate_transformations));

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
    m.def("format_match_details", &format_match_details, py::arg("version"), py::arg("metadata"));
    m.def("format_matches", &format_matches, py::arg("version"), py::arg("metadata"));
    m.def("format_zarr_format", &format_zarr_format, py::arg("version"));
    m.def("format_chunk_key_encoding", &format_chunk_key_encoding, py::arg("version"));
    m.def("format_class_name", &format_class_name, py::arg("version"));
    m.def("format_class_matches",
          &format_class_matches,
          py::arg("version"),
          py::arg("self_module"),
          py::arg("other_module"),
          py::arg("other_name"));
    m.def("format_init_store", &format_init_store, py::arg("path"), py::arg("mode") = "r");
    m.def("generate_well_dict_v04", &generate_well_dict_v04);
    m.def("validate_well_dict_v01", &validate_well_dict_v01);
    m.def("validate_well_dict_v04", &validate_well_dict_v04);
    m.def("generate_coordinate_transformations", &generate_coordinate_transformations);
    m.def("validate_coordinate_transformations", &validate_coordinate_transformations);
}
