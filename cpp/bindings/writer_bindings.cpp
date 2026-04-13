#include <pybind11/pybind11.h>

#include <optional>
#include <string>
#include <vector>

#include "../native/format.hpp"
#include "../native/writer.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

std::optional<std::string> optional_version(const py::object& version) {
    if (version.is_none()) {
        return std::nullopt;
    }
    return py::cast<std::string>(version);
}

std::string default_writer_version() {
    return "0.5";
}

py::object open_group_for_writer(
    const py::object& path,
    const std::string& mode,
    const std::string& version) {
    py::object zarr = py::module_::import("zarr");
    return zarr.attr("open_group")(
        path,
        py::arg("mode") = mode,
        py::arg("zarr_format") =
            ome_zarr_c::native_code::format_zarr_format(version));
}

py::dict copy_group_attrs(const py::object& group) {
    return py::cast<py::dict>(group.attr("attrs").attr("asdict")());
}

void merge_metadata_dicts(py::dict& attrs, const py::dict& metadata) {
    for (const py::handle& item_handle : metadata.attr("items")()) {
        py::tuple item = py::cast<py::tuple>(item_handle);
        py::object key = item[0];
        py::object value = item[1];
        if (py::isinstance<py::dict>(value) &&
            attrs.contains(key) &&
            py::isinstance<py::dict>(attrs[key])) {
            py::cast<py::dict>(attrs[key]).attr("update")(value);
        } else {
            attrs[key] = value;
        }
    }
}

py::dict writer_get_metadata_from_group(const py::object& group) {
    py::dict attrs = copy_group_attrs(group);
    const int zarr_format = py::cast<int>(group.attr("info").attr("_zarr_format"));
    if (zarr_format == 3) {
        py::object ome = attrs.attr("get")(py::str("ome"), py::dict());
        return py::cast<py::dict>(ome);
    }
    return attrs;
}

void writer_add_metadata_to_group(
    const py::object& group,
    const py::dict& metadata,
    const std::string& version) {
    py::dict attrs = writer_get_metadata_from_group(group);
    merge_metadata_dicts(attrs, metadata);

    if (ome_zarr_c::native_code::writer_uses_legacy_root_attrs(version)) {
        for (const py::handle& item_handle : attrs.attr("items")()) {
            py::tuple item = py::cast<py::tuple>(item_handle);
            group.attr("attrs").attr("__setitem__")(item[0], item[1]);
        }
    } else {
        group.attr("attrs").attr("__setitem__")(py::str("ome"), attrs);
    }
}

std::string writer_check_format_version(
    const py::object& group,
    const py::object& requested_version) {
    const auto plan = ome_zarr_c::native_code::resolve_writer_format(
        py::cast<int>(group.attr("info").attr("_zarr_format")),
        optional_version(requested_version));
    return plan.resolved_version;
}

py::tuple writer_check_group_fmt(
    py::object group,
    py::object requested_version = py::none(),
    const std::string& mode = "a") {
    if (py::isinstance<py::str>(group)) {
        const std::string version =
            optional_version(requested_version).value_or(default_writer_version());
        py::object opened = open_group_for_writer(group, mode, version);
        return py::make_tuple(opened, py::str(version));
    }

    const std::string version = writer_check_format_version(group, requested_version);
    return py::make_tuple(group, py::str(version));
}

void validate_omero_metadata(const py::object& omero_metadata) {
    if (omero_metadata.is_none()) {
        throw py::key_error("If `'omero'` is present, value cannot be `None`.");
    }

    for (const py::handle& channel_handle :
         py::iterable(omero_metadata.attr("__getitem__")(py::str("channels")))) {
        py::object channel = py::reinterpret_borrow<py::object>(channel_handle);
        if (channel.attr("__contains__")(py::str("color")).cast<bool>()) {
            py::object color = channel.attr("__getitem__")(py::str("color"));
            if (!py::isinstance<py::str>(color) ||
                py::cast<std::string>(color).size() != 6U) {
                throw py::type_error("`'color'` must be a hex code string.");
            }
        }
        if (channel.attr("__contains__")(py::str("window")).cast<bool>()) {
            py::object window = channel.attr("__getitem__")(py::str("window"));
            if (!py::isinstance<py::dict>(window)) {
                throw py::type_error("`'window'` must be a dict.");
            }
            for (const char* key : {"min", "max", "start", "end"}) {
                py::str py_key(key);
                if (!window.attr("__contains__")(py_key).cast<bool>()) {
                    throw py::key_error(
                        std::string("`'") + key + "'` not found in `'window'`.");
                }
                py::object value = window.attr("__getitem__")(py_key);
                if (!PyLong_Check(value.ptr()) && !PyFloat_Check(value.ptr())) {
                    throw py::type_error(
                        std::string("`'") + key + "'` must be an int or float.");
                }
            }
        }
    }
}

py::object writer_get_metadata(py::object group) {
    if (py::isinstance<py::str>(group)) {
        py::object zarr = py::module_::import("zarr");
        group = zarr.attr("open_group")(group, py::arg("mode") = "r");
    }
    return writer_get_metadata_from_group(group);
}

void writer_add_metadata(
    py::object group,
    py::dict metadata,
    py::object requested_version = py::none()) {
    py::tuple checked = writer_check_group_fmt(group, requested_version, "a");
    writer_add_metadata_to_group(
        checked[0],
        metadata,
        py::cast<std::string>(checked[1]));
}

py::object writer_check_format(py::object group, py::object requested_version = py::none()) {
    return py::str(writer_check_format_version(group, requested_version));
}

void writer_write_multiscales_metadata(
    const py::object& group,
    const py::object& datasets,
    const std::string& version,
    const py::object& axes = py::none(),
    const py::object& name = py::none(),
    const py::object& metadata = py::dict()) {
    py::object copy = py::module_::import("copy").attr("deepcopy")(metadata);
    py::dict metadata_dict = py::cast<py::dict>(copy);
    py::object metadata_block = metadata_dict.attr("get")(py::str("metadata"), py::dict());

    if (py::isinstance<py::dict>(metadata_block) &&
        metadata_block.attr("__contains__")(py::str("omero")).cast<bool>()) {
        py::object omero_metadata =
            metadata_block.attr("pop")(py::str("omero"));
        validate_omero_metadata(omero_metadata);
        py::dict omero_payload;
        omero_payload["omero"] = omero_metadata;
        writer_add_metadata_to_group(group, omero_payload, version);
    }

    const auto plan = ome_zarr_c::native_code::writer_multiscales_metadata_plan(
        version,
        name.is_none()
            ? py::cast<std::string>(group.attr("name"))
            : py::cast<std::string>(name));

    py::dict multiscale;
    multiscale["datasets"] = datasets;
    multiscale["name"] = py::str(plan.group_name);
    if (!axes.is_none()) {
        multiscale["axes"] = axes;
    }
    if (py::len(metadata_block) > 0) {
        multiscale["metadata"] = metadata_block;
    }
    if (plan.embed_version_in_multiscales) {
        multiscale["version"] = py::str(version);
    }

    py::list multiscales;
    multiscales.append(multiscale);

    if (plan.write_root_version) {
        py::dict version_payload;
        version_payload["version"] = py::str(version);
        writer_add_metadata_to_group(group, version_payload, version);
    }

    py::dict multiscales_payload;
    multiscales_payload["multiscales"] = multiscales;
    writer_add_metadata_to_group(group, multiscales_payload, version);
}

void writer_write_plate_metadata(
    const py::object& group,
    const py::object& rows,
    const py::object& columns,
    const py::object& wells,
    const std::string& version,
    const py::object& acquisitions = py::none(),
    const py::object& field_count = py::none(),
    const py::object& name = py::none()) {
    const auto plan = ome_zarr_c::native_code::writer_plate_metadata_plan(version);

    py::dict plate;
    plate["columns"] = columns;
    plate["rows"] = rows;
    plate["wells"] = wells;
    if (!name.is_none()) {
        plate["name"] = name;
    }
    if (!field_count.is_none()) {
        plate["field_count"] = field_count;
    }
    if (!acquisitions.is_none()) {
        plate["acquisitions"] = acquisitions;
    }
    if (plan.embed_plate_version) {
        plate["version"] = py::str(version);
    }

    if (plan.legacy_root_attrs) {
        group.attr("attrs").attr("__setitem__")(py::str("plate"), plate);
    } else {
        py::dict payload;
        payload["version"] = py::str(version);
        payload["plate"] = plate;
        group.attr("attrs").attr("__setitem__")(py::str("ome"), payload);
    }
}

void writer_write_well_metadata(
    const py::object& group,
    const py::object& images,
    const std::string& version) {
    const auto plan = ome_zarr_c::native_code::writer_well_metadata_plan(version);

    py::dict well;
    well["images"] = images;
    if (plan.embed_well_version) {
        well["version"] = py::str(version);
    }

    if (plan.legacy_root_attrs) {
        group.attr("attrs").attr("__setitem__")(py::str("well"), well);
    } else {
        py::dict payload;
        payload["version"] = py::str(version);
        payload["well"] = well;
        group.attr("attrs").attr("__setitem__")(py::str("ome"), payload);
    }
}

void writer_write_label_metadata(
    const py::object& group,
    const std::string& name,
    const py::object& colors,
    const py::object& properties,
    const std::string& version,
    const py::object& metadata = py::dict()) {
    const auto plan = ome_zarr_c::native_code::writer_label_metadata_plan(version);
    (void)plan;

    py::object copy = py::module_::import("copy").attr("deepcopy")(metadata);
    py::dict image_label_metadata = py::cast<py::dict>(copy);
    if (!colors.is_none()) {
        image_label_metadata["colors"] = colors;
    }
    if (!properties.is_none()) {
        image_label_metadata["properties"] = properties;
    }
    image_label_metadata["version"] = py::str(version);

    py::object label_group = group.attr("__getitem__")(py::str(name));
    py::dict labels_metadata = writer_get_metadata_from_group(group);
    py::object label_list = labels_metadata.attr("get")(py::str("labels"), py::list());
    label_list.attr("append")(py::str(name));

    py::dict labels_payload;
    labels_payload["labels"] = label_list;
    writer_add_metadata_to_group(group, labels_payload, version);

    py::dict image_label_payload;
    image_label_payload["image-label"] = image_label_metadata;
    writer_add_metadata_to_group(label_group, image_label_payload, version);
}

py::list validate_plate_wells(
    py::object wells,
    py::object rows,
    py::object columns,
    py::object fmt = py::none()) {
    if (wells.is_none() || py::len(wells) == 0) {
        throw py::value_error("Empty wells list");
    }

    py::list validated_wells;
    for (const py::handle& well_handle : py::iterable(wells)) {
        py::object well = py::reinterpret_borrow<py::object>(well_handle);
        if (py::isinstance<py::str>(well)) {
            py::object well_dict = fmt.attr("generate_well_dict")(well, rows, columns);
            fmt.attr("validate_well_dict")(well_dict, rows, columns);
            validated_wells.append(well_dict);
        } else if (py::isinstance<py::dict>(well)) {
            fmt.attr("validate_well_dict")(well, rows, columns);
            validated_wells.append(well);
        } else {
            throw py::value_error(
                "Unrecognized type for " +
                py::cast<std::string>(py::str(well)));
        }
    }

    return validated_wells;
}

py::object blosc_compressor() {
    py::object blosc = py::module_::import("numcodecs").attr("Blosc");
    const auto plan = ome_zarr_c::native_code::writer_blosc_plan();
    return blosc(
        py::arg("cname") = py::str(plan.cname),
        py::arg("clevel") = py::int_(plan.clevel),
        py::arg("shuffle") = blosc.attr(plan.shuffle_attr.c_str()));
}

py::object resolve_storage_options(py::object storage_options, py::object path) {
    const auto plan = ome_zarr_c::native_code::writer_storage_options_plan(
        ome_zarr_c::bindings::object_truthy(storage_options),
        py::isinstance<py::list>(storage_options));
    py::dict options;
    if (plan.return_copy) {
        options = py::cast<py::dict>(storage_options.attr("copy")());
        return options;
    }
    if (plan.return_item) {
        return storage_options.attr("__getitem__")(path);
    }
    return options;
}

py::dict writer_pyramid_plan(
    py::object pyramid,
    int zarr_format,
    py::object axes = py::none(),
    py::object storage_options = py::none()) {
    std::vector<std::vector<std::int64_t>> shapes;
    shapes.reserve(static_cast<std::size_t>(py::len(pyramid)));
    for (const py::handle& level_handle : py::iterable(pyramid)) {
        py::object level = py::reinterpret_borrow<py::object>(level_handle);
        std::vector<std::int64_t> shape;
        for (const py::handle& dim : py::iterable(level.attr("shape"))) {
            shape.push_back(py::cast<std::int64_t>(dim));
        }
        shapes.push_back(std::move(shape));
    }

    std::vector<std::string> axis_names;
    if (!axes.is_none()) {
        for (const py::handle& axis_handle : py::iterable(axes)) {
            py::object axis = py::reinterpret_borrow<py::object>(axis_handle);
            if (py::isinstance<py::dict>(axis) &&
                py::cast<py::dict>(axis).contains("name")) {
                axis_names.push_back(
                    py::cast<std::string>(py::cast<py::dict>(axis)["name"]));
            }
        }
    }

    std::vector<std::vector<std::size_t>> explicit_chunks;
    explicit_chunks.reserve(shapes.size());
    const py::ssize_t pyramid_len = py::len(pyramid);
    for (py::ssize_t index = 0; index < pyramid_len; ++index) {
        py::object chunks = py::none();
        if (py::isinstance<py::list>(storage_options) &&
            py::len(storage_options) > static_cast<py::size_t>(index) &&
            py::isinstance<py::dict>(storage_options.attr("__getitem__")(index)) &&
            py::cast<py::dict>(storage_options.attr("__getitem__")(index)).contains("chunks")) {
            chunks =
                storage_options.attr("__getitem__")(index).attr("get")(py::str("chunks"));
        } else if (py::isinstance<py::dict>(storage_options) &&
                   py::cast<py::dict>(storage_options).contains("chunks")) {
            chunks = storage_options.attr("get")(py::str("chunks"));
        }

        if (chunks.is_none()) {
            explicit_chunks.push_back({});
            continue;
        }

        std::vector<std::size_t> chunk_values;
        if (PyLong_Check(chunks.ptr())) {
            chunk_values.push_back(py::cast<std::size_t>(chunks));
        } else {
            for (const py::handle& chunk_value : py::iterable(chunks)) {
                chunk_values.push_back(py::cast<std::size_t>(chunk_value));
            }
        }
        explicit_chunks.push_back(std::move(chunk_values));
    }

    const auto plan = ome_zarr_c::native_code::writer_pyramid_plan(
        shapes,
        zarr_format,
        axis_names,
        explicit_chunks);

    py::dict payload;
    payload["zarr_format"] = py::int_(plan.zarr_format);
    payload["use_v2_chunk_key_encoding"] = py::bool_(plan.use_v2_chunk_key_encoding);
    py::list dimension_names;
    for (const auto& axis_name : plan.dimension_names) {
        dimension_names.append(py::str(axis_name));
    }
    payload["dimension_names"] = dimension_names;

    py::list levels;
    for (const auto& level_plan : plan.levels) {
        py::dict level;
        level["component"] = py::str(level_plan.component);
        level["has_chunks"] = py::bool_(level_plan.has_chunks);
        levels.append(level);
    }
    payload["levels"] = levels;
    return payload;
}

std::optional<std::string> optional_method_name(py::object method) {
    if (method.is_none()) {
        return std::nullopt;
    }
    if (py::isinstance<py::str>(method)) {
        return py::cast<std::string>(method);
    }
    if (py::hasattr(method, "value") && py::isinstance<py::str>(method.attr("value"))) {
        return py::cast<std::string>(method.attr("value"));
    }
    return py::cast<std::string>(py::str(method));
}

py::dict writer_labels_plan(
    py::object dims,
    bool use_default_scaler,
    bool scaler_is_none,
    int scaler_max_layer,
    py::object method = py::none()) {
    std::vector<std::string> native_dims;
    native_dims.reserve(py::len(dims));
    for (const py::handle& dim_handle : py::iterable(dims)) {
        native_dims.push_back(py::cast<std::string>(dim_handle));
    }

    const auto plan = ome_zarr_c::native_code::writer_labels_plan(
        native_dims,
        use_default_scaler,
        scaler_is_none,
        scaler_max_layer,
        optional_method_name(method));

    py::dict payload;
    payload["resolved_method"] = py::str(plan.resolved_method);
    payload["warn_scaler_deprecated"] = py::bool_(plan.warn_scaler_deprecated);
    py::list scale_factors;
    for (const auto& level : plan.scale_factors) {
        py::dict scale_factor;
        for (const auto& [dim, factor] : level) {
            scale_factor[py::str(dim)] = py::int_(factor);
        }
        scale_factors.append(scale_factor);
    }
    payload["scale_factors"] = scale_factors;
    return payload;
}

py::dict writer_image_plan(
    py::object dims,
    bool scaler_present,
    int scaler_max_layer,
    const std::string& scaler_method,
    py::object method = py::none()) {
    std::vector<std::string> native_dims;
    native_dims.reserve(py::len(dims));
    for (const py::handle& dim_handle : py::iterable(dims)) {
        native_dims.push_back(py::cast<std::string>(dim_handle));
    }

    const auto plan = ome_zarr_c::native_code::writer_image_plan(
        native_dims,
        scaler_present,
        scaler_max_layer,
        scaler_method,
        optional_method_name(method));

    py::dict payload;
    payload["resolved_method"] = py::str(plan.resolved_method);
    payload["warn_scaler_deprecated"] = py::bool_(plan.warn_scaler_deprecated);
    payload["warn_laplacian_fallback"] = py::bool_(plan.warn_laplacian_fallback);
    py::list scale_factors;
    for (const auto& level : plan.scale_factors) {
        py::dict scale_factor;
        for (const auto& [dim, factor] : level) {
            scale_factor[py::str(dim)] = py::int_(factor);
        }
        scale_factors.append(scale_factor);
    }
    payload["scale_factors"] = scale_factors;
    return payload;
}

}  // namespace

void register_writer_bindings(py::module_& m) {
    m.def("writer_check_format", &writer_check_format, py::arg("group"), py::arg("version") = py::none());
    m.def(
        "writer_check_group_fmt",
        &writer_check_group_fmt,
        py::arg("group"),
        py::arg("version") = py::none(),
        py::arg("mode") = "a");
    m.def("writer_get_metadata", &writer_get_metadata, py::arg("group"));
    m.def(
        "writer_add_metadata",
        &writer_add_metadata,
        py::arg("group"),
        py::arg("metadata"),
        py::arg("version") = py::none());
    m.def(
        "writer_write_multiscales_metadata",
        &writer_write_multiscales_metadata,
        py::arg("group"),
        py::arg("datasets"),
        py::arg("version"),
        py::arg("axes") = py::none(),
        py::arg("name") = py::none(),
        py::arg("metadata") = py::dict());
    m.def(
        "writer_write_plate_metadata",
        &writer_write_plate_metadata,
        py::arg("group"),
        py::arg("rows"),
        py::arg("columns"),
        py::arg("wells"),
        py::arg("version"),
        py::arg("acquisitions") = py::none(),
        py::arg("field_count") = py::none(),
        py::arg("name") = py::none());
    m.def(
        "writer_write_well_metadata",
        &writer_write_well_metadata,
        py::arg("group"),
        py::arg("images"),
        py::arg("version"));
    m.def(
        "writer_write_label_metadata",
        &writer_write_label_metadata,
        py::arg("group"),
        py::arg("name"),
        py::arg("colors") = py::none(),
        py::arg("properties") = py::none(),
        py::arg("version") = default_writer_version(),
        py::arg("metadata") = py::dict());

    m.def("_validate_plate_wells",
          &validate_plate_wells,
          py::arg("wells"),
          py::arg("rows"),
          py::arg("columns"),
          py::arg("fmt"));
    m.def("_blosc_compressor", &blosc_compressor);
    m.def(
        "_resolve_storage_options",
        &resolve_storage_options,
        py::arg("storage_options"),
        py::arg("path"));
    m.def(
        "writer_pyramid_plan",
        &writer_pyramid_plan,
        py::arg("pyramid"),
        py::arg("zarr_format"),
        py::arg("axes") = py::none(),
        py::arg("storage_options") = py::none());
    m.def(
        "writer_labels_plan",
        &writer_labels_plan,
        py::arg("dims"),
        py::arg("use_default_scaler"),
        py::arg("scaler_is_none"),
        py::arg("scaler_max_layer"),
        py::arg("method") = py::none());
    m.def(
        "writer_image_plan",
        &writer_image_plan,
        py::arg("dims"),
        py::arg("scaler_present"),
        py::arg("scaler_max_layer"),
        py::arg("scaler_method"),
        py::arg("method") = py::none());
}
