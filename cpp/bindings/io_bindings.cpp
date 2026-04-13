#include <pybind11/pybind11.h>

#include <string>
#include <vector>

#include "common.hpp"
#include "../native/io.hpp"

namespace py = pybind11;

namespace {

ome_zarr_c::native_code::IoPathKind io_path_kind_from_object(
    const py::object& path,
    const py::object& path_type,
    const py::object& fsspec_store_type,
    const py::object& local_store_type) {
    if (py::isinstance(path, path_type)) {
        return ome_zarr_c::native_code::IoPathKind::pathlib_path;
    }
    if (py::isinstance<py::str>(path)) {
        return ome_zarr_c::native_code::IoPathKind::string_path;
    }
    if (py::isinstance(path, fsspec_store_type)) {
        return ome_zarr_c::native_code::IoPathKind::fsspec_store;
    }
    if (py::isinstance(path, local_store_type)) {
        return ome_zarr_c::native_code::IoPathKind::local_store;
    }
    throw py::type_error(
        "not expecting: " + py::cast<std::string>(py::str(path.attr("__class__"))));
}

py::dict load_io_metadata(
    const py::object& store,
    const py::object& fmt,
    const std::string& mode) {
    py::object zarr = py::module_::import("zarr");
    py::dict zgroup;
    py::dict zarray;
    py::dict metadata;
    bool metadata_loaded = false;
    bool has_ome_namespace = false;

    try {
        py::object group = zarr.attr("open_group")(
            py::arg("store") = store,
            py::arg("path") = "/",
            py::arg("mode") = "r",
            py::arg("zarr_format") = py::none());
        zgroup = py::cast<py::dict>(group.attr("attrs").attr("asdict")());
        metadata_loaded = true;
        has_ome_namespace = zgroup.contains("ome");
    } catch (const py::error_already_set& ex) {
        if (!ex.matches(PyExc_ValueError) && !ex.matches(PyExc_FileNotFoundError)) {
            throw;
        }
    }

    const auto plan = ome_zarr_c::native_code::io_metadata_plan(
        metadata_loaded,
        mode == "w",
        has_ome_namespace);
    if (plan.create_group) {
        zarr.attr("open_group")(
            py::arg("store") = store,
            py::arg("path") = "/",
            py::arg("mode") = "w",
            py::arg("zarr_format") = fmt.attr("zarr_format"));
    }
    if (plan.unwrap_ome_namespace) {
        metadata = py::cast<py::dict>(zgroup["ome"]);
    } else {
        metadata = zgroup;
    }

    py::dict state;
    state["zgroup"] = metadata;
    state["zarray"] = zarray;
    state["metadata"] = metadata;
    state["exists"] = py::bool_(plan.exists);
    return state;
}

py::dict io_location_state(
    py::object path,
    const std::string& mode,
    py::object fmt) {
    py::object pathlib = py::module_::import("pathlib");
    py::object path_type = pathlib.attr("Path");
    py::object zarr_storage = py::module_::import("zarr.storage");
    py::object fsspec_store_type = zarr_storage.attr("FsspecStore");
    py::object local_store_type = zarr_storage.attr("LocalStore");
    py::object format_module = py::module_::import("ome_zarr_c.format");
    py::object logger =
        py::module_::import("logging").attr("getLogger")(py::str("ome_zarr.io"));

    if (fmt.is_none()) {
        fmt = format_module.attr("CurrentFormat")();
    }

    const auto kind = io_path_kind_from_object(
        path, path_type, fsspec_store_type, local_store_type);

    std::string raw_path;
    if (kind == ome_zarr_c::native_code::IoPathKind::pathlib_path) {
        raw_path = py::cast<std::string>(py::str(path.attr("resolve")()));
    } else if (kind == ome_zarr_c::native_code::IoPathKind::string_path) {
        raw_path = py::cast<std::string>(path);
    } else if (kind == ome_zarr_c::native_code::IoPathKind::fsspec_store) {
        raw_path = py::cast<std::string>(path.attr("path"));
    } else {
        raw_path = py::cast<std::string>(py::str(path.attr("root")));
    }

    const auto constructor_plan =
        ome_zarr_c::native_code::io_constructor_plan(kind, raw_path);
    py::object store = constructor_plan.use_input_store
        ? path
        : fmt.attr("init_store")(constructor_plan.normalized_path, mode);
    py::dict state = load_io_metadata(store, fmt, mode);

    py::object detected = format_module.attr("detect_format")(state["metadata"], fmt);
    if (!ome_zarr_c::bindings::objects_equal(detected, fmt)) {
        logger.attr("warning")(
            "version mismatch: detected: %s, requested: %s",
            detected,
            fmt);
        fmt = detected;
        store = detected.attr("init_store")(constructor_plan.normalized_path, mode);
        state = load_io_metadata(store, fmt, mode);
    }

    py::dict payload;
    payload["fmt"] = fmt;
    payload["mode"] = py::str(mode);
    payload["path"] = py::str(constructor_plan.normalized_path);
    payload["store"] = store;
    payload["zgroup"] = state["zgroup"];
    payload["zarray"] = state["zarray"];
    payload["metadata"] = state["metadata"];
    payload["exists"] = state["exists"];
    return payload;
}

py::str io_basename(const std::string& path) {
    return py::str(ome_zarr_c::native_code::io_basename(path));
}

py::list io_parts(const std::string& path, bool is_file) {
    py::list parts;
    for (const auto& part : ome_zarr_c::native_code::io_parts(path, is_file)) {
        parts.append(py::str(part));
    }
    return parts;
}

py::str io_subpath(
    const std::string& path,
    const std::string& subpath,
    bool is_file,
    bool is_http) {
    return py::str(
        ome_zarr_c::native_code::io_subpath(path, subpath, is_file, is_http));
}

py::str io_repr(
    const std::string& subpath,
    bool has_zgroup,
    bool has_zarray) {
    return py::str(
        ome_zarr_c::native_code::io_repr(subpath, has_zgroup, has_zarray));
}

bool io_is_local_store(py::object store) {
    py::object local_store_type = py::module_::import("zarr.storage").attr("LocalStore");
    const auto kind = py::isinstance(store, local_store_type)
        ? ome_zarr_c::native_code::IoPathKind::local_store
        : ome_zarr_c::native_code::IoPathKind::fsspec_store;
    return ome_zarr_c::native_code::io_is_local_store(kind);
}

bool io_parse_url_returns_none(const std::string& mode, bool exists) {
    return ome_zarr_c::native_code::io_parse_url_returns_none(mode, exists);
}

bool io_protocol_is_http(py::object protocol) {
    std::vector<std::string> protocols;
    if (py::isinstance<py::tuple>(protocol) || py::isinstance<py::list>(protocol)) {
        for (const py::handle& entry : py::iterable(protocol)) {
            protocols.push_back(py::cast<std::string>(entry));
        }
    } else {
        protocols.push_back(py::cast<std::string>(protocol));
    }
    return ome_zarr_c::native_code::io_protocol_is_http(protocols);
}

}  // namespace

void register_io_bindings(py::module_& m) {
    m.def("io_location_state", &io_location_state, py::arg("path"), py::arg("mode"), py::arg("fmt"));
    m.def("io_basename", &io_basename, py::arg("path"));
    m.def("io_parts", &io_parts, py::arg("path"), py::arg("is_file"));
    m.def(
        "io_subpath",
        &io_subpath,
        py::arg("path"),
        py::arg("subpath"),
        py::arg("is_file"),
        py::arg("is_http"));
    m.def(
        "io_repr",
        &io_repr,
        py::arg("subpath"),
        py::arg("has_zgroup"),
        py::arg("has_zarray"));
    m.def("io_is_local_store", &io_is_local_store, py::arg("store"));
    m.def(
        "io_parse_url_returns_none",
        &io_parse_url_returns_none,
        py::arg("mode"),
        py::arg("exists"));
    m.def("io_protocol_is_http", &io_protocol_is_http, py::arg("protocol"));
}
