#include <pybind11/pybind11.h>

#include <functional>
#include <optional>
#include <string>

#include "../native/utils.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

py::object read_text_with_open(const py::object& path) {
    py::object builtins = py::module_::import("builtins");
    py::object handle = builtins.attr("open")(path);
    try {
        py::object text = handle.attr("read")();
        handle.attr("close")();
        return text;
    } catch (...) {
        try {
            handle.attr("close")();
        } catch (...) {
        }
        throw;
    }
}

py::list discovered_images_to_pylist(
    const std::vector<ome_zarr_c::native_code::UtilsDiscoveredImage>& images,
    bool return_string_paths) {
    py::object pathlib = py::module_::import("pathlib");
    py::list result;
    for (const auto& image : images) {
        py::list row;
        if (return_string_paths) {
            row.append(py::str(image.path));
        } else {
            row.append(pathlib.attr("Path")(py::str(image.path)));
        }
        row.append(py::str(image.name));
        row.append(py::str(image.dirname));
        result.append(row);
    }
    return result;
}

py::list find_multiscales_impl(py::object path_to_zattrs) {
    py::object builtins = py::module_::import("builtins");
    py::object json = py::module_::import("json");
    py::object logging = py::module_::import("logging");
    py::object os_path = py::module_::import("os").attr("path");
    py::object element_tree = py::module_::import("xml.etree.ElementTree");
    py::object logger = logging.attr("getLogger")(py::str("ome_zarr.utils"));
    const bool return_string_paths = py::isinstance<py::str>(path_to_zattrs);
    const std::string native_path = py::cast<std::string>(py::str(path_to_zattrs));
    const std::string basename =
        py::cast<std::string>(os_path.attr("basename")(path_to_zattrs));
    const std::string dirname =
        py::cast<std::string>(os_path.attr("dirname")(path_to_zattrs));

    py::object text = py::none();
    for (const char* name : {".zattrs", "zarr.json"}) {
        py::object candidate = ome_zarr_c::bindings::true_divide(
            path_to_zattrs, py::str(name));
        if (py::cast<bool>(candidate.attr("exists")())) {
            text = read_text_with_open(
                ome_zarr_c::bindings::true_divide(path_to_zattrs, py::str(name)));
            break;
        }
    }

    if (text.is_none()) {
        builtins.attr("print")(
            py::str(ome_zarr_c::native_code::utils_missing_metadata_message()));
        return py::list();
    }

    py::dict zattrs = py::cast<py::dict>(json.attr("loads")(text));
    py::object attributes = zattrs.attr("get")("attributes", py::none());
    if (!attributes.is_none() &&
        py::cast<bool>(attributes.attr("__contains__")("ome"))) {
        zattrs = py::cast<py::dict>(attributes.attr("get")("ome"));
    }

    if (py::cast<bool>(zattrs.attr("__contains__")("plate"))) {
        py::dict plate = py::cast<py::dict>(zattrs.attr("get")("plate"));
        py::object wells = plate.attr("get")("wells");
        py::list wells_list = py::cast<py::list>(wells);
        if (py::len(wells_list) > 0) {
            std::vector<std::string> wells;
            wells.reserve(py::len(wells_list));
            for (const py::handle& well_handle : wells_list) {
                py::object well = py::reinterpret_borrow<py::object>(well_handle);
                wells.push_back(py::cast<std::string>(well.attr("get")("path")));
            }
            return discovered_images_to_pylist(
                ome_zarr_c::native_code::utils_plate_images(
                    native_path,
                    basename,
                    dirname,
                    wells),
                return_string_paths);
        }
        logger.attr("info")("No wells found in plate%s", path_to_zattrs);
        return py::list();
    }

    if (ome_zarr_c::bindings::objects_equal(
            zattrs.attr("get")("bioformats2raw.layout"),
            py::int_(3))) {
        try {
            py::object metadata_xml = ome_zarr_c::bindings::true_divide(
                ome_zarr_c::bindings::true_divide(path_to_zattrs, py::str("OME")),
                py::str("METADATA.ome.xml"));
            py::object tree = element_tree.attr("parse")(metadata_xml);
            py::object root = tree.attr("getroot")();

            std::vector<std::optional<std::string>> image_names;
            for (const py::handle& child_handle : root) {
                py::object child = py::reinterpret_borrow<py::object>(child_handle);
                if (!py::cast<bool>(child.attr("tag").attr("endswith")("Image"))) {
                    continue;
                }
                py::object img_name = child.attr("attrib").attr("get")("Name", py::none());
                if (img_name.is_none()) {
                    image_names.push_back(std::nullopt);
                } else {
                    image_names.push_back(py::cast<std::string>(img_name));
                }
            }
            return discovered_images_to_pylist(
                ome_zarr_c::native_code::utils_bioformats_images(
                    native_path,
                    basename,
                    dirname,
                    image_names),
                return_string_paths);
        } catch (const py::error_already_set& ex) {
            builtins.attr("print")(ex.value());
        }
    }

    if (ome_zarr_c::bindings::object_truthy(zattrs.attr("get")("multiscales"))) {
        return discovered_images_to_pylist(
            ome_zarr_c::native_code::utils_single_multiscales_image(
                native_path,
                basename,
                dirname),
            return_string_paths);
    }

    return py::list();
}

py::list find_multiscales(py::object path_to_zattrs) {
    return find_multiscales_impl(path_to_zattrs);
}

py::dict utils_view_plan(
    py::object input_path,
    int port = 8000,
    bool force = false) {
    std::size_t discovered_count = 0;
    if (!force) {
        py::object pathlib = py::module_::import("pathlib");
        py::object path_obj = pathlib.attr("Path")(input_path);
        const bool has_metadata =
            py::cast<bool>(ome_zarr_c::bindings::true_divide(path_obj, py::str(".zattrs")).attr("exists")()) ||
            py::cast<bool>(ome_zarr_c::bindings::true_divide(path_obj, py::str("zarr.json")).attr("exists")());
        if (has_metadata) {
            discovered_count = static_cast<std::size_t>(py::len(find_multiscales_impl(path_obj)));
        }
    }

    const auto plan = ome_zarr_c::native_code::utils_view_plan(
        py::cast<std::string>(py::str(input_path)),
        port,
        force,
        discovered_count);
    py::dict payload;
    payload["should_warn"] = py::bool_(plan.should_warn);
    payload["warning_message"] = py::str(plan.warning_message);
    payload["parent_dir"] = py::str(plan.parent_dir);
    payload["image_name"] = py::str(plan.image_name);
    payload["url"] = py::str(plan.url);
    return payload;
}

py::list utils_finder_discover_images(py::object input_path) {
    py::object pathlib = py::module_::import("pathlib");
    py::object root = pathlib.attr("Path")(input_path);

    std::function<py::list(py::object)> walk = [&](py::object path) -> py::list {
        py::list discovered;
        const bool has_metadata =
            py::cast<bool>(ome_zarr_c::bindings::true_divide(path, py::str(".zattrs")).attr("exists")()) ||
            py::cast<bool>(ome_zarr_c::bindings::true_divide(path, py::str("zarr.json")).attr("exists")());
        if (has_metadata) {
            py::list direct = find_multiscales_impl(path);
            for (const py::handle& item : direct) {
                discovered.append(py::reinterpret_borrow<py::object>(item));
            }
            return discovered;
        }

        for (const py::handle& child_handle : path.attr("iterdir")()) {
            py::object child = py::reinterpret_borrow<py::object>(child_handle);
            const bool child_has_metadata =
                py::cast<bool>(ome_zarr_c::bindings::true_divide(child, py::str(".zattrs")).attr("exists")()) ||
                py::cast<bool>(ome_zarr_c::bindings::true_divide(child, py::str("zarr.json")).attr("exists")());
            if (child_has_metadata) {
                py::list child_images = find_multiscales_impl(child);
                for (const py::handle& item : child_images) {
                    discovered.append(py::reinterpret_borrow<py::object>(item));
                }
            } else if (py::cast<bool>(child.attr("is_dir")())) {
                py::list nested = walk(child);
                for (const py::handle& item : nested) {
                    discovered.append(py::reinterpret_borrow<py::object>(item));
                }
            }
        }
        return discovered;
    };

    return walk(root);
}

py::dict utils_finder_plan(py::object input_path, int port = 8000) {
    py::object json = py::module_::import("json");
    py::object urllib_parse = py::module_::import("urllib.parse");
    const auto plan = ome_zarr_c::native_code::utils_finder_plan(
        py::cast<std::string>(py::str(input_path)),
        port);

    py::dict source;
    source["uri"] = py::str(plan.source_uri);
    source["type"] = py::str("csv");
    source["name"] = py::str("biofile_finder.csv");

    py::dict payload;
    payload["parent_path"] = py::str(plan.parent_path);
    payload["server_dir"] = py::str(plan.server_dir);
    payload["csv_path"] = py::str(plan.csv_path);
    payload["source"] = source;
    payload["url"] = (
        py::str(plan.url) +
        py::str("?source=") +
        urllib_parse.attr("quote")(json.attr("dumps")(source)) +
        py::str("&v=2"));
    return payload;
}

py::list utils_finder_rows(
    py::object input_path,
    int port,
    py::object images,
    const std::string& server_dir) {
    py::object datetime = py::module_::import("datetime").attr("datetime");
    py::object os_path = py::module_::import("os").attr("path");

    py::list rows;
    for (const py::handle& image_handle : py::iterable(images)) {
        py::object image = py::reinterpret_borrow<py::object>(image_handle);
        const std::string image_path =
            py::cast<std::string>(py::str(image.attr("__getitem__")(py::int_(0))));
        const std::string image_name =
            py::cast<std::string>(image.attr("__getitem__")(py::int_(1)));
        const std::string image_dir =
            py::cast<std::string>(py::str(image.attr("__getitem__")(py::int_(2))));
        const std::string relpath = py::cast<std::string>(
            os_path.attr("relpath")(py::str(image_path), input_path));
        const std::string folders_path = py::cast<std::string>(
            os_path.attr("relpath")(py::str(image_dir), input_path));
        const std::string display_name =
            image_name.empty()
                ? py::cast<std::string>(os_path.attr("basename")(py::str(image_path)))
                : image_name;

        std::string uploaded;
        try {
            py::object mtime = os_path.attr("getmtime")(py::str(image_path));
            uploaded = py::cast<std::string>(
                datetime.attr("fromtimestamp")(mtime).attr("strftime")(
                    py::str("%Y-%m-%d %H:%M:%S.%Z")));
        } catch (const py::error_already_set& ex) {
            if (!ex.matches(PyExc_OSError)) {
                throw;
            }
        }

        const auto row = ome_zarr_c::native_code::utils_finder_row(
            port,
            server_dir,
            relpath,
            display_name,
            folders_path,
            uploaded);
        py::list py_row;
        py_row.append(py::str(row.file_path));
        py_row.append(py::str(row.name));
        py_row.append(py::str(row.folders));
        py_row.append(py::str(row.uploaded));
        rows.append(py_row);
    }
    return rows;
}

py::list info_lines(py::object node, bool stats = false) {
    py::object dask = py::module_::import("dask");
    py::object logger =
        py::module_::import("logging").attr("getLogger")(py::str("ome_zarr.utils"));

    py::list lines;
    lines.append(py::str(node));

    py::object loc = node.attr("zarr");
    py::object zgroup = loc.attr("zgroup");
    py::object version = zgroup.attr("get")(py::str("version"));
    if (version.is_none()) {
        py::list fallback;
        fallback.append(py::dict());
        py::object multiscales = zgroup.attr("get")(py::str("multiscales"), fallback);
        py::object first = multiscales.attr("__getitem__")(0);
        version = first.attr("get")(py::str("version"), py::str(""));
    }
    lines.append(py::str(" - version: ") + py::str(version));
    lines.append(py::str(" - metadata"));

    for (const py::handle& spec_handle : node.attr("specs")) {
        py::object spec = py::reinterpret_borrow<py::object>(spec_handle);
        lines.append(py::str("   - ") +
                     py::str(spec.attr("__class__").attr("__name__")));
    }

    lines.append(py::str(" - data"));
    for (const py::handle& array_handle : node.attr("data")) {
        py::object array = py::reinterpret_borrow<py::object>(array_handle);
        py::str line = py::str("   - ") + py::str(array.attr("shape"));
        if (stats) {
            line = line + py::str(" minmax=") +
                   py::str(
                       dask.attr("compute")(array.attr("min")(), array.attr("max")()));
        }
        lines.append(line);
    }

    logger.attr("debug")(node.attr("data"));
    return lines;
}

}  // namespace

void register_utils_bindings(py::module_& m) {
    m.def("find_multiscales", &find_multiscales);
    m.def("utils_view_plan", &utils_view_plan, py::arg("input_path"), py::arg("port") = 8000, py::arg("force") = false);
    m.def("utils_finder_discover_images", &utils_finder_discover_images, py::arg("input_path"));
    m.def("utils_finder_plan", &utils_finder_plan, py::arg("input_path"), py::arg("port") = 8000);
    m.def("utils_finder_rows", &utils_finder_rows, py::arg("input_path"), py::arg("port"), py::arg("images"), py::arg("server_dir"));
    m.def("info_lines", &info_lines, py::arg("node"), py::arg("stats") = false);
}
