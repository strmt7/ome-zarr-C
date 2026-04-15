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
    py::object pathlib = py::module_::import("pathlib");
    py::object element_tree = py::module_::import("xml.etree.ElementTree");
    py::object logger = logging.attr("getLogger")(py::str("ome_zarr.utils"));
    const bool return_string_paths = py::isinstance<py::str>(path_to_zattrs);
    py::object path_obj = pathlib.attr("Path")(path_to_zattrs);
    const std::string native_path = py::cast<std::string>(py::str(path_to_zattrs));
    const std::string basename =
        py::cast<std::string>(os_path.attr("basename")(path_to_zattrs));
    const std::string dirname =
        py::cast<std::string>(os_path.attr("dirname")(path_to_zattrs));

    py::object text = py::none();
    for (const char* name : {".zattrs", "zarr.json"}) {
        py::object candidate =
            ome_zarr_c::bindings::true_divide(path_obj, py::str(name));
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

}  // namespace

void register_utils_bindings(py::module_& m) {
    m.def("find_multiscales", &find_multiscales);
}
