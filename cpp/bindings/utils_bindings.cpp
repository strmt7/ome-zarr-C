#include <pybind11/pybind11.h>

#include <string>

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

py::list find_multiscales(py::object path_to_zattrs) {
    py::object builtins = py::module_::import("builtins");
    py::object json = py::module_::import("json");
    py::object logging = py::module_::import("logging");
    py::object os_path = py::module_::import("os").attr("path");
    py::object path_cls = py::module_::import("pathlib").attr("Path");
    py::object element_tree = py::module_::import("xml.etree.ElementTree");
    py::object logger = logging.attr("getLogger")(py::str("ome_zarr.utils"));

    py::object text = py::none();
    for (const char* name : {".zattrs", "zarr.json"}) {
        py::object candidate = ome_zarr_c::bindings::true_divide(path_cls(path_to_zattrs), py::str(name));
        if (py::cast<bool>(candidate.attr("exists")())) {
            text = read_text_with_open(
                ome_zarr_c::bindings::true_divide(path_to_zattrs, py::str(name)));
            break;
        }
    }

    if (text.is_none()) {
        builtins.attr("print")(
            py::str("No .zattrs or zarr.json found in {path_to_zattrs}"));
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
            py::dict first_well = py::cast<py::dict>(wells_list[0]);
            py::object path_to_zarr = ome_zarr_c::bindings::true_divide(
                ome_zarr_c::bindings::true_divide(
                    path_to_zattrs, first_well.attr("get")("path")),
                py::str("0"));
            py::list image;
            image.append(path_to_zarr);
            image.append(os_path.attr("basename")(path_to_zattrs));
            image.append(os_path.attr("dirname")(path_to_zattrs));

            py::list images;
            images.append(image);
            return images;
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

            py::list images;
            int series = 0;
            for (const py::handle& child_handle : root) {
                py::object child = py::reinterpret_borrow<py::object>(child_handle);
                if (!py::cast<bool>(child.attr("tag").attr("endswith")("Image"))) {
                    continue;
                }

                py::str default_name = py::str(
                    py::cast<std::string>(os_path.attr("basename")(path_to_zattrs)) +
                    " Series:" + std::to_string(series));
                py::object img_name = child.attr("attrib").attr("get")("Name", default_name);

                py::list image;
                image.append(
                    ome_zarr_c::bindings::true_divide(
                        path_to_zattrs, py::str(std::to_string(series))));
                image.append(img_name);
                image.append(os_path.attr("dirname")(path_to_zattrs));
                images.append(image);
                series += 1;
            }
            return images;
        } catch (const py::error_already_set& ex) {
            builtins.attr("print")(ex.value());
        }
    }

    if (ome_zarr_c::bindings::object_truthy(zattrs.attr("get")("multiscales"))) {
        py::list image;
        image.append(path_to_zattrs);
        image.append(os_path.attr("basename")(path_to_zattrs));
        image.append(os_path.attr("dirname")(path_to_zattrs));

        py::list images;
        images.append(image);
        return images;
    }

    return py::list();
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
    m.def("info_lines", &info_lines, py::arg("node"), py::arg("stats") = false);
}
