#pragma once

#include <pybind11/pybind11.h>

#include <map>
#include <string>
#include <vector>

#include "../native/axes.hpp"

namespace py = pybind11;

namespace ome_zarr_c::bindings {

inline std::string repr_object(const py::handle& obj) {
    return py::cast<std::string>(py::repr(obj));
}

[[noreturn]] inline void raise_plain_exception(const std::string& message) {
    PyErr_SetString(PyExc_Exception, message.c_str());
    throw py::error_already_set();
}

[[noreturn]] inline void raise_value_error_args(const py::tuple& args) {
    PyErr_SetObject(PyExc_ValueError, args.ptr());
    throw py::error_already_set();
}

[[noreturn]] inline void raise_overflow_error(const std::string& message) {
    PyErr_SetString(PyExc_OverflowError, message.c_str());
    throw py::error_already_set();
}

inline bool objects_equal(const py::handle& left, const py::handle& right) {
    const int result = PyObject_RichCompareBool(left.ptr(), right.ptr(), Py_EQ);
    if (result < 0) {
        throw py::error_already_set();
    }
    return result == 1;
}

inline bool object_truthy(const py::handle& obj) {
    const int result = PyObject_IsTrue(obj.ptr());
    if (result < 0) {
        throw py::error_already_set();
    }
    return result == 1;
}

inline bool rich_compare_bool(
    const py::handle& left,
    const py::handle& right,
    int op) {
    const int result = PyObject_RichCompareBool(left.ptr(), right.ptr(), op);
    if (result < 0) {
        throw py::error_already_set();
    }
    return result == 1;
}

inline py::object true_divide(const py::handle& left, const py::handle& right) {
    PyObject* result = PyNumber_TrueDivide(left.ptr(), right.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
}

inline py::object floor_divide(const py::handle& left, const py::handle& right) {
    PyObject* result = PyNumber_FloorDivide(left.ptr(), right.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
}

inline py::object call_callable(
    const py::handle& callable,
    const std::vector<py::object>& leading_args,
    const py::tuple& extra_args = py::tuple(),
    const py::dict& kwargs = py::dict()) {
    py::tuple call_args(leading_args.size() + extra_args.size());
    py::size_t index = 0;
    for (const py::object& arg : leading_args) {
        call_args[index++] = arg;
    }
    for (const py::handle& arg : extra_args) {
        call_args[index++] = py::reinterpret_borrow<py::object>(arg);
    }

    PyObject* result = PyObject_Call(callable.ptr(), call_args.ptr(), kwargs.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
}

inline void set_item(
    const py::handle& obj,
    const py::handle& key,
    const py::handle& value) {
    if (PyObject_SetItem(obj.ptr(), key.ptr(), value.ptr()) < 0) {
        throw py::error_already_set();
    }
}

inline std::string repr_joined_lines(const py::list& parts) {
    std::string message = "No common prefix:\n";
    for (const py::handle& part_handle : parts) {
        message += repr_object(part_handle);
        message += "\n";
    }
    return message;
}

inline py::tuple output_slices_for_shape(const py::handle& shape) {
    py::sequence shape_seq = py::cast<py::sequence>(shape);
    const py::size_t ndim = py::len(shape_seq);
    py::tuple slices(ndim);
    for (py::size_t index = 0; index < ndim; ++index) {
        slices[index] = py::slice(
            py::int_(0),
            py::reinterpret_borrow<py::object>(shape_seq[index]),
            py::int_(1));
    }
    return slices;
}

inline std::vector<ome_zarr_c::native_code::AxisRecord> axis_records_from_sequence(
    const py::sequence& axes) {
    std::vector<ome_zarr_c::native_code::AxisRecord> records;
    records.reserve(py::len(axes));

    for (const py::handle& axis_handle : axes) {
        ome_zarr_c::native_code::AxisRecord record{};
        if (py::isinstance<py::str>(axis_handle)) {
            record.has_name = true;
            record.name = py::cast<std::string>(axis_handle);
            record.has_type = false;
            record.type = "";
            record.axis_repr = "";
            record.type_repr = "None";
        } else {
            py::dict axis = py::cast<py::dict>(axis_handle);
            record.has_name = axis.contains("name");
            if (record.has_name) {
                record.name = py::cast<std::string>(axis["name"]);
            }
            py::object axis_type = axis.attr("get")("type");
            record.has_type = !axis_type.is_none();
            if (record.has_type) {
                record.type = py::cast<std::string>(axis_type);
            }
            record.axis_repr = repr_object(axis);
            record.type_repr = repr_object(axis_type);
        }
        records.push_back(std::move(record));
    }

    return records;
}

}  // namespace ome_zarr_c::bindings
