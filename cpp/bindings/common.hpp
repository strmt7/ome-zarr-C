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

inline std::vector<std::string> sequence_to_string_vector(const py::sequence& values) {
    PyObject* fast = PySequence_Fast(values.ptr(), "expected a sequence");
    if (fast == nullptr) {
        throw py::error_already_set();
    }

    py::object fast_holder = py::reinterpret_steal<py::object>(fast);
    const auto size = static_cast<std::size_t>(PySequence_Fast_GET_SIZE(fast_holder.ptr()));
    PyObject** items = PySequence_Fast_ITEMS(fast_holder.ptr());

    std::vector<std::string> result;
    result.reserve(size);
    for (std::size_t index = 0; index < size; ++index) {
        result.push_back(py::cast<std::string>(
            py::reinterpret_borrow<py::object>(items[index])));
    }
    return result;
}

inline std::vector<ome_zarr_c::native_code::AxisRecord> axis_records_from_sequence(
    const py::sequence& axes) {
    std::vector<ome_zarr_c::native_code::AxisRecord> records;
    records.reserve(py::len(axes));

    for (const py::handle& axis_handle : axes) {
        ome_zarr_c::native_code::AxisRecord record{};
        if (PyUnicode_Check(axis_handle.ptr())) {
            record.has_name = true;
            record.name = py::cast<std::string>(axis_handle);
            record.has_type = false;
            record.type = "";
            record.axis_repr = "";
            record.type_repr = "None";
        } else {
            py::dict axis = py::cast<py::dict>(axis_handle);
            record.has_name = PyDict_GetItemString(axis.ptr(), "name") != nullptr;
            if (record.has_name) {
                record.name = py::cast<std::string>(
                    py::reinterpret_borrow<py::object>(
                        PyDict_GetItemString(axis.ptr(), "name")));
            }
            PyObject* axis_type_obj = PyDict_GetItemString(axis.ptr(), "type");
            py::object axis_type = axis_type_obj == nullptr
                ? py::none()
                : py::reinterpret_borrow<py::object>(axis_type_obj);
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

inline py::list axis_records_to_dict_list(
    const std::vector<ome_zarr_c::native_code::AxisRecord>& records) {
    py::list result;
    for (const auto& record : records) {
        py::dict axis_dict;
        axis_dict["name"] = py::str(record.name);
        if (record.has_type) {
            axis_dict["type"] = py::str(record.type);
        }
        result.append(std::move(axis_dict));
    }
    return result;
}

inline py::list axis_records_to_dict_list_preserving_original_dicts(
    const std::vector<ome_zarr_c::native_code::AxisRecord>& records,
    const py::sequence& original_axes) {
    py::list result;
    const auto original_size = static_cast<std::size_t>(py::len(original_axes));
    if (records.size() != original_size) {
        throw std::runtime_error("axis record count mismatch");
    }

    for (std::size_t index = 0; index < records.size(); ++index) {
        const auto& record = records[index];
        py::handle original_axis = original_axes[py::int_(index)];
        if (PyUnicode_Check(original_axis.ptr())) {
            py::dict axis_dict;
            axis_dict["name"] = py::str(record.name);
            if (record.has_type) {
                axis_dict["type"] = py::str(record.type);
            }
            result.append(std::move(axis_dict));
            continue;
        }

        PyObject* copied = PyDict_Copy(original_axis.ptr());
        if (copied == nullptr) {
            throw py::error_already_set();
        }
        result.append(py::reinterpret_steal<py::dict>(copied));
    }

    return result;
}

}  // namespace ome_zarr_c::bindings
