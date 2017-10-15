#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <zlib.h>
#include "deviceapps.pb-c.h"

#define MAGIC  0xFFFFFFFF
#define DEVICE_APPS_TYPE 1

typedef struct pbheader_s {
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;

#define PBHEADER_INIT {MAGIC, 0, 0}

size_t pack_and_write(PyObject *dict, gzFile fi) {
    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
    unsigned len;
    void *buf;

    PyObject *device_val = PyDict_GetItemString(dict, "device");
    PyObject *lat_val    = PyDict_GetItemString(dict, "lat");
    PyObject *lon_val    = PyDict_GetItemString(dict, "lon");
    PyObject *apps_val   = PyDict_GetItemString(dict, "apps");

    if (device_val) {
        if (PyDict_Check(device_val)) {
            PyObject *id_val = PyDict_GetItemString(device_val, "id");
            PyObject *type_val = PyDict_GetItemString(device_val, "type");
            if (id_val) {
                if (PyString_Check(id_val)) {
                    char *device_id = PyString_AsString(id_val);
                    device.has_id = 1;
                    device.id.data = (uint8_t*) device_id;
                    device.id.len = strlen(device_id);
                } else {
                    PyErr_SetString(PyExc_ValueError, "`id` must be a string type");
                    return 0;
                }
            }
            if (type_val) {
                if (PyString_Check(id_val)) {
                    char *device_type = PyString_AsString(type_val);
                    device.has_type = 1;
                    device.type.data = (uint8_t*) device_type;
                    device.type.len = strlen(device_type);
                } else {
                    PyErr_SetString(PyExc_ValueError, "`type` must be a string type");
                    return 0;
                }
            }
        } else {
            PyErr_SetString(PyExc_ValueError, "`device` must be a dictionary type");
            return 0;
        }
    }

    msg.device = &device;

    if (lat_val) {
        if (PyFloat_Check(lat_val)) {
            msg.has_lat = 1;
            msg.lat = PyFloat_AsDouble(lat_val);
        } else if (PyInt_Check(lat_val)) {
            msg.has_lat = 1;
            msg.lat = PyInt_AsLong(lat_val);
        } else {
            PyErr_SetString(PyExc_ValueError, "`lat` must be a float or an integer type");
            return 0;
        }
    }

    if (lon_val) {
        if (PyFloat_Check(lon_val)) {
            msg.has_lon = 1;
            msg.lon = PyFloat_AsDouble(lon_val);
        } else if (PyInt_Check(lon_val)) {
            msg.has_lon = 1;
            msg.lon = PyInt_AsLong(lon_val);
        } else {
            PyErr_SetString(PyExc_ValueError, "`lon` must be a float or an int type");
            return 0;
        }
    }

    if (apps_val) {
        if (PyList_Check(apps_val)) {
            int i = 0;
            int n_apps = PySequence_Size(apps_val);
            msg.n_apps = n_apps;
            if (n_apps > 0) {
                msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
                if (! msg.apps) {
                    PyErr_SetString(PyExc_ValueError, "Cannot allocate the memory block");
                    return 0;
                }
                while (n_apps > 0) {
                    PyObject *app = PyList_GET_ITEM(apps_val, i);
                    if (PyInt_Check(app)) {
                        msg.apps[i] = PyInt_AsLong(app);
                        i++;
                    } else {
                        PyErr_SetString(PyExc_ValueError, "`app` must be an integer type");
                        return 0;
                    }
                    n_apps--;
                }
            }
        } else {
            PyErr_SetString(PyExc_ValueError, "`apps` must be a list type");
            return 0;
        }
    }

    len = device_apps__get_packed_size(&msg);

    buf = malloc(len);
    if (! buf) {
        PyErr_SetString(PyExc_ValueError, "Cannot allocate the memory block");
        return 0;
    }

    device_apps__pack(&msg, buf);

    pbheader_t pbheader = PBHEADER_INIT;
    pbheader.type = DEVICE_APPS_TYPE;
    pbheader.length = len;

    gzwrite(fi, &pbheader, sizeof(pbheader)); // Write message header
    gzwrite(fi, buf, len); // Write protobuf message

    free(msg.apps);
    free(buf);

    return (len + sizeof(pbheader));
}


// Read iterator of Python dicts
// Pack them to DeviceApps protobuf and write to file with appropriate header
// Return number of written bytes as Python integer
// https://github.com/protobuf-c/protobuf-c/wiki/Examples
static PyObject* py_deviceapps_xwrite_pb(PyObject* self, PyObject* args) {
    const char *path;
    unsigned bytes_written = 0;
    PyObject *o;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    PyObject *iterator = PyObject_GetIter(o);
    PyObject *item;

    if (! iterator) {
        PyErr_SetString(PyExc_ValueError, "First argument must be iterable");
        return NULL;
    }

    gzFile fi = gzopen(path, "a6h");

    if (! fi) {
        PyErr_SetString(PyExc_ValueError, "Cannot open the file");
        return NULL;
    }

    while (item = PyIter_Next(iterator)) {
        if (! item) {
            break;
        }

        if (! PyDict_Check(item)) {
            PyErr_SetString(PyExc_ValueError, "`deviceapps` must be a dictionary type");
            gzclose(fi);
            return NULL;
        }

        size_t len;

        if (! (len = pack_and_write(item, fi))) {
            Py_DECREF(item);
            gzclose(fi);
            return NULL;
        }

        bytes_written += len;

        Py_DECREF(item);
    }

    gzclose(fi);

    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown error has occurred");
        return NULL;
    }

    return Py_BuildValue("i", bytes_written);
}

// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject* py_deviceapps_xread_pb(PyObject* self, PyObject* args) {
    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    printf("Read from: %s\n", path);
    Py_RETURN_NONE;
}


static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
     {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initpb(void) {
     (void) Py_InitModule("pb", PBMethods);
}
