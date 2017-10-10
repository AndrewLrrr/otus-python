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


// https://github.com/protobuf-c/protobuf-c/wiki/Examples
void example() {
    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
    void *buf;
    unsigned len;

    char *device_id = "e7e1a50c0ec2747ca56cd9e1558c0d7c";
    char *device_type = "idfa";
    device.has_id = 1;
    device.id.data = (uint8_t*)device_id;
    device.id.len = strlen(device_id);
    device.has_type = 1;
    device.type.data = (uint8_t*)device_type;
    device.type.len = strlen(device_type);
    msg.device = &device;

    msg.has_lat = 1;
    msg.lat = 67.7835424444;
    msg.has_lon = 1;
    msg.lon = -22.8044005471;

    msg.n_apps = 3;
    msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
    msg.apps[0] = 42;
    msg.apps[1] = 43;
    msg.apps[2] = 44;
    len = device_apps__get_packed_size(&msg);

    buf = malloc(len);
    device_apps__pack(&msg, buf);

    fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
    fwrite(buf, len, 1, stdout); // Write to stdout to allow direct command line piping

    free(msg.apps);
    free(buf);
}

// Read iterator of Python dicts
// Pack them to DeviceApps protobuf and write to file with appropriate header
// Return number of written bytes as Python integer
static PyObject* py_deviceapps_xwrite_pb(PyObject* self, PyObject* args) {
    const char *path;
    PyObject *o;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    PyObject *iterator = PyObject_GetIter(o);
    PyObject *item;
    unsigned bytes_written = 0;

    const char *device_key = "device";
    const char *type_key   = "type";
    const char *id_key     = "id";
    const char *lat_key    = "lat";
    const char *lon_key    = "lon";
    const char *apps_key   = "apps";

    if (! iterator) {
        PyErr_SetString(PyExc_ValueError, "First argument should be iterable");
        return NULL;
    }

    while (item = PyIter_Next(iterator)) {
        if (! item) {
            break;
        }

        if (! PyDict_Check(item)) {
            PyErr_SetString(PyExc_ValueError, "The deviceapps type must be a dictionary");
            return NULL;
        }

        DeviceApps msg = DEVICE_APPS__INIT;
        DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
        void *buf;
        unsigned len;

        PyObject *device_val = PyDict_GetItemString(item, device_key);
        PyObject *lat_val    = PyDict_GetItemString(item, lat_key);
        PyObject *lon_val    = PyDict_GetItemString(item, lon_key);
        PyObject *apps_val   = PyDict_GetItemString(item, apps_key);

        if (device_val && PyDict_Check(device_val)) {
            PyObject *id_val = PyDict_GetItemString(device_val, id_key);
            PyObject *type_val = PyDict_GetItemString(device_val, type_key);
            if (id_val && PyString_Check(id_val)) {
                char *device_id = PyString_AsString(id_val);
                device.has_id = 1;
                device.id.data = (uint8_t*) device_id;
                device.id.len = strlen(device_id);
            }
            if (type_val && PyString_Check(type_val)) {
                char *device_type = PyString_AsString(type_val);
                device.has_type = 1;
                device.type.data = (uint8_t*) device_type;
                device.type.len = strlen(device_type);
            }
        }

        msg.device = &device;

        if (lat_val && PyFloat_Check(lat_val)) {
            msg.has_lat = 1;
            msg.lat = PyFloat_AsDouble(lat_val);
        }

        if (lon_val && PyFloat_Check(lon_val)) {
            msg.has_lon = 1;
            msg.lon = PyFloat_AsDouble(lon_val);
        }

        if (apps_val && PyList_Check(apps_val)) {
            int i = 0;
            int n_apps = PySequence_Size(apps_val);
            msg.n_apps = n_apps;
            if (n_apps > 0) {
                msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
                while (n_apps > 0) {
                    PyObject *app = PyList_GET_ITEM(apps_val, i);
                    if (PyInt_Check(app)) {
                        msg.apps[i] = PyInt_AsLong(app);
                        i++;
                    }
                    n_apps--;
                    Py_XDECREF(app);
                }
            }
        }

        len = device_apps__get_packed_size(&msg);
        buf = malloc(len);
        device_apps__pack(&msg, buf);

        pbheader_t pbheader = PBHEADER_INIT;
        pbheader.type = DEVICE_APPS_TYPE;
        pbheader.length = len;

        gzFile fi = gzopen(path, "a6h");
        gzwrite(fi, &pbheader, sizeof(pbheader)); // Write protobuf header
        gzwrite(fi, buf, len); // Write protobuf message
        gzclose(fi);

        bytes_written += (len + sizeof(pbheader));
        free(msg.apps);
        free(buf);

        Py_XDECREF(device_val);
        Py_XDECREF(lat_val);
        Py_XDECREF(lon_val);
        Py_XDECREF(apps_val);
        Py_XDECREF(item);
    }

    Py_XDECREF(iterator);

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
