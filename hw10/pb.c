#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
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
    const char* path;
    PyObject* o;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    PyObject *iterator = PyObject_GetIter(o);
    PyObject *item;
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    char *key_str;

    const char *device_key = "device";
    const char *type_key = "type";
    const char *id_key = "id";
    const char *lat_key = "lat";
    const char *lon_key = "lon";
    const char *apps_key = "apps";

    if (! iterator) {
        PyErr_SetString(PyExc_ValueError, "First argument should be iterable");
        return NULL;
    }

    while (item = PyIter_Next(iterator)) {
        if (! item) {
            break;
        }

        while (PyDict_Next(item, &pos, &key, &value)) {
            key_str = PyString_AsString(key);
            if (strcmp(key_str, device_key) == 0) {
                PyObject *sub_key, *sub_value;
                Py_ssize_t sub_pos = 0;
                char *sub_key_str;

                while (PyDict_Next(value, &sub_pos, &sub_key, &sub_value)) {
                    sub_key_str = PyString_AsString(sub_key);
                    if (strcmp(sub_key_str, id_key) == 0) {
                        printf("id - %s\n", PyString_AsString(sub_value));
                    }
                    if (strcmp(sub_key_str, type_key) == 0) {
                        printf("type - %s\n", PyString_AsString(sub_value));
                    }
                }

                sub_pos = 0;
            }

            if (strcmp(key_str, lat_key) == 0) {
                printf("lat - %.8f\n", PyFloat_AsDouble(value));
            }

            if (strcmp(key_str, lon_key) == 0) {
                printf("lon - %.8f\n", PyFloat_AsDouble(value));
            }

            if (strcmp(key_str, apps_key) == 0) {
                PyObject *app_id;
                int n_apps = 0;
                int i = 0;

                n_apps = PySequence_Size(value);
                if (PyList_Check(value) && n_apps > 0) {
                    while (n_apps > 0) {
                        app_id = PyList_GET_ITEM(value, i);
                        printf("app_id - %d\n", PyInt_AsLong(app_id));
                        n_apps--;
                        i++;
                    }
                }
            }
        }

        pos = 0;

        printf("\n\n");

        Py_DECREF(item);
    }

    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown error has occurred");
        return NULL;
    }
    else {
        printf("continue doing useful work\n");
    }

    printf("Write to: %s\n", path);
    Py_RETURN_NONE;
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
