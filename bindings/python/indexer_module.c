#include <Python.h>
#include "db.h"
#include "variant.h"

static PyObject *
pyindexer_db_open(PyObject *self, PyObject *args)
{
    const char *basedir;

    if (!PyArg_ParseTuple(args, "s", &basedir))
        return NULL;

    DB* db = db_open(basedir);
    return Py_BuildValue("l", db);
}

static PyObject *
pyindexer_db_add(PyObject *self, PyObject *args)
{
    Variant key, value;
    long db;
    int ret;

    if (!PyArg_ParseTuple(args, "ls#s#", &db, &key.mem, &key.length, &value.mem, &value.length))
        return NULL;
    ret = db_add((DB*)db, &key, &value);
    return Py_BuildValue("i", ret);
}


static PyObject *
pyindexer_db_remove(PyObject *self, PyObject *args)
{
    Variant key;
    long db;

    if (!PyArg_ParseTuple(args, "ls#", &db, &key.mem, &key.length))
        return NULL;
    db_remove((DB*)db, &key);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
pyindexer_db_get(PyObject *self, PyObject *args)
{
    Variant key;
    Variant* value = (Variant *)buffer_new(255);
    long db;
    int ret;

    if (!PyArg_ParseTuple(args, "ls#", &db, &key.mem, &key.length))
        return NULL;
    ret = db_get((DB*)db, &key, value);

    if (ret == 1)
    {
        PyObject *obj = PyString_FromStringAndSize(value->mem, value->length);
        buffer_free(value);
        return obj;
    }

    buffer_free(value);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
pyindexer_db_close(PyObject *self, PyObject *args)
{
    long db=0;

    if (!PyArg_ParseTuple(args, "l", &db))
        return NULL;

    db_close((DB*)db);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef IndexerMethods[] = {
    {"db_open",  pyindexer_db_open, METH_VARARGS,""},
    {"db_add",  pyindexer_db_add, METH_VARARGS,""},
    {"db_remove",  pyindexer_db_remove, METH_VARARGS,""},
    {"db_get",  pyindexer_db_get, METH_VARARGS,""},
//    {"db_exists",  pynessdb_db_exists, METH_VARARGS,""},
//    {"db_info", pynessdb_db_info, METH_VARARGS,""},
    {"db_close",  pyindexer_db_close, METH_VARARGS,""},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
init_indexer(void)
{
    (void)Py_InitModule("_indexer", IndexerMethods);
}
