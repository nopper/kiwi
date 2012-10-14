#include <stdio.h>
#include <stdlib.h>
#include "db.h"
#include "variant.h"
#include "com_kv_kiwi_DB.h"

DB* _db = NULL;

JNIEXPORT jint JNICALL Java_com_kv_kiwi_DB_open(JNIEnv* jenv, jobject jclass, jstring jpath)
{
    (void)jclass;
    int ret = 0;

    if (_db)
        return ret;

    const char* basedir = (*jenv)->GetStringUTFChars(jenv, jpath, NULL);

    if (!basedir)
        return ret;

    if ((_db = db_open(basedir)))
        ret = 1;

    (*jenv)->ReleaseStringUTFChars(jenv, jpath, basedir);
    return ret;
}

JNIEXPORT jint JNICALL Java_com_kv_kiwi_DB_add(JNIEnv *jenv, jobject jclass, jbyteArray jkey, jint jklen, jbyteArray jval, jint jvlen)
{
    (void)jclass;
    Variant key, value;
    int ret = 0;

    if (!_db)
        return ret;

    key.mem = (char *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    key.length = jklen;

    value.mem = (char *)(*jenv)->GetByteArrayElements(jenv, jval, 0);
    value.length = jvlen;


    if (key.mem == NULL || value.mem == NULL)
    {
    	fprintf(stderr, "Empty arrays\n");
        goto RET;
    }

    ret = db_add(_db, &key, &value);

RET:
    if (key.mem) {
        (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key.mem, 0);
    }
    if (value.mem){
        (*jenv)->ReleaseByteArrayElements(jenv, jval, (jbyte*)value.mem, 0);
    }

    return ret;
}

JNIEXPORT jbyteArray JNICALL Java_com_kv_kiwi_DB_get(JNIEnv *jenv, jobject jclass, jbyteArray jkey, jint jklen)
{
    (void)jclass;
    int ret;
    jbyteArray arr = NULL;
    Variant key;

    if (!_db)
        return arr;

    key.mem = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    if (key.mem == NULL)
        return arr;

    key.length = jklen;

    Variant* value = buffer_new(255);

    ret = db_get(_db, &key, value);
    
    if ((ret == 1) && (value->length > 0))
    {
    	arr = (*jenv)->NewByteArray(jenv, value->length);
    	(*jenv)->SetByteArrayRegion(jenv, arr, 0, value->length, value->mem);
    }

    buffer_free(value);

    return arr;
}

JNIEXPORT jint JNICALL Java_com_kv_kiwi_DB_remove(JNIEnv *jenv, jobject jclass, jbyteArray jkey, jint jklen)
{
    (void)jclass;
    Variant key;
    int ret = 0;

    if (!_db)
        return ret;

    key.mem = (char*)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    if (key.mem == NULL)
        return ret;

    key.length = jklen;

    ret = db_remove(_db, &key);

    /* release */
    (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte*)key.mem, 0);

    return ret;
}

JNIEXPORT jint JNICALL Java_com_kv_kiwi_DB_close(JNIEnv* jenv, jobject jclass)
{
    (void)jenv;
    (void)jclass;
    int ret = 1;

    if (_db)
    {
        db_close(_db);
        _db = NULL;
    }
    
    return ret;
}
