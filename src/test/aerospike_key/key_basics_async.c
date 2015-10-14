/*
 * Copyright 2008-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_msgpack_serializer.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_event.h>

#include "../test.h"
#include "../util/monitor.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;
static as_monitor monitor;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_basics"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct {
	atf_test_result* __result__;
	uint32_t counter;
} counter_data;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite * suite)
{
	as_monitor_init(&monitor);
	return true;
}

static bool
after(atf_suite * suite)
{
	as_monitor_destroy(&monitor);
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static void
as_get_callback1(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_record* rec = result;
    assert_int_eq_async(&monitor, as_record_numbins(rec), 1);
    assert_int_eq_async(&monitor, as_record_get_int64(rec, "a", 0), 123);
	as_monitor_notify(&monitor);
}

static void
as_put_callback1(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa1");
	
	aerospike_key_get_async(as, NULL, &key, event_loop, false, as_get_callback1, __result__);
}

TEST(key_basics_async_get, "async get")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa1");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 123);
	
	as_policy_write p;
	as_policy_write_init(&p);
	p.timeout = 0;
	
	aerospike_key_put_async(as, &p, &key, &rec, 0, false, as_put_callback1, __result__);

	as_key_destroy(&key);
	as_record_destroy(&rec);
	as_monitor_wait(&monitor);
}

static void
as_get_callback2(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_record* rec = result;
    assert_int_eq_async(&monitor, as_record_numbins(rec), 1);
    assert_string_eq_async(&monitor, as_record_get_str(rec, "bbb"), "pa2 value");
	as_monitor_notify(&monitor);
}

static void
as_put_callback2(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa2");
	const char* select[] = {"bbb", NULL};
	
	aerospike_key_select_async(as, NULL, &key, select, event_loop, false, as_get_callback2, __result__);
}

TEST(key_basics_async_select, "async select")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa2");
	
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_strp(&rec, "bbb", "pa2 value", false);
	
	aerospike_key_put_async(as, NULL, &key, &rec, 0, false, as_put_callback2, __result__);
	
	as_key_destroy(&key);
	as_record_destroy(&rec);
	as_monitor_wait(&monitor);
}

static void
as_get_callback_found(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_record* rec = result;
    assert_async(&monitor, rec);
    assert_int_eq_async(&monitor, as_record_numbins(rec), 0);
    assert_async(&monitor, rec->gen > 0);
	
	counter_data* cdata = udata;
	cdata->counter++;
	if (cdata->counter == 2) {
		as_monitor_notify(&monitor);
	}
}

static void
as_get_callback_not_found(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	atf_test_result* __result__ = udata;
    assert_async(&monitor, err && err->code == AEROSPIKE_ERR_RECORD_NOT_FOUND);
    assert_async(&monitor, !result);
	
	counter_data* cdata = udata;
	cdata->counter++;
	if (cdata->counter == 2) {
		as_monitor_notify(&monitor);
	}
}

static void
as_put_callback3(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa3");
	aerospike_key_exists_async(as, NULL, &key, event_loop, false, as_get_callback_found, udata);
	
	as_key_init(&key, NAMESPACE, SET, "notfound");
	aerospike_key_exists_async(as, NULL, &key, event_loop, false, as_get_callback_not_found, udata);
}

TEST(key_basics_async_exists, "async exists")
{
	as_monitor_begin(&monitor);
	
	counter_data udata;
	udata.__result__ = __result__;
	udata.counter = 0;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa3");
	
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "c", 55);
	
	aerospike_key_put_async(as, NULL, &key, &rec, 0, false, as_put_callback3, &udata);
	
	as_key_destroy(&key);
	as_record_destroy(&rec);
	as_monitor_wait(&monitor);
}

static void
as_remove_callback(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	as_monitor_notify(&monitor);
}

static void
as_put_callback4(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa4");
	aerospike_key_remove_async(as, NULL, &key, event_loop, false, as_remove_callback, __result__);
}

TEST(key_basics_async_remove, "async remove")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa4");
	
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "c", 55);
	
	aerospike_key_put_async(as, NULL, &key, &rec, 0, false, as_put_callback4, __result__);
	
	as_key_destroy(&key);
	as_record_destroy(&rec);
	as_monitor_wait(&monitor);
}

static void
as_operate_callback(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_record* rec = result;
    assert_async(&monitor, rec);
    assert_int_eq_async(&monitor, as_record_numbins(rec), 2);
    assert_int_eq_async(&monitor, as_record_get_int64(rec, "a", 0), 316);
    assert_string_eq_async(&monitor, as_record_get_str(rec, "b"), "abcmiddef");
	as_monitor_notify(&monitor);
}

static void
as_put_operate_callback(as_error* err, void* result, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa5");

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_add_incr(&ops, "a", -5);
	as_operations_add_append_str(&ops, "b", "def");
	as_operations_add_prepend_str(&ops, "b", "abc");
	as_operations_add_read(&ops, "a");
	as_operations_add_read(&ops, "b");
	
	aerospike_key_operate_async(as, NULL, &key, &ops, event_loop, false, as_operate_callback, __result__);

	as_key_destroy(&key);
	as_operations_destroy(&ops);
}

TEST(key_basics_async_operate, "async operate")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa5");
	
	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "a", 321);
	as_record_set_strp(&rec, "b", "mid", false);

	aerospike_key_put_async(as, NULL, &key, &rec, 0, false, as_put_operate_callback, __result__);
	
	as_key_destroy(&key);
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(key_basics_async, "aerospike_key basic tests") {
	
	suite_before(before);
	suite_after(after);

    suite_add(key_basics_async_get);
	suite_add(key_basics_async_select);
	suite_add(key_basics_async_exists);
	suite_add(key_basics_async_remove);
	suite_add(key_basics_async_operate);
}
