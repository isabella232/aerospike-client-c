/*******************************************************************************
 * Copyright 2008-2018 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Typedefs and Structs
//

struct counter {
	uint32_t count;
	uint32_t max;
	as_digest digest;
};


//==========================================================
// Constants
//

const char TEST_INDEX_NAME[] = "test-bin-index";


//==========================================================
// Forward Declarations
//

bool query_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
static bool query_terminate_cb(const as_val* val, void* udata);
static bool query_resume_cb(const as_val* val, void* udata);


//==========================================================
// SIMPLE QUERY Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_MULTI_KEY_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_records(&as);
	example_remove_index(&as, TEST_INDEX_NAME);

	// Create a numeric secondary index on test-bin.
	if (! example_create_integer_index(&as, "test-bin", TEST_INDEX_NAME)) {
		cleanup(&as);
		exit(-1);
	}

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	if (! example_read_test_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition. Note that as_query_destroy() takes
	// care of destroying all the query's member objects if necessary. However
	// using as_query_where_inita() does avoid internal heap usage.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", as_integer_range(0, g_n_keys / 2));

	LOG("executing query: where test-bin = 0 to g_n_keys / 2");

	LOG("start query terminate");

	struct counter c;
	c.count = 0;
	c.max = 3;

	as_query_set_paginate(&query, true);

	as_status status = aerospike_query_foreach(&as, &err, NULL, &query, query_terminate_cb, &c);

	if (status != AEROSPIKE_OK) {
		as_query_destroy(&query);
		return status;
	}

	LOG("terminate records returned: %u", c.count);
	LOG("start query resume");

	// Store completion status of all partitions.
	as_partitions_status* parts_all = as_partitions_status_reserve(query.parts_all);

	// Destroy query
	as_query_destroy(&query);

	as_query query_resume;
	as_partition_filter pf;

	do
	{
		// Resume query using new query instance.
		as_query_init(&query_resume, g_namespace, g_set);
		as_query_where_inita(&query_resume, 1);
		as_query_where(&query_resume, "test-bin", as_integer_range(0, g_n_keys / 2));

		as_partition_filter_set_partitions(&pf, parts_all);

		c.count = 0;
		c.max = 3;

		status = aerospike_query_partitions(&as, &err, NULL, &query_resume, &pf, query_terminate_cb, &c);

		LOG("resume records returned: %u", c.count);

		as_partitions_status_release(parts_all);

		if (c.count != 0) {
			parts_all = as_partitions_status_reserve(query_resume.parts_all);
		}

		as_query_destroy(&query_resume);
	} while (c.count != 0);

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	return 0;
}


//==========================================================
// Query Callback
//

bool
query_cb(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	// The query didn't use a UDF, so the as_val object should be an as_record.
	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		LOG("query callback returned non-as_record object");
		return true;
	}

	LOG("query callback returned record:");
	example_dump_record(p_rec);

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_remove_index(p_as, TEST_INDEX_NAME);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with one (integer value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 1);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.
		as_record_set_int64(&rec, "test-bin", (int64_t)i);

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}


//==========================================================
// Query Terminate and Resume
//

static bool
query_terminate_cb(const as_val* val, void* udata)
{
	if (! val) {
		// Query complete.
		return true;
	}

	struct counter* c = udata;

	// query.concurrent is false, so atomics are not necessary.
	if (c->count >= c->max) {
		// Since we are terminating the query here, the query last digest
		// will not be set and the current record will be returned again
		// if the query resumes at a later time.
		return false;
	}

	c->count++;
	return true;
}

static bool
query_resume_cb(const as_val* val, void* udata)
{
	if (! val) {
		// Query complete.
		return true;
	}

	struct counter* c = udata;
	c->count++;
	return true;
}

