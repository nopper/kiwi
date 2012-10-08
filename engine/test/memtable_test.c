#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include "memtable.h"
#include "utils.h"

#if 0
void main()

#else

START_TEST (test_simple)
{
	MemTable* memtable = memtable_new();

	Variant* key1 = buffer_new(5);
	Variant* key2 = buffer_new(5);

	buffer_putstr(key1, "12345");
	memtable_put(memtable, key1, key1);
	fail_if(memtable_get(memtable, key1, key2) != 1,
		    "Retrieving key must succeed");

	printf("Key is: %.*s\n", 5, key2->mem);

	fail_if(strncmp(key1->mem, key2->mem, 5) != 0,
		    "Retrieved key must be equal to 12345");
}
END_TEST

START_TEST (test_stress)
{
	char c = 'a';
	size_t insertions = 0;
	Variant* key = buffer_new(1024);
	MemTable* memtable = memtable_new();

	long long start, end;

	start = get_ustime_sec();

	for (int i = 0; i < 2024; i++)
	{
		key->length = i + 1;
		for (int j = 0; j < 26; j++)
		{
			key->mem[i] = c + j;
			memtable_put(memtable, key, key);
			insertions++;
		}
	}

	end = get_ustime_sec();

	printf("Operations: %d Op/sec: %.6f Cost:%.3f(sec)\n", insertions, insertions / (end - start), (end - start) / insertions);

	buffer_free(key);
	memtable_free(memtable);
}
END_TEST

START_TEST (test_memtable_replace_same)
{
	MemTable* memtable = memtable_new();

	Variant* key = buffer_new(5);
	buffer_putstr(key, "ciao");
	memtable_put(memtable, key, key);

	buffer_clear(key);
	buffer_putstr(key, "ciao");
	memtable_put(memtable, key, key);

	memtable_free(memtable);
}
END_TEST

START_TEST (test_memtable_replace_small)
{
	MemTable* memtable = memtable_new();

	Variant* key = buffer_new(5);
	Variant* val = buffer_new(5);

	buffer_putstr(key, "ciao");
	buffer_putstr(val, "mamma");

	memtable_put(memtable, key, val);

	buffer_clear(val);
	buffer_putstr(val, "mammina bella");
	memtable_put(memtable, key, val);

	// 4 (klen) + 4 + 5 + 1 + 1 + tag + 4 (pointer - 1 level) = 20
	fail_if(memtable->list->wasted_bytes != 20,
		    "Wasted bytes are not correct");

	buffer_putstr(val, " yoo");
	memtable_put(memtable, key, val);

	SkipNode* node = NODE_FIRST(memtable->list);

	while (node != NODE_END(memtable->list))
	{
		printf("%.*s\n", 17, (char *)(NODE_KEY(node) + 1 + 4 + 1));
		node = NODE_NEXT(node);
	}

	fail_if(memtable_del(memtable, key) != 1, NULL);
	fail_if(memtable_get(memtable, key, val) != 0,
		    "It must be empty");
	fail_if(memtable->list->count != 1, NULL);

	memtable_free(memtable);
}
END_TEST

Suite* memtable_suit(void)
{
	Suite* s = suite_create("MemTable");
	TCase *tc_core = tcase_create("Core");
	// TODO: add big
	//tcase_add_test(tc_core, test_memtable_replace_same);
	tcase_add_test(tc_core, test_memtable_replace_small);
	//tcase_add_test(tc_core, test_simple);
	//tcase_add_test(tc_core, test_stress);
	suite_add_tcase(s, tc_core);
	return s;
}

int main(void)
{
	int number_failed;
	Suite *s = memtable_suit();
	SRunner *sr = srunner_create(s);
	srunner_run_all(sr, CK_NORMAL);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

#endif