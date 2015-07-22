#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "mingo.h"


int main(void) {
    
    init();

    mongoc_database_create_collection("db","coll");
    
    mongoc_collection_insert("db", "coll", "{ \"a\": \"b\"}");
    mongoc_collection_insert("db", "coll", "{ \"a\": \"b\"}");
    mongoc_collection_insert("db", "coll", "{ \"a\": \"cc\"}");
    mongoc_collection_insert("db", "coll", "{ \"a\": \"ddd\"}");

    mongoc_collection_count("db", "coll", "a", "b");

    int* count;
    char** res = mongoc_collection_find(count, "db", "coll", "a", "b");
    printf("count is %d\n", (*count));
    for (int i = 0; i < (*count); i++) {
        printf("%s\n", res[i]);
    }


    mongoc_database_drop("db");

    return 0;
}