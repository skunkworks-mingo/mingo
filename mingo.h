#include "mingo.c"

void init();

void mongoc_database_create_collection(char* database, char* name);

void mongoc_database_drop (char* database);

void mongoc_collection_insert(char* database, char* collection, char* document);

int mongoc_collection_count(char* database, char* collection, char* fieldName, char* fieldValue);

char** mongoc_collection_find(int* count, char* database, char* collection, char* fieldName, char* fieldValue);

char *index_create(char* collection, char* key, char* err_msg);

void index_drop(char* name);

int sql_execute(char* sql);