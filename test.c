#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "mingo.h"
#include "aggregate.h"

int main(void) {
    
    int rc = demoScript();
    printf("%d\n",rc);
}

int demoScript() {
    
    init();
    char* DB = "testDB";
    char* collection = "skunkworks";

    mongoc_collection_dropAll(DB);

    int DOCS_LEN = 4;   
    char *documents[DOCS_LEN];
    documents[0] = "{ \"mongo\": \"webscale\"}";
    documents[1] = "{ \"mongo\": \"green\", \"office\": \"Palo Alto\"}";
    documents[2] = "{ \"mongo\": \"webscale\", \"office\": \"NYC\"}";
    documents[3] = "{ \"mongo\": \"leaf\"}";

    printf("Number of \n");
    printf("Creating new database 'testDB', with collection 'skunkworks'\n");
    mongoc_database_create_collection(DB, collection);	

    printf("There are currently %d documents with field 'mongo' and value 'webscale'\n", 
                mongoc_collection_count(DB,collection,"mongo","webscale"));

    printf("\n");
    printf("Inserting: \n");
    for(int i = 0; i < DOCS_LEN; i++) {
        printf("%s\n", documents[i]);
        mongoc_collection_insert(DB, collection, documents[i]);
    }

    printf("\n");
    printf("There are now %d documents with field 'mongo' and value 'webscale'\n", 
                mongoc_collection_count(DB,collection,"mongo","webscale"));

    int* count;
    char** res = mongoc_collection_find(count, DB, collection, "mongo", "webscale");
    
    for (int i = 0; i < (*count); i++) {
        printf("%s\n", res[i]);
    }

    
    return 0;

}

int testScript() {
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