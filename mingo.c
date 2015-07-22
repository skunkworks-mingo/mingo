#include <bson.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

sqlite3* db;
int id;

void init() {
    int rc = sqlite3_open(":memory:", &db);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
    }

    id = 0;
}

/*
 * Returns -1 for error, 0 if database does not exist, 1 if database exists
 */
int checkDatabase(sqlite3* db, char* database) {
    int rc;
    int foundDatabase = 0;

    char* sql = "PRAGMA database_list";
    sqlite3_stmt* findStmt;
    sqlite3_prepare_v2(db, sql, strlen(sql) + 1, &findStmt, NULL);

    // Look through all the databases for specified database
    while ((rc = sqlite3_step(findStmt)) == SQLITE_ROW) {
        if (strcmp((const char*)sqlite3_column_text(findStmt, 1), database) == 0) {
            foundDatabase = 1;
        }
    }

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Cannot list databases: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    return ((foundDatabase == 1) ? 1 : 0);
}

/*
 * Creates the specified database if it doesn't exist yet.
 * Returns 0 for success, 1 for error
 */
int createDatabaseIfNeeded(sqlite3* db, char* database) {
    int rc;
    sqlite3_stmt* attachStmt;
    char* attachSql;

    if (checkDatabase(db, database) == 0) {
        // If the database doesn't exist yet, attach it
        attachSql = "ATTACH DATABASE ? AS ?";
        sqlite3_prepare_v2(db, attachSql, strlen(attachSql) + 1, &attachStmt, NULL);
        sqlite3_bind_text(attachStmt, 1, database, strlen(database), 0);
        sqlite3_bind_text(attachStmt, 2, database, strlen(database), 0);
        rc = sqlite3_step(attachStmt);
        if (rc != SQLITE_DONE) {
            fprintf(stderr, "Cannot create database: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            return 1;
        }
    }

    return 0;
}

/*
 * Creates the collection in the database. Assumes the database has already been created.
 * Returns 0 for success, 1 for error.
 */
int createCollection(sqlite3* db, char* database, char* collection) {
    int rc;
    char* errormsg;
    char* s1;
    char* s2;
    char* createSql;

    // Prepare the SQL statement to create the collection in the given database
    s1 = "CREATE TABLE ";
    s2 = "(id TEXT, data BLOB)";
    createSql = malloc(strlen(s1) + +strlen(database) + 1 + strlen(collection) + strlen(s2) + 1);
    strcpy(createSql, s1);
    strcat(createSql, database);
    strcat(createSql, ".");
    strcat(createSql, collection);
    strcat(createSql, s2);

    errormsg = "Error creating collection";

    // Execute the SQL command
    rc = sqlite3_exec(db, createSql, NULL, NULL, &errormsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot create table: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        free(createSql);
        return 1;
    }

    free(createSql);
    return 0;
}

/*
 * Helper function to free a char**.
 */
void freeTables(char** tables, int n) {
    for (int i = 0; i < n; i++) {
        free(tables[i]);
    }
    free(tables);
}

/*
 * Drops all the tables in the database. Returns 0 if success, 1 for error.
 */
int dropTables(sqlite3* db, char* database) {
    char* errormsg;
    char** tables;
    char* s1;
    char* s2;
    char* sql;
    sqlite3_stmt* findTablesStmt;
    int i;
    int j;
    int rc;
    char* dropTableStart;


    // Initialize array of table names
    tables = malloc(1000 * sizeof(char*));  // TODO upper bound on number of tables

    // Prepare SQL statement to list all tables in the database
    s1 = "SELECT name FROM ";
    s2 = ".sqlite_master WHERE type='table'";
    sql = malloc(strlen(s1) + strlen(database) + strlen(s2) + 1);
    strcpy(sql, s1);
    strcat(sql, database);
    strcat(sql, s2);
    sqlite3_prepare_v2(db, sql, strlen(sql) + 1, &findTablesStmt, NULL);

    // Put all tables in the database into the "tables" array
    i = 0;
    while (sqlite3_step(findTablesStmt) == SQLITE_ROW) {
        char* res = (char*)sqlite3_column_text(findTablesStmt, 0);
        tables[i] = malloc(strlen(res) + 1);
        strcpy(tables[i], res);
        i++;
    }

    // Drop each table in the "tables" array
    dropTableStart = "DROP TABLE ";
    j = 0;
    for (j = 0; j < i; j++) {
        char* dropSql = malloc(strlen(dropTableStart) + strlen(tables[j]) + 1);
        strcpy(dropSql, dropTableStart);
        strcat(dropSql, tables[j]);

        rc = sqlite3_exec(db, dropSql, NULL, NULL, &errormsg);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Cannot drop table: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);

            free(sql);
            free(dropSql);
            freeTables(tables, i);

            return 1;
        }
        free(dropSql);
    }

    free(sql);
    freeTables(tables, i);

    return 0;
}

/*
 * Detach the database. Returns 0 for success, 1 for error.
 */
int detachDatabase(sqlite3* db, char* database) {
    char* errormsg;
    char* s1;
    char* sql;

    // Prepare the SQL command
    s1 = "DETACH DATABASE ";
    sql = malloc(strlen(s1) + strlen(database) + 1);
    strcpy(sql, s1);
    strcat(sql, database);

    // Execute the command to detach the database
    errormsg = "Error detaching database";
    int rc = sqlite3_exec(db, sql, NULL, NULL, &errormsg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot drop database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        free(sql);
        return 1;
    }

    free(sql);
    return 0;
}


void mongoc_collection_dropAll(char* database) {

    dropTables(db, database);
}
void mongoc_database_create_collection(char* database, char* name) {
    // Assume that the database connection is already open

    // Check whether the specified database already exists, create it if it doesn't exist yet
    createDatabaseIfNeeded(db, database);

    // Create collection - pass back error if one occurs
    createCollection(db, database, name);
}

void mongoc_database_drop(char* database) {
    dropTables(db, database);
    detachDatabase(db, database);
}

void mongoc_collection_insert(char* database, char* collection, char* document) {
    // generate ID - TODO: do it better

    bson_t* data = bson_new_from_json((uint8_t*)document, strlen(document) + 1, NULL);

    // Insert the ID and the blob into the database

    // Prepare statement
    char* s1 = "INSERT INTO ";
    char* s2 = ".";
    char* s3 = " VALUES (?, ?)";
    char* sql =
        malloc(strlen(s1) + strlen(database) + strlen(s2) + strlen(collection) + strlen(s3));
    strcpy(sql, s1);
    strcat(sql, database);
    strcat(sql, s2);
    strcat(sql, collection);
    strcat(sql, s3);

    sqlite3_stmt* insertStmt;
    int rc;

    rc = sqlite3_prepare_v2(db, sql, -1, &insertStmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot prepare insert statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        free(sql);
        return;
    }
    sqlite3_bind_int(insertStmt, 1, id);
    sqlite3_bind_text(insertStmt, 2, bson_get_data(data), data->len, SQLITE_STATIC);

    rc = sqlite3_step(insertStmt);

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Cannot insert: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        free(sql);
        return;
    }

    size_t len;
    id++;
}

/*
 * Returns 1 if the document contains the field/value pair, 0 otherwise
 */
int match(const uint8_t* document, char* fieldName, bson_value_t* fieldValue, int documentLength) {
    bson_error_t error;
    size_t len;

    bson_t* b;
    bson_iter_t iter;

    if ((b = bson_new_from_data(document, documentLength))) {
        if (bson_iter_init(&iter, b)) {
            while (bson_iter_next(&iter)) {
                if (strcmp((char*)bson_iter_key(&iter), fieldName) == 0) {
                    bson_t new_doc1;
                    bson_init(&new_doc1);
                    bson_append_value(&new_doc1, "_id", -1, fieldValue);

                    bson_t new_doc2;
                    bson_init(&new_doc2);
                    bson_append_value(&new_doc2, "_id", -1, bson_iter_value(&iter));

                    bool are_equal = bson_equal(&new_doc1, &new_doc2);

                    return are_equal;
                }
            }
        }
    }

    return 0;
}


int mongoc_collection_count(char* database, char* collection, char* fieldName, char* fieldValue) {
    bson_value_t* field = bson_malloc(sizeof(bson_value_t));
    field->value_type = BSON_TYPE_UTF8;
    field->value.v_utf8.str = fieldValue;
    field->value.v_utf8.len = strlen(fieldValue);

    char* s1 = "SELECT * FROM ";
    char* s2 = ".";
    char* sql = malloc(strlen(s1) + strlen(database) + strlen(s2) + strlen(collection) + 1);
    strcpy(sql, s1);
    strcat(sql, database);
    strcat(sql, s2);
    strcat(sql, collection);

    sqlite3_stmt* findStmt;
    int rc;

    rc = sqlite3_prepare_v2(db, sql, -1, &findStmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot prepare count statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        free(sql);
        return 1;
    }

    int count = 0;

    while ((rc = sqlite3_step(findStmt)) == SQLITE_ROW) {
        // Get the BSON blob
        // Pass it into match; if it matches, increase count
        const uint8_t* document = sqlite3_column_blob(findStmt, 1);

        if (match(document, fieldName, field, sqlite3_column_bytes(findStmt, 1))) {
            count++;
        }
    }

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Cannot perform count: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        free(sql);
        return 1;
    }

    return count;
}


char** mongoc_collection_find(
    int* count, char* database, char* collection, char* fieldName, char* fieldValue) {
    bson_value_t* field = bson_malloc(sizeof(bson_value_t));
    field->value_type = BSON_TYPE_UTF8;
    field->value.v_utf8.str = fieldValue;
    field->value.v_utf8.len = strlen(fieldValue);

    *count = mongoc_collection_count(database, collection, fieldName, fieldValue);
    char** res = malloc((*count) * sizeof(char*));

    char* s1 = "SELECT * FROM ";
    char* s2 = ".";
    char* sql = malloc(strlen(s1) + strlen(database) + strlen(s2) + strlen(collection) + 1);
    strcpy(sql, s1);
    strcat(sql, database);
    strcat(sql, s2);
    strcat(sql, collection);

    sqlite3_stmt* findStmt;
    int rc;

    rc = sqlite3_prepare_v2(db, sql, -1, &findStmt, NULL);

    int i = 0;
    while ((rc = sqlite3_step(findStmt)) == SQLITE_ROW) {
        const uint8_t* document = sqlite3_column_blob(findStmt, 1);
        if (match(document, fieldName, field, sqlite3_column_bytes(findStmt, 1))) {
            bson_t* b;
            size_t len;
            b = bson_new_from_data(document, sqlite3_column_bytes(findStmt, 1));
            char* data = bson_as_json(b, &len);
            res[i] = malloc(strlen(data) * sizeof(char));
            strcpy(res[i], data);
            i++;
        }
    }
    return res;
}

/* execute some sql that doesn't require a callback */
int sql_execute(char* sql) {
    char* err_msg = 0;
    int rc;
    if (db == NULL) {
        init();
    }
    rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }
    return 0;
}

char* index_create(char* collection, char* key, char* err_msg) {
    char index_table_name[500];
    sprintf(index_table_name, "index_%s_%s", collection, key);
    char collection_table_name[500];
    sprintf(collection_table_name, "collection_%s", collection);
    char sql[500];  // TODO: make this safe from buffer overflow
    sprintf(sql, "CREATE TABLE %s(index BLOB PRIMARY KEY, id ROWID);", index_table_name);
    // TODO: import indexed values into table
    sprintf(sql + strlen(sql),
            "INSERT INTO %s (id) SELECT rowid FROM %s;",
            collection_table_name,
            index_table_name);
    sprintf(sql + strlen(sql),
            "INSERT INTO %s (id) SELECT (rowid, document) FROM %s;",
            collection_table_name,
            index_table_name);
    sql_execute(sql);
    return NULL;
}

void index_drop(char* index_table_name) {
    char sql[500];  // TODO: make this safe from buffer overflow
    sprintf(sql, "DROP TABLE %s;", index_table_name);
    sql_execute(sql);
}
