#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

sqlite3* db;

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
        if (strcmp(sqlite3_column_text(findStmt, 1), database) == 0) {
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
        char* res = sqlite3_column_text(findTablesStmt, 0);
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


void mongoc_database_create_collection(char* database, char* name) {

    // Assume that the database connection is already open

    // Check whether the specified database already exists, create it if it doesn't exist yet
    createDatabaseIfNeeded(db, database);

    // Create collection - pass back error if one occurs
    createCollection(db, database, name);

}

void mongoc_database_drop (char* database) {
    dropTables(db, database);
    detachDatabase(db, database);
}
