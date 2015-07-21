#pragma once

#include "create_delete.c"

int createDatabaseIfNeeded(sqlite3 *db, char* database);

int checkDatabase(sqlite3 *db, char* database);

int createCollection(sqlite3 *db, char* database, char* collection);

int dropTables(sqlite3 *db, char* database);

int detachDatabase(sqlite3 *db, char* database);