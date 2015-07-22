
#include <bson.h>
#include "aggregate.h"

aggregation_state_t* aggregate_start(char *database, char *collection)
{
    aggregation_state_t state = {database, collection, NULL, 0};
    aggregation_state_t *ret_state  = bson_malloc(sizeof(aggregation_state_t));
    memcpy(ret_state, &state, sizeof(aggregation_state_t));
    return ret_state;
}

aggregation_result_t* aggregate_end(aggregation_state_t *state)
{
    char **result = malloc((*state).docs_len * sizeof(char*));
    size_t index;
    for(index = 0; index < (*state).docs_len; index++)
    {
        bson_t orig_doc = (*state).docs[index];
        char *str;
        str = bson_as_json (&orig_doc, NULL);
        result[index] = str;
    }

    aggregation_result_t *ret_state  = bson_malloc(sizeof(aggregation_result_t));
    (*ret_state).docs = result;
    (*ret_state).docs_len = (*state).docs_len;
    return ret_state;
}

void aggregate_match(aggregation_state_t *state, char* query) {
    //TODO: Run a find on the database/collection with that query.

    bson_t *doc1 = BCON_NEW("zip", BCON_UTF8("10001"), "state", BCON_UTF8("NY"), "population", BCON_INT64(5));
    bson_t *doc2 = BCON_NEW("zip", BCON_UTF8("10002"), "state", BCON_UTF8("NY"), "population", BCON_INT64(5));
    bson_t *doc3 = BCON_NEW("zip", BCON_UTF8("08520"), "state", BCON_UTF8("NJ"), "population", BCON_INT64(5));

    bson_t docs[3] = {*doc1, *doc2, *doc3};

    (*state).docs = docs;
    (*state).docs_len = 3;
}

bson_value_t* _aggregate_get_value_at_key(bson_t *doc, char *key) {
        bson_iter_t iter;
        bson_iter_t child_iter;
        if (!bson_iter_init (&iter, doc) ||
            !bson_iter_find_descendant (&iter, key, &child_iter)) {
            return NULL;
        }

        bson_value_t *result = bson_malloc(sizeof(bson_value_t));
        bson_value_copy(bson_iter_value(&child_iter), result);
        return result;
}

bool _aggregate_have_same_id(bson_t *doc, bson_value_t *id_value) {;
    bson_value_t *doc_id_value = _aggregate_get_value_at_key(doc, "_id");

    bson_t new_doc1;
    bson_init (&new_doc1);
    bson_append_value(&new_doc1, "_id", -1, doc_id_value);

    bson_t new_doc2;
    bson_init (&new_doc2);
    bson_append_value(&new_doc2, "_id", -1, id_value);

    bool are_equal = bson_equal(&new_doc1, &new_doc2);
    bson_value_destroy(doc_id_value);

    return are_equal;
}


bson_value_t* _aggregate_group_id_replace(bson_value_t *id_spec, bson_t *source_doc) {
    if ((*id_spec).value_type == BSON_TYPE_UTF8) {
        char *field_value = (*id_spec).value.v_utf8.str;
        uint32_t field_len = (*id_spec).value.v_utf8.len;
        if (strncmp("$", field_value, 1) != 0) {
            return id_spec;
        }

        bson_value_t *value = _aggregate_get_value_at_key(source_doc, field_value + 1);
        if (value == NULL) {
            return id_spec;
        }

        return value;
    } else {
        // Only simple strings are support at the moment
        return id_spec;
    }
}

const char * const agg_operators[] = { "$sum" };

bool _aggregate_is_agg_operator(const char *key) {
    size_t index = 0;
    size_t agg_operator_count = 1;

    for(index = 0; index < agg_operator_count; index++) {
        if (strncmp(key, agg_operators[index], strlen(key)) == 0) {
            return true;
        }
    }

    return false;
}

bson_value_t* _aggregate(bson_value_t *value1, bson_value_t *value2, const char *agg_operator) {
    bson_value_t *result = bson_malloc(sizeof(bson_value_t));
    (*result).value_type = BSON_TYPE_INT64;
    (*result).value.v_int64 = 0;

    if (strncmp("$sum", agg_operator, 4) == 0) {
        if (value1 != NULL) {
            (*result).value.v_int64 += (*value1).value.v_int64;
        }
        if (value2 != NULL) {
            (*result).value.v_int64 += (*value2).value.v_int64;
        }
    }

    return result;
}

void _aggregate_recurse_fill(bson_iter_t *iter, bson_t* new_doc, bson_t* existing_aggregate_doc, bson_t* merged_aggregate_doc, const char *key) {
    bson_iter_t child_iter;
    bson_t child_doc;

    while (bson_iter_next (iter)) {
        int new_key_length = strlen(bson_iter_key(iter));
        if (strcmp("", key) != 0) {
            new_key_length += strlen(key) + 1;
        }

        char new_key[new_key_length];
        if (strcmp("", key) == 0) {
            strcpy(new_key, bson_iter_key(iter));
        } else {
            strcpy(new_key, key);
            strcat(new_key, ".");
            strcat(new_key, bson_iter_key(iter));
        }

        if (strcmp("_id", new_key) == 0) {
            bson_value_t *existing_id = _aggregate_get_value_at_key(existing_aggregate_doc, "_id");
            bson_append_value(merged_aggregate_doc, "_id", -1, existing_id);
            continue;
        }

        if (BSON_ITER_HOLDS_DOCUMENT (iter)) {

            const char *agg_key = NULL;
            const bson_value_t *agg_field = NULL;

            if (bson_iter_recurse (iter, &child_iter)) {
                if (bson_iter_next (&child_iter) &&
                    _aggregate_is_agg_operator(bson_iter_key(&child_iter))) {
                    agg_key = bson_iter_key(&child_iter);
                    agg_field = bson_iter_value(&child_iter);
                }

                if (agg_key && !bson_iter_next (&child_iter)) {
                    bson_value_t *existing_value = _aggregate_get_value_at_key(existing_aggregate_doc, new_key);
                    bson_value_t *new_doc_value = _aggregate_get_value_at_key(new_doc, (*agg_field).value.v_utf8.str + 1);
                    bson_value_t * agg_result = _aggregate(existing_value, new_doc_value, agg_key);
                    bson_append_value(merged_aggregate_doc, bson_iter_key(iter), -1, agg_result);
                    continue;

                }
            }

            bson_append_document_begin (merged_aggregate_doc, bson_iter_key(iter), -1, &child_doc);

            if (bson_iter_recurse (iter, &child_iter)) {
                _aggregate_recurse_fill (&child_iter, new_doc, existing_aggregate_doc, &child_doc, new_key);
            }

            bson_append_document_end (merged_aggregate_doc, &child_doc);
        } else {
            bson_append_value(merged_aggregate_doc, bson_iter_key(iter), -1, bson_iter_value(iter));
        }
    }
}

void aggregate_group(aggregation_state_t *state, char *document_json) {
    bson_t *group_document;
    bson_error_t error;
    group_document = bson_new_from_json (document_json, -1, &error);
    if (!group_document) {
        //TODO: Handle error
        return;
    }

    // Step 1: Find the _specification of the _id from the group documnent
    bson_value_t *id_spec = _aggregate_get_value_at_key(group_document, "_id");

    bson_t *new_docs[(*state).docs_len];
    int new_docs_len = 0;

    // Step 2: Aggregate each document in the state
    size_t index;
    for(index = 0; index < (*state).docs_len; index++)
    {
        // Step 2a: Create a copy of the _id spec, with replacements from the source documnent
        bson_t orig_doc;
        orig_doc = (*state).docs[index];
        bson_value_t *id_value = _aggregate_group_id_replace(id_spec, &orig_doc);

        // // Step 2b: Figure of if this _id already exists in the result set and create a new one
        // // if it doesn't
        bson_t *aggregate_doc = NULL;
        size_t aggregate_doc_index = 0;

        size_t new_docs_index;
        for(new_docs_index = 0; new_docs_index < new_docs_len; new_docs_index++) {
            bson_t *new_doc = new_docs[new_docs_index];
            if (_aggregate_have_same_id(new_doc, id_value)) {
                aggregate_doc = new_doc;
                aggregate_doc_index = new_docs_index;
                break;
            }
        }

        if (aggregate_doc == NULL) {
            bson_t *doc = bson_new();
            bson_append_value(doc, "_id", -1, id_value);
            aggregate_doc = doc;
            aggregate_doc_index = new_docs_len;
            new_docs[new_docs_len++] = aggregate_doc;
        }

        // // Step 2c: Build rest of the result document using the current result and
        // // group document
        bson_iter_t iter;
        if (!bson_iter_init (&iter, group_document)) {
            //TODO: Handle error
            continue;
        }

        bson_t *merged_aggregate_doc = bson_new();
        _aggregate_recurse_fill(&iter, &orig_doc, aggregate_doc, merged_aggregate_doc , "");
        new_docs[aggregate_doc_index] = merged_aggregate_doc;
    }

    size_t result_index = 0;
    size_t new_docs_index = 0;


    bson_t *result_docs = malloc(new_docs_len * sizeof(bson_t));

    for(new_docs_index = 0; new_docs_index < new_docs_len; new_docs_index++)
    {
        bson_t *new_doc = new_docs[new_docs_index];
        result_docs[result_index] = *new_doc;
        result_index++;
    }
    (*state).docs = result_docs;
    (*state).docs_len = result_index;
}

int
main (int   argc,
      char *argv[])
{
    aggregation_state_t *state = aggregate_start(NULL, NULL);
    aggregate_match(state, "{}");
    aggregate_group(state, "{\"_id\" : \"$state\", \"data\" : {\"views\" : 0, \"total_population\" : {\"$sum\" : \"$population\"} } }");
    aggregation_result_t *result = aggregate_end(state);

    printf("Result:\n");
    size_t index;
    for(index = 0; index < (*result).docs_len; index++) {
        printf("%s\n", (*result).docs[index]);
    }

}
