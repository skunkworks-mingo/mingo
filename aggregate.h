typedef struct {
	char *database;
	char *collection;
	bson_t *docs;
	size_t docs_len;
} aggregation_state_t;

typedef struct {
	char **docs;
	size_t docs_len;
} aggregation_result_t;

aggregation_state_t* aggregate_start(char *database, char *collection);
aggregation_result_t* aggregate_end(aggregation_state_t *state);
void aggregate_match(aggregation_state_t *state, char* query);
void aggregate_group(aggregation_state_t *state, char *document_json);