
#pragma once

#ifdef __cplusplus
extern "C" {
#endif


struct gribjump_handle_t;
typedef struct gribjump_handle_t gribjump_handle_t;

struct gribjump_extraction_result_t;
typedef struct gribjump_extraction_result_t gribjump_extraction_result_t;

struct gribjump_extraction_request_t;
typedef struct gribjump_extraction_request_t gribjump_extraction_request_t;

int gribjump_new_handle(gribjump_handle_t** gj);
int gribjump_delete_handle(gribjump_handle_t* gj);

int extract(gribjump_handle_t* gj, gribjump_extraction_request_t* request, gribjump_extraction_result_t** results_array);

int gj_new_request(gribjump_extraction_request_t** request, const char* reqstr, const char* rangesstr);

int gribjump_initialise();


#ifdef __cplusplus
} // extern "C"
#endif