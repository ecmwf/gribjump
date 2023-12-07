// Debugging c api

#include <stdio.h>

#include "gribjump/gribjump_c.h"

int main() {

    // use gribjump_initialise
    gribjump_initialise();

    // use gribjump_new_handle
    gribjump_handle_t* gj;
    int y = gribjump_new_handle(&gj);
    printf("gribjump_new_handle returned %d\n", y);

    char reqstr[] = "retrieve,class=od,type=fc,stream=oper,expver=0001,repres=gg,grid=45/45,levtype=sfc,param=151130,date=20230710,time=1200,step=1/2/3/4,domain=g";
    char rangesstr[] = "1-5,10-13,15-16";
    gribjump_extraction_request_t* request;
    gj_new_request(&request, reqstr, rangesstr);

    // use extract
    gribjump_extraction_result_t** resultarray;
    int w = extract(gj, request, resultarray);
    printf("extract returned %d\n", w);

    // use gribjump_delete_handle
    int z = gribjump_delete_handle(gj);
    printf("gribjump_delete_handle returned %d\n", z);
    return 0;
}