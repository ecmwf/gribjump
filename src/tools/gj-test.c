// Debugging c api

#include <stdio.h>

#include "gribjump/gribjump_c.h"

int main() {

    // use gribjump_initialise
    gribjump_initialise();

    // use gribjump_new_handle
    gribjump_handle_t* gj;
    int y = gribjump_new_handle(&gj);

    char reqstr[] = "retrieve,class=od,type=fc,stream=oper,expver=0001,repres=gg,grid=45/45,levtype=sfc,param=151130,date=20230710,time=1200,step=1/2/3/4,domain=g";
    char rangesstr[] = "1-5,10-13,15-16";
    gribjump_extraction_request_t* request;
    gribjump_new_request(&request, reqstr, rangesstr);

    // use extract
    gribjump_extraction_result_t** resultarray;
    unsigned short nfields;

    int w = extract_single(gj, request, &resultarray, &nfields);

    // print the values from resultarray
    printf("nfields = %d\n", nfields);
    for (int i = 0; i < nfields; i++) {
        double** values;
        unsigned long nrange;
        unsigned long* nvalues;
        gribjump_result_values(resultarray[i], &values, &nrange, &nvalues);
        for (int j = 0; j < nrange; j++) {
            printf("range %d: ", j);
            for (int k = 0; k < nvalues[j]; k++) {
                printf("%f ", values[j][k]);
            }
            printf("\n");
        }
    }
    // use gribjump_delete_handle
    int z = gribjump_delete_handle(gj);
    return 0;
}