#include <mysql/mysql.h>
#include <iostream>
#include "unistd.h"

std::string bytearray2hex(const unsigned char byte_arr[], int arr_len)
{
    std::string hexstr = "";
    for (int i = 0; i < arr_len; i++)
    {
        char hex1;
        char hex2;
        int value = byte_arr[i];
        int v1 = value / 16;
        int v2 = value % 16;

        if (v1 >= 0 && v1 <= 9)
            hex1 = (char)(48 + v1);
        else
            hex1 = (char)(55 + v1);

        if (v2 >= 0 && v2 <= 9)
            hex2 = (char)(48 + v2);
        else
            hex2 = (char)(55 + v2);

        if (hex1 >= 'A' && hex1 <= 'F')
            hex1 += 32;

        if (hex2 >= 'A' && hex2 <= 'F')
            hex2 += 32;

        hexstr = hexstr + hex1 + hex2 + " ";
    }
    return hexstr;
}

enum Log_event_type {

    UNKNOWN_EVENT = 0,
    START_EVENT_V3 = 1,
    QUERY_EVENT = 2,
    STOP_EVENT = 3,
    ROTATE_EVENT = 4,
    INTVAR_EVENT = 5,
    SLAVE_EVENT = 7,
    APPEND_BLOCK_EVENT = 9,
    DELETE_FILE_EVENT = 11,
    RAND_EVENT = 13,
    USER_VAR_EVENT = 14,
    FORMAT_DESCRIPTION_EVENT = 15,
    XID_EVENT = 16,
    BEGIN_LOAD_QUERY_EVENT = 17,
    EXECUTE_LOAD_QUERY_EVENT = 18,
    TABLE_MAP_EVENT = 19,
    /**

    The V1 event numbers are used from 5.1.16 until mysql-5.6.

                                                                    */

    WRITE_ROWS_EVENT_V1 = 23,

    UPDATE_ROWS_EVENT_V1 = 24,

    DELETE_ROWS_EVENT_V1 = 25,

    INCIDENT_EVENT = 26,
    HEARTBEAT_LOG_EVENT = 27,
    IGNORABLE_LOG_EVENT = 28,
    ROWS_QUERY_LOG_EVENT = 29,

    /** Version 2 of the Row events */

    WRITE_ROWS_EVENT = 30,
    UPDATE_ROWS_EVENT = 31,
    DELETE_ROWS_EVENT = 32,

    GTID_LOG_EVENT = 33,
    ANONYMOUS_GTID_LOG_EVENT = 34,
    PREVIOUS_GTIDS_LOG_EVENT = 35,
    TRANSACTION_CONTEXT_EVENT = 36,

    VIEW_CHANGE_EVENT = 37,
    /* Prepared XA transaction terminal event similar to Xid */
    XA_PREPARE_LOG_EVENT = 38,
    PARTIAL_UPDATE_ROWS_EVENT = 39,
    TRANSACTION_PAYLOAD_EVENT = 40,
    HEARTBEAT_LOG_EVENT_V2 = 41,
    ENUM_END_EVENT /* end marker */

};
// ref : https://github.com/mixigroup/mixi-pgw/blob/main/binlog/main.cc
int main()
{
    MYSQL* con = mysql_init(NULL);
    if (con == nullptr)
    {
        fprintf(stderr, "%s\n", mysql_error(con));
        exit(1);
    }

    if (mysql_real_connect(con, "0.0.0.0", "root", "123456", "log_test", 3306, nullptr, 0) == nullptr)
    {
        fprintf(stderr, "%s\n", mysql_error(con));
        mysql_close(con);
        exit(1);
    }

    if (mysql_query(con, "SET @source_binlog_checksum='ALL'"))
    {
        fprintf(stderr, "mysql_query() failed\n");
        fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
        exit(1);
    }

    //    std::string file_name = "mysql-bin.000004";

    MYSQL_RPL rpl = {0, nullptr, 4, 1, MYSQL_RPL_SKIP_HEARTBEAT, 0, NULL, NULL, 0, NULL};

    if (mysql_binlog_open(con, &rpl))
    {
        fprintf(stderr, "mysql_binlog_open() failed\n");
        fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
        exit(1);
    }
    for (;;) /* read events until error or EOF */
    {
        if (mysql_binlog_fetch(con, &rpl))
        {
            fprintf(stderr, "mysql_binlog_fetch() failed\n");
            fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
            sleep(2);
            break;
        }
        if (rpl.size == 0) /* EOF */
        {
            fprintf(stderr, "EOF event received\n");
            sleep(2);
            break;
        }
        fprintf(stderr, "Event received of size %lu.\n", rpl.size);

        Log_event_type type = (Log_event_type)rpl.buffer[1 + 4];
        const char* ev = (const char*)(rpl.buffer + 1);

        // processing by event type
        switch (type)
        {
            case ROTATE_EVENT:
                printf("ROTATE_EVENT\n");
                break;
            case FORMAT_DESCRIPTION_EVENT:
                printf("FORMAT_DESCRIPTION_EVENT\n");
                //                if (des_ev) { delete des_ev; }
                //                des_ev = new Format_description_event(ev, des_ev);
                break;
            case TRANSACTION_PAYLOAD_EVENT:
                printf("TRANSACTION_PAYLOAD_EVENT\n");
                break;
            case TABLE_MAP_EVENT:
                printf("TABLE_MAP_EVENT\n");
                //                if (des_ev) {
                //                    if (tbl_ev) { delete tbl_ev; }
                //                    tbl_ev = new Table_map_log_event(ev, des_ev);
                //                    // target table
                //                    if (strncasecmp(TARGET_TBL, tbl_ev->m_tblnam.c_str(), strlen(TARGET_TBL))== 0){
                //                        printf("target >> %s\n", TARGET_TBL);
                //                    }
                //                }
                //                break;
            case WRITE_ROWS_EVENT:
            case UPDATE_ROWS_EVENT:
            case DELETE_ROWS_EVENT:
                //                if (tbl_ev) {
                //                    ParseEvent event(ev, des_ev);
                //                    if (event.parse(tbl_ev->create_table_def())) {
                //                        if (type == WRITE_ROWS_EVENT) {
                //                        } else if (type == UPDATE_ROWS_EVENT) {
                //                        } else if (type == DELETE_ROWS_EVENT) {
                //                        }
                //                    }
                //                }
                break;
            default:
                break;
        }
    }
    mysql_binlog_close(con, &rpl);
    mysql_close(con);
    return 0;
}