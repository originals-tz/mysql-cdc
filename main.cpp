#include <mysql/mysql.h>
#include <iostream>

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

class BinlogReader
{
public:
    bool Run(const std::string& host, const std::string& username, const std::string& password, int32_t port = 3306)
    {
        MYSQL* con = mysql_init(nullptr);
        if (mysql_real_connect(con, host.c_str(), username.c_str(), password.c_str(), nullptr, port, nullptr, 0) == nullptr)
        {
            fprintf(stderr, "%s\n", mysql_error(con));
            mysql_close(con);
            return false;
        }

        if (mysql_query(con, "SET @master_binlog_checksum='NONE', @source_binlog_checksum = 'NONE'"))
        {
            fprintf(stderr, "mysql_query() failed\n");
            fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
            return false;
        }

        MYSQL_RPL rpl = {
             0,
             nullptr,
            4,
            1,
            MYSQL_RPL_SKIP_HEARTBEAT,
            0, nullptr,
            nullptr,
            0,
            nullptr};
        FetchBinlog(con, rpl);
        return true;
    }

    void FetchBinlog(MYSQL* con, MYSQL_RPL& rpl)
    {
        if (mysql_binlog_open(con, &rpl))
        {
            fprintf(stderr, "mysql_binlog_open() failed\n");
            fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
            return;
        }

        while(true)
        {
            if (mysql_binlog_fetch(con, &rpl))
            {
                fprintf(stderr, "mysql_binlog_fetch() failed\n");
                fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
                return;
            }
            if (rpl.size == 0)
            {
                fprintf(stderr, "EOF event received\n");
                return;
            }

            // (Log_event_type)net->read_pos[1 + EVENT_TYPE_OFFSET] : https://github.com/mysql/mysql-server/blob/8.0/sql-common/client.cc#L7267
            // rpl.buffer[0] is packet header, https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
            auto type = (Log_event_type)rpl.buffer[1 + 4];
            const char* event_content = (const char*)(rpl.buffer + 1);

            switch (type)
            {
                case ROTATE_EVENT:
                {
                    HexStr(rpl.buffer + 1, rpl.size - 1);
                    std::string log(event_content + 19 + 8);
                    printf("next_log : %s\n", log.c_str());
                    break;
                }
                default:
                    break;
            };
        }
    }

private:
    void HexStr(const unsigned char byte_arr[], int arr_len, int offset = 0)
    {
        std::string hexstr;
        for (int i = offset; i < arr_len; i++)
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
        std::cout << hexstr << std::endl;
    }
};

int main()
{
    BinlogReader reader;
    reader.Run("0.0.0.0", "root", "123456");
    return 0;
}
