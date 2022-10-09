#include <mysql/mysql.h>
#include <array>
#include <cstring>
#include <iostream>
#include <vector>

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

std::string GetName(Log_event_type type)
{
#define Case(x) \
    case x:     \
        return #x;
    switch (type)
    {
        Case(UNKNOWN_EVENT);
        Case(START_EVENT_V3);
        Case(QUERY_EVENT);
        Case(STOP_EVENT);
        Case(ROTATE_EVENT);
        Case(INTVAR_EVENT);
        Case(SLAVE_EVENT);
        Case(APPEND_BLOCK_EVENT);
        Case(DELETE_FILE_EVENT);
        Case(RAND_EVENT);
        Case(USER_VAR_EVENT);
        Case(FORMAT_DESCRIPTION_EVENT);
        Case(XID_EVENT);
        Case(BEGIN_LOAD_QUERY_EVENT);
        Case(EXECUTE_LOAD_QUERY_EVENT);
        Case(TABLE_MAP_EVENT);
        Case(WRITE_ROWS_EVENT_V1);
        Case(UPDATE_ROWS_EVENT_V1);
        Case(DELETE_ROWS_EVENT_V1);
        Case(INCIDENT_EVENT);
        Case(HEARTBEAT_LOG_EVENT);
        Case(IGNORABLE_LOG_EVENT);
        Case(ROWS_QUERY_LOG_EVENT);
        Case(WRITE_ROWS_EVENT);
        Case(UPDATE_ROWS_EVENT);
        Case(DELETE_ROWS_EVENT);
        Case(GTID_LOG_EVENT);
        Case(ANONYMOUS_GTID_LOG_EVENT);
        Case(PREVIOUS_GTIDS_LOG_EVENT);
        Case(TRANSACTION_CONTEXT_EVENT);
        Case(VIEW_CHANGE_EVENT);
        Case(XA_PREPARE_LOG_EVENT);
        Case(PARTIAL_UPDATE_ROWS_EVENT);
        Case(TRANSACTION_PAYLOAD_EVENT);
        Case(HEARTBEAT_LOG_EVENT_V2);
        default:
            return "";
    };
}
struct MYSQL_INT_LENENC
{
   void Init(const unsigned char* ptr)
   {
       if (*ptr < 0xfb)
       {
           m_value = uint64_t(*ptr);
           m_len = 1;
       }

       if (*ptr < 0xfc)
       {
           memcpy((unsigned char*)m_value, ptr, 2);
           m_len = 3;
       }

       if (*ptr < 0xfd)
       {
           memcpy((unsigned char*)m_value, ptr, 3);
           m_len = 4;
       }

       if (*ptr < 0xfe)
       {
           memcpy((unsigned char*)m_value, ptr, 8);
           m_len = 9;
       }

   }

   uint64_t m_value = 0;
   uint16_t m_len = 0;
};

struct MYSQL_FORMAT_DESCRIPTION_EVENT
{
    void Init(const unsigned char* ptr)
    {
        /**
        https://dev.mysql.com/doc/internals/en/format-description-event.html
        2                binlog-version
        string[50]       mysql-server version
        4                create timestamp
        1                event header length
        string[p]        event type header lengths
         **/

        m_version = *(uint16_t*)ptr;
        ptr += 2;
        m_mysql_version = (char*)ptr;
        ptr += 50;
        m_create_time = *(uint32_t*)ptr;
        ptr += 4;
        m_event_header_len = uint8_t(*ptr);
        ptr += 1;
        for (int i = 1; i < ENUM_END_EVENT + 1; i++)
        {
            m_event_type_header_len[i] = *ptr;
            ptr++;
        }
    }
    uint16_t m_version = 0;
    std::string m_mysql_version;
    uint32_t m_create_time = 0;
    uint8_t m_event_header_len = 0;
    unsigned char m_event_type_header_len[ENUM_END_EVENT + 1]{0};
};



struct MYSQL_TABLE_MAP_EVENT
{
    void Init(const unsigned char* ptr, uint32_t event_header_len)
    {
        /**
        https://dev.mysql.com/doc/internals/en/table-map-event.html
        post-header:
        if post_header_len == 6 {
            4              table id
        } else {
            6              table id
        }
        2              flags
        payload:
        1              schema name length
        string         schema name
        1              [00]
        1              table name length
        string         table name
        1              [00]
        lenenc-int     column-count
        string.var_len [length=$column-count] column-def
        TODO lenenc-str     column-meta-def
        TODO n              NULL-bitmask, length: (column-count + 8) / 7
        **/
        if (event_header_len == 6)
        {
            memcpy((unsigned char*)&m_table_id, ptr, 4);
            memcpy((unsigned char*)&m_flag, ptr+4, 2);
        }
        else
        {
            memcpy((unsigned char*)&m_table_id, ptr, 6);
            memcpy((unsigned char*)&m_flag, ptr+6, 2);
        }
        ptr += event_header_len;
        m_schema_name_len = uint8_t(*ptr);
        ptr++;
        m_schema_name = (char*)ptr;
        ptr += m_schema_name.size() + 1;
        m_table_name_len = uint8_t(*ptr);
        ptr++;
        m_table_name = (char*)ptr;
        ptr += m_table_name.size() + 1;
        m_column_count.Init(ptr);
        ptr += m_column_count.m_len;
        m_column_type.assign((char*)ptr, m_column_count.m_len);
    }

    uint64_t m_table_id = 0;
    uint16_t m_flag = 0;
    uint8_t m_schema_name_len = 0;
    std::string m_schema_name;
    uint8_t m_table_name_len = 0;
    std::string m_table_name;
    MYSQL_INT_LENENC m_column_count;
    std::string m_column_type;
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

        MYSQL_RPL rpl = {0, nullptr, 4, 1, MYSQL_RPL_SKIP_HEARTBEAT, 0, nullptr, nullptr, 0, nullptr};
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

        MYSQL_FORMAT_DESCRIPTION_EVENT fde;
        while (true)
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
            auto* event_content = (const unsigned char*)(rpl.buffer + 1);

            // https://dev.mysql.com/doc/internals/en/binlog-event-header.html
            auto* event_body = (const unsigned char*)(rpl.buffer + 1 + 19);
            int body_len = rpl.size - 1 - 19;

            switch (type)
            {
                case ROTATE_EVENT:
                {
                    HexStr(rpl.buffer + 1, rpl.size - 1);
                    std::string log((char*)event_content + 19 + 8);
                    printf("next_log : %s\n", log.c_str());
                    break;
                }
                case FORMAT_DESCRIPTION_EVENT:
                {
                    fde.Init(event_body);
                    break;
                }
                case TABLE_MAP_EVENT:
                {
                    int len  = (int)fde.m_event_type_header_len[TABLE_MAP_EVENT];
                    MYSQL_TABLE_MAP_EVENT event;
                    event.Init(event_body, len);
                    std::cout << GetName(type) << std::endl;
                }
                default:
                    break;
            };
        }
    }

private:
    void HexStr(const unsigned char* byte_arr, int arr_len, int offset = 0)
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
