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

template<typename T>
void ReadData(T& data, const unsigned char* ptr, int32_t bytes)
{
    memcpy((unsigned char*)&data, ptr, bytes);
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
        else if (*ptr < 0xfc)
        {
            ReadData(m_value, ptr+1, 2);
            m_len = 3;
        }
        else if (*ptr < 0xfd)
        {
            ReadData(m_value, ptr+1, 3);
            m_len = 4;
        }
        else if (*ptr < 0xfe)
        {
            ReadData(m_value, ptr+1, 8);
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

        m_binlog_version = *(uint16_t*)ptr;
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
    uint16_t m_binlog_version = 0;
    std::string m_mysql_version;
    uint32_t m_create_time = 0;
    uint8_t m_event_header_len = 0;
    unsigned char m_event_type_header_len[ENUM_END_EVENT + 1]{0};
};


struct MYSQL_TABLE_MAP_EVENT
{
    void Init(const unsigned char* ptr, MYSQL_FORMAT_DESCRIPTION_EVENT& fde, const unsigned char* end)
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
        // 10+ 1+4 + 1+5 + 1+3
        int event_header_len  = (int)fde.m_event_type_header_len[TABLE_MAP_EVENT];
        if (event_header_len == 6)
        {
            ReadData(m_table_id, ptr, 4);
            ReadData(m_flag, ptr + 4, 2);
        }
        else
        {
            ReadData(m_table_id, ptr, 6);
            ReadData(m_flag, ptr + 6, 2);
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

        m_column_type.assign((char*)ptr, m_column_count.m_value);
        ptr += m_column_count.m_value;
ptr++;
        ptr++;
        ptr++;
        for (int i = 0; i < m_column_count.m_value; i++)
        {
            uint32_t type = (unsigned char)m_column_type[i];
            uint32_t len = MetaDataLen(type);
            if (len == 1)
            {
                len = (uint32_t)*ptr;
                ptr +=1;
            }
            else if (len == 2)
            {
                uint16_t val1 = *(uint16_t*)ptr;
                ptr += 2;
            }
            m_meta_vect.emplace_back(len);
        }
        uint32_t null_bitmask_len = (m_column_count.m_value + 8) / 7;
        m_nullbitmask.assign((char*)ptr, null_bitmask_len);
        size_t diff = end - ptr;
        ptr += null_bitmask_len;
    }

    uint32_t MetaDataLen(uint32_t type)
    {
        switch (type)
        {
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_FLOAT:
                return 1;
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_SET:
                return 2;
            case MYSQL_TYPE_BIT:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
            default:
                return 0;
        }
    }

    uint64_t m_table_id = 0;
    uint16_t m_flag = 0;
    uint8_t m_schema_name_len = 0;
    std::string m_schema_name;
    uint8_t m_table_name_len = 0;
    std::string m_table_name;
    MYSQL_INT_LENENC m_column_count;
    std::string m_column_type;
    std::vector<uint32_t> m_meta_vect;
    std::string m_nullbitmask;
};

struct MYSQL_WRITE_ROW_EVENT
{
    void Init(const unsigned char* ptr, MYSQL_FORMAT_DESCRIPTION_EVENT& fde, MYSQL_TABLE_MAP_EVENT& tme)
    {
        /**
    header:
        if post_header_len == 6 {
            4                    table id
        } else {
            6                    table id
        }
        2                    flags
        if version == 2 {
            2                    extra-data-length
            string.var_len       extra-data
        }
        body:
        lenenc_int           number of columns
        string.var_len       columns-present-bitmap1, length: (num of columns+7)/8
        rows:
        string.var_len       nul-bitmap, length (bits set in 'columns-present-bitmap1'+7)/8
        string.var_len       value of each field as defined in table-map
        ... repeat rows until event-end
         **/
        int event_header_len  = (int)fde.m_event_type_header_len[WRITE_ROWS_EVENT];
        if (event_header_len == 6)
        {
            ReadData(m_table_id, ptr, 4);
            ReadData(m_flag, ptr + 4, 2);
        }
        else
        {
            ReadData(m_table_id, ptr, 6);
            ReadData(m_flag, ptr + 6, 2);
            m_extra_data_len = *(uint16_t*)(ptr + 8) - 2;
        }

        ptr += event_header_len;
        if (m_extra_data_len)
        {
            ptr += m_extra_data_len;
        }
        m_number_of_columns.Init(ptr);
        ptr += m_number_of_columns.m_len;

        uint64_t bitmap_len = (m_number_of_columns.m_len + 7) / 8;
        m_columns_present_bitmap1.assign((char*)ptr, bitmap_len);
        ptr += bitmap_len;

        // string.var_len       nul-bitmap, length (bits set in 'columns-present-bitmap1'+7)/8
        // columns-present-bitmap1 = 0xff = 1111 1111, length = (8+7)/8
        uint64_t nullmap_len = bitmap_len;
        m_null_bitmap.assign((char*)ptr, nullmap_len);
        ptr += nullmap_len;
        for (int i = 0; i < tme.m_column_count.m_value; i++)
        {
            uint32_t type = (unsigned char)tme.m_column_type[i];
            //https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
            //TODO using meta to parse
            switch (type)
            {
                case MYSQL_TYPE_LONG:
                {
                    auto* i32_p = (int32_t*)ptr;
                    std::cout << *i32_p << std::endl;
                    ptr += 4;
                    break;
                }
                case MYSQL_TYPE_STRING:
                {
                    return;
                }
                default: break;
            }
        }
    }

    uint32_t m_table_id = 0;
    uint16_t m_flag = 0;
    uint16_t m_extra_data_len = 0;
    MYSQL_INT_LENENC m_number_of_columns;
    std::string m_columns_present_bitmap1;
    std::string m_null_bitmap;
};


class BinlogReader
{
public:
    void SetTargetTable(const std::string& schema, const std::string& table)
    {
        m_schema = schema;
        m_table = table;
    }

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
        MYSQL_TABLE_MAP_EVENT tme;
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
                    tme.Init(event_body, fde, &rpl.buffer[rpl.size - 1]);
                    m_parse_row = tme.m_schema_name == m_schema && tme.m_table_name == m_table;
                    if (m_parse_row)
                    {
                        HexStr(rpl.buffer + 1 + 19, rpl.size - 1 - 19);
                        tme.Init(event_body, fde, &rpl.buffer[rpl.size - 1]);
                    }
                    break;
                }
                case WRITE_ROWS_EVENT:
                case WRITE_ROWS_EVENT_V1:
                {
                    if (m_parse_row)
                    {
//                        HexStr(rpl.buffer + 1 + 19, rpl.size - 1 - 19);
                        MYSQL_WRITE_ROW_EVENT event;
                        event.Init(event_body, fde, tme);
                        std::cout <<  GetName(type) << std::endl;
                    }
                    break;
                }
                default:
                    break;
            };
        }
    }

private:
    bool m_parse_row = false;
    std::string m_schema;
    std::string m_table;
};

int main()
{
    BinlogReader reader;
    reader.SetTargetTable("cdc", "test3");
    reader.Run("0.0.0.0", "root", "123456");
    return 0;
}
