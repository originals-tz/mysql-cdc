#ifndef MYSQL_CDC_WRITE_ROW_EVENT_H
#define MYSQL_CDC_WRITE_ROW_EVENT_H

#include "byte_buffer.h"

namespace binlog
{
class WriteRowEvent
{
public:
    void Deserialize(ByteBuffer& buffer, FormatDescriptionEvent& fde)
    {
        auto event_header_len = fde.GetHeaderLen(WRITE_ROWS_EVENT);
        if (event_header_len == 6)
        {
            buffer.Read(&m_table_id, 4);
            buffer.Skip(2);
        }
        else
        {
            buffer.Read(&m_table_id, 6);
            buffer.Skip(2);
            uint16_t extra_data_len = 0;
            buffer.ReadUint16(extra_data_len);
            buffer.Skip(extra_data_len - 2);
        }

        m_number_of_column = buffer.ReadPackedInteger();
        m_include_columns.resize(m_number_of_column);
        buffer.ReadBitSet(&m_include_columns.front(), m_number_of_column);
    }


private:
    void DeserializeRow(ByteBuffer& buffer)
    {
        
    }
    uint64_t m_table_id = 0;
    uint64_t m_number_of_column = 0;
    std::vector<uint8_t> m_include_columns;
};
}

#endif  // MYSQL_CDC_WRITE_ROW_EVENT_H
