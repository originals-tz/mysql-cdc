#ifndef MYSQL_CDC_TABLE_MAP_EVENT_H
#define MYSQL_CDC_TABLE_MAP_EVENT_H

#include <mysql/field_types.h>
#include <vector>
#include "format_descirption_event.h"

namespace binlog
{
class TableMapEvent
{
public:
    void Deserialize(ByteBuffer& buffer, FormatDescriptionEvent& fde)
    {
        uint8_t len = fde.GetHeaderLen(TABLE_MAP_EVENT);
        if (len == 6)
        {
            buffer.Read(&m_table_id, 4);
        }
        else
        {
            buffer.Read(&m_table_id, 6);
        }
        buffer.Skip(2);
        buffer.ReadUint8(len);
        buffer.ReadString(m_schema_name, len);
        buffer.Skip(1);

        buffer.ReadUint8(len);
        buffer.ReadString(m_table_name, len);
        buffer.Skip(1);

        m_number_of_column = buffer.ReadPackedInteger();
        m_column_type.resize(m_number_of_column);
        buffer.ReadBitSet(&m_column_type.front(), m_number_of_column);
        buffer.ReadPackedInteger(); //metadata length
        ReadMetaData(buffer);
        m_nullability_vect.resize(m_number_of_column);
        buffer.ReadBitSet(&m_nullability_vect.front(), m_number_of_column);
    }

    void ReadMetaData(ByteBuffer& buffer)
    {
        m_column_metadata_vect.resize(m_number_of_column);
        for (size_t i = 0; i < m_column_type.size(); i++)
        {
            switch (m_column_type[i])
            {
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_FLOAT:
                    buffer.Read(&m_column_metadata_vect[i], 1);
                    break;
                case MYSQL_TYPE_STRING:
                case MYSQL_TYPE_VAR_STRING:
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET:
                    buffer.Read(&m_column_metadata_vect[i], 2);
                    break;
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
                    m_column_metadata_vect[i] = 0;
                    break;
            }
        }
    }

    uint64_t GetTableId() const
    {
        return m_table_id;
    }

    const std::string& GetDataBase()
    {
        return m_schema_name;
    }

    const std::string& GetTable()
    {
        return m_table_name;
    }

    uint64_t GetNumberOfColumn() const
    {
        return m_number_of_column;
    }

    uint32_t GetMetaData(size_t column) const
    {
        return m_column_metadata_vect[column];
    }

private:
    uint64_t m_table_id = 0;
    std::string m_schema_name;
    std::string m_table_name;
    uint64_t m_number_of_column = 0;
    std::vector<uint8_t> m_column_type;
    std::vector<uint32_t> m_column_metadata_vect;
    std::vector<uint8_t> m_nullability_vect;
};
}

#endif  // MYSQL_CDC_TABLE_MAP_EVENT_H
