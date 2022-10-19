#ifndef MYSQL_CDC_BINLOG_DESERIALIZER_H
#define MYSQL_CDC_BINLOG_DESERIALIZER_H

#include "byte_buffer.h"
#include "format_descirption_event.h"
#include "table_map_event.h"

namespace binlog
{
class Deserializer
{
public:
    void Deserialize(const unsigned char* buffer, unsigned long size);
    void Attach();
private:
    ByteBuffer m_buffer;
    FormatDescriptionEvent m_format_description_event;
    TableMapEvent m_table_map_event;
};
}
#endif  // MYSQL_CDC_BINLOG_DESERIALIZER_H
