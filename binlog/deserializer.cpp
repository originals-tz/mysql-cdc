#include "deserializer.h"
#include "event.h"
#include "event_header.h"
#include "format_descirption_event.h"
#include "table_map_event.h"
#include "write_row_event.h"

namespace binlog
{
void Deserializer::Deserialize(const unsigned char* buffer, unsigned long size)
{
    m_buffer.Init(buffer, size);
    m_buffer.Skip(1); // mysql network packet header
    EventHeader header;
    header.Deserialize(m_buffer);
    switch (header.GetEventType())
    {
        case FORMAT_DESCRIPTION_EVENT:
        {
            m_format_description_event.Deserialize(m_buffer);
            break;
        }
        case TABLE_MAP_EVENT:
        {
            m_table_map_event.Deserialize(m_buffer, m_format_description_event);
            break;
        }
        case WRITE_ROWS_EVENT:
        {
            WriteRowEvent event;
            event.Deserialize(m_buffer, m_format_description_event);
        }
        default:
            break;
    }
}

void Deserializer::Attach()
{
}

}
