#include "deserializer.h"
#include "event.h"
#include "event_header.h"
#include "format_descirption_event.h"
#include "table_map_event.h"

namespace binlog
{
void Deserializer::Deserialize(const unsigned char* buffer, unsigned long size)
{
    m_buffer.Init(buffer, size);
    m_buffer.Skip(1); // mysql network packet header
    EventHeader header;
    header.Deserialize(m_buffer);
    FormatDescriptionEvent formatDescriptionEvent;
    switch (header.GetEventType())
    {
        case FORMAT_DESCRIPTION_EVENT:
        {
            formatDescriptionEvent.Deserialize(m_buffer);
            break;
        }
        case TABLE_MAP_EVENT:
        {
            TableMapEvent event;
            event.Deserialize(m_buffer, formatDescriptionEvent);
        }
        default:
            break;
    }
}

void Deserializer::Attach()
{
}

}
