#include "deserializer.h"
#include "event.h"
#include "event_header.h"
#include "format_descirption_event.h"

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
            FormatDescriptionEvent formatDescriptionEvent;
            formatDescriptionEvent.Deserialize(m_buffer);
            break;
        }
        case TABLE_MAP_EVENT:
        default:
            break;
    }
}

void Deserializer::Attach()
{
}

}
