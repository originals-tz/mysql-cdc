#ifndef MYSQL_CDC_FORMAT_DESCIRPTION_EVENT_H
#define MYSQL_CDC_FORMAT_DESCIRPTION_EVENT_H

#include <cstdint>
#include <string>
#include "byte_buffer.h"
#include "event.h"

namespace binlog
{
class FormatDescriptionEvent
{
public:
    void Deserialize(ByteBuffer& buffer)
    {
        buffer.ReadUint16(m_binlog_version);
        buffer.ReadString(m_mysql_version, 50);
        buffer.ReadUint32(m_create_time);
        buffer.ReadUint8(m_event_header_len);
        buffer.Read(m_event_type_header_len, ENUM_END_EVENT);
    }

    uint8_t GetHeaderLen(EventType type)
    {
        if (type == UNKNOWN_EVENT)
        {
            return 0;
        }
        return m_event_type_header_len[int(type) - 1];
    }
private:
    uint16_t m_binlog_version = 0;
    std::string m_mysql_version;
    uint32_t m_create_time = 0;
    uint8_t m_event_header_len = 0;
    uint8_t m_event_type_header_len[ENUM_END_EVENT]{0};
};
}


#endif  // MYSQL_CDC_FORMAT_DESCIRPTION_EVENT_H
