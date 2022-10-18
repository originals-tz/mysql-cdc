#ifndef MYSQL_CDC_EVENT_HEADER_H
#define MYSQL_CDC_EVENT_HEADER_H

namespace binlog
{
class EventHeader
{
public:
    void Deserialize(ByteBuffer& buffer)
    {
        buffer.ReadUint32(m_timestamp);
        uint8_t type = 0;
        buffer.ReadUint8(type);
        m_event_type = (EventType)type;

        buffer.ReadUint32(m_server_id);
        buffer.ReadUint32(m_event_length);
        buffer.ReadUint32(m_next_position);
        buffer.ReadUint16(m_flags);
    }

    int64_t GetTimestamp() {return m_timestamp;}
    EventType GetEventType() {return m_event_type;}
    uint32_t GetServerID() { return m_server_id;}
    uint32_t GetNextPosition() {return m_next_position;}
    uint16_t GetFlags() {return m_flags;}
private:
    uint32_t m_timestamp = 0;
    EventType m_event_type = ENUM_END_EVENT;
    uint32_t m_server_id = 0;
    uint32_t m_event_length = 0;
    uint32_t m_next_position = 0;
    uint16_t m_flags = 0;
};
}

#endif  // MYSQL_CDC_EVENT_HEADER_H
