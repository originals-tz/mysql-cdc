#ifndef MYSQL_CDC_BINLOG_DESERIALIZER_H
#define MYSQL_CDC_BINLOG_DESERIALIZER_H

#include "byte_buffer.h"

namespace binlog
{
class Deserializer
{
public:
    void Deserialize(const unsigned char* buffer, unsigned long size);
    void Attach();
private:
    ByteBuffer m_buffer;
};
}
#endif  // MYSQL_CDC_BINLOG_DESERIALIZER_H
