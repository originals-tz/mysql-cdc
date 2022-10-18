#ifndef MYSQL_CDC_BINLOG_CLIENT_H
#define MYSQL_CDC_BINLOG_CLIENT_H

#include <string>
#include <memory>
#include "deserializer.h"

namespace binlog
{
class Client
{
public:
    void SetDeserializer(std::shared_ptr<Deserializer> deserializer);
    bool Run(const std::string& host, const std::string& username, const std::string& password, int32_t port = 3306);
private:
    std::shared_ptr<Deserializer> m_deserializer;
};
}

#endif  // MYSQL_CDC_BINLOG_CLIENT_H
