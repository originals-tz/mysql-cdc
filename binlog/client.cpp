#include "client.h"
#include <mysql/mysql.h>

#include <utility>

namespace binlog
{

void Client::SetDeserializer(std::shared_ptr<Deserializer> deserializer)
{
    m_deserializer = std::move(deserializer);
}

bool Client::Run(const std::string& host, const std::string& username, const std::string& password, int32_t port)
{
    if (!m_deserializer)
    {
        return false;
    }

    MYSQL* con = mysql_init(nullptr);
    if (mysql_real_connect(con, host.c_str(), username.c_str(), password.c_str(), nullptr, port, nullptr, 0) == nullptr)
    {
        fprintf(stderr, "%s\n", mysql_error(con));
        mysql_close(con);
        return false;
    }

    if (mysql_query(con, "SET @master_binlog_checksum='NONE', @source_binlog_checksum = 'NONE'"))
    {
        fprintf(stderr, "mysql_query() failed\n");
        fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
        return false;
    }

    MYSQL_RPL rpl = {0, nullptr, 4, 1, MYSQL_RPL_SKIP_HEARTBEAT, 0, nullptr, nullptr, 0, nullptr};

    if (mysql_binlog_open(con, &rpl))
    {
        fprintf(stderr, "mysql_binlog_open() failed\n");
        fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
        return false;
    }

    while (true)
    {
        if (mysql_binlog_fetch(con, &rpl))
        {
            fprintf(stderr, "mysql_binlog_fetch() failed\n");
            fprintf(stderr, "Error %u: %s\n", mysql_errno(con), mysql_error(con));
            return false;
        }

        if (rpl.size == 0)
        {
            fprintf(stderr, "EOF event received\n");
            return false;
        }

        m_deserializer->Deserialize(rpl.buffer, rpl.size);
    }
}

}