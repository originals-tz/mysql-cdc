#include <mysql/mysql.h>
#include <string>
#include <vector>
#include "../binlog/client.h"

int main()
{
    MYSQL* con = mysql_init(nullptr);
    while (mysql_real_connect(con, "0.0.0.0", "root", "123456", "cdc", 3306, nullptr, 0) == nullptr);
    mysql_query(con, "drop table test_decimal");
    mysql_query(con, R"(
        create table test_decimal
        (
            val decimal(3,2)
        );
    )");
    mysql_query(con, "insert into test_decimal values(3.25)");

    binlog::Client client;
    auto deserializer = std::make_shared<binlog::Deserializer>();
    deserializer->SetTable("test_decimal");
    client.SetDeserializer(deserializer);
    client.Run("0.0.0.0", "root", "123456");
    return 0;
}