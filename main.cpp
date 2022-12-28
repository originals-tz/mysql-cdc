#include "binlog/client.h"


int main()
{
    binlog::Client client;
    auto deserializer = std::make_shared<binlog::Deserializer>();
    deserializer->SetTable("test_str");
    client.SetDeserializer(deserializer);
    client.Run("0.0.0.0", "root", "123456");
    return 0;
}
