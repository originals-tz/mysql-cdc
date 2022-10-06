#include <mysql/mysql.h>
#include <iostream>

int main()
{
    MYSQL* con = mysql_init(NULL);
    if (con == nullptr)
    {
        fprintf(stderr, "%s\n", mysql_error(con));
        exit(1);
    }

    if (mysql_real_connect(con, "0.0.0.0", "root", "123456", "log_test", 3306, nullptr, 0) == nullptr)
    {
        fprintf(stderr, "%s\n", mysql_error(con));
        mysql_close(con);
        exit(1);
    }

    if (mysql_query(con, "SET @source_binlog_checksum='ALL'"))
    {
        fprintf(stderr, "mysql_query() failed\n");
        fprintf(stderr, "Error %u: %s\n",
                mysql_errno(con), mysql_error(con));
        exit(1);
    }

    MYSQL_RPL rpl;

    rpl.file_name_length = 0;
    rpl.file_name = NULL;
    rpl.start_position = 4;
    rpl.server_id = 0;
    rpl.flags = 0;

    if (mysql_binlog_open(con, &rpl))
    {
        fprintf(stderr, "mysql_binlog_open() failed\n");
        fprintf(stderr, "Error %u: %s\n",
                mysql_errno(con), mysql_error(con));
        exit(1);
    }
    for (;;)  /* read events until error or EOF */
    {
        if (mysql_binlog_fetch(con, &rpl))
        {
            fprintf(stderr, "mysql_binlog_fetch() failed\n");
            fprintf(stderr, "Error %u: %s\n",
                    mysql_errno(con), mysql_error(con));
            break;
        }
        if (rpl.size == 0)  /* EOF */
        {
            fprintf(stderr, "EOF event received\n");
            break;
        }
        fprintf(stderr, "Event received of size %lu.\n", rpl.size);
    }
    mysql_binlog_close(con, &rpl);
    mysql_close(con);
    return 0;
}
