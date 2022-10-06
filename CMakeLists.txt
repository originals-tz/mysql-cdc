cmake_minimum_required(VERSION 3.16)
project(mysql_cdc)

set(CMAKE_CXX_STANDARD 11)
add_executable(mysql_cdc main.cpp)
target_link_libraries(mysql_cdc  libmysqlclient.a
        ssl
        crypto
        dl
        z
        pthread)
