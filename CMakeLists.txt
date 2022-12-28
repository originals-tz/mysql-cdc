cmake_minimum_required(VERSION 3.16)
project(mysql_cdc)

set(CMAKE_CXX_STANDARD 11)
set(binlog binlog/client.cpp binlog/deserializer.cpp)
file(GLOB_RECURSE CPP_SRC_LIST "test/*.cpp")
foreach(test ${CPP_SRC_LIST})
    string(REGEX MATCH "test/.*" relative_path ${test})
    string(REGEX REPLACE "test/" "" target_name ${relative_path})
    string(REGEX REPLACE ".cpp" "" target_name ${target_name})
    add_executable(${target_name} ${test} ${binlog})
    target_link_libraries(${target_name} libmysqlclient.a
        ssl
        crypto
        dl
        z
        pthread)
    add_test(NAME ${target_name} COMMAND ${target_name}
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
endforeach()

add_executable(mysql_cdc main.cpp binlog/client.cpp binlog/deserializer.cpp)
target_link_libraries(mysql_cdc  libmysqlclient.a
        ssl
        crypto
        dl
        z
        pthread)