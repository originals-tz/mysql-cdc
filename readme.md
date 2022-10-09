## MYSQL-CDC

### env

```
dockerfile: https://github.com/docker-library/mysql/tree/master/8.0

build : docker build . -t dev

run : docker run -itd --name mysql-dev -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 dev
```

### build

depend : libmysqlclient.a (mysqlclient-21)

```
mkdir build 
cd build
make -j
```