#### 创建表

​	Master 发送命令给 RegionServer

​	@后面的ip为127.0.0.1 则表示暂时不创建副本

```
create table [tableName]([xxx]) @ [副本机器名称],[副本机器ip],[副本机器socket port],[副本机器sqlUsr],[副本机器sqlPwd]
例
create table school(name char(20), id int) @ server_1,127.0.0.1,1222,root,123
```



#### 创建副本

Master 发送给需要创建副本的RegionServer

```
dump tableName @ [主机机器名称],[主机机器ip],[主机机器socket port],[主机机器sqlUsr],[主机机器sqlPwd]
```



#### 迁移

Master 发送给需要迁移到的RegionServer

```
migrate tableName @ [主机机器名称],[主机机器ip],[主机机器socket port],[主机机器sqlUsr],[主机机器sqlPwd]
```

