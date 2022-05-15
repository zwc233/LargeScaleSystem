#### Zookeeper Node 设计 v0.01

```bash
├─lss
│  ├─master
│  │  ├─ip
│  │  └─port
│  └─region_servers
│      ├─server_1
│      │  ├─ip
│      │  ├─mysql
│      │  │  ├─port
│      │  │  ├─pwd
│      │  │  └─user
│      │  ├─tables
│      │  │  ├─table_1
│      │  │  │  └─payload(number of item)
│      │  │  └─table_2
│      │  │      └─payload(number of item)
│      │  ├─to_master_port
│      │  └─to_region_server_port
│      ├─server_2
│      │  ├─ip
│      │  ├─mysql
│      │  │  ├─port
│      │  │  ├─pwd
│      │  │  └─user
│      │  ├─tables
│      │  │  └─table_1_slave
│      │  │      └─payload
│      │  ├─to_master_port
│      │  └─to_region_server_port
│      └─server_3
│          ├─ip
│          ├─mysql
│          │  ├─port
│          │  ├─pwd
│          │  └─user
│          ├─to_master_port
│          └─to_region_server_port
└─zookeeper
```

#### 

#### 注：

现在	server_x	节点为临时节点，其中数据储存格式如下

ip,port,mysql_port,mysql_user,mysql_pwd,table_num,table_name_1,table_name_2,table_name_3...

例：

127.0.0.1,1001,3306,123,root,3,school,student,teacher_slave