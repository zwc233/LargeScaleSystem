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

