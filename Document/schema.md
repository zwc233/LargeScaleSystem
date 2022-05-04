#### Test Schema

测试数据库可以如下

学生 table

```sql
create table student(id int, name char(20), teacher_name char(20));
```

##### 数据

```mysql
insert into student value(0,'aaa','AAA');
insert into student value(1,'bbb','BBB');
insert into student value(2,'ccc','CCC');
insert into student value(3,'ddd','DDD');
insert into student value(4,'eee','EEE');
```

老师 table

```sql
create table teacher(id int, name char(20), sex int);
```

场所 table

```
create table place(id int, name char(20), address char(20));
```

课程 table

```sql
create table course(id int, name char(20), score float);
```

