# 天池比赛数据库说明

## 配置

- 数据库地址：202.120.37.201:3306
- 用户名：admin
- 密码：tianchi

## 数据库结构

- 数据库名称: tianchi
- 包含表格
    + item：商品信息数据库
    + user：用户信息数据库

### item表内容说明

- id：主键 int(10)
- item_id：商品编号 int(11)
- item_geohash：地址哈希 char(7)
- item_category：商品类别 int(11)

### user表内容说明

- id：主键 int(10)
- user_id：用户编号 int(11)
- item_id：商品编号 int(11)
- item_category：商品类别 int(11)
- behavior_type：动作类别 int(11)
- user_geohash：用户地址哈希 char(7)
- event_date：动作发生日期（2014-11-17为第0天，以1, 2, ... 30格式存储） int(11)
- event_time：动作发生时间  int(11)