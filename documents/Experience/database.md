# 数据导入总结 BY 黄帅

## “暴力”导入方法

商品信息约有44W条，在`python shell`上，通过局域网传输，采用**逐条导入**的方式，共耗时**660秒**。

## 批量导入方法

- 用户信息约有1200W条，为提高导入速度，采用mysqldb中的批量导入命令`executemany`，读取并转换1W条数据后批量写入数据库，速度估算约为**每1W条30秒**。
- 改进原有格式转换的方法，将对列表中元素的操作改为对变量的操作，速度提升至**10W条数据耗时240秒**
- 使用`python.exe`代替`python shell`执行脚本文件，导入速度提升至**500W条数据约30分**

## 进一步思考

- 为何`python.exe`执行效率比`python shell`效率高？
- 如果采用`事物`，效率能否再提高？
- 一般情况下，如何确定是`python`导致导入速度慢还是`mysql语句`使用不当导致导入速度慢？

## 参考资料：
[MySQL批量SQL插入性能优化](http://blog.csdn.net/xiaoxian8023/article/details/20155429)

[sql存储过程总结](http://blog.chinaunix.net/uid-24929997-id-3080734.html)

[java调用存储过程实现批量入库](http://www.360doc.com/content/11/0322/15/1107705_103532123.shtml)