package my.group.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record Key;
    private Record Value;

    public void setup(TaskContext context) throws IOException {
        Key = context.createMapOutputKeyRecord();
        Value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        Key.set("item_category", record.getString(2));
        Value.set("user_id",record.getString(0));
        Value.set("behavior_type", record.getBigint(3));
        Value.set("event_date", record.getBigint(5));
        context.write(Key, Value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}