package my.group.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record Item;
    private Record ItemInfo;

    public void setup(TaskContext context) throws IOException {
        Item = context.createMapOutputKeyRecord();
        ItemInfo = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        Item.set("item_id", record.getString(1));
        ItemInfo.set("user_id",record.getString(0));
        ItemInfo.set("behavior_type", record.getBigint(3));
        ItemInfo.set("event_date", record.getBigint(5));
        context.write(Item, ItemInfo);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}