package my.group.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        long ui_l1_buy= 0;
        long ui_l3_clk= 0;
        long ui_l5_act= 0;
        while (values.hasNext()) {
            Record val = values.next();
            ui_l1_buy += val.getBigint(0);
            ui_l3_clk += val.getBigint(1);
            ui_l5_act += val.getBigint(2);
        }
        result.set(0, key.get(0));
        result.set(1, key.get(1));
        result.set(2, ui_l1_buy);
        result.set(3, ui_l3_clk);
        result.set(4, ui_l5_act);
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
