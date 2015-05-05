package my.group.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;

    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
//        one.setBigint(0, 1L);
    }


    
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
//        String w = record.getString(0);
//        word.setString(0, w);
//        context.write(word, one);
    	  key.set("user_id",record.getString(0));
    	  key.set("item_id",record.getString(1));
    	  long type=record.getBigint(3);
    	  long date=record.getBigint(5);
    	  
//    	  user_item last 1 day buy times
    	  if(type==4 && date==30)
    	  {
    		  value.set("ui_l1_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l1_buy",0);
    	  }
    	  
//    	  user_item last 3 days click times
    	  if(type==1 && (date>=28 && date<=30))
    	  {
    		  value.set("ui_l3_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l3_clk",0);
    	  }
    	  
//    	  user_item last 5 days action times
    	  if(date>=26 && date<=30)
    	  {
    		  value.set("ui_l5_act",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l5_act",0);
    	  }
    	  
          context.write(key, value);
    	  
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}