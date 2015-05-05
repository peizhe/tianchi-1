package my.group.train_ui_part1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;

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

//        ***user_item last N days buy times***
    	  
//    	  0 user_item last 1 day buy times
    	  if(type==4 && date==30)
    	  {
    		  value.set("ui_l1_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l1_buy",0);
    	  }
    	  
//    	  1 user_item last 2 days buy times
    	  if(type==4 && (date>=29 && date<=30))
    	  {
    		  value.set("ui_l2_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l2_buy",0);
    	  }
    	  
//    	  2 user_item last 3 days buy times
    	  if(type==4 && (date>=28 && date<=30))
    	  {
    		  value.set("ui_l3_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l3_buy",0);
    	  }
    	  
//    	  3 user_item last 5 days buy times
    	  if(type==4 && (date>=26 && date<=30))
    	  {
    		  value.set("ui_l5_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l5_buy",0);
    	  }
    	  
//    	  4 user_item last 7 days buy times
    	  if(type==4 && ((date>=26 && date<=30)||(date>=22 && date<=23)))
    	  {
    		  value.set("ui_l7_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l7_buy",0);
    	  }
    	  
//    	  5 user_item last 10 days buy times
    	  if(type==4 && ((date>=26 && date<=30)||(date>=19 && date<=23)))
    	  {
    		  value.set("ui_l10_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l10_buy",0);
    	  }
    	  
//    	  6 user_item last 14 days buy times
    	  if(type==4 && ((date>=26 && date<=30)||(date>=15 && date<=23)))
    	  {
    		  value.set("ui_l14_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l14_buy",0);
    	  }
    	  
//    	  7 user_item last 18 days buy times
    	  if(type==4 && ((date>=26 && date<=30)||(date>=11 && date<=23)))
    	  {
    		  value.set("ui_l18_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l18_buy",0);
    	  }
    	  
    	  
//        ***user_item last N days click times***
    	  
//    	  8 user_item last 1 day click times
    	  if(type==1 && date==30)
    	  {
    		  value.set("ui_l1_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l1_clk",0);
    	  }
    	  
//    	  9 user_item last 2 days click times
    	  if(type==1 && (date>=29 && date<=30))
    	  {
    		  value.set("ui_l2_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l2_clk",0);
    	  }
    	  
//    	  10 user_item last 3 days click times
    	  if(type==1 && (date>=28 && date<=30))
    	  {
    		  value.set("ui_l3_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l3_clk",0);
    	  }
    	  
//    	  11 user_item last 5 days click times
    	  if(type==1 && (date>=26 && date<=30))
    	  {
    		  value.set("ui_l5_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l5_clk",0);
    	  }
    	  
//    	  12 user_item last 7 days click times
    	  if(type==1 && ((date>=26 && date<=30)||(date>=22 && date<=23)))
    	  {
    		  value.set("ui_l7_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l7_clk",0);
    	  }
    	  
//    	  13 user_item last 10 days click times
    	  if(type==1 && ((date>=26 && date<=30)||(date>=19 && date<=23)))
    	  {
    		  value.set("ui_l10_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l10_clk",0);
    	  }
    	  
//    	  14 user_item last 14 days click times
    	  if(type==1 && ((date>=26 && date<=30)||(date>=15 && date<=23)))
    	  {
    		  value.set("ui_l14_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l14_clk",0);
    	  }
    	  
//    	  15 user_item last 18 days click times
    	  if(type==1 && ((date>=26 && date<=30)||(date>=11 && date<=23)))
    	  {
    		  value.set("ui_l18_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l18_clk",0);
    	  }
    	  
    	  
//        ***user_item action in double 12***
    	  
//    	  16 user_item double 12 click times
    	  if(type==1 && (date>=24 && date<=25))
    	  {
    		  value.set("ui_d12_clk",1);
    	  }
    	  else
    	  {
    		  value.set("ui_d12_clk",0);
    	  }
    	  
//    	  17 user_item double 12 buy times
    	  if(type==4 && (date>=24 && date<=25))
    	  {
    		  value.set("ui_d12_buy",1);
    	  }
    	  else
    	  {
    		  value.set("ui_d12_buy",0);
    	  }
    	  
    	  

//        ***user_item last N days cart times***
    	  
//    	  18 user_item last 1 day cart times
    	  if(type==3 && date==30)
    	  {
    		  value.set("ui_l1_cart",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l1_cart",0);
    	  }
    	  
//    	  19 user_item last 2 days cart times
    	  if(type==3 && (date>=29 && date<=30))
    	  {
    		  value.set("ui_l2_cart",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l2_cart",0);
    	  }
    	  
//    	  20 user_item last 3 days cart times
    	  if(type==3 && (date>=28 && date<=30))
    	  {
    		  value.set("ui_l3_cart",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l3_cart",0);
    	  }


//        21 user_item last 7 days cart times
          if(type==3 && ((date>=26 && date<=30)||(date>=22 && date<=23)))
          {
              value.set("ui_l7_cart",1);
          }
          else
          {
              value.set("ui_l7_cart",0);
          }
    	  
    	  

//        ***user_item last N days favor times***

//    	  22 user_item last 7 days favor times
    	  if(type==2 && ((date>=26 && date<=30)||(date>=22 && date<=23)))
    	  {
    		  value.set("ui_l7_favor",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l7_favor",0);
    	  }

    	  
//    	  23 user_item last 14 days favor times
    	  if(type==2 && ((date>=26 && date<=30)||(date>=15 && date<=23)))
    	  {
    		  value.set("ui_l14_favor",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l14_favor",0);
    	  }
    	  
//    	  24 user_item last 18 days favor times
    	  if(type==2 && ((date>=26 && date<=30)||(date>=11 && date<=23)))
    	  {
    		  value.set("ui_l18_favor",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l18_favor",0);
    	  }
    	  
    	  
//    	  ***other user_item features***
    	  
//    	  25 user_item buy date
    	  if(type==4)
    	  {
    		  value.set("buy_date",date);
    	  }
    	  else
    	  {
    		  value.set("buy_date",0);
    	  }
    	    
    	  
//    	  26 user_item act date
    	  
    	  value.set("act_date",date);
    	  
    	  
//    	  27 user_item last 3 days later times
    	  
    	  if((type==2 || type==3) && (date>=28 && date<=30))
    	  {
    		  value.set("ui_l3_later",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l3_later",0);
    	  }
    	  
//    	  28 user_item last 7 days later times
    	  
    	  if((type==2 || type==3) && ((date>=26 && date<=30)||(date>=22 && date<=23)))
    	  {
    		  value.set("ui_l7_later",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l7_later",0);
    	  }
    	  
//    	  29 user_item last 14 days later times
    	  
    	  if((type==2 || type==3) && ((date>=26 && date<=30)||(date>=15 && date<=23)))
    	  {
    		  value.set("ui_l14_later",1);
    	  }
    	  else
    	  {
    		  value.set("ui_l14_later",0);
    	  }
    	 
    	  
          context.write(key, value);
    	  
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}