package my.group.train_ui_part1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;

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
        long ui_l2_buy= 0;
        long ui_l3_buy= 0;
        long ui_l5_buy= 0;
        long ui_l7_buy= 0;
        long ui_l10_buy= 0;
        long ui_l14_buy= 0;
        long ui_l18_buy= 0;
        
        long ui_l1_clk= 0;
        long ui_l2_clk= 0;
        long ui_l3_clk= 0;
        long ui_l5_clk= 0;
        long ui_l7_clk= 0;
        long ui_l10_clk= 0;
        long ui_l14_clk= 0;
        long ui_l18_clk= 0;
        
        long ui_d12_clk= 0;
        long ui_d12_buy= 0;
        
        long ui_l1_cart= 0;
        long ui_l2_cart= 0;
        long ui_l3_cart= 0;
        long ui_l7_cart= 0;
        
        long ui_l7_favor= 0;
        long ui_l14_favor= 0;
        long ui_l18_favor= 0;
        
        long buy_date;
        long act_date;
        long ui_l_act_date=0;
        long ui_l_buy_date=0;
        long ui_f_buy_date=31;
        
        long ui_l3_later= 0;
        long ui_l7_later= 0;
        long ui_l14_later= 0;
        
        double ui_l3_clk2buy=0;
        double ui_l7_clk2buy=0;
        double ui_l14_clk2buy=0;
        double ui_l3_later2buy=0;
        double ui_l7_later2buy=0;
        double ui_l14_later2buy=0;
        
        while (values.hasNext()) {
            Record val = values.next();
            ui_l1_buy += val.getBigint(0);
            ui_l2_buy += val.getBigint(1);
            ui_l3_buy += val.getBigint(2);
            ui_l5_buy += val.getBigint(3);
            ui_l7_buy += val.getBigint(4);
            ui_l10_buy += val.getBigint(5);
            ui_l14_buy += val.getBigint(6);
            ui_l18_buy += val.getBigint(7);
            ui_l1_clk += val.getBigint(8);
            ui_l2_clk += val.getBigint(9);
            ui_l3_clk += val.getBigint(10);
            ui_l5_clk += val.getBigint(11);
            ui_l7_clk += val.getBigint(12);
            ui_l10_clk += val.getBigint(13);
            ui_l14_clk += val.getBigint(14);
            ui_l18_clk += val.getBigint(15);
            ui_d12_clk += val.getBigint(16);
            ui_d12_buy += val.getBigint(17);
            ui_l1_cart += val.getBigint(18);
            ui_l2_cart += val.getBigint(19);
            ui_l3_cart += val.getBigint(20);
            ui_l7_cart += val.getBigint(21);
            ui_l7_favor += val.getBigint(22);
            ui_l14_favor += val.getBigint(23);
            ui_l18_favor += val.getBigint(24);
            ui_l3_later += val.getBigint(27);
            ui_l7_later += val.getBigint(28);
            ui_l14_later += val.getBigint(29);
            
            buy_date=val.getBigint(25);
            act_date=val.getBigint(26);
            if(buy_date>ui_l_buy_date)
            {
            	ui_l_buy_date=buy_date;
            }
            
            if(buy_date<ui_f_buy_date && buy_date>0)
            {
            	ui_f_buy_date=buy_date;
            }
            
            if(act_date>ui_l_act_date)
            {
            	ui_l_act_date=act_date;
            }
            
        }
        
        ui_l_buy_date=31-ui_l_buy_date;
        ui_f_buy_date=31-ui_f_buy_date;
        long ui_fl_buy_distance;
        ui_fl_buy_distance=ui_f_buy_date-ui_l_buy_date;
        ui_l_act_date=31-ui_l_act_date;
        
        if(ui_l3_buy>0)
        {
        	ui_l3_clk2buy=ui_l3_clk/ui_l3_buy;
        }
        
        if(ui_l7_buy>0)
        {
        	ui_l7_clk2buy=ui_l7_clk/ui_l7_buy;
        }
        
        if(ui_l14_buy>0)
        {
        	ui_l14_clk2buy=ui_l14_clk/ui_l14_buy;
        }
        
        if(ui_l3_buy>0)
        {
        	ui_l3_later2buy=ui_l3_later/ui_l3_buy;
        }
        
        if(ui_l7_buy>0)
        {
        	ui_l7_later2buy=ui_l7_later/ui_l7_buy;
        }
        
        if(ui_l14_buy>0)
        {
        	ui_l14_later2buy=ui_l14_later/ui_l14_buy;
        }
        
        
        result.set(0, key.get(0));
        result.set(1, key.get(1));
        result.set(2, ui_l1_buy);
        result.set(3, ui_l2_buy);
        result.set(4, ui_l3_buy);
        result.set(5, ui_l5_buy);
        result.set(6, ui_l7_buy);
        result.set(7, ui_l10_buy);
        result.set(8, ui_l14_buy);
        result.set(9, ui_l18_buy);
        result.set(10, ui_l1_clk);
        result.set(11, ui_l2_clk);
        result.set(12, ui_l3_clk);
        result.set(13, ui_l5_clk);
        result.set(14, ui_l7_clk);
        result.set(15, ui_l10_clk);
        result.set(16, ui_l14_clk);
        result.set(17, ui_l18_clk);
        result.set(18, ui_d12_clk);
        result.set(19, ui_d12_buy);
        result.set(20, ui_l1_cart);
        result.set(21, ui_l2_cart);
        result.set(22, ui_l3_cart);
        result.set(23, ui_l7_cart);
        result.set(24, ui_l7_favor);
        result.set(25, ui_l14_favor);
        result.set(26, ui_l18_favor);
        result.set(27, ui_l_buy_date);
        result.set(28, ui_l_act_date);
        result.set(29, ui_fl_buy_distance);
        result.set(30, ui_l3_clk2buy);
        result.set(31, ui_l7_clk2buy);
        result.set(32, ui_l14_clk2buy);
        result.set(33, ui_l3_later2buy);
        result.set(34, ui_l7_later2buy);
        result.set(35, ui_l14_later2buy);
        
     
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
