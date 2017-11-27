package com.hyren.instance.hive_udf_sample;
 
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 
public class PassExam extends UDF {
   
    public Text evaluate(Long score)
    {
        Text result = new Text();
       
        if(score < 101000)
            result.set("Failed");
        else
            result.set("Pass");
       
        return result;    
    }
}