package com.imooc.hive.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class StrLen extends UDF {
    public int evaluate(final Text col){
        return col.getLength();
    }
}
