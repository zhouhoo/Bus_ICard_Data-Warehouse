package DataProProcess

import org.apache.spark.sql.functions.{substring, unix_timestamp}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
/**
  * Created by june on 3/24/17.
  */
object DataPreprocess {


        def CleanData(df: DataFrame,sqlcontext: SQLContext):DataFrame={

          import sqlcontext.implicits._
          val filtered_df = df.select($"卡号".alias("CardNum"),
            $"线路号".alias("lineNo").cast("Int"),
            $"车辆号".alias("busNo"),
            $"上车站".alias("getOnNo").cast("Int"),
            $"下车站".alias("getDownNo").cast("Int"),
            $"上车时间".alias("ontime"),
            $"交易时间".alias("downtime"),
            $"交易日期".alias("date"))


          val getYMD: (Column,Int,Int) => Column = (x:Column,index:Int,len:Int) => {substring(x,index,len)}
          val getTimeBreak: (Column,Int,Int) => Column = (time,y,z) => {(substring(time,y,2)*60+substring(time,z,2))/15 +1}


          val badDf=filtered_df.withColumn("yearOn",getYMD($"ontime",0,4).cast("Int"))
            .withColumn("monthOn",getYMD($"ontime",5,2).cast("Int"))
            .withColumn("yearDown",getYMD($"date",0,4).cast("Int"))
            .withColumn("monthDown",getYMD($"date",5,2).cast("Int"))
            .withColumn("dayOn",getYMD($"ontime",7,2).cast("Int"))
            .withColumn("dayDown",getYMD($"date",7,2).cast("Int"))
            .withColumn("Oncheckpoint",getTimeBreak($"ontime",9,11).cast("Int"))
            .withColumn("Downcheckpoint",getTimeBreak($"downtime",0,2).cast("Int"))
            .withColumn("direction",$"getOnNo">$"getDownNo").na.drop()



          val final_df = badDf.filter($"monthOn" >0 && $"dayOn" >0
          &&(unix_timestamp($"date","yyyyMMdd")-unix_timestamp(getYMD($"ontime",0,8),"yyyyMMdd"))<86400)// can not more than one day
          final_df
        }


}
