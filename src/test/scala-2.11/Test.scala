import Calculator.GetOnDownSum.getOnDownSumDF
import DataProProcess.DataPreprocess.CleanData
import org.apache.spark.sql.functions.{substring,unix_timestamp, to_date}
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Created by june on 3/27/17.
  */
object Test {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("HBaseSourceExample").master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/home/june/data/data.csv"); //spark 2.0 api


    val sqlcontext=spark.sqlContext

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
      .withColumn("direction",$"getOnNo">$"getDownNo")


    badDf.select(to_date(unix_timestamp($"date","yyyyMMdd").cast("timestamp"))).show()

   // date.show()

    print(badDf.count())
    val final_df = badDf.filter(
      (unix_timestamp($"date","yyyyMMdd")-unix_timestamp(getYMD($"ontime",0,8),"yyyyMMdd"))<86400)

    print(final_df.count())
    spark.stop()
  }
}
