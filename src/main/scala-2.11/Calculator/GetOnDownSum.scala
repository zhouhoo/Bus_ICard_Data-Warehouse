package Calculator

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by june on 3/24/17.
  */
object GetOnDownSum {

  def getOnDownSumDF  (sqlcontext: SQLContext ,final_df: DataFrame) ={
    import sqlcontext.implicits._

    val lineStopGetOnData=final_df.groupBy($"yearOn",$"monthOn",$"dayOn",$"lineNo",$"direction",$"Oncheckpoint"
      ,$"getOnNo").count()
      .withColumnRenamed("count","OnCount")
      .withColumnRenamed("getOnNo","stopNo")
      .withColumnRenamed("Oncheckpoint","checkpoint")
      .withColumnRenamed("dayOn","day")
      .withColumnRenamed("yearOn","year")
      .withColumnRenamed("monthOn","month")




    val lineStopGetDownData=final_df.groupBy($"yearDown",$"monthDown",$"dayDown",$"lineNo",$"direction",$"Downcheckpoint",
      $"getDownNo").count().withColumnRenamed("count","DownCount")
      .withColumnRenamed("getDownNo","stopNo")
      .withColumnRenamed("Downcheckpoint","checkpoint")
      .withColumnRenamed("dayDown","day")
      .withColumnRenamed("yearDown","year")
      .withColumnRenamed("monthDown","month")


    val lineStopData=lineStopGetOnData.as("d1")
      .join(lineStopGetDownData.as("d2"),
        Seq("year","month","day","lineNo","checkpoint","direction","stopNo"),"outer")
      .na.fill(0,Seq("OnCount","DownCount"))
      .withColumn("totalCount",$"OnCount"+$"DownCount".alias("totalCount"))

    lineStopData
  }
}
