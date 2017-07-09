package Entry

import db.Operation.{DBtoDF, DFsaveToDB}
import db.schema.OnDownSum

import DataProProcess.DataPreprocess.CleanData
import org.apache.spark.sql.{SparkSession}
import Calculator.GetOnDownSum.getOnDownSumDF

/**
  * Created by june on 3/21/17.
  */
object runJob {


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

    //clean data
    val final_df=CleanData(df,sqlcontext)



    // 1. get on and get down stat

    val lineStopData=getOnDownSumDF(sqlcontext,final_df)

    print(lineStopData.filter($"lineNo"==="00042"&&$"checkpoint"===84&&$"direction"===true).count())

    //save data
    DFsaveToDB(OnDownSum,lineStopData)


    //query data

    val returnData = DBtoDF(OnDownSum,sqlcontext)

    print(returnData.filter($"lineNo"==="00042"&&$"checkpoint"===84&&$"direction"===true).count())

    spark.stop()
  }
}
