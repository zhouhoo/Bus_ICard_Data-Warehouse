package db

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

/**
  * Created by june on 3/24/17.
  */
object Operation {

  def DBtoDF(cat: String, sQLContext: SQLContext): DataFrame = {
    sQLContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def DFsaveToDB(cat:String, dataFrame: DataFrame)={
    /**
      * save whole DF to db and create new 4 regions for  it every day .
      * Do not use this function too often and too long,this may make region server more burden after 10 years.
      * but you can feel good within several years and with more hbase cluster.
      * so next optimization is insert DF to exist and fixed region .
      * hope you guy can have a try, good lucky.
      *
      */

      dataFrame.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
