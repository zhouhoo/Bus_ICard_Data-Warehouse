package db

/**
  * Created by june on 3/24/17.
  */
object schema {

  val OnDownSum = s"""{
               |"table":{"namespace":"default", "name":"OnDownSum"},
               |"rowkey":"year:month:day:lineNo:stopNo:checkpoint:direction",
               |"columns":{
                  |"year":{"cf":"rowkey", "col":"year", "type":"int"},
                  |"month":{"cf":"rowkey", "col":"month", "type":"int"},
                  |"day":{"cf":"rowkey", "col":"day", "type":"int"},
                  |"lineNo":{"cf":"rowkey", "col":"lineNo", "type":"int"},
                  |"stopNo":{"cf":"rowkey", "col":"stopNo", "type":"int"},
                  |"checkpoint":{"cf":"rowkey", "col":"checkpoint", "type":"int"},
                  |"direction":{"cf":"rowkey", "col":"direction", "type":"boolean"},
                  |"OnCount":{"cf":"value", "col":"onSum", "type":"long"},
                  |"DownCount":{"cf":"value", "col":"downSum", "type":"long"},
                  |"totalCount":{"cf":"value", "col":"totoalSum", "type":"long"}
                  |}
               |}""".stripMargin
}
