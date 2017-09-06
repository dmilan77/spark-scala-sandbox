import java.net.URI

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.TimestampType

object SparkTest {
 def main(args: Array[String]): Unit = {
  implicit val sc = new SparkContext()
  val hiveContext = new HiveContext(sc)
  val outputDF = hiveContext.sql("select * from dv_ebphqx1ph_allob_r000_wh.hqx_clm_extract limit 10")
   val col_select_list = outputDF.schema.fields
     .map(structField => {
       structField.dataType match {
         case _: TimestampType =>  "date_format(" + structField.name.toString() + ",\"YYYY/MM/dd\")"
         case _ => structField.name.toString()
       }
     })
     .mkString(",")
   println("***********************************************")
   println(col_select_list)

   println("***********************************************")
   val r = scala.util.Random.nextInt(99999).toString
   val tempTableName = "tmptsvxport".concat(r)
   // scala.util.Try(hiveContext.dropTempTable(tempTableName))

   outputDF.registerTempTable(tempTableName)

   val outputDF_1 =
     hiveContext.sql("select " + "*" + " from " + tempTableName)
   //hiveContext.sql("select " + col_select_list + " from " + tempTableName)
   outputDF_1.write
     .mode(SaveMode.Overwrite)
     .format("com.databricks.spark.csv")
     .option("delimiter", ",")
     .option("header", "true")
     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
     .option("inferSchema", "true")
     .option("quoteMode", "NON_NUMERIC")
     .option("timestampformat", "yyyy/mm/dd")
     .save("hdfs://nameservice1/dv/hdfsdata/vs2/ebp/hqx1/phi/all_lob/r000/work/hqx_clm_extract.tsv")
   merge("file:/tmp/milan/out/hqx_clm_extract.tsv", "hdfs://nameservice1/dv/hdfsdata/vs2/ebp/hqx1/phi/all_lob/r000/work/hqx_clm_extract.tsv")
   scala.util.Try(hiveContext.dropTempTable(tempTableName))


 }

  private def merge(srcPath: String, dstPath: String)(
    implicit sc: SparkContext): Unit = {
    val srcFileSystem = FileSystem.get(new URI(srcPath), sc.hadoopConfiguration)
    val dstFileSystem = FileSystem.get(new URI(dstPath), sc.hadoopConfiguration)
    dstFileSystem.delete(new Path(dstPath), true)
    FileUtil.copyMerge(srcFileSystem,
      new Path(srcPath),
      dstFileSystem,
      new Path(dstPath),
      true,
      sc.hadoopConfiguration,
      null)
  }

}
