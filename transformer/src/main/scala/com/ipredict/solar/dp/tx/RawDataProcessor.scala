package com.ipredict.solar.dp.tx
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{ StructType, StructField, StringType };
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.explode
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RawDataProcessor {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("RawDataProcessor"))
    val dgadata = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/ipredict/rawdata/flume/events/17_01_08/dga-*")

    //val schemaString = "DateTested,SamplingDate,Mnemonic,Site,TXNo,SamplingPoint,SerialNumber,LoadMVA,Voltage_KV,Maker,YearBuilt,H2,O2,N2,CH4,CO,CO2";
    val schemaString = "DateTested,SamplingDate";
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val  filteredData = dgadata.map(line => parse(line))
    
    val toDF = sqlContext.createDataFrame(filteredData,  schema)
    println("Hello11133333")

  //----  toDF.write.parquet("hdfs://sandbox.hortonworks.com:8020/user/ipredict/processed/flume/events/dga-" + java.time.LocalDate.now + ".parquet")

   // val parquetFile1 = sqlContext.read.parquet("hdfs://sandbox.hortonworks.com:8020/user/ipredict/processed/flume/events/dga-" + java.time.LocalDate.now + ".parquet")

   // parquetFile1.registerTempTable("dga")
   // val results = sqlContext.sql("select stepXml from checkindata")
    //results.show()

   }
  def dateFormat(dateInStr: String) = {
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val d = format.parse(dateInStr);
    val sdf = new SimpleDateFormat("EEEEEEEEE")
    val day = sdf.format(d);
    day
  }

  def dateFormatMonth(dateInStr: String) = {
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd")
    //val format = new SimpleDateFormat("ddd,ddyyyyMMMHH:mm:ss")
    val d = format.parse(dateInStr);
    val sdf = new SimpleDateFormat("MMM")
    val month = sdf.format(d);
    month
  }



  def parse(line: String) = {
    System.out.println("write   jggg  onto  line"+line)
    val pieces = line.split(",")
    
    //DateTested,Sampling Date,Mnemonic,Site,TX No.,Sampling Point,Serial Number,Load MVA,Voltage/KV,Maker,Year Built,H2,O2,N2,CH4,CO,CO2
    // if(pieces.length>=4){
    val DateTested = pieces(0);
    val SamplingDate=pieces(1);
    /*val Mnemonic=pieces(2);
    val Site=pieces(3);
    val TXNo=pieces(4);
    val SamplingPoint=pieces(5);
    val SerialNumber=pieces(6);
    val LoadMVA=pieces(7);
    val Voltage_KV=pieces(8);
    val Maker=pieces(9);
    val YearBuilt=pieces(10);
    val H2=pieces(11);
    val O2=pieces(12);
    val N2=pieces(13);
    val CH4=pieces(14);
    val CO=pieces(15);
    val CO2=pieces(16);*/
     
    //Row(DateTested,SamplingDate,Mnemonic,Site,TXNo,SamplingPoint,SerialNumber,LoadMVA,Voltage_KV,Maker,YearBuilt,H2,O2,N2,CH4,CO,CO2)
   Row(DateTested,SamplingDate)
  }
  }
