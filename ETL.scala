package conekta

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import com.mongodb.spark._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import com.mongodb.spark.config._
import org.apache.commons.lang.exception.ExceptionUtils
import com.typesafe.scalalogging.LazyLogging

object ETL extends LazyLogging with App {

  val data = extracData()  //extraccion y limpieza de datos

  val final_data = transformData(data)  //transformacion de datos y aplicación de reglas de negocio

  writeData(final_data)  //persistencia de datos limpios y transformados en postgres

  def extracData(): DataFrame = {

    try{
      val spark = SparkSession.builder()
        .appName("conekta").master("local[*]")
        .config("spark.mongodb.input.uri", "mongodb://localhost/test.testc")
        .config("spark.mongodb.output.uri", "mongodb://localhost/test.testc")
        .config("spark.mongodb.input.partitioner" ,"MongoPaginateByCountPartitioner")
        .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "1")
        .config("spark.mongodb.input.sampleSize", 10000)
        .getOrCreate()

      val sc = spark.sparkContext

      val readConfig = ReadConfig( Map( "collection" -> "testc", "readPreference.name" -> "secondaryPreferred" ), Some(ReadConfig(sc)) )

      val loadData = MongoSpark.load(sc, readConfig)

      val extrated_data = loadData.toDF.drop("_id").filter( col("id").isNotNull ) //se elimina el campo "_id" generado automaticamente por mongoDB y se eliminan todos aquellos "id" que sean nulos, dado que el esquema en postgres esta definido como NOT NULL

      extrated_data.coalesce(1).write.mode("overwrite").parquet("/home/rzarate/Documentos/Conekta")  //persistencia en Local en formato parquet(es un formato comprimido), dado que la muestra es de 10000 registros y 7 columnas, esto es posible

      extrated_data
    }catch {
      case e: Exception =>
        this.logger.error(s"extracData: ${ExceptionUtils.getStackTrace(e)}")
        throw e
    }

  }

  def transformData(data: DataFrame): (DataFrame, DataFrame) = {

    try{
      val data_transformed = data.select( col("id").substr(1,24).alias("id")
        , when( col("name").startsWith("MiP") || col("name").isNull || col("company_id") === "cbf1c8b09cd5b549416d49d2" , lit("MiPasajefy") )
          .otherwise(col("name")).alias("company_name")
        , when( col("company_id") === "*******" || col("company_id").isNull && col("name") === "MiPasajefy", lit("cbf1c8b09cd5b549416d49d2") )
          .otherwise(col("company_id")).substr(1,24).alias("company_id")
        , when(col("amount").contains("e"), setAmount( col("amount") ) )
          .otherwise(col("amount")).alias("amount").cast("Decimal(16,2)")
        , col("status"), col("created_at").cast("timestamp"), col("paid_at").alias("update_at").cast("timestamp") )

      val charges = data_transformed.select(col("id"), col("company_id"), col("amount"), col("status"), col("created_at"), col("update_at"))

      val companies = data_transformed.select(col("company_id"), col("company_name"))

      (companies, charges )
    }catch {
      case e: Exception =>
        this.logger.error(s"transformData: ${ExceptionUtils.getStackTrace(e)}")
        throw e
    }


  }

  def setAmount(amount: Column): Column = {

    try{
      val leftnum = split(amount, "e").getItem(0)
      val rightnum = split(amount, "e").getItem(1).substr(1, 2)

      val final_amount = leftnum*pow(10 , rightnum).cast("Long").substr(1,14)

      when(length(final_amount) > 14, lit("99999999999999.99") ).otherwise(final_amount)
    }catch {
      case e: Exception =>
        this.logger.error(s"setAmount: ${ExceptionUtils.getStackTrace(e)}")
        throw e
    }

  }

  def writeData(final_data: (DataFrame, DataFrame)): Unit = {

    try{
      final_data._1.write		//persistencia en postgres de la tabla companies
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/coneckta")
        .option("dbtable", "schema.companies")
        .option("user", "postgres")
        .option("password", "rambo")
        .save()

      final_data._2.write		//persistencia en postgres de la tabla charges
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/coneckta")
        .option("dbtable", "schema.charges")
        .option("user", "postgres")
        .option("password", "rambo")
        .save()
    }catch {
      case e: Exception =>
        this.logger.error(s"writeData: ${ExceptionUtils.getStackTrace(e)}")
        throw e
    }

  }

}

