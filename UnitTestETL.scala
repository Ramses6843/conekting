package conekta

import org.scalatest.FlatSpec
import conekta.ETL._
import org.apache.spark.sql.DataFrame

class UnitTestETL extends FlatSpec{
  var nullDataFrame: DataFrame = _

  "The extracData method" should "return a DataFrame" in {
    val extractedData = extracData()

    assert(extractedData.isInstanceOf[DataFrame] && extractedData.columns.size == 7
    && extractedData.count() == 9997)
  }

  "The transformData method" should "return two DataFrames" in {
    val transformedData = transformData(extracData().limit(100))

    assert(transformedData._1.isInstanceOf[DataFrame] && transformedData._1.columns.size == 2
      && transformedData._1.count() == 100)

    assert(transformedData._2.isInstanceOf[DataFrame] && transformedData._2.columns.size == 6
      && transformedData._1.count() == 100)
  }
  it should "throw an exception when transformData is null" in {
    assertThrows[Exception] {
      transformData(nullDataFrame)
    }
  }

  "The writeData method" should "write data in postgres" in {
    val transformedData = transformData(extracData().limit(100))
    assert(writeData(transformedData).isInstanceOf[Unit])
  }
  it should "throw an exception when writeData receives null dataframes" in {
    assertThrows[Exception] {
      writeData(nullDataFrame, nullDataFrame)
    }
  }



}


