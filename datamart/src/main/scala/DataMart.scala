import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.functions.col
import db.{DbConnection}


object DataMart {
  private val spark = SparkSession.builder
    .config("spark.app.name", "Clustering")
    .config("spark.driver.cores", 1)
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.master", "local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  private val database = new DbConnection(spark)

  def readAndProccess(): DataFrame = {
//    database.readTable("train_X")
//    val df = spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv("truncated.csv").na.fill(0.0)
    val df = database.readTable("train_X").na.fill(0.0)
    val inputCols: Array[String] = Array(
      "energy-kcal_100g",
      "sugars_100g",
      "energy_100g",
      "fat_100g",
      "saturated-fat_100g",
      "carbohydrates_100g"
    )

    val vec_assembler = new VectorAssembler()
      .setInputCols(inputCols).setOutputCol("features").setHandleInvalid("skip")

    //    val final_data = vec_assembler.transform(df_select)
    val final_data = vec_assembler.transform(df)

    val scaler = new StandardScaler().setInputCol("features")
      .setOutputCol("scaledFeatures").setWithStd(true).setWithMean(false)
    val scalerModel = scaler.fit(final_data)
    val scaled_final_data = scalerModel.transform(final_data)
    scaled_final_data


  }
}