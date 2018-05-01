package timeusage

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    //val finalDf = timeUsageGrouped(summaryDf)
    val finalDf = timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf))
    finalDf.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {
    val headHeader = StructField(columnNames.head, StringType, nullable = false)
    val tailHeaders = columnNames.tail.map {
      case (column) => StructField(column, DoubleType, nullable = false)
    }
    StructType(headHeader :: tailHeaders)
  }


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
    val r = line.tail.map{
      case (c) => c.toDouble // converting all the columnar values to Double except the first element of a row (which stays as a String)
    }
    Row.fromSeq(line.head.replace("\"","") :: Row.fromSeq(r).toSeq.toList) // create a row from a Sequance of values
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val columnPrimaryNeeds = Array("t01", "t03", "t11", "t1801", "t1803")
    val columnWorkingActivities = Array("t05", "t1805")
    val columnOtherActivities = Array("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")

    val listColumnPrimaryNeeds = columnNames.filter(x => columnPrimaryNeeds.map(x.startsWith(_)).reduce(_ || _)) // OR reduce operation
    val listColumnWorkingActivities = columnNames.filter(x => columnWorkingActivities.map(x.startsWith(_)).reduce(_ || _))
    //val listColumnOtherActivitiess = columnNames.filter(x => columnOtherActivities.map(x.startsWith(_)).reduce(_ || _))

    // to optimize by excluding the previously created lists
    val newList = columnNames diff listColumnWorkingActivities diff listColumnPrimaryNeeds
    // then doing the filter operation
    val listColumnOtherActivities = newList.filter(x => columnOtherActivities.map(x.startsWith(_)).reduce(_ || _))

    // to transfrom the elements to Column object, from String list to Column list
    (listColumnPrimaryNeeds.map(new Column(_)), listColumnWorkingActivities.map(new Column(_)), listColumnOtherActivities.map(new Column(_)))
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method
    val workingStatusProjection: Column = when((col("telfs") < 3) && (col("telfs") >= 1), lit("working"))
      .otherwise(lit("not working")).alias("working")
    val sexProjection: Column = when((col("tesex") === 1), lit("male"))
      .otherwise(lit("female")).alias("sex")
    val ageProjection: Column = when((col("teage") <= 22) && (col("teage") >= 15), lit("young"))
      .when(col("teage") <= 55 && col("teage") >= 23, lit("active"))
      .otherwise(lit("elder")).alias("age")

    // Create columns that sum columns of the initial dataset
    // Hint: you want to create a complex column expression that sums other columns
    //       by using the `+` operator between them
    // Hint: don’t forget to convert the value to hours
    val primaryNeedsProjection: Column = primaryNeedsColumns.reduce(_ + _) / 60 alias ("primaryNeeds")
    val workProjection: Column = workColumns.reduce(_ + _) / 60 alias ("work")
    val otherProjection: Column = otherColumns.reduce(_ + _) / 60 alias "other"

    // the filters are created and stored in the variables, with select it creates a new dataframe
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" =!= 5 ).persist()  // Discard people who are not in labor force
  }

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    summed.groupBy(col("working"), col("sex"), col("age"))
      .agg(round(avg(col("primaryNeeds")), 1).alias("primaryNeeds"), round(avg(col("work")), 1).alias("work"), round(avg(col("other")), 1).alias("other"))
      .orderBy(col("working"), col("sex"), col("age"))
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed" // s before " is to indicate that it is a String
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    "select working, sex, age, round(avg(primaryNeeds),1) primaryNeeds, round(avg(work),1) work, round(avg(other),1) other from "
      .concat(viewName) // viewName is the name of the table
      .concat(" group by working, sex, age order by working, sex, age")

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */

  // the strings in () are the column names
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.map(row =>
      TimeUsageRow(row.getAs[String]("working")
        , row.getAs[String]("sex")
        , row.getAs[String]("age")
        , row.getAs[Double]("primaryNeeds")
        , row.getAs[Double]("work")
        , row.getAs[Double]("other")))

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed._
    def round1(d: Double) = (d * 10).round / 10d

    summed.groupByKey(timeUsageRow => (timeUsageRow.working, timeUsageRow.sex, timeUsageRow.age)) // to precise the Key thats why we used parameters in groupByKey
      .agg(avg(_.primaryNeeds), avg(_.work), avg(_.other))
      .map(x => TimeUsageRow(x._1._1, x._1._2, x._1._3, round1(x._2), round1(x._3), round1(x._4)))
      .orderBy('working, 'sex, 'age)
  }
}


/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */

// This class is to construct the TimeUsageRow object type and we use it when constructing the dataset in line 230
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)