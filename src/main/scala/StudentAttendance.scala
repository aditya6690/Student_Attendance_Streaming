import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object StudentAttendance {

  def main(args: Array[String]): Unit = {

    // ==================================================
    // 1. Spark Configuration
    // ==================================================

    val conf = new SparkConf()
      .setAppName("Student Attendance Streaming")
      .setMaster("local[*]")


    // ==================================================
    // 2. SparkSession
    // Required for DataFrame / Spark SQL
    // ==================================================

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    // ==================================================
    // 3. Streaming Context
    // Batch interval = 5 seconds
    // ==================================================

    val ssc =
      new StreamingContext(spark.sparkContext, Seconds(5))


    // ==================================================
    // 4. Checkpoint
    // Required for updateStateByKey
    // ==================================================

    ssc.checkpoint("data/checkpoint")


    // ==================================================
    // 5. Accumulator
    // Counts invalid attendance records
    // ==================================================

    val invalidRecordCount =
      ssc.sparkContext.longAccumulator(
        "Invalid Attendance Records"
      )


    // ==================================================
    // 6. Broadcast Variable
    // Valid attendance statuses
    // ==================================================

    val validStatuses =
      ssc.sparkContext.broadcast(
        Set(
          "Present",
          "Absent",
          "Late"
        )
      )


    // ==================================================
    // 7. Read attendance files as stream
    // ==================================================

    val attendanceStream =
      ssc.textFileStream("data/stream_input")


    // ==================================================
    // 8. Remove CSV Header
    // ==================================================

    val records =
      attendanceStream
        .filter(line => !line.startsWith("studentId"))


    // ==================================================
    // 9. Validate Attendance Records
    // ==================================================

    val studentStatus =
      records.flatMap { line =>

        val fields =
          line.split(",")

        if (fields.length != 5) {

          invalidRecordCount.add(1)

          None

        } else {

          val studentId =
            fields(0).trim

          val studentName =
            fields(1).trim

          val date =
            fields(2).trim

          val time =
            fields(3).trim

          val status =
            fields(4).trim


          if (
            studentId.isEmpty ||
            studentName.isEmpty ||
            date.isEmpty ||
            time.isEmpty ||
            !validStatuses.value.contains(status)
          ) {

            invalidRecordCount.add(1)

            None

          } else {

            Some(
              (
                studentId,
                studentName,
                date,
                time,
                status
              )
            )
          }
        }
      }


    // ==================================================
    // 10. Persist Valid Attendance Data
    // ==================================================

    val persistedStudentStatus =
      studentStatus.persist(StorageLevel.MEMORY_ONLY)


    // ==================================================
    // 11. Pair DStream
    // Used for stateful processing
    // ==================================================

    val studentStatusPair =
      persistedStudentStatus.map {

        case (
          studentId,
          studentName,
          date,
          time,
          status
        ) =>

          ((studentId, status), 1)
      }


    val studentStatusCount =
      studentStatusPair
        .reduceByKey(_ + _)


    // ==================================================
    // 12. Partition Information
    // ==================================================

    val partitionedData =
      studentStatusCount.transform { rdd =>

        println(
          s"Initial partitions: ${rdd.getNumPartitions}"
        )

        val repartitioned =
          rdd.repartition(4)

        println(
          s"After repartition: ${repartitioned.getNumPartitions}"
        )

        val coalesced =
          repartitioned.coalesce(2)

        println(
          s"After coalesce: ${coalesced.getNumPartitions}"
        )

        coalesced
      }


    // ==================================================
    // 13. Stateful Processing
    // ==================================================

    val updateFunction = (
        newValues: Seq[Int],
        runningCount: Option[Int]
      ) => {

      val newCount =
        newValues.sum

      val previousCount =
        runningCount.getOrElse(0)

      Some(
        previousCount + newCount
      )
    }


    val runningStudentStatusCount =
      studentStatusCount
        .updateStateByKey(updateFunction)


    // ==================================================
    // 14. Detect Repeated Absence / Late
    // ==================================================

    val alerts =
      runningStudentStatusCount
        .filter {

          case ((studentId, status), count) =>

            (status == "Absent" ||
             status == "Late") &&
            count >= 2
        }
        .map {

          case ((studentId, status), count) =>

            s"ALERT: Student $studentId has $count $status records"
        }


    // ==================================================
    // 15. Print Alerts
    // ==================================================

    alerts.print()


    // ==================================================
    // 16. Window Operation
    // ==================================================

    val windowedStudentStatusCount =
      studentStatusCount.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(10)
      )


    // ==================================================
    // 17. Print Window Results
    // ==================================================

    windowedStudentStatusCount.print()


    // ==================================================
    // 18. Spark SQL / DataFrame Processing
    // ==================================================

    persistedStudentStatus.foreachRDD {

      rdd =>

        if (!rdd.isEmpty()) {

          // ------------------------------------------------
          // Convert RDD to DataFrame
          // ------------------------------------------------

          val attendanceDF =
            rdd.toDF(
              "studentId",
              "studentName",
              "date",
              "time",
              "status"
            )


          // ------------------------------------------------
          // Display DataFrame
          // ------------------------------------------------

          println()
          println("========== ATTENDANCE DATAFRAME ==========")

          attendanceDF.show(false)


          // ==================================================
          // 19. Register DataFrame as Temporary SQL Table
          // ==================================================

          attendanceDF.createOrReplaceTempView(
            "attendance"
          )


          // ==================================================
          // 20. UDF
          // Categorize Attendance
          // ==================================================

          val attendanceCategory =
            udf { status: String =>

              status match {

                case "Present" =>
                  "Regular"

                case "Late" =>
                  "Needs Attention"

                case "Absent" =>
                  "Critical"

                case _ =>
                  "Unknown"
              }
            }


          // ------------------------------------------------
          // Register UDF
          // ------------------------------------------------

          spark.udf.register(
            "attendanceCategory",
            attendanceCategory
          )


          // ==================================================
          // 21. Apply UDF
          // ==================================================

          val categorizedDF =
            attendanceDF.withColumn(
              "attendanceCategory",
              attendanceCategory(
                col("status")
              )
            )


          println()
          println("========== UDF RESULT ==========")

          categorizedDF.show(false)


          // ==================================================
          // 22. Aggregation
          // Count Present / Absent / Late
          // Per Student
          // ==================================================

          val studentSummary =
            attendanceDF
              .groupBy(
                "studentId",
                "studentName"
              )
              .agg(

                sum(
                  when(
                    col("status") === "Present",
                    1
                  ).otherwise(0)
                ).alias("presentCount"),

                sum(
                  when(
                    col("status") === "Absent",
                    1
                  ).otherwise(0)
                ).alias("absentCount"),

                sum(
                  when(
                    col("status") === "Late",
                    1
                  ).otherwise(0)
                ).alias("lateCount")
              )


          println()
          println("========== STUDENT SUMMARY ==========")

          studentSummary.show(false)


          // ==================================================
          // 23. Overall Aggregation
          // ==================================================

          val overallSummary =
            attendanceDF
              .groupBy("status")
              .count()
              .orderBy(
                desc("count")
              )


          println()
          println("========== OVERALL ATTENDANCE ==========")

          overallSummary.show(false)


          // ==================================================
          // 24. Reference Data for JOIN
          // ==================================================
          //
          // This represents master/reference information.
          //
          // studentId | department
          //
          // ==================================================

          val referenceData =
            Seq(

              ("S001", "ECE"),
              ("S002", "CSE"),
              ("S003", "ECE"),
              ("S004", "EEE"),
              ("S005", "CSE"),
              ("S006", "ECE"),
              ("S007", "ME"),
              ("S008", "CSE"),
              ("S009", "ECE"),
              ("S010", "EEE"),
              ("S011", "CSE"),
              ("S012", "ECE"),
              ("S013", "CSE"),
              ("S014", "EEE")

            ).toDF(
              "studentId",
              "department"
            )


          // ==================================================
          // 25. Join Attendance with Reference Data
          // ==================================================

          val joinedDF =
            attendanceDF
              .join(
                referenceData,
                Seq("studentId"),
                "left"
              )


          println()
          println("========== JOIN RESULT ==========")

          joinedDF.show(false)


          // ==================================================
          // 26. Department-wise Aggregation
          // ==================================================

          val departmentSummary =
            joinedDF
              .groupBy("department")
              .agg(

                count("*")
                  .alias("totalRecords"),

                sum(
                  when(
                    col("status") === "Present",
                    1
                  ).otherwise(0)
                ).alias("presentCount"),

                sum(
                  when(
                    col("status") === "Absent",
                    1
                  ).otherwise(0)
                ).alias("absentCount"),

                sum(
                  when(
                    col("status") === "Late",
                    1
                  ).otherwise(0)
                ).alias("lateCount")
              )
              .orderBy(
                desc("totalRecords")
              )


          println()
          println("========== DEPARTMENT SUMMARY ==========")

          departmentSummary.show(false)


          // ==================================================
          // 27. Spark SQL Query
          // ==================================================

          val sqlResult =
            spark.sql(
              """
                |SELECT
                |    studentId,
                |    studentName,
                |    status,
                |    COUNT(*) AS total
                |FROM attendance
                |GROUP BY
                |    studentId,
                |    studentName,
                |    status
                |ORDER BY
                |    studentId
                |""".stripMargin
            )


          println()
          println("========== SPARK SQL RESULT ==========")

          sqlResult.show(false)


          // ==================================================
          // 28. Invalid Record Count
          // ==================================================

          println()
          println(
            s"Invalid Records: ${invalidRecordCount.value}"
          )
        }
    }


    // ==================================================
    // 29. Start Streaming
    // ==================================================

    ssc.start()


    // ==================================================
    // 30. Wait for Termination
    // ==================================================

    ssc.awaitTermination()
  }
}
