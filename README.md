# Student Attendance Streaming

A Scala + Apache Spark Streaming mini-project that processes student attendance events in micro-batches and demonstrates core Spark concepts from RDDs and DStreams through Spark SQL.

## Features

- Spark DStreams with 5-second micro-batches
- CSV header filtering and record validation
- Broadcast variable for valid attendance statuses
- Accumulator for invalid-record counting
- Pair DStream and `reduceByKey`
- Stateful processing with `updateStateByKey`
- Repeated absence/late alerts
- Sliding window: 20 seconds, sliding every 10 seconds
- `persist(StorageLevel.MEMORY_ONLY)`
- `repartition(4)` and `coalesce(2)` demonstration
- DataFrame processing
- Spark UDF for attendance classification
- Student-wise aggregation
- Overall status aggregation
- Join with student department reference data
- Department-wise aggregation
- Spark SQL
- Checkpointing for stateful streaming
- Output CSV/text files under `data/output`

## Technology

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Core, SQL and Streaming
- SBT 2.0.7+
- Java 17
- WSL2 / Linux

## Project Structure

```text
student-attendance-streaming/
├── build.sbt
├── data/
│   ├── input/
│   ├── reference/
│   ├── stream_input/
│   ├── checkpoint/
│   └── output/
├── docs/
│   ├── architecture.png
│   └── Student_Attendance_Streaming_Documentation.docx
└── src/
    └── main/
        └── scala/
            ├── Models.scala
            └── StudentAttendance.scala
```

## Input Schema

```text
studentId,studentName,date,time,status
```

Valid statuses:

```text
Present
Absent
Late
```

## How to Run

From the project directory:

```bash
cd ~/student-attendance-streaming
sbt clean compile
sbt run
```

Keep the Spark terminal running.

Open a second terminal:

```bash
cd ~/student-attendance-streaming
```

Add a **new** file while the application is running:

```bash
cat > data/stream_input/real_attendance.csv <<'EOF'
studentId,studentName,date,time,status
S001,Aditya,2026-09-03,09:05,Present
S002,Rahul,2026-09-03,09:12,Late
S003,Priya,2026-09-03,09:00,Present
S004,Aman,2026-09-03,09:35,Late
S005,Neha,2026-09-03,09:10,Absent
S006,Rohit,2026-09-03,09:03,Present
S007,Anjali,2026-09-03,09:25,Late
S008,Vikas,2026-09-03,09:08,Present
S009,Pooja,2026-09-03,09:40,Absent
S010,Karan,2026-09-03,09:02,Present
S001,Aditya,2026-09-03,09:15,Late
S002,Rahul,2026-09-03,09:45,Absent
S005,Neha,2026-09-03,09:50,Absent
S007,Anjali,2026-09-03,09:55,Late
S009,Pooja,2026-09-03,09:58,Absent
EOF
```

Wait for the next micro-batch.

## Important `textFileStream` behavior

The program uses:

```scala
ssc.textFileStream("data/stream_input")
```

Therefore, test by adding a **new file while the streaming application is running**. Do not expect files that were already present before the stream started to behave like a normal batch input.

## Output

The application prints and writes:

```text
data/output/
├── attendance_categorized/
├── student_summary/
├── overall_attendance/
├── joined_attendance/
├── department_summary/
├── sql_result/
├── window_counts/
└── alerts/
```

Spark writes CSV results as directories containing `part-*.csv` files and `_SUCCESS`.

## Spark Concepts

### Narrow transformations
- `filter`
- `map`
- `flatMap`

### Wide transformations
- `reduceByKey`
- `repartition`
- aggregations and joins may also cause shuffle depending on execution plan

### Stateful processing
`updateStateByKey` maintains running counts and requires checkpointing.

### Window processing
20-second window with a 10-second slide.

### Broadcast
Valid status values are broadcast to workers.

### Accumulator
Invalid records are counted with a `LongAccumulator`.

### Persistence
Validated attendance data is persisted because it feeds multiple downstream operations.

## Expected SQL Example

```text
+---------+-----------+-------+-----+
|studentId|studentName|status |total|
+---------+-----------+-------+-----+
|S001     |Aditya     |Present|1    |
|S001     |Aditya     |Late   |1    |
|S002     |Rahul      |Late   |1    |
|S002     |Rahul      |Absent |1    |
...
+---------+-----------+-------+-----+
```

## Expected overall result for the 15-record demonstration batch

```text
Present = 5
Absent  = 5
Late    = 5
Total   = 15
```

## Expected department result

```text
ECE | 6 | 3 | 2 | 1
CSE | 5 | 1 | 3 | 1
ME  | 2 | 0 | 0 | 2
EEE | 2 | 1 | 0 | 1
```

Columns:

```text
department | totalRecords | presentCount | absentCount | lateCount
```

## Alert Examples

Repeated status counts can generate:

```text
ALERT: Student S005 has 2 Absent records
ALERT: Student S007 has 2 Late records
ALERT: Student S009 has 2 Absent records
```

## Execution Model

```text
Input File
   ↓
DStream
   ↓
Validation
   ↓
Pair DStream
   ↓
reduceByKey
   ↓
State / Window
   ↓
DataFrame
   ↓
UDF / Aggregation / Join / SQL
   ↓
Terminal + data/output
```

## Fault Tolerance

Spark uses RDD/DStream lineage to recompute lost partitions. Stateful streaming also uses checkpointing:

```scala
ssc.checkpoint("data/checkpoint")
```

## YARN

The project runs locally using:

```scala
.setMaster("local[*]")
```

For cluster deployment, Spark can run with YARN. YARN deployment is documented conceptually in the project documentation.

## Final Demo Checklist

- [ ] `sbt clean compile` succeeds
- [ ] `sbt run` starts
- [ ] New CSV file is detected
- [ ] Attendance DataFrame appears
- [ ] UDF result appears
- [ ] Student summary appears
- [ ] Overall attendance appears
- [ ] Join result appears
- [ ] Department summary appears
- [ ] Spark SQL result appears
- [ ] Invalid record count appears
- [ ] Alerts are demonstrated
- [ ] Window processing is demonstrated
- [ ] Output directories are created
- [ ] Screenshots are included in documentat
