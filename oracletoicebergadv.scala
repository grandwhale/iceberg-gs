import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties
import scala.util.{Try, Success, Failure}

object OracleToIcebergAdvanced {

def main(args: Array[String]): Unit = {

```
// 설정 파라미터
val config = Config(
  oracleUrl = "jdbc:oracle:thin:@//hostname:1521/service_name",
  oracleUser = "your_username",
  oraclePassword = "your_password",
  sourceSchema = "SCHEMA_NAME",
  sourceTable = "TABLE_NAME",
  icebergDatabase = "iceberg_db",
  icebergTable = "migrated_table",
  partitionColumn = Some("created_date"), // 파티션 컬럼 (옵션)
  numPartitions = 8, // 병렬 처리를 위한 파티션 수
  incrementalMode = false, // 증분 처리 여부
  incrementalColumn = "updated_at" // 증분 처리 컬럼
)

// Spark Session 생성
val spark = createSparkSession()

try {
  val migrator = new IcebergMigrator(spark, config)
  migrator.migrate()
} catch {
  case e: Exception =>
    println(s"❌ 마이그레이션 실패: ${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
} finally {
  spark.stop()
}
```

}

def createSparkSession(): SparkSession = {
SparkSession.builder()
.appName(“Oracle to Iceberg V2 Migration - Advanced”)
.config(“spark.sql.extensions”, “org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions”)
.config(“spark.sql.catalog.spark_catalog”, “org.apache.iceberg.spark.SparkCatalog”)
.config(“spark.sql.catalog.spark_catalog.type”, “hive”)
.config(“spark.sql.catalog.spark_catalog.uri”, “thrift://localhost:9083”)
.config(“spark.sql.warehouse.dir”, “/user/hive/warehouse”)
// Iceberg 최적화 설정
.config(“spark.sql.iceberg.handle-timestamp-without-timezone”, “true”)
.config(“spark.sql.sources.partitionOverwriteMode”, “dynamic”)
// 성능 최적화
.config(“spark.sql.adaptive.enabled”, “true”)
.config(“spark.sql.adaptive.coalescePartitions.enabled”, “true”)
.enableHiveSupport()
.getOrCreate()
}
}

case class Config(
oracleUrl: String,
oracleUser: String,
oraclePassword: String,
sourceSchema: String,
sourceTable: String,
icebergDatabase: String,
icebergTable: String,
partitionColumn: Option[String],
numPartitions: Int,
incrementalMode: Boolean,
incrementalColumn: String
)

class IcebergMigrator(spark: SparkSession, config: Config) {

import spark.implicits._

private val sourceFullTable = s”${config.sourceSchema}.${config.sourceTable}”
private val icebergFullTable = s”${config.icebergDatabase}.${config.icebergTable}”

def migrate(): Unit = {
println(s”=== Oracle to Iceberg V2 마이그레이션 시작 ===”)
println(s”소스: $sourceFullTable”)
println(s”타겟: $icebergFullTable”)

```
// 1. Oracle에서 데이터 읽기
val sourceDF = readFromOracle()

// 2. 데이터 검증
validateData(sourceDF)

// 3. Iceberg 데이터베이스 생성
createIcebergDatabase()

// 4. Iceberg 테이블로 데이터 쓰기
if (config.incrementalMode) {
  writeIncrementalToIceberg(sourceDF)
} else {
  writeFullToIceberg(sourceDF)
}

// 5. 마이그레이션 검증
verifyMigration(sourceDF)

println(s"=== 마이그레이션 완료 ===")
```

}

private def readFromOracle(): DataFrame = {
println(s”\n📥 Oracle 테이블에서 데이터 읽기…”)

```
val connectionProperties = new Properties()
connectionProperties.put("user", config.oracleUser)
connectionProperties.put("password", config.oraclePassword)
connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")
connectionProperties.put("fetchsize", "10000")
connectionProperties.put("sessionInitStatement", 
  "BEGIN execute immediate 'alter session set NLS_DATE_FORMAT=\"YYYY-MM-DD HH24:MI:SS\"'; END;")

// 증분 처리를 위한 쿼리 생성
val query = if (config.incrementalMode) {
  val lastProcessedValue = getLastProcessedValue()
  lastProcessedValue match {
    case Some(value) =>
      s"(SELECT * FROM $sourceFullTable WHERE ${config.incrementalColumn} > TO_TIMESTAMP('$value', 'YYYY-MM-DD HH24:MI:SS')) as incremental_data"
    case None =>
      s"(SELECT * FROM $sourceFullTable) as full_data"
  }
} else {
  s"(SELECT * FROM $sourceFullTable) as full_data"
}

// 병렬 처리를 위한 파티셔닝 (숫자 컬럼 필요)
val df = Try {
  spark.read
    .option("numPartitions", config.numPartitions.toString)
    .jdbc(config.oracleUrl, query, connectionProperties)
} match {
  case Success(df) => df
  case Failure(e) =>
    println(s"⚠️  병렬 읽기 실패, 단일 파티션으로 재시도...")
    spark.read.jdbc(config.oracleUrl, query, connectionProperties)
}

println(s"✓ 읽어온 레코드 수: ${df.count()}")
println(s"✓ 파티션 수: ${df.rdd.getNumPartitions}")

df
```

}

private def validateData(df: DataFrame): Unit = {
println(s”\n🔍 데이터 검증 중…”)

```
// 스키마 출력
println("스키마:")
df.printSchema()

// 기본 통계
val rowCount = df.count()
println(s"총 레코드 수: $rowCount")

if (rowCount == 0) {
  println("⚠️  경고: 읽어온 데이터가 없습니다!")
}

// NULL 값 체크 (중요 컬럼)
val nullCounts = df.columns.map { col =>
  col -> df.filter(df(col).isNull).count()
}.filter(_._2 > 0)

if (nullCounts.nonEmpty) {
  println("\nNULL 값이 있는 컬럼:")
  nullCounts.foreach { case (col, count) =>
    println(s"  - $col: $count개")
  }
}

// 샘플 데이터
println("\n샘플 데이터:")
df.show(5, truncate = false)
```

}

private def createIcebergDatabase(): Unit = {
println(s”\n🗂️  Iceberg 데이터베이스 생성…”)
spark.sql(s”CREATE DATABASE IF NOT EXISTS ${config.icebergDatabase}”)
spark.sql(s”USE ${config.icebergDatabase}”)
println(s”✓ 데이터베이스 준비 완료: ${config.icebergDatabase}”)
}

private def writeFullToIceberg(df: DataFrame): Unit = {
println(s”\n💾 Iceberg V2 테이블로 전체 데이터 쓰기…”)

```
val writer = df.writeTo(icebergFullTable)
  .using("iceberg")
  .tableProperty("format-version", "2")
  .tableProperty("write.format.default", "parquet")
  .tableProperty("write.parquet.compression-codec", "snappy")
  .tableProperty("write.metadata.compression-codec", "gzip")
  // 최적화 설정
  .tableProperty("write.target-file-size-bytes", "536870912") // 512MB
  .tableProperty("write.parquet.page-size-bytes", "1048576") // 1MB

// 파티션 설정
config.partitionColumn match {
  case Some(col) =>
    println(s"✓ 파티션 컬럼 설정: $col")
    writer.partitionedBy(column(col)).createOrReplace()
  case None =>
    writer.createOrReplace()
}

println(s"✓ Iceberg V2 테이블 생성 완료")
```

}

private def writeIncrementalToIceberg(df: DataFrame): Unit = {
println(s”\n💾 Iceberg 테이블로 증분 데이터 추가…”)

```
val tableExists = Try(spark.table(icebergFullTable)).isSuccess

if (tableExists) {
  // 테이블이 존재하면 append
  df.writeTo(icebergFullTable)
    .using("iceberg")
    .append()
  
  println(s"✓ 증분 데이터 추가 완료")
  
  // 마지막 처리 값 업데이트
  updateLastProcessedValue(df)
} else {
  // 테이블이 없으면 전체 쓰기
  println("⚠️  테이블이 존재하지 않아 전체 쓰기 모드로 전환")
  writeFullToIceberg(df)
}
```

}

private def getLastProcessedValue(): Option[String] = {
// 실제 환경에서는 별도의 메타데이터 테이블이나 파일에서 읽어오기
// 여기서는 Iceberg 테이블의 최대값 사용
Try {
spark.table(icebergFullTable)
.selectExpr(s”MAX(${config.incrementalColumn}) as max_value”)
.first()
.getAs[String]("max_value")
}.toOption.flatten
}

private def updateLastProcessedValue(df: DataFrame): Unit = {
val maxValue = df.selectExpr(s”MAX(${config.incrementalColumn}) as max_value”)
.first()
.getAs[String]("max_value")

```
println(s"✓ 마지막 처리 값: $maxValue")
// 실제 환경에서는 메타데이터 테이블에 저장
```

}

private def verifyMigration(sourceDF: DataFrame): Unit = {
println(s”\n✅ 마이그레이션 검증 중…”)

```
val icebergDF = spark.table(icebergFullTable)
val sourceCount = sourceDF.count()
val targetCount = icebergDF.count()

println(s"소스 레코드 수: $sourceCount")
println(s"타겟 레코드 수: $targetCount")

if (config.incrementalMode) {
  println(s"✓ 증분 처리 모드 - 타겟 레코드가 더 많을 수 있습니다")
} else if (sourceCount == targetCount) {
  println(s"✓ 레코드 수 일치")
} else {
  println(s"⚠️  경고: 레코드 수 불일치!")
}

// 테이블 속성 확인
println("\nIceberg 테이블 속성:")
spark.sql(s"SHOW TBLPROPERTIES $icebergFullTable")
  .filter("key like '%format%' or key like '%compression%'")
  .show(false)

// 스냅샷 정보
println("\nIceberg 스냅샷 정보:")
spark.sql(s"SELECT * FROM ${config.icebergDatabase}.${config.icebergTable}.snapshots")
  .show(false)
```

}
}
