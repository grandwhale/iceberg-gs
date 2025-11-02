import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object OracleToIcebergMigration {

def main(args: Array[String]): Unit = {

```
// Spark Session 생성 (Hive Metastore 및 Iceberg 설정 포함)
val spark = SparkSession.builder()
  .appName("Oracle to Iceberg V2 Migration")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.spark_catalog.type", "hive")
  .config("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083") // Hive Metastore URI
  .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

// Oracle JDBC 연결 정보 설정
val oracleUrl = "jdbc:oracle:thin:@//hostname:1521/service_name"
val oracleUser = "your_username"
val oraclePassword = "your_password"
val sourceTable = "SCHEMA_NAME.TABLE_NAME"

// JDBC 연결 속성 설정
val connectionProperties = new Properties()
connectionProperties.put("user", oracleUser)
connectionProperties.put("password", oraclePassword)
connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")
connectionProperties.put("fetchsize", "10000") // Oracle 최적화를 위한 fetchsize

// Iceberg 테이블 정보
val icebergDatabase = "iceberg_db"
val icebergTable = "migrated_table"
val icebergFullTableName = s"$icebergDatabase.$icebergTable"

try {
  println(s"=== Oracle 테이블에서 데이터 읽기 시작: $sourceTable ===")
  
  // Oracle 테이블에서 데이터 읽기
  val oracleDF = spark.read
    .jdbc(oracleUrl, sourceTable, connectionProperties)
  
  // 읽어온 데이터 정보 출력
  println(s"읽어온 레코드 수: ${oracleDF.count()}")
  println("스키마 정보:")
  oracleDF.printSchema()
  
  // 샘플 데이터 확인
  println("샘플 데이터 (상위 5개):")
  oracleDF.show(5, truncate = false)
  
  // Iceberg 데이터베이스 생성 (없는 경우)
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $icebergDatabase")
  spark.sql(s"USE $icebergDatabase")
  
  println(s"\n=== Iceberg V2 테이블 생성 및 데이터 쓰기: $icebergFullTableName ===")
  
  // Iceberg V2 테이블로 데이터 쓰기
  oracleDF.writeTo(icebergFullTableName)
    .using("iceberg")
    .tableProperty("format-version", "2") // Iceberg V2 포맷 명시
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace() // 테이블이 이미 존재하면 대체
  
  println(s"✓ Iceberg V2 테이블 생성 완료: $icebergFullTableName")
  
  // 생성된 Iceberg 테이블 확인
  val icebergDF = spark.table(icebergFullTableName)
  println(s"\nIceberg 테이블 레코드 수: ${icebergDF.count()}")
  
  // Iceberg 테이블 메타데이터 확인
  println("\nIceberg 테이블 속성:")
  spark.sql(s"DESCRIBE EXTENDED $icebergFullTableName").show(false)
  
  println("\n=== 마이그레이션 완료 ===")
  
} catch {
  case e: Exception =>
    println(s"에러 발생: ${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
} finally {
  spark.stop()
}
```

}
}
