from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, desc, length

# 1. SparkSession 생성
spark = (
    SparkSession.builder.appName("NLP_Week3_DataFrame").master("local[*]").getOrCreate()
)

# 2. 구조화된 데이터 생성
# 실제 현업에서는 JSON, Parquet, CSV 파일을 로드한다.
data = [
    ("UserA", "Spark is awesome", "INFO", 100),
    ("UserB", "Error in processing", "ERROR", 500),
    ("UserA", "Spark SQL is fast", "INFO", 120),
    ("UserC", "Memory overflow", "FATAL", 1000),
    ("UserB", "Timeout occurred", "ERROR", 600),
]

# 컬럼명 정의 (Schema)
columns = ["user_id", "message", "level", "response_time_ms"]

# DataFrame 생성
df = spark.createDataFrame(data, columns)

# 3. 데이터 확인 (Action)
# show(): 상위 20개 해을 예브게 출력
df.show(truncate=False)

# 4. DataFrame DSL(Domain Specific Language) 사용
# 마치 ORM(SQLAlchemy, Django ORM)을 쓰는 느낌

# 시나리오: ERROR 레벨인 로그만 필터링하여 사용자별로 카운트
error_analysis = df.filter(col("level") == "ERROR").groupBy("user_id").count()

print("--- ERROR Analysis (DSL) ---")
error_analysis.show()

# 5. SQL 사용하기
# DataFrame을 SQL에서 사용하려면 TempView로 등록해야 함
df.createOrReplaceTempView("logs")

sql_query = """
    SELECT user_id, avg(response_time_ms) as avg_time
    FROM logs
    WHERE response_time_ms > 200
    GROUP BY user_id
    ORDER BY avg_time DESC
"""
sql_result = spark.sql(sql_query)

print("\n--- SQL Analysis ---")
sql_result.show()


# 6. 실행 계획 (Execution Plan) 확인
# explain(True)를 사용하면 Parsed -> Analyzed -> Optimized -> Physical Plan 단계를 모두 볼 수 있음.
#
# - Parsed Logical Plan: SQL/DSL을 해석한 초기 논리적 계획 (문법 검사)
# - Analyzed Logical Plan: 카탈로그와 대조하여 컬럼/테이블 존재 여부 확인 (의미 검사)
# - Optimized Logical Plan: Catalyst Optimizer가 최적화 규칙을 적용 (필터 푸시다운 등)
# - Physical Plan: 실제 실행될 물리적 연산 (HashAggregate, Exchange 등). 튜닝 시 가장 중요함.
print("\n--- Execution Plan ---")
error_analysis.explain(True)


homework_docs = """
    @NOTE: 02_과제
        - data에 문장 길이를 계산하는 새로운 컬럼 `msg_length`를 추가
        - `level` 별로 메시지 길이의 평균을 구하고, 평균의 길이가 긴 순서대로 출력
        - 이 방식을 DSL과 SQL로 구현해보세요.

        * 힌트: withColumn과 length함수 사용
"""

# @NOTE: DSL로 구현
print("\nDSL로 구현하기")

# 1. data에 문장 길이를 계산하는 새로운 컬럼 `msg_length`를 추가
dsl_df = df.withColumn("msg_length", length(col("message")))
dsl_df.show()

# 2. `level` 별로 메시지 평균 길이 구하기
sort_dsl_df = (
    dsl_df.groupBy("level")
    .agg(F.avg("msg_length").alias("avg_msg_length"))
    .orderBy(desc("avg_msg_length"))
)
sort_dsl_df.show()

print("\nSQL로 구현하기")

# @NOTE: SQL로 구현
homework_sql_result_1 = spark.sql(
    """
    SELECT 
        *,
        LENGTH(message) as msg_length
    FROM logs
"""
)

homework_sql_result_1.show()

# 2. 레벨별 평균 길이
homework_sql_result_1.createTempView("msg_length_logs")

homework_sql_result_2 = spark.sql(
    """
    SELECT 
        level,
        AVG(msg_length) as avg_msg_length
    FROM msg_length_logs
    GROUP BY level
    ORDER BY avg_msg_length DESC
"""
)
homework_sql_result_2.show()


spark.stop()
