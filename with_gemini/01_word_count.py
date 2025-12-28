from pyspark.sql import SparkSession

# 1. SparkSession 생성
spark = (
    SparkSession.builder.appName("NLP_Week1_RDD")
    .master("local[*]")  # 현재 머신의 모든 CPU 코어를 사용해 분산 환경을 시작
    .getOrCreate()
)


# SparkContext는 RDD를 다루기 위한 진입점
sc = spark.sparkContext

# 2. 데이터 생성 (가상의 NLP 코퍼스)
data = [
    "Spark is fast",
    "Spark is focused on in-memory processing",
    "Python is great for NLP",
    "Spark includes libraries for SQL and Machine Learning",
]

# 3. RDD 생성 (Parallelize)
# 데이터를 여러 파티션으로 쪼개서 분산 메모리에 올린다.
rdd = sc.parallelize(data)

print(f"Type of rdd: {type(rdd)}")


# 4. Transformation (지연 연산 - 아직 실행 안 됨)
# FlatMap: 문장을 단어 단위로 쪼개고 평탄화
# (List[List] -> List)
words_rdd = rdd.flatMap(lambda sent: sent.split(" "))

# Map: 각 단어를 (단어, 1) 형태의 튜플로 변환
pairs_rdd = words_rdd.map(lambda word: (word, 1))

# ReduceByKey: 같은 단어를 가진 Value를 합침 (Shuffling 발생)
# 파이썬의 reduce 함수와 비슷하짐나 분산 환경에서 작동한다.
word_counts_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)

# 5. Action (실제 실행)
# collect(): 분산된 결과를 Driver 메모리로 가져옴
# (주의: 대용량 데이터에서는 절대 금지)
results = word_counts_rdd.collect()

# 결과 출력
print("\n=== Word Count Results ===")
for word, count in results:
    print(f"{word}: {count}")


home_work = """
    @NOTE: 01_과제
        - data 리스트에서 "Spark"라는 단어가 포함된 문장만 필터링 후 필터링된 문장의 개수
        - 힌트: rdd.filter(lambda x: ...)
"""
print(f"\n\n{home_work}")
spark_words_rdd = rdd.filter(lambda sent: "Spark" in sent)
homework_results = spark_words_rdd.collect()
print(f"\nHomework results: {len(homework_results)}")
for idx, sent in enumerate(homework_results):
    print(f"{idx + 1}: {sent}")

# 6. 리소스 해제
spark.stop()
