import random
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("NLP_Week3_Optimization")
    .master("local[*]")
    # 셔플 파티션 개수 설정 (기본 값 200은 자긍ㄴ 데이터에 너무 큼)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# 1. 데이터 생성
# Users: 작은 데이터 (100명)
users_data = [(i, f"User_{i}", f"Country_{i%10}") for i in range(100)]
users_df = spark.createDataFrame(users_data, ["user_id", "name", "country"])

# Transactions: 큰 데이터 (10만 건)
# 실제로는 수 억 건이겠죠?
trans_data = [
    (random.randint(0, 99), random.randint(100, 10000)) for _ in range(100000)
]
trans_df = spark.createDataFrame(trans_data, ["user_id", "amount"])

print("--- Data Generation Complete ---")

# 2. 일반적인 Join (Sort-Merge Join)
# 두 DataFrame 모두 user_id 기준으로 정렬 및 셔플링 발생
print("\n--- [Normal Join] Execution Plan ---")
joined_df = trans_df.join(users_df, on="user_id")
joined_df.explain()  # explain()을 통해 'Exchange' 키워드(Shuffle) 확인

# 3. Broadcast Join
# users_df를 브로드캐스팅하여 셔플 제거
print("\n--- [Broadcast Join] Execution Plan ---")
# hint: broadcast() 함수로 감싸주면 Spark에게 힌트를 준다.
# 동작 원리:
# 1. Driver가 작은 데이터(users_df)를 모두 수집(Collect)함.
# 2. 모든 Executor에게 해당 데이터를 복사/전송(Broadcast)함.
# 3. 각 Executor는 큰 데이터(trans_df)의 파티션과 메모리에 있는 작은 데이터를 로컬에서 조인함 (Shuffle 없음).
broadcast_joined_df = trans_df.join(F.broadcast(users_df), on="user_id")
broadcast_joined_df.explain()  # 'BroadcastExchange'와 'BroadcastHashJoin' 확인

# 4. 성능 테스트 (Action 수행)
import time

start_time = time.time()
joined_df.count()  # 일반 조인 실행
print(f"Normal Join Time: {time.time() - start_time:.4f} sec")

start_time = time.time()
broadcast_joined_df.count()  # 브로드캐스팅 조인 실행
print(f"Broadcast Join Time: {time.time() - start_time:.4f} sec")


spark.stop()
