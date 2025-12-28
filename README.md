# Apache Spark Engineering-Focused Mastery Plan

## Introduction
이 저장소는 **Apache Spark 고속 학습**을 위한 프로젝트입니다. 
기초 문법보다는 **분산 컴퓨팅의 원리, 아키텍처, 성능 최적화(Internals)**에 초점을 맞추어 진행합니다.

## 🚀 Learning Roadmap (4-Week Course)

### 1주차: Spark 아키텍처와 패러다임 전환 (The Paradigm Shift)
**목표:** 로컬 메모리 관점(Pandas)에서 분산 메모리 관점(Spark)으로 사고 전환

*   **Core Concepts:**
    *   **Distributed Computing:** Driver, Worker, Executor 구조 이해.
    *   **RDD (Resilient Distributed Dataset):** Spark의 기본 데이터 구조 (C++ STL 컨테이너와 비교).
    *   **Lazy Evaluation:** Transformation vs Action, DAG(Directed Acyclic Graph) 생성 이유.
    *   **Py4J:** Python 프로세스와 JVM 간의 통신 비용 및 오버헤드 이해.
*   **Practice:**
    *   로컬 환경 PySpark 설치 및 세션 생성.
    *   MapReduce 개념을 적용한 WordCount 구현.

### 2주차: Structured API와 데이터 처리 (DataFrames & SQL)
**목표:** RDD의 Low-level 조작을 넘어 최적화된 DataFrame API 마스터

*   **Core Concepts:**
    *   **DataFrame & Dataset:** Pandas와의 메모리 관리 차이 (In-memory vs Distributed).
    *   **Catalyst Optimizer:** Logical Plan -> Physical Plan 최적화 과정 (DB Query Planner와 유사).
    *   **Columnar Storage:** Parquet, ORC 포맷의 효율성.
*   **Practice:**
    *   대용량 데이터 로드, 필터링, 집계(GroupBy), 정렬.
    *   **UDF (User Defined Function):** 바닐라 Python UDF의 성능 이슈와 Pandas UDF/Arrow 활용.

### 3주차: 성능 최적화와 트러블슈팅 (Deep Dive for Engineers)
**목표:** "돌아가기만 하는 코드"가 아닌 **"고성능 코드"** 작성

*   **Core Concepts:**
    *   **Shuffle:** 분산 처리 병목의 핵심. `repartition()` vs `coalesce()` 차이.
    *   **Broadcasting:** Network I/O 최적화를 위한 Broadcast Join.
    *   **Caching & Persistence:** 메모리(`MEMORY_ONLY`) vs 디스크(`DISK_ONLY`) 전략.
    *   **Spark UI:** DAG 시각화 분석 및 데이터 쏠림(Skewness) 병목 발견.
*   **Practice:**
    *   Data Skew 상황 연출 및 튜닝 해결.
    *   `explain()`을 통한 물리적 실행 계획 분석.

### 4주차: NLP 및 ML 파이프라인 (Domain Specialization)
**목표:** NLP 도메인 지식을 Spark 환경으로 확장

*   **Core Concepts:**
    *   **Spark MLlib:** Scikit-learn과 비교한 분산 머신러닝.
    *   **Spark NLP:** John Snow Labs 등 현업 라이브러리 활용.
    *   **Feature Engineering:** 분산 환경에서의 TF-IDF, Word2Vec, Tokenizer 구현.
*   **Practice:**
    *   대용량 텍스트(Wiki Dump 등) 기반 키워드 추출 및 감정 분석 파이프라인 구축.

---

## 💡 Expert Tips for Backend Engineers

1.  **Python vs Scala**
    *   5년차 Python 개발자이므로 PySpark로 시작하는 것이 효율적입니다.
    *   단, Spark Core는 Scala(JVM) 기반입니다. 에러 발생 시 **Java Stack Trace**가 익숙해야 하며, 추후 **JVM GC(Garbage Collection) 튜닝** 이슈를 고려해야 합니다.

2.  **The "Pandas" Trap**
    *   `toPandas()`의 남발은 금물입니다. 이는 분산 데이터를 Driver(단일 머신)로 모두 수집(Collect)하는 행위로, **OOM(Memory Overflow)**의 주범입니다.
    *   **"최대한 처리를 분산 노드에서 끝내고, 결과만 늦게 수집한다"**는 원칙을 유지하세요.

3.  **Environment**
    *   초기에는 복잡한 클러스터(AWS EMR 등)보다 Docker나 Local Standalone 모드에서 API와 원리를 익히는 데 집중하는 것을 권장합니다.
