## 1. Worker 로컬 디스크 공유 불가 

### 문제
- Docker Compose로 구성한 로컬 Spark 클러스터에서 parquet 파일 읽기 시도 시 Job 실패 

```
ERROR TaskSetManager: Task 0 in stage 0.0 failed 4 times; aborting job
```

- Spark는 Task 실패 시 기본적으로 4회 재시도하며, 4회 모두 실패하면 Job 전체를 포기 (Task 0 failed 4 times)

### 원인
- Spark의 Driver는 파일 메타데이터 파악, Worker는 실제 데이터 직접 읽기
- Driver(내 PC)와 Worker(Docker 컨테이너)는 서로 다른 머신이라 파일 시스템이 분리되어 있음
  - 호스트 절대 경로 사용 시 -> Driver 접근 가능, Worker 접근 불가 
  - 컨테이너 절대경로 사용 시 -> Driver 접근 불가, Worker 접근 가능 
- 분산 모드(Cluster Mode)에서는 모든 노드가 동일하게 바라볼 수 있는 공유 스토리지가 없으면 구조적으로 데이터 읽기가 불가능
  - `local[*]` 모드였다면 Driver와 Worker가 하나의 JVM 안에서 실행되므로 같은 파일시스템을 공유해서 문제 없음
  - Docker Compose 환경에서는 드라이버(내 PC)와 워커(컨테이너)가 별개의 OS처럼 동작하기 때문에 내 PC의 경로는 워커 입장에서 접근 불가능한 외부 경로임

### 해결
- Driver와 Worker가 동일한 URL로 접근 가능한 GCS로 이전