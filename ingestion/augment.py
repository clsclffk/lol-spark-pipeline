# ingestion/augment.py
# 실행: python -m ingestion.augment

"""

원본 수집 데이터를 기반으로 대용량 데이터 생성

왜 증강이 필요한가?
- 실제 수집 데이터는 수 MB 수준 (API 한도로 대량 수집 불가)
- 실제 데이터의 분포(챔피언 편향, 게임 시간 등)를 유지하며 증강

"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 원본 데이터 경로 (collector.py 결과물)
RAW_DIR = Path("data/raw")

# 증강 데이터 저장 경로
AUGMENTED_DIR = Path("data/augmented")

# 증강 목표 크기 설정 (5GB)
TARGET_GB = 5


def load_raw_data() -> pd.DataFrame:
    """
    원본 수집 데이터 로드
    날짜별 파티션 폴더를 전부 읽어서 합침
    """
    all_files = list(RAW_DIR.rglob("matches.csv"))

    if not all_files:
        raise FileNotFoundError(
            f"원본 데이터가 없습니다. 먼저 collector.py를 실행하세요.\n"
            f"찾은 경로: {RAW_DIR}"
        )

    logger.info(f"원본 파일 {len(all_files)}개 발견")

    dfs = []
    for f in all_files:
        df = pd.read_csv(f)
        dfs.append(df)
        logger.info(f"로드: {f} ({len(df)}행)")

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"원본 데이터 총 {len(combined)}행 로드 완료")
    return combined


def augment_data(df: pd.DataFrame, target_gb: float = TARGET_GB) -> pd.DataFrame:
    """
    원본 데이터를 기반으로 대용량 데이터 생성

    증강 방식:
    - 날짜 변형: game_date를 다양한 날짜로 바꿔서 파티셔닝 실험 가능하게 함
    - 수치 노이즈: kills/deaths/assists에 소폭 변형 (분포는 유지)
    - match_id 변형: 중복 방지를 위해 suffix 붙임

    왜 이 방식?
    - 챔피언 분포(인기 챔피언 편향)는 그대로 유지 -> Skew 실험 유효
    - 게임 시간 분포 유지 -> GE 범위 검증 실험 유효
    - 날짜 다양화 -> 파티셔닝, 멱등성 실험 유효
    """
    # 현재 원본 1행당 메모리 사용량 추정
    row_size_bytes = df.memory_usage(deep=True).sum() / len(df)
    target_bytes = target_gb * 1024 ** 3
    target_rows = int(target_bytes / row_size_bytes)
    repeat_count = max(1, target_rows // len(df))

    logger.info(f"원본 행 수: {len(df)}")
    logger.info(f"목표 크기: {target_gb}GB")
    logger.info(f"필요 반복 횟수: {repeat_count}배")

    dfs = []
    base_date = datetime(2025, 1, 1)  # 증강 날짜 시작점

    for i in range(repeat_count):
        temp = df.copy()

        # 1. 날짜 변형 (파티셔닝 실험용)
        # 날짜가 다양해야 날짜별 파티션 구조가 의미있음
        fake_date = (base_date + timedelta(days=i % 365)).strftime("%Y-%m-%d")
        temp["game_date"] = fake_date

        # 2. match_id 변형 (중복 방지)
        # 같은 match_id가 반복되면 GE 중복 검증이 무의미해짐
        temp["match_id"] = temp["match_id"] + f"_aug_{i:05d}"

        # 3. 수치 컬럼 노이즈 추가 (분포 유지하면서 다양성 확보)
        # clip(0): 음수가 나오지 않도록 0으로 자름
        noise = np.random.randint(-1, 2, len(temp))
        temp["kills"]   = (temp["kills"]   + noise).clip(0)
        temp["deaths"]  = (temp["deaths"]  + noise).clip(0)
        temp["assists"] = (temp["assists"] + noise).clip(0)

        # 4. game_duration 노이즈 (±30초 이내)
        temp["game_duration"] = (
            temp["game_duration"] + np.random.randint(-30, 31, len(temp))
        ).clip(0)

        dfs.append(temp)

        # 진행 상황 출력
        if (i + 1) % 100 == 0:
            logger.info(f"증강 진행: {i + 1}/{repeat_count}회")

    augmented = pd.concat(dfs, ignore_index=True)
    logger.info(f"증강 완료: {len(augmented)}행")
    return augmented


def save_augmented(df: pd.DataFrame) -> None:
    """
    증강 데이터를 CSV + Parquet 두 형식으로 저장하고 용량 비교

    CSV  -> pandas OOM 실험에 사용 (pandas는 CSV 전체를 RAM에 올림)
    Parquet -> Spark 실험에 사용 (columnar 포맷, 압축률 높음)
    """
    AUGMENTED_DIR.mkdir(parents=True, exist_ok=True)

    # CSV 저장 + 시간 측정
    csv_path = AUGMENTED_DIR / "matches_large.csv"
    logger.info("CSV 저장 시작...")
    start = time.time()
    df.to_csv(csv_path, index=False, encoding="utf-8")
    csv_time = time.time() - start
    csv_size = csv_path.stat().st_size / (1024 ** 3)  # GB 단위

    # Parquet 저장 + 시간 측정
    parquet_path = AUGMENTED_DIR / "matches_large.parquet"
    logger.info("Parquet 저장 시작...")
    start = time.time()
    df.to_parquet(parquet_path, index=False)
    parquet_time = time.time() - start
    parquet_size = parquet_path.stat().st_size / (1024 ** 3)  # GB 단위

    # 결과 비교 출력
    size_reduction = (1 - parquet_size / csv_size) * 100
    time_reduction = (1 - parquet_time / csv_time) * 100

    print("\nCSV vs Parquet 비교")
    print("-" * 40)
    print(f"총 행 수       : {len(df):,}행")
    print(f"CSV 크기       : {csv_size:.2f} GB")
    print(f"Parquet 크기   : {parquet_size:.2f} GB")
    print(f"용량 절감      : {size_reduction:.1f}%")
    print(f"CSV 저장 시간  : {csv_time:.1f}초")
    print(f"Parquet 저장 시간: {parquet_time:.1f}초")
    print(f"저장 시간 단축 : {time_reduction:.1f}%")


def main():
    logger.info("데이터 증강 시작")

    # 1. 원본 로드
    df = load_raw_data()

    # 2. 증강
    augmented = augment_data(df, target_gb=TARGET_GB)

    # 3. 저장 + 비교
    save_augmented(augmented)

    logger.info("데이터 증강 완료")


if __name__ == "__main__":
    main()