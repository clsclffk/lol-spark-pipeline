# ingestion/collector.py
# 매치 데이터 수집 메인 로직
# 실행: python -m ingestion.collector

"""
한국의 챌린저 유저 300명의 실제 경기 전적과 챔피언 통계 데이터
- KDA(킬/데스/어시스트), 챔피언 선택, 승패 여부, 게임 시간

# 흐름:
  1. 챌린저 플레이어 300명 목록 수집
  2. 각 플레이어의 매치 ID 수집 (순차, 딜레이 포함)
  3. 각 매치 상세 데이터 수집 (순차, 딜레이 포함)
  4. pandas로 로컬 CSV + Parquet 저장 (용량 비교)
"""

import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
from ingestion.riot_client import RiotClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/raw")

# Riot API 무료 키 한도: 2분에 100개
# 요청 1개당 1.5초 딜레이 -> 2분에 80개 수준으로 안전하게 유지
REQUEST_DELAY = 1.5


def extract_participant_data(match_detail: dict, region: str) -> list[dict]:
    """
    매치 상세 JSON에서 필요한 컬럼만 추출

    추출 컬럼:
    - match_id, game_date : 날짜 파티셔닝 키
    - champion_name       : Data Skew 유발 핵심 컬럼 (인기 챔피언 편향)
    - kills/deaths/assists/win : 집계 실험용
    - game_duration       : Great Expectations 범위 검증용
    - region              : 멀티 리전 수집 시 구분용
    """
    info = match_detail.get("info", {})
    participants = info.get("participants", [])
    metadata = match_detail.get("metadata", {})

    match_id = metadata.get("matchId", "")
    game_duration = info.get("gameDuration", 0)

    game_start_ts = info.get("gameStartTimestamp", 0)
    game_date = (
        datetime.fromtimestamp(game_start_ts / 1000).strftime("%Y-%m-%d")
        if game_start_ts else ""
    )

    rows = []
    for p in participants:
        rows.append({
            "match_id":      match_id,
            "game_date":     game_date,
            "region":        region,
            "champion_name": p.get("championName", ""),
            "position":      p.get("teamPosition", ""),
            "kills":         p.get("kills", 0),
            "deaths":        p.get("deaths", 0),
            "assists":       p.get("assists", 0),
            "win":           p.get("win", False),
            "game_duration": game_duration,
            "tier":          p.get("tier", ""),
        })

    return rows


async def collect_matches(
    region: str = "KR",
    matches_per_player: int = 5
) -> pd.DataFrame:
    """
    전체 수집 파이프라인

    429 방지를 위해 요청마다 REQUEST_DELAY(1.5초) 딜레이 적용
    300명 * 1.5초 = 약 7.5분 소요 예상
    """
    start = time.time()

    async with RiotClient() as client:

        # 1. 챌린저 플레이어 300명 수집
        logger.info(f"[{region}] 챌린저 플레이어 목록 수집 시작")
        players = await client.get_challenger_players(region=region)

        if not players:
            logger.error("플레이어 목록 수집 실패")
            return pd.DataFrame()

        logger.info(f"[{region}] {len(players)}명 수집 완료")

        # 2. 매치 ID 수집 (순차 + 딜레이)
        logger.info(f"[{region}] 매치 ID 수집 시작")
        all_match_ids = []

        for i, p in enumerate(players):
            match_ids = await client.get_match_ids(
                p["puuid"], region=region, count=matches_per_player
            )
            all_match_ids.extend(match_ids)

            # 진행 상황 출력
            if (i + 1) % 30 == 0:
                logger.info(f"매치 ID 수집 진행: {i + 1}/{len(players)}명")

            # 429 방지 딜레이
            await asyncio.sleep(REQUEST_DELAY)

        # 중복 제거
        all_match_ids = list(set(all_match_ids))
        logger.info(f"[{region}] 총 매치 ID {len(all_match_ids)}개 수집 완료")

        # 3. 매치 상세 데이터 수집 (순차 + 딜레이)
        logger.info(f"[{region}] 매치 상세 데이터 수집 시작")
        all_rows = []

        for i, match_id in enumerate(all_match_ids):
            detail = await client.get_match_detail(match_id, region=region)

            if detail:
                rows = extract_participant_data(detail, region)
                all_rows.extend(rows)

            # 진행 상황 출력
            if (i + 1) % 50 == 0:
                logger.info(f"매치 상세 수집 진행: {i + 1}/{len(all_match_ids)}개")

            # 429 방지 딜레이
            await asyncio.sleep(REQUEST_DELAY)

        elapsed = time.time() - start
        logger.info(f"총 수집 시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
        logger.info(f"총 {len(all_rows)}행 추출 완료")

        return pd.DataFrame(all_rows)


def save_csv_and_parquet(df: pd.DataFrame, region: str) -> None:
    """
    CSV + Parquet 두 형식으로 저장하고 용량 비교

    # 날짜별 파티셔닝 경로:
    data/raw/region=KR/date=2024-01-15/matches.csv
    data/raw/region=KR/date=2024-01-15/matches.parquet
    """
    if df.empty:
        logger.warning("저장할 데이터가 없습니다.")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    output_path = OUTPUT_DIR / f"region={region}" / f"date={today}"
    output_path.mkdir(parents=True, exist_ok=True)

    # CSV 저장
    csv_path = output_path / "matches.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    csv_size = csv_path.stat().st_size / (1024 * 1024)

    # Parquet 저장
    parquet_path = output_path / "matches.parquet"
    df.to_parquet(parquet_path, index=False)
    parquet_size = parquet_path.stat().st_size / (1024 * 1024)

    # 용량 비교
    reduction = (1 - parquet_size / csv_size) * 100
    logger.info(f"저장 완료: {output_path}")
    logger.info(f"CSV 크기    : {csv_size:.2f} MB")
    logger.info(f"Parquet 크기: {parquet_size:.2f} MB")
    logger.info(f"용량 절감   : {reduction:.1f}%")


async def main():
    logger.info("데이터 수집 시작")

    df = await collect_matches(
        region="KR",
        matches_per_player=5   # 300명 * 5경기 = 약 1500경기 목표
    )

    if not df.empty:
        save_csv_and_parquet(df, region="KR")

        print("\n수집 결과 요약")
        print("-" * 40)
        print(f"총 행 수: {len(df)}")
        print(f"컬럼: {list(df.columns)}")
        print(f"\n챔피언별 등장 횟수 (상위 10개):")
        print(df["champion_name"].value_counts().head(10))

    logger.info("데이터 수집 완료")


if __name__ == "__main__":
    asyncio.run(main())