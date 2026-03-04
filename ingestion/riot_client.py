# ingestion/riot_client.py

import asyncio
import logging
import os
from typing import Optional
import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class RiotClient:
    """
    Riot API를 비동기(asyncio)로 호출하는 클라이언트

    - 비동기: 요청 10개 동시에 보내고 응답 오는 순서대로 처리
    - 결과: 수집 시간이 훨씬 짧아짐 
    """

    # Riot API 리전 목록
    # regional: 매치 데이터 조회용 (큰 단위)
    # platform: 소환사/리그 조회용 (서버 단위)
    REGIONS = {
        "KR":  {"regional": "asia",     "platform": "kr"},
        "EUW": {"regional": "europe",   "platform": "euw1"},
        "NA":  {"regional": "americas", "platform": "na1"},
    }

    def __init__(self):
        self.api_key = os.getenv("RIOT_API_KEY")
        if not self.api_key:
            raise ValueError(".env 파일에 RIOT_API_KEY가 없습니다.")

        # httpx.AsyncClient: 비동기 HTTP 요청 객체
        # headers에 API 키를 넣어야 Riot이 인증해줌
        self.session: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """
        'async with RiotClient() as client:' 형태로 쓸 때 자동 호출
        세션을 열어주는 역할.
        """
        self.session = httpx.AsyncClient(
            headers={"X-Riot-Token": self.api_key},
            timeout=10.0
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        'async with' 블록이 끝날 때 자동 호출
        세션을 닫아주는 역할 (메모리 누수 방지)
        """
        if self.session:
            await self.session.aclose()

    async def _make_request(self, url: str) -> Optional[dict]:
        """
        실제 API 요청을 보내는 내부 메서드
        에러 처리 + 재시도 로직 포함

        참고: 아까 저장소의 _make_request 구조 참고
        rate_limiter는 일단 제거, 429 에러 자주 나면 그때 추가
        """
        try:
            response = await self.session.get(url)

            # 401: API 키가 잘못됨
            if response.status_code == 401:
                logger.error("API 키 인증 실패 (401). .env 파일 확인 필요.")
                return None

            # 404: 해당 데이터 없음 (정상 케이스)
            if response.status_code == 404:
                logger.debug(f"데이터 없음 (404): {url}")
                return None

            # 429: API 호출 한도 초과 -> 잠깐 기다렸다가 재시도
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "5"))
                logger.warning(f"API 한도 초과. {retry_after}초 대기 후 재시도.")
                await asyncio.sleep(retry_after)
                return await self._make_request(url)

            if response.status_code >= 400:
                logger.error(f"API 에러 {response.status_code}: {url}")
                return None

            return response.json()

        except httpx.TimeoutException:
            logger.error(f"요청 시간 초과: {url}")
            return None
        except Exception as e:
            logger.error(f"예상치 못한 에러: {e}")
            return None

    # League-V4 

    async def get_challenger_players(self, region: str = "KR") -> list[dict]:
        """
        챌린저 플레이어 목록 가져오기

        반환: [{"summonerId": "...", "leaguePoints": 1000, ...}, ...]
        """
        platform = self.REGIONS[region]["platform"]
        url = (
            f"https://{platform}.api.riotgames.com"
            f"/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
        )

        logger.info(f"챌린저 플레이어 목록 요청 중... (리전: {region})")
        data = await self._make_request(url)

        if not data:
            return []

        players = data.get("entries", [])
        logger.info(f"챌린저 플레이어 {len(players)}명 수집 완료")
        return players

    # Match-V5

    async def get_match_ids(self, puuid: str, region: str = "KR", count: int = 20) -> list[str]:
        """
        플레이어의 최근 매치 ID 목록 가져오기

        count: 한 번에 최대 100개까지 가능
        테스트는 20개로 시작 (API 할당량 절약)
        """
        regional = self.REGIONS[region]["regional"]
        url = (
            f"https://{regional}.api.riotgames.com"
            f"/lol/match/v5/matches/by-puuid/{puuid}/ids"
            f"?queue=420&count={count}"  # queue=420: 솔로랭크
        )

        data = await self._make_request(url)
        return data if data else []

    async def get_match_detail(self, match_id: str, region: str = "KR") -> Optional[dict]:
        """
        매치 ID로 상세 데이터 가져오기

        핵심 데이터
        참가자 10명의 챔피언, KDA, 아이템 등 100개 이상의 필드 포함
        """
        regional = self.REGIONS[region]["regional"]
        url = (
            f"https://{regional}.api.riotgames.com"
            f"/lol/match/v5/matches/{match_id}"
        )

        return await self._make_request(url)