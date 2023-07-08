import os
from typing import Optional, List

from aiohttp import ClientSession

from consts.env_consts import OPENAI_API_KEY
from consts.google_translator_consts import DATA
from consts.openai_consts import ADA_EMBEDDINGS_MODEL, OPENAI_EMBEDDINGS_URL, EMBEDDING, INPUT, MODEL


class OpenAIClient:
    def __init__(self, session: Optional[ClientSession] = None):
        self._session = session

    async def embeddings(self, text: str, model: str = ADA_EMBEDDINGS_MODEL) -> Optional[List[float]]:
        body = {
            INPUT: text,
            MODEL: model
        }

        async with self._session.post(OPENAI_EMBEDDINGS_URL, json=body) as raw_response:
            if not raw_response.ok:
                return

            response = await raw_response.json()

        return self._serialize_embeddings_response(response)

    @staticmethod
    def _serialize_embeddings_response(response: dict) -> Optional[List[float]]:
        data = response.get(DATA, [])
        if not data:
            return

        first_element = data[0]
        return first_element.get(EMBEDDING)

    async def __aenter__(self) -> 'OpenAIClient':
        api_key = os.environ[OPENAI_API_KEY]
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }
        self._session = await ClientSession(headers=headers).__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.__aexit__(exc_type, exc_val, exc_tb)
