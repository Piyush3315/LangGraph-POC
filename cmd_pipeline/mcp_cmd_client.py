import asyncio
from typing import Dict, Any
from mcp.client.sse import sse_client
from mcp.client.session import ClientSession
from utils.config import MCP_SERVER_URL
import asyncio
import random
from typing import Any, Dict, Optional
import traceback


# async def call_cmd_validation(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Send a 'cmd_validation' command to the MCP server and return the response.
#     `state` should be of the form: {"row": {...}}
#     """
#     async with sse_client(MCP_SERVER_URL) as (read_stream, write_stream):
#         async with ClientSession(read_stream, write_stream) as session:
#             await session.initialize()
#             response = await session.call_tool("cmd_validation", {"state": state})
#
#             return response
#
#     raise RuntimeError("No response from MCP server for cmd_validation")
#
#
# async def safe_call_cmd_validation(state: Dict[str, Any]) -> Dict[str, Any]:
#     async with semaphore:
#         return await call_cmd_validation(state)


import asyncio
import math
import traceback
from typing import Dict, Any, List
from mcp.client.session import ClientSession


def clean_for_json(data):
    if isinstance(data, dict):
        return {k: clean_for_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [clean_for_json(v) for v in data]
    if isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return data
    return data


async def _validate_rows_over_single_sse(rows: List[Dict[str, Any]], max_retries: int = 5, base_backoff: float = 1) -> List[Dict[str, Any]]:
    """
    Open one SSE connection, create one ClientSession, call the tool sequentially
    for every row in `rows`. Return list of validated row dicts (fallback to original row
    if validation ultimately fails).
    """
    processed = []

    # Open one long-lived connection
    async with sse_client(MCP_SERVER_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            for idx, row in enumerate(rows):
                original_row = row
                clean_state = {"row": clean_for_json(row)}
                attempt = 0
                validated_row = None

                while attempt < max_retries:
                    try:
                        resp = await session.call_tool("cmd_validation", {"state": clean_state})
                        if resp is None:
                            raise RuntimeError("Empty response")

                        if hasattr(resp, "structuredContent") and resp.structuredContent:
                            validated_row = resp.structuredContent.get("row", None)
                        elif isinstance(resp, dict):
                            validated_row = resp.get("row") or resp.get("result") or None
                        else:
                            validated_row = None

                        if isinstance(validated_row, dict) and validated_row:
                            break

                        attempt += 1
                        backoff = base_backoff * (2 ** (attempt - 1))
                        await asyncio.sleep(backoff)

                    except Exception as e:
                        print(f"_validate_rows: exception on idx={idx} attempt={attempt+1}: {e}")
                        traceback.print_exc()
                        attempt += 1
                        backoff = base_backoff * (2 ** (attempt - 1))
                        await asyncio.sleep(backoff)

                if validated_row is None:
                    try:
                        resp = await session.call_tool("cmd_validation", {"state": {"row": original_row}})
                        if isinstance(resp, dict) and resp.get("row"):
                            validated_row = resp.get("row")
                    except Exception:
                        pass

                if validated_row is None:
                    print(f"_validate_rows: warning — idx={idx} validation failed after {max_retries} attempts, using original row")
                    validated_row = original_row

                processed.append(validated_row)

    return processed


# ----------------------
# Synchronous wrapper to validate a batch of rows
# ----------------------
def validate_rows_sync(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Synchronous wrapper that runs the async single-connection validator once.
    """
    return asyncio.run(_validate_rows_over_single_sse(rows))