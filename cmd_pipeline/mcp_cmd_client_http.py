# # mcp_cmd_client_http.py
# import requests
# import time
# import random
# import math
# import traceback
# from typing import Dict, Any, List, Optional
#
# # Base URL of your MCP server (no suffix). Change if hosted remotely.
# MCP_SERVER_HTTP_URL = "http://127.0.0.1:8050"
#
#
# # ----------------------
# # JSON cleaner (same as before)
# # ----------------------
# def clean_for_json(data):
#     if isinstance(data, dict):
#         return {k: clean_for_json(v) for k, v in data.items()}
#     if isinstance(data, list):
#         return [clean_for_json(v) for v in data]
#     if isinstance(data, float):
#         if math.isnan(data) or math.isinf(data):
#             return None
#         return data
#     return data
#
#
# # ----------------------
# # Synchronous HTTP call helper
# # ----------------------
# def _call_tool_via_http(
#     session: requests.Session,
#     tool_name: str,
#     state: Dict[str, Any],
#     timeout: float = 10.0,
# ) -> Dict[str, Any]:
#     """
#     Try to call the MCP tool via HTTP. Tries a set of possible endpoints and returns parsed JSON.
#     Adjust the `endpoints` list if your server exposes a different path.
#     """
#     endpoints = [
#         "/call_tool",                        # common generic RPC endpoint
#         f"/mcp/call_tool",                   # some deployments put it under /mcp
#         f"/tool/{tool_name}",                # direct tool path (less likely)
#         f"/tools/{tool_name}",               # variant
#         "/rpc",                              # generic RPC endpoint (less likely)
#     ]
#
#     payload_rpc = {"tool": tool_name, "state": state}
#     payload_direct = {"state": state}
#
#     last_exc: Optional[Exception] = None
#
#     for ep in endpoints:
#         url = MCP_SERVER_HTTP_URL.rstrip("/") + ep
#         try:
#             if ep in ("/call_tool", "/mcp/call_tool", "/rpc"):
#                 resp = session.post(url, json=clean_for_json(payload_rpc), timeout=timeout)
#             else:
#                 resp = session.post(url, json=clean_for_json(payload_direct), timeout=timeout)
#
#             resp.raise_for_status()
#
#             try:
#                 return resp.json()
#             except ValueError:
#                 return {"_raw_text": resp.text}
#         except Exception as e:
#             last_exc = e
#             # try next endpoint
#             continue
#
#     raise last_exc or RuntimeError("No endpoint succeeded")
#
#
# # ----------------------
# # Validate rows synchronously (requests-based)
# # ----------------------
# def validate_rows_sync(
#     rows: List[Dict[str, Any]],
#     tool_name: str = "cmd_validation",
#     max_retries: int = 5,
#     base_backoff: float = 1,
#     per_try_timeout: float = 8.0,
# ) -> List[Dict[str, Any]]:
#     """
#     Validate rows by calling the MCP server synchronously via HTTP.
#     Retries each row on transient failure and falls back to original row on final failure.
#     """
#     processed: List[Dict[str, Any]] = []
#     session = requests.Session()
#
#     for idx, row in enumerate(rows):
#         original_row = row
#         clean_state = {"row": clean_for_json(row)}
#         attempt = 0
#         validated_row: Optional[Dict[str, Any]] = None
#
#         while attempt < max_retries:
#             try:
#                 resp_json = _call_tool_via_http(session, tool_name, clean_state, timeout=per_try_timeout)
#
#                 # parse shapes we expect
#                 if isinstance(resp_json, dict):
#                     if "structuredContent" in resp_json and isinstance(resp_json["structuredContent"], dict):
#                         validated_row = resp_json["structuredContent"].get("row") or resp_json["structuredContent"].get("result")
#                     else:
#                         validated_row = resp_json.get("row") or resp_json.get("result") or resp_json.get("data")
#
#                 # success check
#                 if isinstance(validated_row, dict) and validated_row:
#                     break
#
#                 # unexpected response, retry
#                 attempt += 1
#                 backoff = base_backoff * (2 ** (attempt - 1))
#                 sleep_for = backoff + random.uniform(0, base_backoff * 0.1)
#                 print(f"[retry] idx={idx} attempt={attempt} - unexpected response shape, backing off {sleep_for:.2f}s")
#                 time.sleep(sleep_for)
#
#             except Exception as e:
#                 attempt += 1
#                 backoff = base_backoff * (2 ** (attempt - 1))
#                 sleep_for = backoff + random.uniform(0, base_backoff * 0.1)
#                 print(f"[error] idx={idx} attempt={attempt}: {e} - backing off {sleep_for:.2f}s")
#                 traceback.print_exc()
#                 time.sleep(sleep_for)
#
#         if validated_row is None:
#             print(f"[warn] idx={idx} validation failed after {max_retries} attempts, using original row")
#             validated_row = original_row
#
#         processed.append(validated_row)
#
#     session.close()
#     return processed
#
#
# # ----------------------
# # Optional utility: probe common endpoints once to find which returns non-404
# # ----------------------
# def probe_mcp_endpoints(timeout: float = 3.0) -> Dict[str, int]:
#     """
#     Returns dict endpoint -> status_code (or -1 on error).
#     Run this once to see which path your server accepts.
#     """
#     endpoints = [
#         "/", "/openapi.json", "/tools", "/list_tools",
#         "/call_tool", "/mcp/call_tool", "/rpc",
#         "/tool/cmd_validation", "/tools/cmd_validation", "/api/call_tool"
#     ]
#     session = requests.Session()
#     results = {}
#     for ep in endpoints:
#         url = MCP_SERVER_HTTP_URL.rstrip("/") + ep
#         try:
#             r = session.get(url, timeout=timeout)
#             results[ep] = r.status_code
#         except Exception:
#             results[ep] = -1
#     session.close()
#     return results
#
#
# # ----------------------
# # Example usage inside your existing invoke_graph()
# # ----------------------
# # In your main module, keep invoke_graph() but it will call this validate_rows_sync(rows).
# #
# # e.g. (replace the previous validate_rows_sync with this function):
# #
# #    rows = [row.to_dict() for _, row in df.iterrows()]
# #    validated_rows = validate_rows_sync(rows)
# #
# # If the server uses a different endpoint, run:
# #    print(probe_mcp_endpoints())
# # then add the working endpoint to the `endpoints` list in _call_tool_via_http() above.

import asyncio
import math
import random
import time
import traceback
from typing import Any, Dict, List, Optional

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# MCP HTTP endpoint (make sure server runs with `transport="streamable-http"`)
MCP_SERVER_HTTP_URL = "http://127.0.0.1:8050/mcp"


# -------------------------------
# Utility: sanitize JSON payloads
# -------------------------------
def clean_for_json(data):
    """Ensure all NaN/Inf values are replaced with None for JSON serialization."""
    if isinstance(data, dict):
        return {k: clean_for_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [clean_for_json(v) for v in data]
    if isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return data
    return data


# ---------------------------------------------------------
# Core: async validator over HTTP transport (streamable-http)
# ---------------------------------------------------------
async def _validate_rows_over_streamable_http(
    rows: List[Dict[str, Any]],
    max_retries: int = 5,
    base_backoff: float = 1.5,
    per_call_timeout: float = 15.0,
    between_rows_delay: float = 2,
) -> List[Dict[str, Any]]:
    """
    Sequentially validates rows using MCP HTTP client (streamable-http transport).

    Opens one persistent connection, then calls the 'cmd_validation' tool
    for each row, applying exponential backoff retries on error or invalid response.
    """
    processed: List[Dict[str, Any]] = []

    print(f"[init] Connecting to MCP server at {MCP_SERVER_HTTP_URL}")

    async with streamablehttp_client(MCP_SERVER_HTTP_URL) as client_ctx:
        if isinstance(client_ctx, (tuple, list)) and len(client_ctx) >= 2:
            read_stream, write_stream = client_ctx[0], client_ctx[1]
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                print("[init] Connected (tuple mode, ClientSession created)")
                await _process_rows(session, rows, processed, max_retries, base_backoff, per_call_timeout, between_rows_delay,)
        elif hasattr(client_ctx, "call_tool"):
            session = client_ctx
            if hasattr(session, "initialize"):
                await session.initialize()
            print("[init] Connected (session mode, direct client context)")
            await _process_rows(session, rows, processed, max_retries, base_backoff, per_call_timeout, between_rows_delay,)
        else:
            raise RuntimeError(
                f"streamablehttp_client returned unsupported context: {type(client_ctx)}"
            )

    print(f"[done] Validation complete. Total rows processed: {len(processed)}")
    return processed


# ---------------------------------------------------------
# Helper: per-row logic with retries and logging
# ---------------------------------------------------------
async def _process_rows(
    session,
    rows: List[Dict[str, Any]],
    processed: List[Dict[str, Any]],
    max_retries: int,
    base_backoff: float,
    per_call_timeout: float,
    between_rows_delay: float,
):
    """Validate each row sequentially with retries and detailed logs."""
    for idx, row in enumerate(rows):
        original_row = row
        clean_state = {"row": clean_for_json(row)}
        attempt = 0
        validated_row: Optional[Dict[str, Any]] = None

        while attempt < max_retries:
            try:
                attempt += 1
                print(f"[call] idx={idx} attempt={attempt} → sending to MCP server...")

                coro = session.call_tool("cmd_validation", {"state": clean_state})
                resp = await asyncio.wait_for(coro, timeout=per_call_timeout)

                validated_row = None
                if hasattr(resp, "structuredContent") and resp.structuredContent:
                    validated_row = resp.structuredContent.get("row")
                elif isinstance(resp, dict):
                    validated_row = (resp.get("row") or resp.get("result") or (resp.get("structuredContent") or {}).get("row"))

                if isinstance(validated_row, dict) and validated_row:
                    print(f"[success] idx={idx} attempt={attempt} validated successfully")
                    break

                backoff = base_backoff * (2 ** (attempt - 1))
                backoff += random.uniform(0, base_backoff * 0.1)
                print(
                    f"[retry] idx={idx} attempt={attempt} unexpected response shape — retrying in {backoff:.2f}s"
                )
                await asyncio.sleep(backoff)

            except asyncio.TimeoutError:
                backoff = base_backoff * (2 ** (attempt - 1))
                backoff += random.uniform(0, base_backoff * 0.1)
                print(
                    f"[timeout] idx={idx} attempt={attempt} timeout after {per_call_timeout}s — retrying in {backoff:.2f}s"
                )
                traceback.print_exc()
                await asyncio.sleep(backoff)

            except Exception as e:
                backoff = base_backoff * (2 ** (attempt - 1))
                backoff += random.uniform(0, base_backoff * 0.1)
                print(f"[error] idx={idx} attempt={attempt} {e} — retrying in {backoff:.2f}s")
                traceback.print_exc()
                await asyncio.sleep(backoff)

        if validated_row is None:
            print(f"[warn] idx={idx} ❗ validation failed after {max_retries} attempts — using original row")
            validated_row = original_row

        processed.append(validated_row)

        # Give server a small rest between calls
        if between_rows_delay:
            await asyncio.sleep(between_rows_delay)


# ---------------------------------------------------------
# Sync wrapper for your LangGraph node (invoke_graph)
# ---------------------------------------------------------
def validate_rows_sync(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Calls the async validator and returns the validated rows.
    """
    start = time.time()
    print(f"[start] Beginning validation of {len(rows)} rows via MCP HTTP client")
    result = asyncio.run(_validate_rows_over_streamable_http(rows))
    print(f"[end] Finished validating {len(rows)} rows in {time.time() - start:.2f}s")
    return result
