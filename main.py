import asyncio
from typing import TypedDict, Optional, Dict, Any
import traceback
import pandas as pd
from langgraph.graph import StateGraph, START, END
# from cmd_pipeline.mcp_cmd_client import validate_rows_sync
from cmd_pipeline.mcp_cmd_client_http import validate_rows_sync
from cmd_pipeline import mcp_cmd_client
import asyncio
import traceback
from typing import Dict, Any, List, Optional
from utils.config import MCP_SERVER_URL
from mcp.client.sse import sse_client
from mcp.client.session import ClientSession
import random

SLEEP_BETWEEN_CALLS = 1
JITTER_MAX = 0.15


class CsvState(TypedDict, total=False):
    source_df: pd.DataFrame
    df_link: Optional[pd.DataFrame]
    df_unmatched: Optional[pd.DataFrame]
    df_link_created: Optional[pd.DataFrame]
    df_unmatched_validated: Optional[pd.DataFrame]
    df_validated: Optional[pd.DataFrame]
    df_created: Optional[pd.DataFrame]


# ------------------------
# --- Node implementations
# ------------------------


def data_profiling(state: CsvState) -> Dict[str, Any]:
    df = state.get("source_df")
    if df is None:
        print("data_profiling: source_df is None")
        return {}
    print(f"Data profiling: {len(df)} rows, {len(df.columns)} columns")
    return {}


def src_dedup(state: CsvState) -> Dict[str, Any]:
    df = state.get("source_df")
    if df is None:
        print("src_dedup: source_df is None")
        return {}
    print(f"SRC dedup records: {len(df)} rows, {len(df.columns)} columns")
    return {}


def cmd_dedup(state: CsvState) -> Dict[str, Any]:
    df = state.get("source_df")
    if df is None or df.empty:
        cols = df.columns if df is not None else []
        empty_link = pd.DataFrame(columns=cols)
        empty_unmatched = pd.DataFrame(columns=cols)
        print("cmd_dedup: source_df empty/None — nothing to split.")
        # write the two new keys only
        return {"df_link": empty_link, "df_unmatched": empty_unmatched}

    matched = df.iloc[::2].copy()
    unmatched = df.iloc[1::2].copy()

    print(f"CMD dedup: {len(matched)} matched -> link, {len(unmatched)} unmatched -> validation")

    return {"df_link": matched, "df_unmatched": unmatched}


def link_records(state: CsvState) -> Dict[str, Any]:
    df_link = state.get("df_link")
    if df_link is None or df_link.empty:
        print("link_records: No linked records to process.")
        return {}

    df_link_created = df_link.copy()
    print(f"link_records: linked records: {len(df_link_created)} rows, {len(df_link_created.columns)} columns")

    return {"df_link_created": df_link_created}


def address_validation(state: CsvState) -> Dict[str, Any]:
    df_unmatched = state.get("df_unmatched")
    if df_unmatched is None or df_unmatched.empty:
        cols = state["source_df"].columns if state.get("source_df") is not None else []
        empty_validated = pd.DataFrame(columns=cols)
        print("address_validation: No unmatched records to validate.")
        return {"df_unmatched_validated": empty_validated}

    df_validated = df_unmatched.copy()
    print(f"address_validation: validated {len(df_validated)} rows")

    return {"df_unmatched_validated": df_validated}


# async def invoke_graph(state: CsvState) -> Dict[str, Any]:
#     """
#     Read from df_unmatched_validated, call MCP validation for each row,
#     and write results to df_validated (new, separate key).
#     """
#     df_input = state.get("df_unmatched_validated")
#     if df_input is None or df_input.empty:
#         cols = state["source_df"].columns if state.get("source_df") is not None else []
#         empty_validated = pd.DataFrame(columns=cols)
#         print("invoke_graph: no rows to validate; will write empty df_validated")
#         return {"df_validated": empty_validated}
#
#     df = df_input.copy()
#     processed_rows = []
#     for idx, row in df.iterrows():
#         row_dict = row.to_dict()
#         mna_state = {"row": row_dict}
#         try:
#             result_state = await call_cmd_validation(mna_state)
#             if hasattr(result_state, "structuredContent") and result_state.structuredContent:
#                 validated_row = result_state.structuredContent.get("row", row_dict)
#             elif isinstance(result_state, dict) and result_state.get("row"):
#                 validated_row = result_state.get("row")
#             else:
#                 print(f"invoke_graph: warning - structuredContent missing for row idx={idx}, using fallback")
#                 validated_row = row_dict
#         except Exception as e:
#             print(f"invoke_graph: error validating row idx={idx} — keeping original row. Error: {e}")
#             traceback.print_exc()
#             validated_row = row_dict
#
#         processed_rows.append(validated_row)
#
#     final_df = pd.DataFrame(processed_rows)
#     print(f"invoke_graph: MCP validated {len(final_df)} rows and will write to df_validated")
#     return {"df_validated": final_df}


def invoke_graph(state: Dict[str, Any]) -> Dict[str, Any]:
    df_input = state.get("df_unmatched_validated")
    if df_input is None or df_input.empty:
        cols = state["source_df"].columns if state.get("source_df") is not None else []
        empty_validated = pd.DataFrame(columns=cols)
        print("invoke_graph_sync: no rows to validate; will write empty df_validated")
        return {"df_validated": empty_validated}

    df = df_input.copy()
    rows = [row.to_dict() for _, row in df.iterrows()]

    print(f"invoke_graph_sync: validating {len(rows)} rows over single SSE connection...")
    try:
        validated_rows = validate_rows_sync(rows)
    except Exception as e:
        print("invoke_graph_sync: fatal error validating rows:", e)
        traceback.print_exc()
        return {"df_validated": df}

    final_df = pd.DataFrame(validated_rows)
    print(f"invoke_graph_sync: MCP validated {len(final_df)} rows and will write to df_validated")
    return {"df_validated": final_df}


# def invoke_graph(state: Dict[str, Any]) -> Dict[str, Any]:
#     df_input = state.get("df_unmatched_validated")
#     if df_input is None or df_input.empty:
#         cols = state["source_df"].columns if state.get("source_df") is not None else []
#         empty_validated = pd.DataFrame(columns=cols)
#         print("invoke_graph: no rows to validate; will write empty df_validated")
#         return {"df_validated": empty_validated}
#
#     df = df_input.copy()
#     rows = [row.to_dict() for _, row in df.iterrows()]
#
#     print(f"invoke_graph: validating {len(rows)} rows using MCP HTTP client...")
#     try:
#         validated_rows = validate_rows_sync(rows)
#     except Exception as e:
#         print("invoke_graph: fatal error validating rows:", e)
#         traceback.print_exc()
#         return {"df_validated": df}
#
#     final_df = pd.DataFrame(validated_rows)
#     print(f"invoke_graph: MCP validated {len(final_df)} rows and will write to df_validated")
#     return {"df_validated": final_df}


def create_records(state: CsvState) -> Dict[str, Any]:
    df_final = state.get("df_validated")
    if df_final is None:
        print("create_records: no df_validated present")
        return {}
    print(f"Create records (validated): preparing {len(df_final)} rows")

    df_final.to_csv("validated_records.csv", index=False)
    print("✅ File saved as validated_records.csv")

    return {}


# --- Build LangGraph ---
builder = StateGraph(CsvState)

# Nodes
builder.add_node("data_profiling_node", data_profiling)
builder.add_node("src_dedup_node", src_dedup)
builder.add_node("cmd_dedup_node", cmd_dedup)
builder.add_node("link_records_node", link_records)
builder.add_node("address_validation_node", address_validation)
builder.add_node("mcp_validation_node", invoke_graph)
builder.add_node("create_records_node", create_records)

# Flow
builder.add_edge(START, "data_profiling_node")
builder.add_edge("data_profiling_node", "src_dedup_node")
builder.add_edge("src_dedup_node", "cmd_dedup_node")

# Linked branch -> terminal
builder.add_edge("cmd_dedup_node", "link_records_node")
builder.add_edge("link_records_node", END)

# Validation branch -> MCP -> downstream -> terminal
builder.add_edge("cmd_dedup_node", "address_validation_node")
builder.add_edge("address_validation_node", "mcp_validation_node")
builder.add_edge("mcp_validation_node", "create_records_node")
builder.add_edge("create_records_node", END)

graph = builder.compile()


# png_bytes = graph.get_graph().draw_mermaid_png()
# with open("graph_final_mermaid.png", "wb") as f:
#     f.write(png_bytes)


# async def main():
#     source_df = pd.read_csv("cmd_pipeline/LF_TEST_DATA_T2.csv")
#     initial_state: CsvState = {"source_df": source_df}
#     try:
#         await graph.ainvoke(initial_state)
#     except Exception:
#         print("Graph execution failed with exception:")
#         traceback.print_exc()
#     else:
#         print("Processing complete. Output saved to output.csv.")
#
#
# if __name__ == "__main__":
#     asyncio.run(main())


def main_sync():
    try:
        print("Reading input CSV...")
        source_df = pd.read_csv("cmd_pipeline/LF_TEST_DATA_T2.csv")

        initial_state: CsvState = {"source_df": source_df}
        print("Starting LangGraph (synchronous)...")

        final_state = graph.invoke(initial_state)

        if isinstance(final_state, dict) and "df_validated" in final_state:
            final_state["df_validated"].to_csv("output.csv", index=False)
            print("Output written to output.csv")

        print("Processing complete.")

    except Exception as e:
        print("Graph execution failed with exception:")
        traceback.print_exc()
        print(f"Error details: {e}")


if __name__ == "__main__":
    main_sync()
