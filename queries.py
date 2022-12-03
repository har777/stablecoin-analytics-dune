import os
import time
from dune import (
    execute_query,
    get_query_results,
    is_query_complete,
    execute_query_with_params,
)
from utils import get_ens

QUERY_TO_ID = {
    "tether": {
        "all_blacklisted_events": os.getenv("ALL_BLACKLISTED_EVENTS_QUERY_ID"),
        "all_unblacklisted_events": os.getenv("ALL_UNBLACKLISTED_EVENTS_QUERY_ID"),
        "all_destroy_blacklisted_funds_events": os.getenv(
            "ALL_DESTROY_BLACKLISTED_FUNDS_EVENTS_QUERY_ID"
        ),
        "all_mints": os.getenv("ALL_MINTS_QUERY_ID"),
        "all_burns": os.getenv("ALL_BURNS_QUERY_ID"),
        "transfers_involving_particular_user": os.getenv(
            "ALL_TRANSFERS_INVOLVING_PARTICULAR_USER_QUERY_ID"
        ),
        "infinite_approvals_statistics": os.getenv("INFINITE_APPROVALS_STATISTICS"),
    }
}

SLEEP_TIME = 5


def get_all_blacklisted_events():
    execution_id = execute_query(QUERY_TO_ID["tether"]["all_blacklisted_events"])
    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "address": row["_user"],
                "time": row["evt_block_time"],
                "tx_hash": row["evt_tx_hash"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data


def get_all_unblacklisted_events():
    execution_id = execute_query(QUERY_TO_ID["tether"]["all_unblacklisted_events"])
    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "address": row["_user"],
                "time": row["evt_block_time"],
                "tx_hash": row["evt_tx_hash"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data


def get_all_destroy_blacklisted_funds_events():
    execution_id = execute_query(
        QUERY_TO_ID["tether"]["all_destroy_blacklisted_funds_events"]
    )
    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "blacklistedUser": row["blacklistedUser"],
                "destroyedAmount": row["destroyedAmount"],
                "time": row["evt_block_time"],
                "tx_hash": row["evt_tx_hash"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data


def get_all_mints():
    execution_id = execute_query(QUERY_TO_ID["tether"]["all_mints"])
    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "amount": row["amount"],
                "time": row["evt_block_time"],
                "tx_hash": row["evt_tx_hash"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data


def get_all_burns():
    execution_id = execute_query(QUERY_TO_ID["tether"]["all_burns"])
    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "amount": row["amount"],
                "time": row["evt_block_time"],
                "tx_hash": row["evt_tx_hash"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data


def get_all_transfers_involving_particular_user(user):
    if not user.startswith("0x"):
        user = get_ens(user)

    execution_id = execute_query_with_params(
        QUERY_TO_ID["tether"]["transfers_involving_particular_user"], {"address": user}
    )

    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "amount": row["amount"],
                "from": row["from"],
                "to": row["to"],
                "time": row["evt_block_time"],
                "tx_hash": row["evt_tx_hash"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data


def get_all_infinite_approvals_statistics():
    execution_id = execute_query(QUERY_TO_ID["tether"]["infinite_approvals_statistics"])
    while True:
        query_results = get_query_results(execution_id).json()
        if not is_query_complete(query_results):
            time.sleep(SLEEP_TIME)
            continue

        data = {
            "submitted_at": query_results["submitted_at"],
            "count": query_results["result"]["metadata"]["total_row_count"],
        }

        formatted_rows = []

        rows = query_results["result"]["rows"]
        for row in rows:
            formatted_row = {
                "count": row["total_count"],
                "spender": row["spender"],
            }
            formatted_rows.append(formatted_row)

        data["rows"] = formatted_rows

        return data
