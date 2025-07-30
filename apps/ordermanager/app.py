from flask import Flask, jsonify, render_template
import json
import pandas as pd
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout
from databricks.sdk import WorkspaceClient
import os


app = Flask(__name__, template_folder="templates")

# ── routes ─────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/orders")
def orders():
    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        statement="""
            SELECT 
                key.order_id as order_id,
                collect_list(list_element) as events 
            FROM read_statestore(
                "/Volumes/gk_demo/default/checkpoints/orders_in_progress",
                stateVarName => 'events' ) 
            GROUP BY key.order_id
        """,
        warehouse_id="8c9fbc2eb92aab17",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
        wait_timeout="50s"
    )
    data = [
        {"order_id": order_id, "events": json.loads(events_json)}
        for order_id, events_json in result.result.data_array
    ]
    return data

# ── main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=8000)
