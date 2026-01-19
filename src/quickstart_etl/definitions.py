import random
import pandas as pd
import plotly.express as px

from dagster import asset, Definitions, Output, MetadataValue

@asset
def pipeline_with_logs_and_chart(context):
    context.log.info("Pipeline started")
    context.log.info("Processing data")

    run_status = random.choice(["SUCCESS", "FAILURE"])
    context.log.info(f"Pipeline status: {run_status}")

    df = pd.DataFrame({
        "status": ["SUCCESS", "FAILURE"],
        "count": [
            1 if run_status == "SUCCESS" else 0,
            1 if run_status == "FAILURE" else 0
        ]
    })

    fig = px.bar(
        df,
        x="status",
        y="count",
        title="Pipeline Run Status",
        color="status",
        color_discrete_map={
            "SUCCESS": "green",
            "FAILURE": "red"
        }
    )

    if run_status == "FAILURE":
        context.log.error("Pipeline failed intentionally")
        raise Exception("Intentional failure for POC")

    context.log.info("Pipeline completed successfully")

    return Output(
        value=df,
        metadata={
            "Run Status Chart": MetadataValue.plotly(fig)
        }
    )


defs = Definitions(
    assets=[pipeline_with_logs_and_chart]
)
