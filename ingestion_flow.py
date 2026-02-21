from prefect import flow, task
from db.snowflake_client import SnowflakeClient
from config.settings import DATA_CONFIG, SNOWFLAKE_CONFIG
from log_utils.flow_logger import get_log_path, FlowLogger
import os

database = SNOWFLAKE_CONFIG["database"]
schema = SNOWFLAKE_CONFIG["schema"]

@task
def validate_files(files: list[str], log_path: str) -> list[str]:
    logger = FlowLogger(log_path, entity="validator")
    valid = []
    for file in files:
        if os.path.getsize(file) == 0:
            logger.warning(f"Skipping empty file: {file}")
            continue
        valid.append(file)
    logger.info(f"{len(valid)}/{len(files)} files passed validation")
    return valid

@task(retries=3, retry_delay_seconds=10)
def validate_stage(stage_name, log_path):
    logger = FlowLogger(log_path, entity=stage_name)
    try:
        with SnowflakeClient() as client:
            client.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
    except Exception as e:
        logger.error(f"Failed to validate stage", extra={"error": str(e)})
        raise

@task(retries=3, retry_delay_seconds=10)
def upload_to_stage(file_path, stage_name, log_path):
    logger = FlowLogger(log_path, entity=stage_name)
    try:
        with SnowflakeClient() as client:
            result = client.execute(
                f"PUT file://{os.path.abspath(file_path)} @{stage_name} AUTO_COMPRESS=TRUE"
            )
        logger.info(f"Upload complete: {file_path}", extra={"result": str(result)})
        return result
    except Exception as e:
        logger.error(f"Upload failed: {file_path}", extra={"error": str(e)})
        raise

@task(retries=3, retry_delay_seconds=10)
def copy_into_table(table_name, stage_name, log_path):
    logger = FlowLogger(log_path, entity=table_name)
    try:
        with SnowflakeClient() as client:
            result = client.execute(f"""
                COPY INTO {table_name}
                FROM @{stage_name}
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    SKIP_HEADER = 1
                )
                ON_ERROR = 'CONTINUE'
            """)
        logger.info(f"Copy complete into {table_name}", extra={"result": str(result)})
        return result
    except Exception as e:
        logger.error(f"Copy failed into {table_name}", extra={"error": str(e)})
        raise

@flow(log_prints=True)
def load_flow():
    log_path = get_log_path("load_flow")
    logger = FlowLogger(log_path, entity="load_flow")

    files = [f for f in os.listdir(DATA_CONFIG["data_folder"]) if f.endswith(".csv")]

    if not files:
        logger.warning("No CSV files found in data folder — exiting")
        return

    logger.info(f"Found {len(files)} CSV files to load")

    entities = {}
    for file in files:
        entity = file.split(".")[0].lower()
        entities[entity] = {
            "file_path": os.path.join(DATA_CONFIG["data_folder"], file),
            "stage_name": f"{schema}.{entity}_stage",
            "table_name": f"{schema}.{entity}",
        }

    for entity, cfg in entities.items():
        validate_stage(cfg["stage_name"], log_path)

    upload_futures = {
        entity: upload_to_stage.submit(cfg["file_path"], cfg["stage_name"], log_path)
        for entity, cfg in entities.items()
    }

    copy_futures = {
        entity: copy_into_table.submit(
            cfg["table_name"], cfg["stage_name"], log_path,
            wait_for=[upload_futures[entity]]
        )
        for entity, cfg in entities.items()
    }

    for entity, future in copy_futures.items():
        try:
            future.result()
            logger.info(f"Pipeline complete for entity: {entity}")
        except Exception as e:
            logger.error(f"Pipeline failed for entity: {entity}", extra={"error": str(e)})

if __name__ == "__main__":
    load_flow()