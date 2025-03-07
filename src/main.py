from projectFiles import logger
from projectFiles.pipeline.stage_data_ingestion import DataIngestionPipeline

def data_ingestion():
    STAGE_NAME = "Data Ingestion"
    try:
        logger.info(f">>>>> Starting stage: {STAGE_NAME} <<<<<")
        pipeline = DataIngestionPipeline()
        pipeline.ingest_data()
        logger.info(f">>>>> Completed stage: {STAGE_NAME} <<<<<")
    except Exception as e:
        raise e