import os
import time as ttime

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from tiled.client import from_uri

BEAMLINE_OR_ENDSTATION = "ucal"


def get_api_key_from_env():
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task(retries=2, retry_delay_seconds=10)
def get_catalog(api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    catalog = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)[f"{BEAMLINE_OR_ENDSTATION}/raw"]
    return catalog


# Mongo database-backed
@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/raw"][uid]
    return run


# only call if Mongo - remove if SQL
@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


# only call if Mongo - remove if SQL
@flow
def data_validation(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    logger.info(f"Validating uid {uid}")
    start_time = ttime.monotonic()
    run_client = get_run(uid, api_key=api_key)
    for stream in run_client:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = read_stream(run_client, stream)  # noqa: F841
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")
