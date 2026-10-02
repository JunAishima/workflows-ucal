from prefect import flow, get_run_logger
from data_validation import get_run, get_catalog
from export_tools import get_proposal_path
from autoprocess.statelessAnalysis import handle_run
from autoprocess.utils import get_processing_info_file
from os.path import dirname, join
import os
import pickle


@flow(log_prints=True)
def process_tes(uid, api_key=None, reprocess=False):
    """
    Process TES data and save processing information.

    Parameters
    ----------
    uid : str
        Unique identifier for the run to process
    reprocess : bool, optional
        If True, force reprocessing even if data already exists

    Returns
    -------
    dict
        Processing information dictionary
    """
    logger = get_run_logger()
    catalog = get_catalog(api_key=api_key)
    run = get_run(uid, api_key=api_key)

    if "primary" not in run:
        logger.info(f"No Primary stream for {run.start['scan_id']}")
        return False

    logger.info(f"In TES Exporter for {run.start['uid']}")
    save_directory = join(get_proposal_path(run), "ucal_processing")

    # Process the run
    processing_info, data = handle_run(
        uid, catalog, save_directory, reprocess=reprocess
    )
    # Save calibration information
    config_path = "/nsls2/data/sst/legacy/ucal/process_info"
    try:
        if "data_calibration_info" in processing_info:
            cal_path = get_processing_info_file(config_path, "calibration")
            os.makedirs(dirname(cal_path), exist_ok=True)

            with open(cal_path, "wb") as f:
                pickle.dump(processing_info["data_calibration_info"], f)
            logger.info(f"Saved calibration info to {cal_path}")
        # Save processing info if it exists
        if "data_processing_info" in processing_info:
            proc_path = get_processing_info_file(config_path, "processing")
            os.makedirs(dirname(proc_path), exist_ok=True)

            with open(proc_path, "wb") as f:
                pickle.dump(processing_info["data_processing_info"], f)
            logger.info(f"Saved processing info to {proc_path}")
    except Exception:
        logger.exception("Could not write processing info")
    return processing_info
