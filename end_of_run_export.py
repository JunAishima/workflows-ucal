from prefect import flow, get_run_logger, task
from os.path import exists, join
import os
from export_to_xdi import exportToXDI
from export_to_hdf5 import exportToHDF5
from data_validation import get_run
from export_tools import get_proposal_path
import datetime


def get_export_path(run):
    proposal_path = get_proposal_path(run)

    visit_date = datetime.datetime.fromisoformat(
        run.start.get("start_datetime", datetime.datetime.today().isoformat())
    )
    visit_dir = visit_date.strftime("%Y%m%d_export")

    export_path = join(proposal_path, visit_dir)
    return export_path


def create_export_path(export_path):
    logger = get_run_logger()
    export_path_exists = exists(export_path)
    if not export_path_exists:
        os.makedirs(export_path, exist_ok=True)
        logger.info(f"Export path does not exist, making {export_path}")


@task(retries=2, retry_delay_seconds=10)
def export_all_streams(uid, api_key=None, dry_run=False, beamline_acronym="ucal"):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)

    base_export_path = get_export_path(run)
    logger.info(f"Generating Export for uid {run.start['uid']}")
    logger.info(f"Export Data to {base_export_path}")
    if not dry_run:
        create_export_path(base_export_path)

    logger.info("Exporting XDI")
    xdi_export_path = join(base_export_path, "xdi")
    if not dry_run:
        create_export_path(xdi_export_path)
        exportToXDI(xdi_export_path, run)
    else:
        logger.info(f"dry_run: not exporting to {xdi_export_path}")
    logger.info("Exporting HDF5")
    hdf5_export_path = join(base_export_path, "hdf5")
    if not dry_run:
        create_export_path(hdf5_export_path)
        exportToHDF5(hdf5_export_path, run)
    else:
        logger.info(f"dry_run: not exporting to {hdf5_export_path}")
    # logger.info("Exporting Athena")
    # if not dry_run:
    # exportToAthena(export_path, run)


@flow
def general_data_export(uid, api_key=None, dry_run=False, beamline_acronym="ucal"):
    export_all_streams(
        uid, api_key=api_key, dry_run=dry_run, beamline_acronym=beamline_acronym
    )
