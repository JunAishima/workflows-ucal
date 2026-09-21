import traceback

from prefect import flow, get_run_logger, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_UI_URL

from data_validation import data_validation, get_run
from end_of_run_export import general_data_export
from process_tes import process_tes

CATALOG_NAME = "ucal"

SLACK_GENERAL = "mon-prefect"
SLACK_BLUESKY = "mon-bluesky"
SLACK_UCAL = "mon-prefect-ucal"
SLACK_PROGRAM = "mon-prefect-spec"


def slack(func):
    """
    Send a message to the general and program slack channels if the flow-run failed.
    Send a message to the ucal status slack channel with the flow-run status.
    Send a message to the bluesky slack channel if the bluesky-run failed.

    NOTE: the name of this inner function is the same as the real end_of_workflow() function because
    when the decorator is used, Prefect sees the name of this inner function as the name of
    the flow. To keep the naming of workflows consistent, the name of this inner function had to match the expected name.
    """

    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False, reprocess_tes=False):
        logger = get_run_logger()
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")

        # Load slack credentials that are saved in Prefect. If loading a block fails, log it
        # and fall back to None so that a Slack outage/misconfiguration doesn't prevent the
        # wrapped flow from running.
        def _load_block(name):
            try:
                return SlackWebhook.load(name)
            except Exception:
                logger.exception(f"Failed to load Slack webhook block '{name}'")
                return None

        def _notify(webhook, message, description):
            """Best-effort Slack notification; never let a notification failure mask
            the underlying flow result/exception."""
            if webhook is None:
                logger.warning(f"Skipping {description} notification: webhook not available")
                return
            try:
                webhook.notify(message)
            except Exception:
                logger.exception(f"Failed to send {description} notification")

        mon_prefect = _load_block(SLACK_GENERAL)
        mon_bluesky = _load_block(SLACK_BLUESKY)
        mon_prefect_ucal = _load_block(SLACK_UCAL)
        mon_prefect_program = _load_block(SLACK_PROGRAM)

        # Get the uid.
        uid = stop_doc.get("run_start", "unknown")
        scan_id = "unknown"
        try:
            # Get the scan_id.
            run = get_run(uid, api_key=api_key)
            scan_id = run.start["scan_id"]

            # Send a message to mon-bluesky if bluesky-run failed.
            if stop_doc.get("exit_status") == "fail":
                _notify(
                    mon_bluesky,
                    f":bangbang: {CATALOG_NAME} bluesky-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```reason: {stop_doc.get('reason', 'none')}```",
                    "mon-bluesky",
                )
        except Exception:
            logger.exception(f"Exception while checking {uid}/{scan_id} for scan exit status")

        try:
            result = func(
                stop_doc, api_key=api_key, dry_run=dry_run, reprocess_tes=reprocess_tes
            )

            # Send a message to mon-prefect-ucal if flow-run is successful.
            message = f":white_check_mark: {CATALOG_NAME} flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
            _notify(mon_prefect_ucal, message, "mon-prefect-ucal")
            return result
        except Exception as e:
            tb = traceback.format_exception_only(type(e), e)

            # Send a message to mon-prefect-ucal, mon-prefect if flow-run failed.
            message = f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            _notify(mon_prefect, message, "mon-prefect")
            _notify(mon_prefect_ucal, message, "mon-prefect-ucal")

            try:
                flow_run = FlowRunContext.get().flow_run
                # Add link to flow-run for the message to mon-prefect-program.
                program_message = (
                    f":bangbang: {CATALOG_NAME} flow-run failed. <{PREFECT_UI_URL.value()}/flow-runs/"
                    + f"flow-run/{flow_run.id}|the flow run link> (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
                )
            except Exception:
                logger.exception("Failed to build mon-prefect-program message")
                program_message = message

            _notify(mon_prefect_program, program_message, "mon-prefect-program")
            raise

    return end_of_run_workflow


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
@slack
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False, reprocess_tes=False):
    uid = stop_doc["run_start"]
    logger = get_run_logger()

    data_validation(uid, api_key=api_key, dry_run=dry_run)
    run = get_run(uid, api_key=api_key)
    if run.start.get("data_session", "") == "":
        logger.info("No data session found, skipping export")
        return

    process_tes(uid, api_key=api_key, dry_run=dry_run, reprocess=reprocess_tes)
    # Here is where exporters could be added
    exit_status = stop_doc.get("exit_status", "No Status")
    if exit_status == "success":
        general_data_export(uid, api_key=api_key, dry_run=dry_run)
    else:
        logger.info(f"Run had exit status: {exit_status}, skipping export")

    log_completion(dry_run=dry_run)
