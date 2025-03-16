import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

from apscheduler.schedulers.background import BlockingScheduler

from quantdatasource.scheduler import all_jobs

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
DATE_FORMAT = "%Y/%m/%d %H:%M:%S"


def main(
    logfile=None,
    loglevel="INFO",
    show=False,
    job_id="",
    only_collect=False,
    only_import=False,
    dt: datetime.datetime = None,
):
    """
    This is a tool for downloading finance data and import to databases

    Args:
        log_file: log file pathname
        loglevel: log level
        show: show all job id and names and params
        job_id: execute job by id now
        only_collect: only collect data, not import to db
        only_import: only imoport to db
        dt: collect and import on certain date[isoformat]
    """

    if logfile:
        logging.basicConfig(
            level=loglevel,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
            handlers=[
                TimedRotatingFileHandler(
                    logfile, when="D", backupCount=30, encoding="utf-8"
                )
            ],
        )
    else:
        logging.basicConfig(level=loglevel, format=LOG_FORMAT, datefmt=DATE_FORMAT)

    # dt = datetime.datetime.fromisoformat(args.dt) if args.dt else None
    if show:
        for job in all_jobs:
            print(job)
    elif job_id:
        for job in all_jobs:
            if job.id == job_id:
                job(only_collect=only_collect, only_import=only_import, dt=dt)
    else:
        scheduler = BlockingScheduler()
        for job in all_jobs:
            job(sched=scheduler, only_collect=only_collect)
        scheduler.start()
