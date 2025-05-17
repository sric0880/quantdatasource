import datetime
import logging
import signal
from logging.handlers import TimedRotatingFileHandler

import fire
import tabulate
from apscheduler.schedulers.background import BlockingScheduler

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
        logfile: log file pathname
        loglevel: log level
        show: show all job id and names and params
        job_id: execute job by id now
        only_collect: only collect data, not import to db
        only_import: only imoport to db
        dt: collect and import on certain date
    """
    from quantdatasource.jobs.scheduler import all_jobs

    if isinstance(dt, str):
        dt = datetime.datetime.fromisoformat(dt)

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
            force=True
        )
    else:
        logging.basicConfig(level=loglevel, format=LOG_FORMAT, datefmt=DATE_FORMAT, force=True)

    if show:
        keys = ["id", "name", "trigger", "day_of_week", "hour", "minute", "second"]
        print(
            tabulate.tabulate(
                [
                    list(getattr(job, k, job.job_params.get(k, "-")) for k in keys)
                    for job in all_jobs
                ],
                headers=keys,
            )
        )
    elif job_id:
        for job in all_jobs:
            if job.id == job_id:
                job(only_collect=only_collect, only_import=only_import, dt=dt)
                return
        logging.error(f"cannot find job id {job_id}")
    else:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        scheduler = BlockingScheduler()
        for job in all_jobs:
            job(sched=scheduler, only_collect=only_collect)
        scheduler.start()


if __name__ == "__main__":
    fire.Fire(main)
