import json
import logging
import traceback
from datetime import datetime

from quantdata import open_dbs

from quantdatasource.jobs.msg_email import close_email_api, init_email_api, send_email


class AdditionCollectAndImport:
    def __init__(self, func, service_type=None, **job_params) -> None:
        self.func = func
        self.service_type = service_type
        self.job_params = job_params

    @property
    def id(self):
        return self.job_params["id"]

    def _addition_collect_and_import(self, is_collect, is_import, dt=None):
        try:
            dt = dt if dt is not None else datetime.now()
            with open_dbs(stype=self.service_type):
                self.func(dt, is_collect, is_import)
        except Exception as e:
            logging.error("", exc_info=e)
            init_email_api()
            send_email(
                ["532978024@qq.com"],
                title="数据采集入库报错",
                message=traceback.format_exc(),
            )
            close_email_api()

    def __call__(self, sched=None, only_collect=False, only_import=False, dt=None):
        kwargs = {}
        kwargs["is_collect"] = not only_import
        kwargs["is_import"] = not only_collect
        kwargs["dt"] = dt
        if sched is None:
            self._addition_collect_and_import(**kwargs)
        elif "trigger" not in self.job_params:
            print(self.job_params["name"], "不是定时任务，忽略")
        else:
            sched.add_job(
                func=self._addition_collect_and_import, kwargs=kwargs, **self.job_params
            )
            print(self.job_params["name"], "started")

    def __str__(self) -> str:
        return json.dumps(self.job_params, indent=4, ensure_ascii=False)


all_jobs = []


def job(service_type=None, **job_params):
    def wrapper(func):
        job = AdditionCollectAndImport(func, service_type, **job_params)
        all_jobs.append(job)
        return job

    return wrapper
