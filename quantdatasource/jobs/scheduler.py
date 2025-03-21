import json
import logging
from datetime import datetime

from quantdata import open_dbs


class AdditionCollectAndImport:
    def __init__(self, func, **job_params) -> None:
        self.func = func
        self.job_params = job_params

    @property
    def id(self):
        return self.job_params["id"]

    def _addition_collect_and_import(self, is_collect, is_import, dt=None):
        try:
            dt = dt if dt is not None else datetime.now()
            with open_dbs():
                self.func(dt, is_collect, is_import)
        except Exception as e:
            # TODO: send email
            logging.error("", exc_info=e)

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


def job(**job_params):
    def wrapper(func):
        job = AdditionCollectAndImport(func, **job_params)
        all_jobs.append(job)
        return job

    return wrapper
