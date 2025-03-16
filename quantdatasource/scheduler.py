import importlib
import json
import logging
from datetime import datetime


class AdditionCollectAndImport:
    def __init__(self, collect_func, import_funcs, calendar, **job_params) -> None:
        self.collect_func = collect_func
        self.import_funcs = import_funcs
        self.calendar = calendar
        self.job_params = job_params

    @property
    def id(self):
        return self.job_params["id"]

    def _addition_collect_and_import(
        self, db_context_mod=None, import_funcs=None, dt=None
    ):
        try:
            dt = dt if dt is not None else datetime.now()
            if self.calendar is None:
                is_tradeday = False
            else:
                is_tradeday = self.calendar.is_trading_day(dt)
            if self.collect_func is not None:
                self.collect_func(dt, is_tradeday)
            if import_funcs:
                if db_context_mod:
                    context = db_context_mod.make_context()
                for func in import_funcs:
                    func(dt, is_tradeday, **context)
                if db_context_mod:
                    db_context_mod.close_context(context)
        except Exception as e:
            logging.error(exc_info=e, stack_info=True)

    def __call__(self, sched=None, only_collect=False, only_import=False, dt=None):
        kwargs = {}
        if dt is not None:
            kwargs["dt"] = dt
        if only_import:
            self.collect_func = None
        if not only_collect and self.import_funcs:
            mods = {}
            kwargs["import_funcs"] = []
            kwargs["db_context_mod"] = importlib.import_module(
                self.import_mod + ".db_context"
            )
            for funcname in self.import_funcs:
                last_dot = funcname.rindex(".")
                modname = self.import_mod + "." + funcname[:last_dot]
                mod = mods.get(modname, None)
                if mod is None:
                    mod = importlib.import_module(modname)
                    mods[modname] = mod
                funcname = funcname[last_dot + 1 :]
                _import_func = getattr(mod, funcname, None)
                kwargs["import_funcs"].append(_import_func)
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


def job(import_funcs: list = None, calendar=None, **job_params):
    def wrapper(func):
        job = AdditionCollectAndImport(func, import_funcs, calendar, **job_params)
        all_jobs.append(job)
        return job

    return wrapper
