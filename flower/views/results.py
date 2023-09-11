import logging
from collections.abc import Iterable
from typing import Any

import flower.utils.results.stores
from flower.utils.results.stores.abstract import ResultIdWithResultPair
from flower.views import BaseHandler
from tornado import web

logger = logging.getLogger(__name__)


class ResultView(BaseHandler):
    @web.authenticated
    def get(self, result_id: str):
        # TODO: render an individual Result for real
        self.render("result.html")


class ResultsDataTable(BaseHandler):
    @web.authenticated
    def get(self):
        draw = self.get_argument('draw', type=int)
        start = self.get_argument('start', type=int)
        length = self.get_argument('length', type=int)
        search = self.get_argument('search[value]', type=str)  # TODO: implement search

        reverse_sort_order: bool = self.get_argument('order[0][dir]', type=str) == 'desc'

        result_producer = flower.utils.results.stores.store_for_backend(self.capp.backend)
        results_iter: Iterable[ResultIdWithResultPair] = result_producer.results_by_timestamp(reverse=reverse_sort_order)

        filtered_results: list[dict[str, Any]] = []

        c = 0
        for _, result in results_iter:
            if start <= c < start + length:
                filtered_results.append(result.to_render_dict())
            c += 1

        self.write(dict(draw=draw, data=filtered_results,
                        recordsTotal=c,
                        # TODO: do we need a better way of getting `recordsFiltered` (total count)? Maybe
                        #   `results_by_timestamp()` should be a generator with an int return type that is total count?
                        recordsFiltered=len(filtered_results)))

    @web.authenticated
    def post(self):
        return self.get()


class ResultsView(BaseHandler):
    @web.authenticated
    def get(self):
        app = self.application
        capp = self.application.capp

        time = 'natural-time' if app.options.natural_time else 'time'
        if capp.conf.timezone:
            time += '-' + str(capp.conf.timezone)

        self.render(
            "results.html",
            results=[],
            columns="task_id,name,date_done,status,args,kwargs,result,result_extended",
            time=time,
        )
