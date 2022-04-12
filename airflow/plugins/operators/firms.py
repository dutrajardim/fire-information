# fmt: off
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from contextlib import closing
from urllib.request import (Request, urlopen)
from datetime import datetime
import os
import codecs
import csv
# fmt: on


class FirmsOperator(BaseOperator):

    # defining operator box background color
    ui_color = "#7545a1"

    template_fields = ("date",)

    @apply_defaults
    def __init__(
        self,
        details_url,
        date=datetime.now().strftime("%Y-%m-%d"),
        *args,
        **kwargs,
    ):

        # initializing inheritance
        super(FirmsOperator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.details_url = details_url
        self.date = date

    def execute(self, context):

        ti = context["ti"]
        req = Request(self.details_url)

        with closing(urlopen(req)) as resource:

            csv_file = codecs.iterdecode(resource, "utf-8")
            data = csv.DictReader(csv_file)

            row = next(x for x in data if x["last_modified"][:10] == self.date)

            self.log.info(f"Result: {row}")

            ti.xcom_push(key="filename", value=os.path.splitext(row["name"])[0])
            ti.xcom_push(key="last_modified", value=row["last_modified"])
            ti.xcom_push(key="mtime", value=row["mtime"])
            ti.xcom_push(key="link", value=row["downloadsLink"])
