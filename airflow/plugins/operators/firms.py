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
    """
    This is responsible for helping getting information
    about the links of FIRMS daily data for the last two months.
    """

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
        """
        This function is responsible for instantiating a FirmsOperator object.
        For each FIRMS' product NASA makes available a CSV file containing information
        about the daily text file for the last two months. This Operator will return
        the information of the file in that CSV related to the given data.

        Args:
            details_url (str): Link for the FIRMS' detailed csv file.
            date (str, optional): date of the file. Defaults to datetime.now().strftime("%Y-%m-%d").
        """

        # initializing inheritance
        super(FirmsOperator, self).__init__(*args, **kwargs)

        # defining operator properties
        self.details_url = details_url
        self.date = date

    def execute(self, context):
        """
        This will be executed as the operator is activated.

        AArgs:
            context (dict): airflow context
        """

        ti = context["ti"]
        req = Request(self.details_url)

        # opening the csv
        with closing(urlopen(req)) as resource:

            csv_file = codecs.iterdecode(resource, "utf-8")
            data = csv.DictReader(csv_file)

            # searching  for the row of the given date
            row = next(x for x in data if x["last_modified"][:10] == self.date)

            self.log.info(f"Result: {row}")

            # making the returned data accessible for others operators
            ti.xcom_push(key="filename", value=os.path.splitext(row["name"])[0])
            ti.xcom_push(key="last_modified", value=row["last_modified"])
            ti.xcom_push(key="mtime", value=row["mtime"])
            ti.xcom_push(key="link", value=row["downloadsLink"])
