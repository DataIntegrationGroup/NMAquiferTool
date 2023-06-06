# ===============================================================================
# Copyright 2023 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================


"""
This script is used to export a csv from NM_Aquifer to be used in Onyx.

"""
import csv
import os

from nmat.db import get_db_client
from nmat.geo_utils import utm_to_latlon
from nmat.query import execute_fetch, make_select
from nmat.util import write_csv

# ===============================================================================
# Configuration
EXPORT_PATH = "./output/onyx_export.csv"


# ===============================================================================


def get_records():
    client = get_db_client()
    print("Connected to database", client)

    where = "where PublicRelease=1"
    order = "order by PointID"

    sql = make_select(where=where, order=order)

    return execute_fetch(sql, client=client)


def export(path):
    def make_csv_record(record):
        lon, lat = utm_to_latlon(record["Easting"], record["Northing"])
        return [record["PointID"], record["SiteNames"], lat, lon]

    records = get_records()
    write_csv(
        records,
        path,
        func=make_csv_record,
        header=["PointID", "SiteNames", "Latitude", "Longitude"],
    )


def main():
    export(EXPORT_PATH)


if __name__ == "__main__":
    main()

# ============= EOF =============================================
