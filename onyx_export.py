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

from db import get_db_client
from geo_utils import utm_to_latlon

# ===============================================================================
# Configuration
export_name = 'output/onyx_export.csv'


# ===============================================================================


def get_records(client):
    cursor = client.cursor(as_dict=True)

    where = 'where PublicRelease=1'
    order = 'order by PointID'

    sql = f'''select * from dbo.Location {where} {order}'''

    print('executing query================')
    print('sql: ', sql)
    print('===============================')
    cursor.execute(sql)
    return cursor.fetchall()


def make_csv_record(record):
    lon, lat = utm_to_latlon(record['Easting'], record['Northing'])
    return [record['PointID'], record['SiteNames'], lat, lon]


def export_results(records, export_name):
    with open(export_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['PointID', 'SiteNames', 'Latitude', 'Longitude'])

        for record in records:
            row = make_csv_record(record)
            writer.writerow(row)


def main():
    client = get_db_client()
    print('Connected to database', client)

    records = get_records(client)
    export_results(records, export_name)


if __name__ == '__main__':
    main()

# ============= EOF =============================================
