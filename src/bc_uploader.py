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
import pandas as pd
import requests as requests

from src.db import get_db_client
from src.query import execute_fetch, execute_insert, make_insert, make_select

CKAN_URL = 'https://catalog.newmexicowaterdata.org/'
def add_records_to_db(client, group):
    for i, row in group.iterrows():
        sql = make_insert('WaterLevels', row.keys(), row.values)
        execute_insert(sql, client=client)


def add_point_to_db(client, row):
    last_pointid = get_last_point_id_like(client, 'BC-')

    n = int(last_pointid.split('-')[1])
    pointid = f'BC-{n + 1:04n}'

    sql = make_insert('Location', row.keys(), row.values)
    execute_insert(sql, client=client)

    return pointid


def get_last_point_id_like(client, point_id):
    """
    This function is used to get the last PointID from the database that is like point_id.
    :param point_id:
    :return:
    """
    sql = make_select(where=f"PointID LIKE '{point_id}%'",
                      order=f"PointID DESC")
    return execute_fetch(sql, client=client, fetch='fetchfirst')


def get_point_id(client, point_id):
    """
    This function is used to get the point_id from the database.
    :param point_id:
    :return:
    """

    sql = make_select(attributes='PointID', where=f"PointID = '{point_id}'")
    return execute_fetch(sql, client=client, fetch='fetchone')


def get_latest_record(client, pointid):
    sql = make_select(table='WaterLevels',
                      where=f"PointID = '{pointid}'",
                      order='DateMeasured DESC')
    return execute_fetch(sql, client=client, fetch='fetchone')


def get_latest_data():
    resource_id = ''

    url = f'{CKAN_URL}/datastore/dump/{resource_id}'
    resp = requests.get(url)
    return resp.text



def main():
    client = get_db_client()

    get_latest_data()

    p = '../indata/sp2023berncowls.xlsx'
    sheetname = 'Sp2023BernCoWLs'

    df = pd.read_excel(p, sheet_name=sheetname)

    # filter out any rows with Well_Name that starts with Z -
    filtered = df[~df['Well_Name'].str.startswith('Z -')]

    # group df by Well_Name column
    grouped = filtered.groupby('Well_Name')
    for name, group in grouped:
        # check if in database

        repr_row = group.iloc[0]
        pointid = get_point_id(client, repr_row['PointID'])
        if pointid:
            print(f'{name} already in database')
        else:
            pointid = add_point_to_db(client, repr_row)

        # iterate over each row in the group
        # sort group by date
        group = group.sort_values(by='Date')

        # get the latest record from the database
        dbrecord = get_latest_record(client, pointid)
        if dbrecord:
            # filter out all records that are older than the latest record in the database
            group = group[group['Date'] > dbrecord['Date']]

        # add records to database
        add_records_to_db(client, group)


if __name__ == '__main__':
    main()

# ============= EOF =============================================
