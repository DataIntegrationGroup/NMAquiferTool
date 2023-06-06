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
# import requests as requests

from nmat.db import get_db_client
from nmat.geo_utils import latlon_to_utm
from nmat.query import execute_fetch, execute_insert, make_insert, make_select
from nmat.util import message, warning, info

CKAN_URL = "https://catalog.newmexicowaterdata.org/"


def add_records_to_db(client, pointid, group, dry=True, verbose=True):
    for i, row in group.iterrows():
        info(f"Adding {i}, {row['Well_Name']} to database")

        keys = ['PointID',
                'DateMeasured',
                'DepthToWater',
                'DepthToWaterBGS',
                'MPHeight']

        values = [pointid,
                  row['MSRMNT_Dat'].date(),
                  row['Depth_To_W'],
                  row['Depth_To_W'],
                  row['Stick_up'],
                  ]

        sql = make_insert("WaterLevels", keys, values)
        execute_insert(sql, client=client, dry=dry, verbose=verbose)


def add_point_to_db(client, row, dry=True, verbose=True):
    result = get_last_point_id_like(client, "BC-", verbose=verbose)
    last_pointid = result['PointID']

    n = int(last_pointid.split("-")[1])
    pointid = f"BC-{n + 1:04n}"

    keys = ['PointID', 'Easting', 'Northing', 'Altitude']
    easting, northing = latlon_to_utm(row['Long_DD'], row['Lat_DD'])

    values = [pointid, easting, northing, row['Elev_ft']]

    sql = make_insert("Location", keys, values)
    execute_insert(sql, client=client, dry=dry, verbose=verbose)

    return pointid


def get_last_point_id_like(client, point_id, verbose=True):
    """
    This function is used to get the last PointID from the database that is like point_id.
    :param point_id:
    :return:
    """
    sql = make_select(where=f"PointID LIKE '{point_id}%'", order=f"PointID DESC")
    return execute_fetch(sql, client=client, fetch="fetchone", verbose=verbose)


def get_point_id(client, point_id, verbose=True):
    """
    This function is used to get the point_id from the database.
    :param point_id:
    :return:
    """

    sql = make_select(attributes="PointID", where=f"PointID = '{point_id}'")
    return execute_fetch(sql, client=client, fetch="fetchone", verbose=verbose)


def get_latest_record(client, pointid, verbose=True):
    sql = make_select(
        table="WaterLevels", where=f"PointID = '{pointid}'", order="DateMeasured DESC"
    )
    return execute_fetch(sql, client=client, fetch="fetchone", verbose=verbose)


def get_latest_data():
    resource_id = ""

    url = f"{CKAN_URL}/datastore/dump/{resource_id}"
    # resp = requests.get(url)
    # return resp.text


def upload_waterlevels_from_file(p, sheetname, client=None, dry=True, verbose=False):
    message(f'Uploading waterlevels from {p}, sheet={sheetname}, dry={dry}')

    if client is None:
        client = get_db_client()

    df = pd.read_excel(p, sheet_name=sheetname)

    # filter out any rows with Well_Name that starts with Z -
    filtered = df[~df["Well_Name"].str.startswith("Z -")]

    # group df by Well_Name column
    grouped = filtered.groupby("Well_Name")
    for name, group in grouped:
        # check if in database
        repr_row = group.iloc[0]

        pointid = repr_row["PointID"]
        if pointid and pointid != "nan":
            info(f'Checking if {name}, ({pointid}) in database')
            result = get_point_id(client, pointid, verbose=verbose)
            pointid = result['PointID'] if result else None
        else:
            info(f'no PointID provided. Assuming {name} not in database')
            break

        if pointid:
            warning(f"{name} already in database")

        else:
            pointid = add_point_to_db(client, repr_row, dry=dry, verbose=verbose)

        # iterate over each row in the group
        # sort group by date
        group = group.sort_values(by="MSRMNT_Dat")

        # get the latest record from the database
        dbrecord = get_latest_record(client, pointid, verbose=verbose)

        if dbrecord:
            print('asdf', dbrecord['DateMeasured'])
            # filter out all records that are older than the latest record in the database
            group = group[group["MSRMNT_Dat"].dt.date > dbrecord["DateMeasured"]]
            # print(group)
            # print(group['MSRMNT_Dat'].dt.date)

        # add records to database
        add_records_to_db(client, pointid, group, dry=dry, verbose=verbose)


def main():
    client = get_db_client()

    get_latest_data()

    p = "./indata/sp2023berncowls.xlsx"
    sheetname = "Sp2023BernCoWLs"
    upload_waterlevels_from_file(p, sheetname, client=client)


if __name__ == "__main__":
    main()

# ============= EOF =============================================
