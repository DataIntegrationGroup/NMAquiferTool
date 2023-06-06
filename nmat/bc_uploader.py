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
from cmath import isnan

import pandas as pd

# import requests as requests

from nmat.db import get_db_client
from nmat.geo_utils import latlon_to_utm
from nmat.query import execute_fetch, execute_insert, make_insert, make_select
from nmat.util import message, warning, info, error

CKAN_URL = "https://catalog.newmexicowaterdata.org/"


def add_records_to_db(client, wellid, pointid, group, dry=True, verbose=True):
    for i, row in group.iterrows():
        info(f"Adding {i}, {row['PointID']} waterlevels to database")

        keys = [
            "WellID",
            "PointID",
            "DateMeasured",
            "DepthToWaterBGS",
            "MPHeight",
            "MeasuringAgency",
            "DataSource",
            "MeasurementMethod",
            "PublicRelease"
        ]

        values = [
            str(wellid),
            pointid,
            row["Date"].date(),
            row["DepthToWaterBGS"],
            row["MPHeight"],
            row["MeasuringAgency"],
            row["DataSource"],
            row["Method"],
            int(row["PublicRelease"].lower() == 'yes')
        ]

        sql = make_insert("WaterLevels", keys)
        execute_insert(sql, values, client=client, dry=dry, verbose=verbose)


def add_well_to_db(client, row, pointid=None, dry=True, verbose=True):
    if pointid is None:
        result = get_last_point_id_like(client, "BC-", verbose=verbose)
        last_pointid = result["PointID"]

        n = int(last_pointid.split("-")[1])
        pointid = f"BC-{n + 1:04n}"

    keys = ["PointID",
            "SiteNames",
            "Easting", "Northing",
            "Latitude", "Longitude",
            "CoordinateMethod",
            "Altitude",
            "AltitudeMethod",
            "AlternateSiteID",
            "SiteType",
            'DataReliability',
            "DateCreated",
            "County",
            "State"
            ]
    lat, lon = row["Lat_DD"], row["Long_DD"]
    easting, northing = latlon_to_utm(lon, lat)

    values = [pointid,
              row["Site Name"],
              easting, northing, lat, lon,
              row['CoordinateMethod'],
              row["Altitude"],
              row['Alt_Method'],
              row['AlternateSiteID'],
              row['SiteType'],
              row['DataReliability'],
              row['DateCreated'],
              "BERNALILLO",
              "NM"
              ]

    sql = make_insert("Location", keys)
    execute_insert(sql, values, client=client, dry=dry, verbose=verbose)

    result = get_location_id(client, pointid, verbose=verbose)
    if result is None:
        error(f"Failed to get location id for {pointid}")
        return
    location_id = str(result['LocationID'])

    keys = ['PointID', 'LocationID', 'ProjectName']
    for pn in ('ProjectName1', 'ProjectName2'):
        values = [pointid, location_id, row[pn]]
        sql = make_insert("ProjectLocations", keys)
        execute_insert(sql, values, client=client, dry=dry, verbose=verbose)

    keys = ['LocationId', "PointID", "OSEWellID", 'WellDepth', 'WellPdf']
    values = [location_id, pointid,
              isnannone(row, 'OSEWellID'),
              isnannone(row, 'WellDepth'),
              isnannone(row, 'WellPdf')]
    sql = make_insert("WellData", keys)
    execute_insert(sql, values, client=client, dry=dry, verbose=verbose)

    return pointid


def isnannone(row, key):
    v = row[key]
    if isnan(v):
        return None
    return v


def get_location_id(client, pointid, verbose=True):
    sql = make_select(attributes="LocationID", table='Location', where=f"PointID = '{pointid}'")
    return execute_fetch(sql, client=client, fetch="fetchone", verbose=verbose)


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

    sql = make_select(attributes="PointID, WellID", table='WellData', where=f"PointID = '{point_id}'")
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


def upload_wells_from_file(p, sheetname, client=None, dry=True, verbose=False):
    message(f"Uploading wells from {p}, sheet={sheetname}, dry={dry}")
    if client is None:
        client = get_db_client()

    df = pd.read_excel(p, sheet_name=sheetname)
    for i, row in df.iterrows():
        add_well_to_db(client, row, pointid=row['PointID'], dry=dry, verbose=verbose)


def upload_waterlevels_from_file(p, sheetname, client=None, dry=True, verbose=False):
    message(f"Uploading waterlevels from {p}, sheet={sheetname}, dry={dry}")

    if client is None:
        client = get_db_client()

    df = pd.read_excel(p, sheet_name=sheetname)

    # filter out any rows with Well_Name that starts with Z -
    print(df['Site_Name'])
    filtered = df[~df["Site_Name"].str.startswith("Z -")]
    out = []
    # group df by Well_Name column
    grouped = filtered.groupby("Site_Name")
    for i, (name, group) in enumerate(grouped):
        try:
            # check if in database
            repr_row = group.iloc[0]

            pointid = repr_row["PointID"]
            if pointid and pointid != "nan":
                info(f"Checking if {name}, ({pointid}) in database")
                result = get_point_id(client, pointid, verbose=verbose)
                pointid, wellid = (result["PointID"], result['WellID']) if result else (None, None)
            else:
                info(f"no PointID provided. Assuming {name} not in database")
                break

            if not pointid:
                # warning(f"No PointID for {i}, {name}. skipping")
                # continue
                pointid = add_well_to_db(client, repr_row, dry=dry, verbose=verbose)

            # else:

            # iterate over each row in the group
            # sort group by date
            group = group.sort_values(by="Date")

            # get the latest record from the database
            dbrecord = get_latest_record(client, pointid, verbose=verbose)

            if dbrecord:
                print("asdf", dbrecord["DateMeasured"])
                # filter out all records that are older than the latest record in the database
                group = group[group["Date"].dt.date > dbrecord["DateMeasured"]]
                # print(group)
                # print(group['MSRMNT_Dat'].dt.date)

            # get well id for this pointid

            # add records to database
            add_records_to_db(client, wellid, pointid, group, dry=dry, verbose=verbose)
            out.append(pointid)
        except Exception as e:
            error(e)
            break

        # if i >1:
        #     break

    with open('./nmat/output/addpoints.txt', 'w') as f:
        f.write('\n'.join(out))


def main():
    client = get_db_client()

    get_latest_data()

    p = "./indata/sp2023berncowls.xlsx"
    sheetname = "Sp2023BernCoWLs"
    upload_waterlevels_from_file(p, sheetname, client=client)


if __name__ == "__main__":
    main()

# ============= EOF =============================================
