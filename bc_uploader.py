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
import csv
import pandas as pd

from db import get_db_client


def add_point_to_db(client, row):
    cursor = client.cursor()
    cursor.execute(f"INSERT INTO dbo.Location")

    cursor.commit()


def get_point_id(client, point_id):
    """
    This function is used to get the point_id from the database.
    :param point_id:
    :return:
    """
    cursor = client.cursor()
    cursor.execute(f"SELECT PointID FROM dbo.Location WHERE PointID = '{point_id}'")
    return cursor.fetchone()


def main():
    client = get_db_client()

    p = 'indata/sp2023berncowls.xlsx'
    sheetname = 'Sp2023BernCoWLs'

    df = pd.read_excel(p, sheet_name=sheetname)

    # filter out any rows with Well_Name that starts with Z -
    filtered = df[~df['Well_Name'].str.startswith('Z -')]

    # group df by Well_Name column
    grouped = filtered.groupby('Well_Name')
    for name, group in grouped:
        # check if in database

        repr_row = group.iloc[0]
        dbrecord = get_point_id(client, repr_row['PointID'])
        if dbrecord:
            print(f'{name} already in database')
        else:
            add_point_to_db(client, repr_row)

        # iterate over each row in the group
        for i, row in group.iterrows():
            print(i, row['Well_Name'])



if __name__ == '__main__':
    main()

# ============= EOF =============================================
