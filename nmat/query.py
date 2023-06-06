# ===============================================================================
# Copyright 2023 ross
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
from datetime import datetime, date

from nmat.db import get_db_client


def make_insert(table, attributes, values):
    attributes = ", ".join(attributes)
    values = ", ".join(
        [f"'{v}'" if isinstance(v, (str, datetime, date)) else str(v) for v in values]
    )

    sql = f"""
    INSERT INTO dbo.{table} ({attributes})
    VALUES ({values})"""
    return sql


def make_select(attributes="*", table="Location", where=None, order=None):
    sql = f"""
    SELECT {attributes} FROM dbo.{table}"""

    if where:
        sql = f"{sql} WHERE {where}"
    if order:
        sql = f"{sql} ORDER BY {order}"

    return sql


def make_csv(p, records):
    with open(p, "w") as wfile:
        writer = csv.writer(wfile)
        header = [str(k) for k in records[0].keys()]
        writer.writerow(header)
        for record in records:
            writer.writerow(record)


def execute_fetch(sql, client=None, verbose=True, fetch="fetchall"):
    if client is None:
        client = get_db_client()

    if verbose:
        print("executing query================")
        print("sql: ", sql)
        print("===============================")

    cursor = client.cursor(as_dict=True)
    cursor.execute(sql)
    func = getattr(cursor, fetch)
    return func()


def execute_insert(sql, client=None, verbose=True, dry=True):
    if verbose:
        print("executing insert================")
        print("sql: ", sql)
        print("===============================")

    cursor = client.cursor()
    cursor.execute(sql)
    if not dry:
        cursor.commit()


# ============= EOF =============================================
