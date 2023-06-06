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

import click as click


def error(msg):
    click.secho("------------------------------------------------------", fg=13)
    click.secho(f"  {msg}", fg=13)
    click.secho("------------------------------------------------------", fg=13)


def warning(msg):
    click.secho(msg, fg="red")


def info(msg):
    click.secho(msg, fg="green")


def message(msg, fg="yellow"):
    click.secho(msg, fg=fg)


def write_csv(records, export_name, func=None, header=None):
    with open(export_name, "w") as f:
        writer = csv.writer(f)
        if header is None:
            header = func(records[0].keys())
        if func is None:

            def func(r):
                return [r[k] for k in header]

        writer.writerow(header)
        for record in records:
            row = func(record)
            writer.writerow(row)


# ============= EOF =============================================
