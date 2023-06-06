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
import os
from pathlib import Path

import click

from nmat.util import info, message


@click.command()
@click.option(
    "--config", prompt="Config path", help="Path to configuration file. e.g config.yaml"
)
def cli_c(config):
    # click.secho("Doing NMAT config run", fg="green")
    # click.secho(f"Using config file: {config}", fg="green")
    info("Doing NMAT config run")
    message(f"Using config file: {config}")
    from nmat.runner import run

    config = Path("./nmat/config/config.yaml")
    run(config)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--output", prompt="Specify output path", help="Output path")
def onyx_export(output):
    info(f"Exporting to {output}")
    from nmat.runner import onyx_export as export

    export(output)


@cli.group()
def upload():
    pass


@upload.command()
@click.option("--file", prompt="Specify input file path", help="Input file path")
@click.option("--sheetname", default='Sheet1', help="The sheet name")
@click.option("--dry_run", default=True, help="Dry run")
@click.option("--verbose", default=False, help="Verbose logging")
def waterlevels(file, sheetname, dry_run, verbose):
    from nmat.runner import waterlevels

    waterlevels(file, sheetname, dry=dry_run, verbose=verbose)

# ============= EOF =============================================
