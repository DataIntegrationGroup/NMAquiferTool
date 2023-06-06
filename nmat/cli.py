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

import click as click


@click.command()
@click.option('--config', prompt='Config path',
              help='Path to configuration file. e.g config.yaml')
def cli_c(config):
    click.echo('Doing NMAT config run')
    click.echo(f'Using config file: {config}')
    from nmat.runner import run
    # print(os.path.curdir, os.getcwd())
    # config = os.path.join(os.getcwd(), './nmat/config/config.yaml')
    config = Path('./nmat/config/config.yaml')
    run(config)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--output', prompt='Specify output path',
              help='Output path')
def onyx_export(output):
    click.echo(f"Exporting to {output}")

    # """Simple program that greets NAME for a total of COUNT times."""
    # for x in range(count):
    #     click.echo(f"Hello {name}!")

# ============= EOF =============================================
