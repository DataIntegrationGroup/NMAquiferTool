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

import yaml


def run(config):
    print(config, os.path.isfile(config))
    # get configuration
    if os.path.isfile(config):
        with open(config, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

    # run the config
    run_c(config)


def run_c(config):
    for step in config["steps"]:
        func = step["function"]
        func = globals()[func]
        func(**step["args"])


def onyx_export(output):
    from nmat.onyx_export import export

    export(output)


def waterlevels(file, sheetname, **kw):
    # file = "./nmat/indata/BernalilloCountyUpdate2023.xlsx"
    # sheetname = "NewWaterLevelsF

    from nmat.bc_uploader import upload_waterlevels_from_file

    upload_waterlevels_from_file(file, sheetname, **kw)


def add_wells(file, sheetname, **kw):
    from nmat.bc_uploader import upload_wells_from_file

    upload_wells_from_file(file, sheetname, **kw)


# ============= EOF =============================================
