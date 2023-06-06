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
import os

import yaml

# ===============================================================================
# Database credentials
# cp = './config/credentials.yaml'
# ycfg = {}
# if os.path.isfile(cp):
#     with open(cp, 'r') as f:
#         ycfg = yaml.load(f, Loader=yaml.FullLoader)
#

def get_credential(key):
    return os.environ.get(key, 'default')
    # return os.environ.get(key, ycfg.get(key, 'default'))


HOST = get_credential('NM_AQUIFER_HOST')
USER = get_credential('NM_AQUIFER_USER')
PWD = get_credential('NM_AQUIFER_PWD')
DB = get_credential('NM_AQUIFER_DB')


# ===============================================================================
def get_db_client():
    """
    This function is used to connect to the database.
    :return:
    """

    import pymssql
    try:
        client = pymssql.connect(HOST, USER, PWD, DB, login_timeout=1)
    except pymssql.OperationalError as e:
        print('Error connecting to database. Check your credentials.')
        print('Using credentials =============')
        print('HOST: ', HOST)
        print('USER: ', USER)
        print('DB: ', DB)
        print('===============================')
        if HOST == 'default' and USER == 'default' and DB == 'default':
            print('Please set db credentials in your config file or as environment variables ')
        print(e)
        exit()

    return client
# ============= EOF =============================================
