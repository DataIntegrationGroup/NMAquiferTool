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

# ===============================================================================
# Database credentials
HOST = os.environ.get('NM_AQUIFER_HOST', '')
USER = os.environ.get('NM_AQUIFER_USER', '')
PWD = os.environ.get('NM_AQUIFER_PWD', '')
DB = 'NM_Aquifer'


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
        print(e)
        exit()

    return client
# ============= EOF =============================================