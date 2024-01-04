#     Copyright 2022. ThingsBoard
#     Copyright 2024. Dual Matic Tecnología y Desarrollo S.L.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from time import time

from simplejson import dumps, loads

from thingsboard_gateway.extensions.ads.ads_converter import AdsConverter
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class AdsUplinkConverter(AdsConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self.__datatypes = {"attributes": "attributes",
                            "telemetry": "telemetry"}
        
    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')

    def convert(self, config, variable_to_convert, value_to_convert):
        #self._log.debug(self.__config) #must get here the mapping object only.... but we're getting entire config object

        dict_result = {
            'deviceName': self.__config.get('name', 'CustomADSDevice'),
            'deviceType': self.__config.get('deviceType', 'default'),
            'attributes': [],
            'telemetry': []
        }

        dict_incoming_data = {
            'variable' : variable_to_convert,
            'value' : value_to_convert
        }

        self._log.debug(dict_incoming_data)

        try:
            self._log.debug("Convert method")

            for data_type, data_list in self.__config['mapping'].items():
            #for data_type, data_list in self.__config['mapping'].get(datatype, []):
                self._log.debug("Data_type: %s ", data_type)
                self._log.debug("Data_list: %s ", data_list)
                for data in data_list:
                    if 'tag' in data and data['tag'] == dict_incoming_data['variable']:
                        self._log.debug("Data match on %s", data)
                        full_key = data['key']
                        full_value = dict_incoming_data['value']

                        dict_result[self.__datatypes[data_type]].append({full_key: full_value})

        except Exception as e:
            self._log.exception(e)

        return dict_result