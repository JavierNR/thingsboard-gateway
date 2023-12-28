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

"""Import libraries"""

import time
from random import choice
from string import ascii_lowercase
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
import ctypes
from collections import namedtuple
import struct

try:
    import pyads
except ImportError:
    print("PyADS library not found - installing...")
    TBUtility.install_package('pyads')
    import pyads

from thingsboard_gateway.connectors.connector import Connector # Import base class for connector and logger
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import init_logger

#from thingsboard_gateway.extensions.ads.ads_converter import AdsUplinkConverter

# Tuple to hold data needed for ads notification
NotificationItem = namedtuple(
    "NotificationItem", "hnotify huser name plc_datatype callback"
)


class ADSConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()    # Initialize parents classes
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}    # Dictionary, will save information about count received and sent messages.
        self._connector_type = connector_type
        self.__config = config  # Save configuration from the configuration file.
        self.__gateway = gateway  # Save gateway object, we will use some gateway methods for adding devices and saving data from them.
        self.setName(self.__config.get("name",
                                       "Custom %s connector " % self.get_name() + ''.join(
                                           choice(ascii_lowercase) for _ in range(5))))  # get from the configuration or create name for logs.
        
        self._log = init_logger(self.__gateway, self.name, level=self.__config.get('logLevel'))
        self._log.info("Starting Custom %s connector", self.get_name())  # Send message to logger
        self.daemon = True  # Set self thread as daemon
        self.stopped = True  # Service variable for check state
        self.__connected = False  # Service variable for check connection to device
        self.__devices = {}  # Dictionary with devices, will contain devices configurations, converters for devices and serial port objects
        self.__plc = []
        self.__notification_items = {}

        self.__load_converters(connector_type)  # Call function to load converters and save it into devices dictionary
        # done in load_converters devices_config = self.__config.get('devices')
        #self.__create_routes() # Create AMS route to destination PLC
        self.__connect_to_devices()  # Call function for connect to devices
        #self._log.info('Custom connector %s initialization success.', self.get_name())  # Message to logger
        #self._log.info("Devices in configuration file found: %s ", '\n'.join(device for device in self.__devices))  # Message to logger

    def __create_routes(self):
        # Set AMS local address
        pyads.open_port()
        pyads.set_local_address(self.__config.get('SenderAMS'))
        #pyads.close_port()

        # Create route on destination plc's
        for device in self.__config.get('devices'):
            try:
                pyads.add_route_to_plc(self.__config.get('SenderAMS'),self.__config.get('SenderHostName'),device.get('PLCAddress'),device.get('username'),device.get('password'),device.get('routename'))
                #should return true if OK
                pyads.close_port()
                
            except Exception as e:
                self._log.error("Exceptions adding routes")
                self._log.exception(e) 
                pyads.close_port()

    def __connect_to_devices(self): #opens connections to devices
        self._log.debug("Connect to devices. Full device list")
        #self._log.debug(self.__devices)
        for device in self.__devices:
            self._log.debug(self.__devices[device]["device_config"])
        for device in self.__config.get('devices'):    
            #self._log.debug(device)
            try:
                connection_start = time.time()
                self.__plc = pyads.Connection(device.get('AMSnetID'),device.get('port'),device.get('PLCAddress'))
                self.__plc.open() #Should get Info: Connected to xxx.xxx.xx.xx here..

            except pyads.ADSError as err:
                self._log.error("ADS Error: %s connecting to device", err)
                
            except Exception as e:
                self._log.error("Exceptions")
                self._log.exception(e)
            else:  # if no exception handled - add device and change connection state
                #self._log.debug([device]["device_config"]["name"])
                #self._log.debug(self.__devices[device]["device_config"]["name"])
                #self.__gateway.add_device(self.__devices[device]["device_config"]["name"], {"connector": self}, self.__devices[device]["device_config"]["type"])
                self.__connected = True
                def update(name, value):
                    self._log.debug("Variable %s changed its value to %d", name, value)
                    #self.run.converted_data = value

                self.add_device_notification("MAIN.LuxExterior", pyads.PLCTYPE_INT, update)


    def add_device_notification(self, name, plc_datatype, callback):
        """Add notification to the ADS device """
        attr = pyads.NotificationAttrib(ctypes.sizeof(plc_datatype))

        try:
            hnotify, huser = self.__plc.add_device_notification(
                name, attr, self._device_notification_callback
            )
        except pyads.ADSError as err:
            self._log.error("Error subscribing to %s: %s", name, err)
        else:
            hnotify = int(hnotify)
            self.__notification_items[hnotify] = NotificationItem(
                hnotify, huser, name, plc_datatype, callback
            )

            self._log.debug(
                "Added device notification %d for variable %s", hnotify, name
            )

    def _device_notification_callback(self, notification, name):
        """Handle device notifications"""
        contents = notification.contents
        hnotify = int(contents.hNotification)
        self._log.debug("Received notification %d", hnotify)

        #Get dinamically sized data array
        data_size = contents.cbSampleSize
        data = (ctypes.c_ubyte * data_size).from_address(
            ctypes.addressof(contents)
            + pyads.structs.SAdsNotificationHeader.data.offset
        )

        try:
            notification_item = self.__notification_items[hnotify]
        except KeyError:
            self._log.error("Unknown device notification handle: %d", hnotify)
            return
        
        #Parse data
        if notification_item.plc_datatype == pyads.PLCTYPE_BOOL:
            value = bool(struct.unpack("<?", bytearray(data))[0])
        elif notification_item.plc_datatype == pyads.PLCTYPE_INT:
            value = struct.unpack("<h", bytearray(data))[0]
        elif notification_item.plc_datatype == pyads.PLCTYPE_BYTE:
            value = struct.unpack("<B", bytearray(data))[0]
        elif notification_item.plc_datatype == pyads.PLCTYPE_UINT:
            value = struct.unpack("<H", bytearray(data))[0]
        elif notification_item.plc_datatype == pyads.PLCTYPE_DINT:
            value = struct.unpack("<i", bytearray(data))[0]
        elif notification_item.plc_datatype == pyads.PLCTYPE_UDINT:
            value = struct.unpack("<I", bytearray(data))[0]
        else:
            value = bytearray(data)
            self._log.warn("No callback available for this datatype")

        notification_item.callback(notification_item.name, value)

    """def __ads_notification(self):
        keys = ['attributes', 'timeseries']
        device_config = self.__config.get('devices')

        for device in device_config:
            vars_mapping = device['mapping']
            for mapping in vars_mapping:
                for key in keys: #attributes and timeseries
                    tags = mapping[key]
                    for tag in tags:
                        self.__interest_variables.update({tag['tag'] : tag['type']})
                        
                        
                    



        self._log.debug(self.__interest_variables)
        symbol = self.__plc.get_symbol(self.__interest_variables)
        symbol.add_device_notification(cb)

 # define the callback which extracts the value of the variable
    def mycallback(notification, data):
        data_type = tags[data]
        handle, timestamp, value = plc.parse_notification(notification, data_type)
        print(value)

    attr = pyads.NotificationAttrib(sizeof(pyads.PLCTYPE_INT))

    # add_device_notification returns a tuple of notification_handle and
    # user_handle which we just store in handles
    handles = self._plc.add_device_notification('GVL.integer_value', attr, mycallback)
"""


    def open(self):  # Function called by gateway on start
        self.stopped = False
        self.start()

    def get_name(self):  # Function used for logging, sending data and statistic
        return self.name
    
    def get_type(self):
        return self._connector_type

    def is_connected(self):  # Function for checking connection state
        return self.__connected
    
    def get_config(self):
        return self.__config

    def __load_converters(self, connector_type):  # Function for search a converter and save it.
        devices_config = self.__config.get('devices')
        try:
            if devices_config is not None:
                for device_config in devices_config:
                    if device_config.get('converter') is not None:
                        self._log.debug("Converter found,")
                        converter = TBModuleLoader.import_module(connector_type, device_config['converter'])
                        self.__devices[device_config['name']] = {'converter': converter(device_config, self._log),
                                                                 'device_config': device_config}
                    else:
                        self._log.error('Converter configuration for the custom connector %s -- not found, please check your configuration file.', self.get_name())
            else:
                self._log.error('Section "devices" in the configuration not found. A custom connector %s has being stopped.', self.get_name())
                self.close()
        except Exception as e:
            self._log.exception(e)

    def run(self):  # Main loop of thread
        try:
            while not self.stopped:
                for device in self.__devices:

                    """device_serial_port = self.__devices[device]["serial"]
                    received_character = b''
                    data_from_device = b''
                    while not self.stopped and received_character != b'\n':  # We will read until receive LF symbol
                        try:
                            received_character = device_serial_port.read(1)  # Read one symbol per time
                        except AttributeError as e:
                            if device_serial_port is None:
                                self.__connect_to_devices()  # if port not found - try to connect to it
                                raise e
                        except Exception as e:
                            self._log.exception(e)
                            break
                        else:
                            data_from_device = data_from_device + received_character"""
                    try:
                        #if len(data_from_device) > 0:
                        #    converted_data = self.__devices[device]['converter'].convert(self.__devices[device]['device_config'], data_from_device)
                        #    self.__gateway.send_to_storage(self.get_name(), converted_data)
                        time.sleep(.1)
                    except Exception as e:
                        self._log.exception(e)
                        self.close()
                        raise e
                if not self.__connected:
                    break
        except Exception as e:
            self._log.exception(e)

        
    def close(self):  # Close connect function, usually used if exception handled in gateway main loop or in connector main loop
        self.stopped = True
        
        for notification_item in self.__notification_items.values():
            self._log.debug(
                "Deleting device notification %d, %d",
                notification_item.hnotify,
                notification_item.huser
            )
            try:
                self.__plc.del_device_notification(
                    notification_item.hnotify, notification_item.huser
                )
            except pyads.ADSError as err:
                self._log.error(err)
        try:
            self.__plc.close()
        except pyads.ADSError as err:
            self._log.error(err)

        for device in self.__devices:
            self.__gateway.del_device(self.__devices[device]["device_config"]["name"])
            if self.__devices[device]['serial'].isOpen():
                self.__devices[device]['serial'].close()
        self._log.reset()

    def on_attributes_update(self, content):  # Function used for processing attribute update requests from ThingsBoard
        self._log.debug(content)
        if self.__devices.get(content["device"]) is not None:
            device_config = self.__devices[content["device"]].get("device_config")
            if device_config is not None and device_config.get("attributeUpdates") is not None:
                requests = device_config["attributeUpdates"]
                for request in requests:
                    attribute = request.get("attributeOnThingsBoard")
                    self._log.debug(attribute)
                    if attribute is not None and attribute in content["data"]:
                        try:
                            value = content["data"][attribute]
                            str_to_send = str(request["stringToDevice"].replace("${" + attribute + "}", str(value))).encode("UTF-8")
                            self.__devices[content["device"]]["serial"].write(str_to_send)
                            self._log.debug("Attribute update request to device %s : %s", content["device"], str_to_send)
                            time.sleep(.01)
                        except Exception as e:
                            self._log.exception(e)

    def server_side_rpc_handler(self, content):
        pass
