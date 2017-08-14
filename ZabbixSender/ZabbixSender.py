#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import socket
import struct
import logging

try:
    import json
except:
    import simplejson as json


class ZabbixSender:

    def __init__(self, zabbix_host='127.0.0.1',
                 zabbix_port=10051, log=False, logfile=None):
            self.address = (zabbix_host, zabbix_port)
            self.data = []
            self.log = log
            if logfile:
                self.logfile = logfile
            else:
                self.logfile = 'sender.log'
            if self.log:
                self.logger = logging.getLogger(__name__)
                handler = logging.FileHandler(self.logfile)
                formatter = logging.Formatter('%(asctime)s %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.DEBUG)

    def add(self, host, key, value, clock=None):
        if clock is None:
            clock = int(time.time())
        self.data.append({"host": host, "key": key,
                          "value": value, "clock": clock})

    def __log(self, log):
        if self.log:
            self.logger.debug(log)

    def __connect(self):
        self.sock = socket.socket()
        try:
            self.sock.connect(self.address)
        except:
            raise Exception("Can't connect zabbix server.")

    def __close(self):
        self.sock.close()

    def __pack(self, request):
        string = json.dumps(request)
        header = struct.pack('<4sBQ', 'ZBXD', 1, len(string))
        return header + string

    def __unpack(self, response):
        header, version, length = struct.unpack('<4sBQ', response[:13])
        (data, ) = struct.unpack('<%ds' % length, response[13:13 + length])
        return json.loads(data)

    def __request(self, request):
        self.__connect()
        try:
            self.sock.sendall(self.__pack(request))
        except Exception as e:
            raise Exception("Failed sending data.\nERROR: %s" % e)

        response = ''
        while True:
            data = self.sock.recv(4096)
            if not data:
                break
            response += data

        self.__close()
        return self.__unpack(response)

    def send(self):
        if not self.data:
            self.__log("Not found sender data, end without sending.")
            return False

        request = {"request": "sender data", "data": self.data}
        response = self.__request(request)
        result = True if response['response'] == 'success' else False

        if result:
            for d in self.data:
                self.__log("[send data] %s" % d)
            self.__log("[send result] %s" % response['info'])
        else:
            raise Exception("Failed send data.")

        return result
