# -*- coding: utf-8 -*-
# RETRIEVE_DATA.py
# Author: Jeremy
# Description: Client Seedlink adapté à MONA DASH

import argparse
import time

import obspy

from utils import get_network_list
from threading import Thread
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

from calendar import timegm
from datetime import datetime

from obspy.clients.seedlink.client.seedlinkconnection import SeedLinkConnection
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient
from obspy.clients.seedlink.seedlinkexception import SeedLinkException
from obspy.clients.seedlink.slpacket import SLPacket

# from config import *


class SeedLinkInfluxClient(EasySeedLinkClient):
    def __init__(self, server_sl_url, server_influx_url, bucket, token, org,
                 data_retrieval=False, begin_time=None, end_time=None):
        self.network_list_values = []
        # self.network_list = []
        try:
            super(SeedLinkInfluxClient, self).__init__(server_sl_url, autoconnect=True)
            self.conn.timeout = 30
            self.data_retrieval = data_retrieval
            self.begin_time = begin_time
            self.end_time = end_time
            self.connected = 0

            self.connected = get_network_list('server', [], self.network_list_values,
                                              server_hostname=self.server_hostname, server_port=self.server_port)

            print(self.network_list_values)

            self.streams = self.network_list_values

        except SeedLinkException:
            pass

        try:
            self.server_influx = server_influx_url
            self.bucket = bucket
            self.token = token
            self.org = org
            self.client_influx = InfluxDBClient(url=self.server_influx, token=self.token, org=self.org)
            self.write_api = self.client_influx.write_api(SYNCHRONOUS)
        except ValueError:
            pass

    def on_data(self, tr):
        print(tr)

        # tr.resample(sampling_rate=25.0)
        t_start = obspy.UTCDateTime()
        tr.detrend(type='constant')

        if tr is not None:
            if tr.stats.location == '':
                station = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.channel
            else:
                station = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.location + '.' + tr.stats.channel

            data = []
            timestamp_start = int(tr.stats.starttime.timestamp * 1e3)
            for i, seismic_point in enumerate(tr.data):
                timestamp = timestamp_start + i * int(tr.stats.delta * 1e3)
                data.append({
                    "measurement": "SEISMIC_DATA",
                    "tags": {"location": station},
                    "fields": {
                        "trace": int(seismic_point),
                    },
                    "time": timestamp
                })

            self.write_api.write(self.bucket, self.org, record=data, write_precision=WritePrecision.MS)
            t_stop = obspy.UTCDateTime()

            print(f'{station} sent to {self.bucket} in {t_stop-t_start}')
            # try:
            #     data_sta = pd.read_feather(BUFFER_DIR + '/' + station + '.data')
            #     if len(data_sta) <= 30000:
            #         data_sta = pd.concat([data_sta, new_data_sta])
            #     else:
            #         data_sta = pd.concat([data_sta[round(-len(data_sta) / 2):], new_data_sta])
            # except FileNotFoundError:
            #     data_sta = new_data_sta
            # data_sta = data_sta.reset_index(drop=True)
            # data_sta.to_feather(BUFFER_DIR + '/' + station + '.data')
        else:
            print("blockette contains no trace")

    def run(self):
        for station in self.streams:
            full_sta_name = station.split('.')
            net = full_sta_name[0]
            sta = full_sta_name[1]
            cha = full_sta_name[2] + full_sta_name[3]
            self.select_stream(net, sta, cha)
        while True:

            data = self.conn.collect()

            if data == SLPacket.SLTERMINATE:
                self.on_terminate()
                break
            elif data == SLPacket.SLERROR:
                self.on_seedlink_error()
                continue

            # At this point the received data should be a SeedLink packet
            # XXX In SLClient there is a check for data == None, but I think
            #     there is no way that self.conn.collect() can ever return None
            assert(isinstance(data, SLPacket))

            packet_type = data.get_type()

            # Ignore in-stream INFO packets (not supported)
            if packet_type not in (SLPacket.TYPE_SLINF, SLPacket.TYPE_SLINFT):
                # The packet should be a data packet
                trace = data.get_trace()
                # Pass the trace to the on_data callback
                self.on_data(trace)


    # ADAPT
    def on_terminate(self):
        self._EasySeedLinkClient__streaming_started = False
        self.close()

        del self.conn
        self.conn = SeedLinkConnection(timeout=30)
        self.conn.set_sl_address('%s:%d' %
                                 (self.server_hostname, self.server_port))
        if self.data_retrieval is False:
            self.connect()

        # self.conn.begin_time = UTCDateTime()

    def on_seedlink_error(self):
        self._EasySeedLinkClient__streaming_started = False
        self.close()
        self.streams = self.conn.streams.copy()
        del self.conn
        if self.data_retrieval is False:
            self.conn = SeedLinkConnection(timeout=30)
            self.conn.set_sl_address('%s:%d' %
                                     (self.server_hostname, self.server_port))
            self.conn.streams = self.streams.copy()
            self.run()


class SLThread(Thread):
    def __init__(self, name, client):
        Thread.__init__(self)
        self.name = name
        self.client = client

    def run(self):
        print('Starting Thread ', self.name)
        print('Server: ', self.client.server_hostname)
        print('Port:', self.client.server_port)
        print("--------------------------\n")
        print('Network list:', self.client.network_list_values)
        self.client.run()

    def close(self):
        self.client.close()


def get_arguments():
    """returns AttribDict with command line arguments"""
    parser = argparse.ArgumentParser(
        description='Launch a seedlink  and write the data into influxdb v2',
        formatter_class=argparse.RawTextHelpFormatter)

    # Script functionalities
    parser.add_argument('-s', '--server-sl', help='Path to SL server', required=True)
    parser.add_argument('-p', '--port-sl', help='Port of the SL server')
    parser.add_argument('-S', '--server-influx', help='Path of influx server', required=True)
    parser.add_argument('-P', '--port-influx', help='Port of influx server')
    parser.add_argument('-b', '--bucket', help='Name of the bucket', required=True)
    parser.add_argument('-o', '--org', help='Name of the organization', required=True)
    parser.add_argument('-t', '--token', help='Token authorization of influxdb', required=True)
    # parser.add_argument('-m', '--mseed', help='Path to mseed data folder', required=True)

    args = parser.parse_args()

    if args.port_sl is None:
        args.port_sl = '18000'
    if args.port_influx is None:
        args.port_influx = '8086'

    print(f'Server SL: {args.server_sl} ; Port: {args.port_sl}')
    print(f'Server Influx: {args.server_influx} ; Port: {args.port_influx}')
    print("--------------------------\n"
          "Starting Seedlink server and verifying Influx connection...")

    return args


if __name__ == '__main__':
    args = get_arguments()

    network_list = []
    network_list_values = []

    client = SeedLinkInfluxClient(args.server_sl + ':' + args.port_sl, args.server_influx + ':' + args.port_influx,
                                  args.bucket, args.token, args.org)

    print('Network list:', network_list_values)

    client.run()
