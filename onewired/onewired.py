#!/usr/bin/env python

import sys
import time
import Queue
import threading
from socket import socket
from datetime import datetime
import logging
from locationtempstats import LocationTempStats
from settings import Settings

try:
    import ow
except ImportError:
    print "Please install python-ow"
    sys.exit(1)


class OneWireDaemon(object):
    _shutdown = False
    _locationhist = []
    _threads = []

    def __init__(self, cfgfilepath):
        self.cfg = Settings(cfgfilepath)
        self.setup_logging()
        self.locationmap = self.cfg.locationmap
        logging.info("OneWireDaemon Init")
        if self.cfg.webservice['enable'] is True:
            self._start_ws()
        #self.redis = redis.Redis(host=self.cfg.redis['host'], port=6379, db=0)
        self.rawdataqueue = Queue.Queue()
        self.normalizedqueue = Queue.Queue()
        self.start()

    @property
    def shutdown(self):
        return self._shutdown

    @shutdown.setter
    def shutdown(self, please):
        if please is True:
            self._shutdown = True
        return self._shutdown

    def setup_logging(self):
        logging.root.name = "OneWireD"
        logging.root.setLevel(logging.DEBUG)  # level must be set equal or lower to any handler (hard coded for now)
        logformat = logging.Formatter(fmt='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        streamLogger = logging.StreamHandler()
        streamLogger.setLevel(self._get_loglevel(self.cfg.onewire['basicloglevel']))
        streamLogger.setFormatter(logformat)
        logging.root.addHandler(streamLogger)

        if self.cfg.graylog['enable'] is True:
            self.setup_graylog()
        logging.info("Logging Initialized")

    def locationhist(self, location):
        try:
            return [x for x in self._locationhist if x.alias == location.lower()][0]
        except Exception:
            logging.warn("No location history for {}".format(location))
            return None

    def setup_graylog(self):
        try:
            import graypy
        except ImportError:
            logging.critical("Please Install 'graypy'")
        graypyhandler = graypy.GELFHandler('logs', 12201)
        logformat = logging.Formatter(fmt='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        graypyhandler.setFormatter(logformat)
        logging.root.addHandler(graypyhandler)

    def _get_loglevel(self, loglvl):
        lvlmap = {'info': logging.INFO,
                  'warning': logging.WARNING,
                  'warn': logging.WARNING,
                  'crit': logging.CRITICAL,
                  'critical': logging.CRITICAL,
                  'debug': logging.DEBUG}
        try:
            return lvlmap[loglvl.lower()]
        except KeyError:
            raise Exception("Could not determine Log Level")

    def _start_ws(self):
        from ws import WebService
        ws = WebService(self)
        ws.setDaemon(True)
        ws.start()

    def start(self):
        logging.info("OneWireD Starting")
        self.start_threads()
        self.wait()

    def start_threads(self):
        ow_reader = OneWireReader(self)
        # In this simplified version and using a small number of sensors,
        # separate normalizer/datalogger threads aren't needed
        # but I didn't feel like taking them out
        ow_normalizer = OneWireNormalizer(self)
        ow_datalogger = OneWireDataLogger(self)
        self._threads.append(ow_reader)
        self._threads.append(ow_normalizer)
        self._threads.append(ow_datalogger)

        for t in self._threads:
            t.setDaemon(True)
            t.start()

    def wait(self):
        for t in self._threads:
            t.join()


class OneWireReader(threading.Thread):
    def __init__(self, owd):
        self.owd = owd
        threading.Thread.__init__(self)
        self.setName("OW READER")
        logging.info("OW Reader Start")

    def run(self):
        while True:
            try:
                self.ow_reader()
            except Exception as e:
                try:
                    logging.critical(e)
                except Exception:
                    pass

    def ow_reader(self):
        '''just abstracting the 'run' function out for further error handleing'''
        time.sleep(5)
        while True:
            try:
                sensordata = self.read_sensors()
                if sensordata:
                    logging.debug("Adding data to raw data queue for processing")
                    self.owd.rawdataqueue.put((datetime.now(), sensordata))
                del sensordata
            except Exception:
                pass
            self.sleeptillminute()

    def sleeptillminute(self):
        t = datetime.utcnow()
        sleeptime = 60 - (t.second + t.microsecond/1000000.0)
        time.sleep(sleeptime)

    def c_to_f(self, c):  # Convert temp c to f
        temp = (float(c) * 9.0/5.0) + 32.0
        return temp

    def read_sensors(self):
        sensordata = {}
        try:
            ow.init(self.owd.cfg.onewire['controller'])
        except ow.exNoController:
            logging.critical("Can't Access Controller")
            return False
        try:
            logging.debug("Reading OneWire Bus")
            sensors = ow.Sensor("/").sensorList()
            logging.debug("Done reading OneWire Bus")
        except Exception as e:
            logging.warning("Error reading sensors: {0}".format(e))
            return False
        try:
            for sensor in sensors:
                if sensor.type != 'DS18B20':
                    sensors.remove(sensor)
                else:
                    try:
                        tempc = sensor.temperature
                    except Exception:
                        logging.warning("error reading sensor")
                    if tempc == 85:
                        logging.warning("bad temp recieved")
                    else:
                        sensordata[sensor.r_address] = self.c_to_f(tempc)
            logging.debug(str(sensordata))
        except Exception as e:
            logging.warning("error reading temps: {0}".format(e))
        if len(sensordata) == 0:
            logging.critical("No temps read")
        return sensordata


class OneWireNormalizer(threading.Thread):
    def __init__(self, owd):
        self.owd = owd
        threading.Thread.__init__(self)
        self.setName("OW Normalizer")
        logging.info("OW Normalizer Start")

    def run(self):
        while True:
            try:
                self.ow_normalizer()
            except Exception as e:
                try:
                    logging.critical(e)
                except Exception:
                    pass

    def ow_normalizer(self):
        while True:
            newdata = self.owd.rawdataqueue.get()
            logging.debug("Normalizer got item from rawdata queue")
            try:
                self.owd.normalizedqueue.put(self.normalize(newdata))
            except Exception as e:
                logging.warning("Error Normalizing: {0}".format(e), exc_info=1)
            else:
                logging.debug("Normalizer added processed data to queue")

    def normalize(self, rawdata):
        '''Only mapped sensors will get normalized/logged'''
        normalized_data = {}
        ts = rawdata[0]
        data = rawdata[1]

        # easier to iterate over locations when multiple sensors are involved
        for location, sensors in self.owd.locationmap.iteritems():
            temp = None
            try:
                if isinstance(sensors, str):
                    temp = data[sensors]
                elif isinstance(sensors, list):
                    temp = self.avgSensors(sensors, data, location, normalized_data)
                if temp:
                    normalized_data[location] = temp
            except KeyError:
                pass
        return (ts, normalized_data)

    def avgSensors(self, sensors, data, location, normalized_data):
        temps = [data[x] for x in sensors]
        temp = round((sum(temps) / len(temps)), 3)
        for raw_temp in temps:
            if abs(temp - raw_temp) > 1.0:
                logging.warn("Multiple Sensors assigned same location must read within 1 degree of each other")
                logging.warn("Discarding results for '{0}'; temps: {1}".format(location, temps))
                return None
        return temp


class OneWireDataLogger(threading.Thread):
    def __init__(self, owd):
        self.owd = owd
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                self.ow_datalogger()
            except Exception as e:
                try:
                    logging.critical(e)
                except Exception:
                    pass

    def ow_datalogger(self):
        self.setName("OW DataLogger")
        logging.info("OW DataLogger Start")
        while True:
            newdata = self.owd.normalizedqueue.get()
            self.save_locally(newdata)
            self.log_data(newdata)

    def save_locally(self, newdata):
        ts, data = newdata
        for location in data:
            if location not in [x.alias for x in self.owd._locationhist]:
                self.owd._locationhist.append(LocationTempStats(location))
            self.owd.locationhist(location).add(data[location], ts)

    def log_data(self, newdata):
        self.log_to_logging(newdata)
        try:
            if self.owd.cfg.graphite['enable'] is True:
                self.log_to_graphite(newdata)
        except KeyError:
            logging.warning("Please enable or disable Graphite in the config file")
        except Exception as e:
            logging.critical("could not log to graphite: {0}".format(e))
        try:
            if self.owd.cfg.redis['enable'] is True:
                self.log_to_redis(newdata)
        except KeyError:
            logging.warning("Please enable or disable Redis in the config file")
        except Exception as e:
            logging.critical("could not log to redis: {0}".format(e))

    def log_to_logging(self, newdata):
        try:
            for location, temp in newdata[1].iteritems():
                logging.info("{}: {}".format(location, temp))
        except KeyError:
            pass

    def log_to_graphite(self, newdata):
        CARBON_SERVER = self.owd.cfg.graphite['host']
        CARBON_PORT = int(self.owd.cfg.graphite['port'])
        try:
            sock = socket()
            ts = newdata[0]
            data = newdata[1]

            sock.connect((CARBON_SERVER, CARBON_PORT))
            now = int(time.time())
            lines = []
            for loc in data.keys():
                if data[loc] == 999:
                    continue
                lines.append("{0}.{1} {2} {3}".format(self.owd.cfg.graphite['namespace'], loc, data[loc], now))
            message = '\n'.join(lines) + '\n'  # all lines must end in a newline
            sock.sendall(message)
            logging.debug("Graphite Message: {0}".format(message))
        except Exception as e:
            logging.critical("could not log to graphite: {0}".format(e))
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def log_to_redis(self, data):
        data = data[1]
        r = self.owd.redis
        for loc in data.keys():
            rkey = '{0}:{1}'.format(self.owd.cfg.redis['namespace'], loc)
            logging.debug("REDIS -- loc: {0}, rkey: {1} data: {2}".format(loc, rkey, data[loc]))
            r.setex(rkey, data[loc], int(self.owd.cfg.redis['ttl']))
        logging.debug("New data stored in redis, telling cortana")
        #r.publish('cortana:msg:hvac', 'newdata') # can push to pubsub channel on redis,  should be a config option


if __name__ == "__main__":
    pass
