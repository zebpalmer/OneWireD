#!/usr/bin/env python

import sys
import time
import Queue
import threading
from socket import socket
from datetime import datetime
import logging
from ConfigParser import SafeConfigParser

try:
    import ow
except ImportError:
    print "Please install python-ow"
    sys.exit(1)


#import psycopg2
#import psycopg2.extras


class Settings(object):
    def __init__(self, cfgfilepath):
        try:
            settings = SafeConfigParser()
            settings.read(cfgfilepath)
        except Exception as e:
            raise Exception(e)

        try:
            for section in settings.sections():
                temp = {}
                for item in settings.items(section):
                    lines = [x.strip() for x in item[1].split(',')]
                    if len(lines) > 1:
                        temp[item[0]] = lines
                    else:
                        temp[item[0]] = item[1]

                self.__dict__[section] = temp
        except Exception:
            raise Exception('Error in config file')

    def __getattr__(self, name):
        logging.warn("Config Section Not Found (note: inteligent IDE's will commonly/randomly trigger this)")
        return {}


class OneWireDaemon(object):
    def __init__(self, cfgfilepath):
        self.cfg = Settings(cfgfilepath)
        self.setup_logging()
        self.locationmap = self.cfg.locationmap
        logging.info("OneWireDaemon Init")
        #self.redis = redis.Redis(host=self.cfg.redis['host'], port=6379, db=0)
        self.threads = []
        self.rawdataqueue = Queue.Queue()
        self.normalizedqueue = Queue.Queue()
        self.start()

    def setup_logging(self):
        logging.root.name = "OneWireD"
        logging.root.setLevel(logging.DEBUG)  # level must be set equal or lower to any handler (hard coded for now)
        logformat = logging.Formatter(fmt='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        streamLogger = logging.StreamHandler()
        streamLogger.setLevel(self._get_loglevel(self.cfg.onewire['basicloglevel']))
        streamLogger.setFormatter(logformat)
        logging.root.addHandler(streamLogger)

        if self.cfg.graylog['enable'].lower() == 'true':
            self.setup_graylog()
        logging.info("Logging Initialized")


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
                  'debug': logging.DEBUG,}
        try:
            return lvlmap[loglvl.lower()]
        except KeyError:
            raise Exception("Could not determine Log Level")


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
        self.threads.append(ow_reader)
        self.threads.append(ow_normalizer)
        self.threads.append(ow_datalogger)

        for t in self.threads:
            t.setDaemon(True)
            t.start()

    def wait(self):
        for t in self.threads:
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


    def c_to_f(self, c): # Convert temp c to f
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
                    sensors.remove( sensor )
                else:
                    try:
                        tempc =  sensor.temperature
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
                except:
                    pass



    def ow_normalizer(self):
        while True:
            newdata = self.owd.rawdataqueue.get()
            logging.info("Normalizer got item from rawdata queue")
            try:
                self.owd.normalizedqueue.put(self.normalize(newdata))
            except Exception as e:
                logging.warning("Error Normalizing: {0}".format(e))
            logging.info("Normalizer added processed data to queue")

    def normalize(self, rawdata):
        normalized_data = {}
        ts = rawdata[0]
        data = rawdata[1]

        lm = self.owd.locationmap
        for loc in lm.keys():
            sensors = lm[loc].split(',')
            try:
                if len(lm[loc]) == 1:
                    temp = data[lm[loc][0]]
                elif len(lm[loc]) > 1:
                    temps = [ data[x] for x in lm[loc]]
                    temp = round((sum(temps) / len(temps)), 3)
                    for raw_temp in temps:
                        if abs(temp - raw_temp) > 1.0:
                            temp = 999
                normalized_data[loc] = temp
            except KeyError:
                pass
        return (ts, normalized_data)



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
            self.log_data(newdata)

    def log_data(self, newdata):
        try:
            log_to_postgres(newdata)
        except Exception as e:
            logging.critical("could not log to postgres: {0}".format(e))
        try:
            log_to_graphite(newdata)
        except Exception as e:
            log.critical("could not log to graphite: {0}".format(e))
        try:
            log_to_redis(newdata)
        except Exception as e:
            logging.critical("could not log to redis: {0}".format(e))


    def log_to_postgres(self, newdata):
        state_log_type = 'temp'
        ts = newdata[0]
        data = newdata[1]

        db = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(self.owd.cfg.state_log['host'],
                                                                        self.owd.cfg.state_log['dbname'],
                                                                        self.owd.cfg.state_log['user'],
                                                                        self.owd.cfg.state_log['passwd'])
        connection = psycopg2.connect(db)
        cursor = connection.cursor()
        logging.info("feeding data to state_log")
        for loc in data.keys():
            last_temp = None
            temp = round(data[loc], 1)
            if temp == 999:
                continue
            last_temp = self.last_state(state_log_type, loc)[1]
            if last_temp == None or abs(float(last_temp) - temp) >= 0.2:
                insert = """INSERT INTO state_log (ts, type, name, value)
                          VALUES (NOW(), '{0}', '{1}', '{2}');""".format(state_log_type, loc, temp)
                cursor.execute(insert)
        connection.commit()
        connection.close()
        logging.info("Done feeding data to state_log")

    def last_state(self, stype, sname):
        db = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(self.owd.cfg.state_log['host'],
                                                                        self.owd.cfg.state_log['dbname'],
                                                                        self.owd.cfg.state_log['user'],
                                                                        self.owd.cfg.state_log['passwd'])
        connection = psycopg2.connect(db)
        cursor = connection.cursor()
        query = """select ts, value from state_log
                  where type = '{0}'
                  and name = '{1}'
                  order by ts desc
                  limit 1 ;""".format(stype, sname)
        cursor.execute(query)
        results = cursor.fetchall()
        connection.commit()
        connection.close()
        if len(results) == 1:
            return results[0]
        else:
            return (None, None)

    def log_to_graphite(self, newdata):
        CARBON_SERVER = self.owd.cfg.graphite['host']
        CARBON_PORT = int(self.owd.cfg.graphite['port'])
        print CARBON_SERVER
        print CARBON_PORT
        try:
            sock = socket()
            ts = newdata[0]
            data = newdata[1]

            sock.connect( (CARBON_SERVER, CARBON_PORT) )
            now = int( time.time() )
            lines = []
            for loc in data.keys():
                if data[loc] == 999:
                    continue
                lines.append("{0}.{1} {2} {3}".format(self.owd.cfg.graphite['namespace'], loc, data[loc], now))
            message = '\n'.join(lines) + '\n' #all lines must end in a newline
            sock.sendall(message)
            logging.debug(message)
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



if __name__ == '__main__':
    if len(sys.argv) == 2:
        owd = OneWireDaemon(sys.argv[1])
        owd.start()
    else:
        print "I'll need a path to find the config file..."
        sys.exit(1)