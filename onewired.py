#!/usr/bin/env python

import ow
import Queue
import threading
from time import sleep
import psycopg2
import psycopg2.extras
from socket import socket
import time
import logging
from datetime import datetime
import redis
from ConfigParser import SafeConfigParser


#locationmap = { "crawlspace": ["7C000003A5A5AE28"],
                #"serverroom": ["E4000003C7184928"],
                #"livingroom": ["83000003A5A34328"],
                #"stairs": ["BB000003C7455528", "38000003A5B02E28"],
                #"garage": ["E2000003A5C5F628"],
                #"bedroom": ["15000003A5B7A728"],
                #"attic": ["6A000003A5A62828"],
                #"office": ["4D0000031BF7B528"],
                #"guestroom": ["F0000003A5B6C228"],
                #"outside": ["32000003A5BEEF28"]
                #}


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
        print 'Config Section Not Found'
        return {}


class OneWireDaemon(object):
    def __init__(self, cfgfilepath):
        self.cfg = Settings(cfgfilepath)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')
        logging = logging.getLogger()
        self.locationmap = self.cfg.locationmap
        logging.info("OneWireDaemon Init")
        self.threads = []
        self.rawdataqueue = Queue.Queue()
        self.normalizedqueue = Queue.Queue()
        self.start_threads()
        self.redis = redis.Redis(host=self.cfg.redis['host'], port=6379, db=0)
        self.wait()


    def start_threads(self):
        ow_reader = OneWireReader(self)
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
        sleep(5)
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
        sleep(sleeptime)


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
        self.postgrestoggle = True
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
            logging_data(newdata)

    def log_data(self, newdata):
        '''we only want to log every other run (at max) to the state_log, hack that'''
        try:
            if self.postgrestoggle == True:
                logging_to_postgres(newdata)
                self.postgrestoggle = False
            else:
                self.postgrestoggle = True
        except Exception as e:
            logging.critical("could not log to postgres: {0}".format(e))
        try:
            logging_to_graphite(newdata)
        except Exception as e:
            logging.critical("could not log to graphite: {0}".format(e))
        try:
            logging_to_redis(newdata)
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
                insert = "INSERT INTO state_log (ts, type, name, value) VALUES (NOW(), '{0}', '{1}', '{2}');".format(state_log_type, loc, temp)
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
                if loc == 'outside':
                    lines.append("cortana.env.external.temp.{0} {1} {2}".format(loc, data[loc], now))
                else:
                    lines.append("cortana.env.internal.temp.{0} {1} {2}".format(loc, data[loc], now))
            message = '\n'.join(lines) + '\n' #all lines must end in a newline
            sock.sendall(message)
            logging.debug(message)
        except Exception as e:
            logging.critical("could not log to graphite: {0}".format(e))
        finally:
            sock.close()

    def log_to_redis(self, data):
        data = data[1]
        r = self.owd.redis
        for loc in data.keys():
            rkey = 'cortana:env:raw:temp:{0}'.format(loc)
            logging.debug("REDIS -- loc: {0}, rkey: {1} data: {2}".format(loc, rkey, data[loc]))
            r.setex(rkey, data[loc], 300)
        logging.debug("New data stored in redis, telling cortana")
        r.publish('cortana:msg:hvac', 'newdata')



if __name__ == '__main__':
    cfg = Settings()
    owd = OneWireDaemon(locationmap, cfg)