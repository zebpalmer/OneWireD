#!/usr/bin/env python
import threading
import logging
import json
from datetime import datetime
from time import sleep
# pylint: disable=W0611,W0612,R0912
from bottle import route, run, request, response, get, post, template


def jsonhandler(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError('Object of type {0} with value of {1} is not JSON serializable'.format(type(obj), repr(obj)))


def jsondump(raw):
    return json.dumps(raw, default=jsonhandler)


class WebService(threading.Thread):
    def __init__(self, owd):
        self.owd = owd
        threading.Thread.__init__(self)

    def run(self):
        self.setName("WEBSVC")
        logging.info("Starting Webservice")
        # pylint: disable=W0212
        while self.owd._shutdown is False:
            sleep(1)
            self.server()

    def server(self):
        @route('/')
        def index():
            return ("OneWireD")

        @get('/health')
        def health():
            response.content_type = 'application/json'
            health = {"status_code": 0, "status_msg": "OneWireD is running"}
            return jsondump(health)

        @get('/temp/<alias>/average')
        def average_temp(alias):
            #response.content_type = 'application/text'
            th = self.owd.locationhist(alias)
            if th:
                return str(th.average())
            else:
                return "NONE"

        @get('/temp/<alias>/current')
        def current_temp(alias):
            th = self.owd.locationhist(alias)
            if th:
                return str(th.current())
            else:
                return "NONE"

        # Start Server
        while self.owd.shutdown is False:
            status = "STARTED"
            try:
                wsport = self.owd.cfg.webservice['port']
            except KeyError:
                logging.critical("Cannot Start: No WS Port Defined in Config", exc_info=1)
                self.owd.shutdown = True
            else:
                try:
                    status = "RESTARTED"
                    run(host='0.0.0.0', port=wsport, quiet=True)
                except Exception as e:
                    logging.critical(e, exc_info=1)
                    sleep(1)
