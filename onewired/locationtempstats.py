from datetime import datetime, timedelta
import logging


class LocationTempStats(object):
    _rawhist = []

    def __init__(self, alias, maxage_mins=60, recent_cutoff_mins=5):
        self._maxage_mins = maxage_mins
        self._recent = recent_cutoff_mins
        self.alias = alias

    def __repr__(self):
        return "<LocationTempStats: {0}>".format(self.alias)

    def add(self, temp, ts=datetime.now()):
        self._rawhist.insert(0, (temp, ts))
        self._prunehist()

    def _prunehist(self):
        cutoff = datetime.now() - timedelta(minutes=self._maxage_mins)
        while len(self._rawhist) > 0 and self._rawhist[-1][1] < cutoff:
            self._rawhist.pop()

    def current(self):
        '''Return most recent temperature, assuming its recent'''
        cutoff = datetime.now() - timedelta(minutes=5)
        current = self._rawhist[0]
        if current[1] > cutoff:
            return current[0]
        else:
            raise Exception("No Recent Data")

    def average(self, mins=5):
        cutoff = datetime.now() - timedelta(minutes=mins)
        l = [x[0] for x in self._rawhist if x[1] > cutoff]
        try:
            return round(float(sum(l)) / len(l), 2)
        except Exception:
            logging.warning('error', exc_info=1)
            return None

    @property
    def datapointcount(self):
        self._prunehist()
        return len(self._rawhist)
