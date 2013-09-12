from temperaturehistory import LocationTempStats
import unittest
from datetime import datetime, timedelta


#pylint: disable-msg=R0904
class Test_TemperatureHistory(unittest.TestCase):
    def setUp(self):
        pass

    def test_prune(self):
        now = datetime.now()
        th = LocationTempStats('test', maxage_mins=60, recent_cutoff_mins=5)
        for x in xrange(100, 0, -1):
            th.add(x, now - timedelta(minutes=x))
        self.assertTrue(th.current(), 1)
        self.assertTrue(th.datapointcount, 59)

    def test_avg(self):
        now = datetime.now()
        th = LocationTempStats('test', maxage_mins=60, recent_cutoff_mins=5)
        for x in xrange(100, 0, -1):
            th.add(x, now - timedelta(minutes=x))
        self.assertTrue(th.average() == 2.5)


if __name__ == "__main__":
    unittest.main()
