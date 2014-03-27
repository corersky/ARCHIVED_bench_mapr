import base64, string

from disco.test import TestCase, TestJob
from disco.util import kvgroup, shuffled
from disco.compat import bytes_to_str, str_to_bytes

alphanum = list(string.ascii_letters) + list(map(str, range(10)))
print alphanum

class SortJob(TestJob):
    sort = True

    @staticmethod
    def map(string, params):
        print "TRACE: in map"
        print string
        print params
        print shuffled((base64.encodestring(str_to_bytes(c)), b'') for c in bytes_to_str(string * 10))
        return shuffled((base64.encodestring(str_to_bytes(c)), b'') for c in bytes_to_str(string * 10))

    @staticmethod
    def reduce(iter, params):
        print "TRACE: in reduce"
        print iter
        print params
        for k, vs in kvgroup(iter):
            print "TRACE: in for loop"
            print k
            print vs
            print bytes_to_str(base64.decodestring(k)), len(list(vs))
            yield bytes_to_str(base64.decodestring(k)), len(list(vs))

class SortTestCase(TestCase):

    def serve(self, path):
        print "TRACE in serve"
        return ''.join(alphanum)

    def runTest(self):
        print "TRACE: in runTest"
        self.job = SortJob().run(input=self.test_server.urls([''] * 100))
        self.assertResults(self.job, sorted((c, 1000) for c in alphanum))
