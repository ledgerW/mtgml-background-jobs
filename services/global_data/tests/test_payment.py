import sys
sys.path.append('../..')

import unittest
from libs.payment_lib import *


class PaymentTests(unittest.TestCase):

    def set_up(self):
        pass

    def test_low_tier(self):
        storage = 10
        cost = 4000

        self.assertEqual(calculate_cost(storage), cost)

    def test_mid_tier(self):
        storage = 100
        cost = 20000

        self.assertEqual(calculate_cost(storage), cost)

    def test_high_tier(self):
        storage = 101
        cost = 10100

        self.assertEqual(calculate_cost(storage), cost)

if __name__ == '__main__':
    unittest.main()
