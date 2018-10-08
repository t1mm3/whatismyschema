#!/bin/env python

import unittest
from schema import *

class ComplexTest(unittest.TestCase):
	def fix_type(self, t):
		return t.lower().replace(" ", "")

	def check_type(self, col, expect):
		types = col.determine_type()
		self.assertTrue(len(types) > 0)
		expect = self.fix_type(expect)
		data = self.fix_type(types[0])
		self.assertEqual(data, expect)

	def check_types(self, cols, types):
		self.assertEqual(len(cols), len(types))
		for (col, tpe) in zip(cols, types):
			self.check_type(col, tpe)

	def testDates1(self):
		table = Table(",")
		table.push("2013-08-29,2013-08-05 15:23:13.716532")

		self.check_types(table.columns,
			["date", "datetime"])

	def testSep1(self):
		table = Table("seperator")
		table.push("Hallo|seperator|Welt")

		self.check_types(table.columns,
			["varchar(6)", "varchar(5)"])

	def test1(self):
		table = Table("|")
		table.push("Str1|Str2|42|42|13")
		table.push("Ha|Str3333|42.42|Test|34543534543543")

		self.check_types(table.columns,
			["varchar(4)", "varchar(7)", "decimal(4,2)", "varchar(4)", "bigint"])

if __name__ == '__main__':
	unittest.main()