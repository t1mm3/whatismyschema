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

	def check_null(self, cols, isnull):
		self.assertEqual(len(cols), len(isnull))
		for (col, null) in zip(cols, isnull):
			if null:
				self.assertTrue(col.num_nulls > 0)
			else:
				self.assertEqual(col.num_nulls, 0)

	def check_all_null_val(self, cols, val):
		isnull = []
		for r in range(0, len(cols)):
			isnull.append(val)

		self.check_null(cols, isnull)

	def check_all_null(self, cols):
		self.check_all_null_val(cols, True)
	def check_none_null(self, cols):
		self.check_all_null_val(cols, False)

	def testDates1(self):
		table = Table(",")
		table.push("2013-08-29,2013-08-05 15:23:13.716532")

		self.check_types(table.columns,
			["date", "datetime"])

		self.check_none_null(table.columns)

	def testSep1(self):
		table = Table("seperator")
		table.push("Hallo|seperator|Welt")

		self.check_types(table.columns,
			["varchar(6)", "varchar(5)"])

		self.check_none_null(table.columns)

	def testDec1(self):
		table = Table("|")
		table.push("42")
		table.push("42.44")
		table.push("42.424")
		table.push("4.424")

		self.check_types(table.columns,
			["decimal(5,3)"])

		self.check_none_null(table.columns)

	def test1(self):
		table = Table("|")
		table.push("Str1|Str2|42|42|13")
		table.push("Ha|Str3333|42.42|Test|34543534543543")

		self.check_types(table.columns,
			["varchar(4)", "varchar(7)", "decimal(4,2)", "varchar(4)", "bigint"])

		self.check_none_null(table.columns)

	def testColMismatch1(self):
		table = Table(",")
		table.push("1")
		table.push("1,2")
		table.push("1")

		self.check_types(table.columns,
			["tinyint", "tinyint"])

		self.check_null(table.columns, [False, True])

if __name__ == '__main__':
	unittest.main()