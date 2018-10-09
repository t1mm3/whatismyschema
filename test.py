#!/bin/env python
#
# WhatIsMySchema
#
# Copyright (c) 2018 Tim Gubner
#
#

import unittest
from whatismyschema import *

class WhatIsMySchemaTestCase(unittest.TestCase):
	def fix_type(self, t):
		return t.lower().replace(" ", "").replace("\n", "")

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



class TableTests(WhatIsMySchemaTestCase):
	def testDates1(self):
		table = Table()
		table.seperator = ","
		table.push("2013-08-29,2013-08-05 15:23:13.716532")

		self.check_types(table.columns,
			["date", "datetime"])

		self.check_none_null(table.columns)
		table.check()

	def testSep1(self):
		table = Table()
		table.seperator = "seperator"
		table.push("Hallo|seperator|Welt")

		self.check_types(table.columns,
			["varchar(6)", "varchar(5)"])

		self.check_none_null(table.columns)
		table.check()

	def testDec1(self):
		table = Table()
		table.seperator = "|"
		table.push("42")
		table.push("42.44")
		table.push("42.424")
		table.push("4.424")

		self.check_types(table.columns,
			["decimal(5,3)"])

		self.check_none_null(table.columns)
		table.check()

	def test1(self):
		table = Table()
		table.seperator = "|"
		table.push("Str1|Str2|42|42|13")
		table.push("Ha|Str3333|42.42|Test|34543534543543")

		self.check_types(table.columns,
			["varchar(4)", "varchar(7)", "decimal(4,2)", "varchar(4)", "bigint"])

		self.check_none_null(table.columns)
		table.check()

	def testColMismatch1(self):
		table = Table()
		table.seperator = ","
		table.push("1")
		table.push("1,2")
		table.push("1")

		self.check_types(table.columns,
			["tinyint", "tinyint"])

		self.check_null(table.columns, [False, True])
		table.check()


class CliTests(WhatIsMySchemaTestCase):
	def exec(self, cmd, file):
		path = os.path.dirname(__file__)
		p = subprocess.Popen("python {path}/whatismyschema.py{sep}{cmd}{sep}{path}/{file}".format(
			path=path, cmd=cmd, file=file,
			sep=" " if len(cmd) > 0 else ""), shell=True,
		stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = p.communicate()
		if p.returncode:
			raise Exception(err)
		else:
			# Print stdout from cmd call
			if err is None:
				err = ""
			if out is None:
				out = ""
			
			self.assertEqual(0, len(err.decode('utf8').strip()))
			return self.fix_type(out.decode('utf8').strip())

	def testParallel1(self):
		for num_process in [1, 2, 4, 8]:
			for chunk_size in [1, 10, 100]:
				for begin in [0, 1]:
					flags = "--parallel-chunk-size {chunk_size} --parallelism {parallel} --begin {begin}".format(
						chunk_size=chunk_size, parallel=num_process, begin=begin)
					out = self.exec(flags, "test1.txt")
					if begin == 0:
						expect = self.fix_type("col0varchar(5)notnullcol1varchar(2)notnullcol2varchar(4)notnull")
						self.assertEqual(out, expect)
					elif begin == 1:
						expect = self.fix_type("col0decimal(4,2)notnullcol1tinyintnotnullcol2smallintnotnull")
						self.assertEqual(out, expect)
					else:
						assert(False)


if __name__ == '__main__':
	unittest.main()