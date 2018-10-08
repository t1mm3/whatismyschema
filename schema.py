#!/bin/env python

from decimal import *
from datetime import datetime

class MinMax:
	dmin = None
	dmax = None

	def push(self, x):
		if self.dmax is None or x > self.dmax:
			self.dmax = x
		if self.dmin is None or x < self.dmin:
			self.dmin = x

	def exists(self):
		return self.dmax is not None and self.dmin is not None

def check_int(s):
	if s[0] in ('-', '+'):
		return s[1:].isdigit()
	return s.isdigit()

class FormatTryAndError:
	def __init__(self, formats):
		self.formats = formats
		self.valid = True

	def match_format(self, attr, fmt):
		return None

	def test(self, attr):
		num_formats = len(self.formats)
		if num_formats < 1:
			self.valid = False
			return
		elif num_formats == 1:
			for fmt in self.formats:
				val = self.match_format(attr, fmt)
				if val is None:
					self.valid = False
					return
		else:
			# Figure out format and eliminate invalid formats
			for fmt in self.formats:
				val = self.match_format(attr, fmt)
				if val is not None:
					new_formats.add(fmt)

			self.formats = new_formats
			if len(new_formats) < 1:
				self.valid = False


class DateTimeFormatTryAndError(FormatTryAndError):
	def __init__(self, formats):
		self.formats = formats
		self.valid = True

	def match_format(self, attr, fmt):
		try:
			return datetime.strptime(attr, fmt)
		except ValueError:
			return None

	
class Column:
	int_ranges = [
			(0, 255, "tinyint"),
			(-32768, 32767, "smallint"),
			(-2147483648, 2147483647, "int"),
			(-9223372036854775808, 9223372036854775807, "bigint")
		]

	def __init__(self, table, name):
		if name is None:
			self.name = "col{}".format(len(table.columns))
		else:
			self.name = name

		self.null_value = ""
		self.num_nulls = 0

		self.int_minmax = MinMax()
		self.decdigits_minmax = MinMax()
		self.decprecision_minmax = MinMax()
		self.len_minmax = MinMax()

		self.guess_date = DateTimeFormatTryAndError([
				"%Y-%m-%d"
			])

		self.guess_datetime= DateTimeFormatTryAndError([
				"%Y-%m-%d %H:%M:%S.%f"
			])

	def push_attribute(self, attr, table):
		if attr == self.null_value:
			self.num_nulls = self.num_nulls + 1
			return

		self.len_minmax.push(len(attr))

		if self.int_minmax is not None:
			try:
				self.int_minmax.push(int(attr))
			except:
				self.int_minmax = None

		if self.decdigits_minmax is not None:
			try:
				dec = Decimal(attr)
				exp = -dec.as_tuple().exponent
				if exp > 0:
					self.int_minmax = None
				self.decprecision_minmax.push(exp)
				self.decdigits_minmax.push(len(dec.as_tuple().digits))
			except:
				self.decdigits_minmax = None
				self.decprecision_minmax = None

		if self.guess_date is not None:
			self.guess_date.test(attr)
			if not self.guess_date.valid:
				self.guess_date = None

		if self.guess_datetime is not None:
			self.guess_datetime.test(attr)
			if not self.guess_datetime.valid:
				self.guess_datetime = None


	def determine_type(self):
		r = []
		if self.int_minmax is not None:
			for (dmin, dmax, name) in self.int_ranges:
				if self.int_minmax.dmax > dmax:
					continue
				if self.int_minmax.dmin < dmin:
					continue

				r.append(name)

		if self.decdigits_minmax is not None:
			r.append("decimal({n}, {p})".format(
				n=self.decdigits_minmax.dmax,
				p=self.decprecision_minmax.dmax))

		if self.guess_date is not None:
			r.append("date")

		if self.guess_datetime is not None:
			r.append("datetime")

		if self.len_minmax is not None:
			r.append("varchar({})".format(self.len_minmax.dmax))

		return r

	def print_types(self, table):
		tpe_str = self.determine_type()

		assert(len(tpe_str) > 0)

		print("{n} {t} {a}".format(
			n=self.name, t=tpe_str[0],
			a="NOT NULL" if self.num_nulls == 0 else ""))


class Table:
	def __init__(self, seperator):
		self.seperator = "|"
		if seperator is not None:
			self.seperator = seperator

		self.columns = []
		self.line_number = 0
		self.fixed_schema = False

	def push_line(self, line):
		attrs = line.split(self.seperator)
		self.line_number = self.line_number + 1

		num_attrs = len(attrs)
		num_cols = len(self.columns)

		if num_attrs != num_cols and self.fixed_schema:
			raise Exception("Number of columns does not match fixed schema")
		
		if num_attrs > num_cols:
			diff = num_attrs - num_cols
			for r in range(0, diff):
				self.columns.append(Column(self, None))

		for (attr, col) in zip(attrs, self.columns):
			col.push_attribute(attr, self)

	def push(self, x):
		self.push_line(x)

	def print_schema(self):
		for col in self.columns:
			col.print_types(self)


import sys

if __name__ == '__main__':
	table = Table("|")

	for line in sys.stdin:
		table.push_line(line)

	table.print_schema()