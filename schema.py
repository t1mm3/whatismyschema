#!/bin/env python

from decimal import *
from datetime import datetime

class MinMax:
	def __init__(self):
		self.dmin = None
		self.dmax = None

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
			new_formats = []
			for fmt in self.formats:
				val = self.match_format(attr, fmt)
				if val is not None:
					new_formats.append(fmt)

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
		self.id = len(table.columns)
		if name is None:
			self.name = "col{}".format(self.id)
		else:
			self.name = name

		self.null_value = ""
		self.num_nulls = 0
		self.num_values = 0

		self.int_minmax = MinMax()
		self.decdigits_minmax = MinMax()
		self.decprecision_minmax = MinMax()
		self.len_minmax = MinMax()

		self.guess_date = DateTimeFormatTryAndError([
				"%Y-%m-%d"
			])

		self.guess_datetime= DateTimeFormatTryAndError([
				"%Y-%m-%d %H:%M:%S.%f",
				"%Y-%m-%d %H:%M:%S"
			])

	def push_attribute(self, attr, table):
		self.num_values = self.num_values + 1

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

		return("{n} {t} {a}".format(
			n=self.name, t=tpe_str[0],
			a="NOT NULL" if self.num_nulls == 0 else ""))

	def check(self, table):
		pass


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

		num_attrs = len(attrs)
		num_cols = len(self.columns)

		if num_attrs != num_cols:
			if self.fixed_schema:
				raise Exception("Number of columns does not match fixed schema")
		
			diff = num_attrs - num_cols
			if num_attrs > num_cols:
				for r in range(0, diff):
					c = Column(self, None)

					# Add NULLs because these columns are new
					# Hence before that they are considered missing values
					c.num_nulls = self.line_number

					self.columns.append(c)
			else:
				assert(num_attrs < num_cols)
				for r in range(0, diff):
					# Append safe NULL values
					c = self.columns[r+num_attrs]
					attr.append(c.null_value)

		for (attr, col) in zip(attrs, self.columns):
			col.push_attribute(attr, self)

		self.line_number = self.line_number + 1

	def push(self, x):
		self.push_line(x)

	def check(self):
		num_values = None
		empty_tail = False

		for col in self.columns:
			if num_values is None:
				num_values = col.num_values
			else:
				assert(num_values >= col.num_values)
				num_values = col.num_values

			col.check(self)


import sys
import argparse
import subprocess

def process_file(table, f, begin):
	nr = 0
	for line in f:
		if nr < begin:
			continue
		
		table.push_line(line)
		nr = nr + 1

def schema_main(table, args):
	if len(args.files) == 0:
		process_file(table, sys.stdin, args.begin)
	else:
		for file in args.files:
			with open(file) as f:
				process_file(table, f, args.begin)

	return table

def load_column_info(table, f):
	for line in f:
		line = line.strip()
		if len(line) == 0:
			continue

		table.columns.append(Column(table, line))

if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description="""Figures out SQL data types from schema.""")

	parser.add_argument('files', metavar='FILES', nargs='*',
		help='CSV files to process. Stdin if none given')
	parser.add_argument("-F", "--sep", dest="seperator",
		help="Use <SEPERATOR> as delimiter between columns", default="|")
	parser.add_argument("-B", "--begin", type=int, dest="begin",
		help="Skips first <BEGIN> rows", default="0")
	parser.add_argument("--sql", dest="sql", type=str,
		help="Creates SQL schema using given table name")
	parser.add_argument("--colnamefile", dest="colnamefile", type=str,
		help="Loads column names from file")
	parser.add_argument("--colnamecmd", dest="colnamecmd", type=str,
		help="Loads column names from command's stdout")
	parser.add_argument('--validate-schema', dest='strictschema', action='store_true',
		help="Validates schema when column information or names is given")
	parser.set_defaults(strictschema=False)

	args = parser.parse_args()

	table = Table(args.seperator)

	if args.colnamefile:
		table.fixed_schema = args.strictschema
		with open(args.colnamefile) as f:
			load_column_info(table, f)

	if args.colnamecmd:
		table.fixed_schema = args.strictschema
		cmd = subprocess.Popen(args.colnamecmd, shell=True, stdout=subprocess.PIPE)
		load_column_info(table, cmd.communicate()[0].decode('ascii', 'ignore'))

	
	schema_main(table, args)
	table.check()

	print_cols = list(filter(lambda col: col.num_values > 0, table.columns))
	num_cols = len(print_cols)

	if args.sql:
		print("CREATE TABLE {} (".format(args.sql))

		col_counter = 0

		for col in print_cols:
			col_counter = col_counter + 1
			last_col = col_counter == num_cols

			t=col.print_types(table)
			print("{t}{post}".format(
				t=t,
				post="" if last_col else ","))
		print("}")
	else:
		for col in print_cols:
			print(col.print_types(table))