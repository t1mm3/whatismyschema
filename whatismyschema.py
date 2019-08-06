#!/bin/env python
# coding: utf8
#
# WhatIsMySchema
#
# Copyright (c) 2018 Tim Gubner
#
#

from decimal import *
from datetime import datetime
import itertools
try:
    from itertools import zip_longest as zip_longest
except:
    from itertools import izip_longest as zip_longest
from multiprocessing.pool import ThreadPool

class MinMax(object):
	__slots__ = "dmin", "dmax"

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

	def merge(self, other):
		self.push(other.dmin)
		self.push(other.dmax)

def check_int(s):
	if s[0] in ('-', '+'):
		return s[1:].isdigit()
	return s.isdigit()

class FormatTryAndError(object):
	__slots__ = "formats", "valid"
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

class Column(object):
	int_ranges = [
			(0, 255, "tinyint"),
			(-32768, 32767, "smallint"),
			(-2147483648, 2147483647, "int"),
			(-9223372036854775808, 9223372036854775807, "bigint")
		]

	__slots__ = "id", "name", "null_value", "num_nulls", "num_values", "int_minmax", "decpre_minmax", "decpost_minmax", "len_minmax", "guess_date", "guess_datetime"

	def __init__(self, table, name):
		self.id = len(table.columns)
		if name is None:
			self.name = "col{}".format(self.id)
		else:
			self.name = name

		self.null_value = table.parent_null_value
		self.num_nulls = 0
		self.num_values = 0

		self.int_minmax = MinMax()
		self.decpre_minmax = MinMax()
		self.decpost_minmax = MinMax()
		self.len_minmax = MinMax()

		self.guess_date = DateTimeFormatTryAndError([
				"%Y-%m-%d"
			])

		self.guess_datetime= DateTimeFormatTryAndError([
				"%Y-%m-%d %H:%M:%S.%f",
				"%Y-%m-%d %H:%M:%S"
			])

	def push_attribute(self, attr, table):
		self.num_values += 1

		if attr == self.null_value:
			self.num_nulls += 1
			return

		self.len_minmax.push(len(attr))

		if self.int_minmax is not None:
			try:
				self.int_minmax.push(int(attr))
			except:
				self.int_minmax = None

		if self.decpre_minmax is not None:
			valid = True

			decimal_sep = "."
			data = attr

			# remove leading zeros
			data = data.lstrip("0")

			# remove trailing zeros
			data = data.rstrip("0")

			# find dot
			parts = data.split(decimal_sep, 1)
			num_parts = len(parts)

			len_pre = 0
			len_post = 0

			if num_parts == 1:
				pre = parts[0]
				post = ""
			elif num_parts == 2:
				pre = parts[0]
				post = parts[1]
			else:
				valid = False

			if valid:
				# compute scale & precision
				len_post = len(post)
				len_pre = len(pre)


				# empty 'pre' means implicit 0
				if len_pre != 0:
					try:
						int(pre)
					except:
						valid = False

				# decimal places must be integer
				if len_post != 0:
					try:
						int(post)
					except:
						valid = False

				#print("attr='{}' pre='{}' post='{}' decimal({}, {})".format(
				#	attr, pre, post, len_pre, len_post))


			if valid:
				self.decpre_minmax.push(len_pre)
				self.decpost_minmax.push(len_post)

				# print("DECIMAL '{attr}': {a} {b}\n".format(attr=attr, a=precision, b=scale))

			if not valid:
				self.decpre_minmax = None
				self.decpost_minmax = None

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
		if self.num_values == self.num_nulls:
			# undefine, make tight choice
			r.append("tinyint")

		if self.int_minmax is not None and self.int_minmax.dmax is not None:
			for (dmin, dmax, name) in self.int_ranges:
				if self.int_minmax.dmax > dmax:
					continue
				if self.int_minmax.dmin < dmin:
					continue

				r.append(name)

		if self.decpre_minmax is not None and self.decpre_minmax.dmax is not None:
			precision = self.decpost_minmax.dmax + self.decpre_minmax.dmax
			scale = self.decpost_minmax.dmax

			assert(precision >= scale)

			r.append("decimal({}, {})".format(
				precision, scale))

		if self.guess_date is not None:
			r.append("date")

		if self.guess_datetime is not None:
			r.append("datetime")

		if self.len_minmax is not None:
			r.append("varchar({})".format(self.len_minmax.dmax))

		return r

	def check(self, table):
		pass

	def merge(self, other):
		assert(other.id == self.id)

		if self.int_minmax is not None and other.int_minmax is not None:
			self.int_minmax.merge(other.int_minmax)
		else:
			self.int_minmax = None

		if self.decpre_minmax is not None and other.decpre_minmax is not None:
			self.decpre_minmax.merge(other.decpre_minmax)
		else:
			self.decpre_minmax = None

		if self.decpost_minmax is not None and other.decpost_minmax is not None:
			self.decpost_minmax.merge(other.decpost_minmax)
		else:
			self.decpost_minmax = None

		if self.len_minmax is not None and other.len_minmax is not None:
			self.len_minmax.merge(other.len_minmax)
		else:
			self.len_minmax = None

		if other.guess_date is None:
			self.guess_date = None

		if other.guess_datetime is None:
			self.guess_datetime


class Table:
	__slots__ = "seperator", "columns", "line_number", "parent_null_value"
	def __init__(self):
		self.seperator = "|"

		self.columns = []
		self.line_number = 0

		self.parent_null_value = ""

	def push_line(self, line):
		attrs = line.split(self.seperator)

		num_attrs = len(attrs)
		num_cols = len(self.columns)

		if num_attrs != num_cols:		
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

	def merge(self, other):
		self.check()
		other.check()

		num_self = len(self.columns)
		num_other = len(other.columns)

		zipped = list(zip_longest(self.columns, other.columns))

		new_cols = []

		for (scol, ocol) in zipped:
			if scol is not None:
				if ocol is not None:
					scol.merge(ocol)
				new_cols.append(scol)
			else:
				new_cols.append(ocol)

		self.columns = new_cols

class FileDriver:
	__slots__ = "mutex", "file", "chunk_size", "begin", "count", "done"
	def __init__(self, file, args):
		self.mutex = multiprocessing.Lock()
		self.file = file
		assert(args.chunk_size >= 1)
		self.chunk_size = args.chunk_size
		self.begin = args.begin
		self.count = 0
		self.done = False

	def _skip_begin(self):
		while self.count < self.begin:
			l = self.file.readline()
			if not l:
				self.done = True
				return False
			self.count = self.count + 1
		return True

	def nextTuple(self):
		if self.done:
			return None

		success = self._skip_begin()
		if not success:
			return None

		l = self.file.readline()
		if not l:
			self.done = True
			return None

		self.count = self.count + 1
		return l

	def nextMorsel(self):
		with self.mutex:
			r = []

			if self.done:
				return None

			success = self._skip_begin()
			if not success:
				return None

			for i in range(0, self.chunk_size):
				l = self.file.readline()
				if not l:
					self.done = True
					break
				self.count = self.count + 1

				r.append(l)

			return r

import os
import sys
import argparse
import subprocess
import multiprocessing
from contextlib import closing

def driver_loop(table, driver, parallel):
	if parallel:
		while True:
			lines = driver.nextMorsel()
			if lines is None:
				break

			for line in lines:
				table.push_line(line)
	else:
		while True:
			line = driver.nextTuple()
			if line is None:
				break

			table.push_line(line)

	return table

def schema_main_parallel(master_table, args, drivers):
	parallelism = args.num_parallel

	if parallelism < 0:
		parallelism = multiprocessing.cpu_count()

	# Allocate tables
	tables = []
	for i in range(0, parallelism):
		for file in drivers:
			tables.append(Table())

	# Set settings
	apply_settings([master_table] + tables, args)

	with closing(ThreadPool(processes=parallelism)) as pool:
		# spawn jobs
		jobs = []

		for i in range(0, parallelism):
			for driver in drivers:
				new_table = tables.pop()
				jobs.append(pool.apply_async(driver_loop, (new_table, driver, True)))

		# wait for all and merge
		for task in jobs:
			master_table.merge(task.get())

	return master_table


def schema_main(table, args):
	drivers = []
	files = []

	try:
		if len(args.files) == 0:
			drivers = [FileDriver(os.fdopen(os.dup(sys.stdin.fileno())), args)]
		else:
			files = list(map(lambda fn: open(fn, 'r'), args.files))
			drivers = list(map(lambda x: FileDriver(x, args), files))


		if args.num_parallel != 1:
			return schema_main_parallel(table, args, drivers)

		apply_settings([table], args)

		for driver in drivers:
			driver_loop(table, driver, False)

	finally:
		for f in files:
			f.close()
		drivers = []

	return table

def load_column_info(table, f):
	r = []
	for line in f:
		line = line.strip()
		if len(line) == 0:
			continue

		r.append(line)

	return r


def apply_settings(tables, args):
	colfile = []
	if args.colnamefile:
		with open(args.colnamefile) as f:
			colfile = load_column_info(table, f)

	colcmd = []
	if args.colnamecmd:
		cmd = subprocess.Popen(args.colnamecmd, shell=True, stdout=subprocess.PIPE)
		colcmd = load_column_info(table, cmd.communicate()[0].decode('ascii', 'ignore'))

	for table in tables:
		table.seperator = args.seperator
		if args.null:
			table.parent_null_value = args.null

		for line in colfile:
			table.columns.append(Column(table, line))
		for line in colcmd:
			table.columns.append(Column(table, line))

class TableOutput(object):

	__slots__ = "widths", "num_cols", "hfill"

	def __init__(self, widths):
		for w in widths:
			assert(w > 0)

		self.widths = widths
		self.num_cols = len(widths)

	def _put(self, values, sep):
		r = ""

		num = len(values)
		assert(self.num_cols == num)
		idx = 0

		for (val, width) in list(zip(values, self.widths)):
			idx = idx + 1

			s = sep if idx < num else ""
			r = "{0}{1:<{width}}{2}".format(r, val, s, width=width)

		return r

class TtyOutput(TableOutput):
	vline = "│"
	hline = "─"

	ljoint = "├"
	cjoint = "┼"
	rjoint = "┤"

	tljoint = "┌"
	trjoint = "┐"
	tcjoint = "┬"

	bljoint = "└"
	brjoint = "┘"
	bcjoint = "┴"

	def __init__(self, widths):
		TableOutput.__init__(self, widths)
		self.hfill = list(
			map(lambda width: self.hline * width,
				self.widths))

	def put_first(self):
		return "{}{}{}".format(self.tljoint, self._put(self.hfill, self.tcjoint), self.trjoint)

	def put_linesep(self):
		return "{}{}{}".format(self.ljoint, self._put(self.hfill, self.cjoint), self.rjoint)

	def put_last(self):
		return "{}{}{}".format(self.bljoint, self._put(self.hfill, self.bcjoint), self.brjoint)

	def put(self, values):
		return "{}{}{}".format(self.vline, self._put(values, self.vline), self.vline)


class TerminalOutput(object):
	__slots__ = "tty_table", "create_table", "no_header"

	def __init__(self, args):
		self.tty_table = sys.stdout.isatty()
		self.create_table = args.sql
		self.no_header = args.no_table_header

	def _unpackTypeString(self, col):
		tpe_str = col.determine_type()
		assert(len(tpe_str) > 0)
		return tpe_str[0]

	def render(self, table):
		print_cols = list(
			map(lambda col: (col, self._unpackTypeString(col)),
				filter(lambda col: col.num_values > 0,
					table.columns)))
		num_cols = len(print_cols)

		if self.create_table:
			print("CREATE TABLE {} (".format(self.create_table))

			col_counter = 0

			for (col, tpe_str) in print_cols:
				col_counter = col_counter + 1
				last_col = col_counter == num_cols


				t="{n} {t}{a}".format(
					n=col.name, t=tpe_str,
					a=" NOT NULL" if col.num_nulls == 0 else "")

				print("{t}{post}".format(
					t=t,
					post="" if last_col else ","))
			print(")")

			return

		if not self.tty_table:
			for (col, tpe_str) in print_cols:
				print("{n} {t}{a}".format(
					n=col.name, t=tpe_str,
					a=" NOT NULL" if col.num_nulls == 0 else ""))
			return

		w_name = 0
		w_type = 0
		w_null = len("NOT NULL")

		for (col, tpe_str) in print_cols:
			w_name = max(w_name, len(col.name))
			w_type = max(w_type, len(tpe_str))

		out = TtyOutput([w_name, w_type, w_null])

		first = True

		if not self.no_header:
			print(out.put_first())
			print(out.put(["Name", "Type", "Null"]))
			first = False

		for (col, tpe_str) in print_cols:
			if first:
				first = False
			else:
				print(out.put_linesep())

			print(out.put([col.name, tpe_str,  "NOT NULL" if col.num_nulls == 0 else ""]))

		print(out.put_last())

def main():
	parser = argparse.ArgumentParser(
		description="""Determine SQL schema from CSV data."""
	)

	parser.add_argument('files', metavar='FILES', nargs='*',
		help='CSV files to process. Stdin if none given')
	parser.add_argument("-F", "--sep", dest="seperator",
		help="Use <SEPERATOR> as delimiter between columns", default="|")
	parser.add_argument("-B", "--begin", type=int, dest="begin",
		help="Skips first <BEGIN> rows", default="0")
	parser.add_argument("--create-table", dest="sql", type=str,
		help="Creates SQL schema using given table name")
	parser.add_argument("--null", dest="null", type=str,
		help="Interprets <NULL> as NULLs", default="")
	parser.add_argument("--colnamefile", dest="colnamefile", type=str,
		help="Loads column names from file")
	parser.add_argument("--colnamecmd", dest="colnamecmd", type=str,
		help="Loads column names from command's stdout")
	parser.add_argument("--no-header", dest="no_table_header",
		help="Print no table header", action='store_true')
	parser.set_defaults(no_table_header=False)
	parser.add_argument("-P", "--parallelism", "--parallel", dest="num_parallel", type=int,
		help="Parallelizes using <NUM_PARALLEL> threads. If <NUM_PARALLEL> is less than 0 the degree of parallelism will be chosen.", default="1")
	parser.add_argument("--parallel-chunk-size", dest="chunk_size", type=int,
		help="Sets chunk size for parallel reading. Default is 16k lines.", default="16384")

	args = parser.parse_args()

	table = Table()

	schema_main(table, args)
	table.check()

	output = TerminalOutput(args)

	output.render(table)


if __name__ == '__main__':
	main()
