
include TRD

def Command.create_database
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} create-database <db name>

description:
  Create a database.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	require 'trd/api'

	arg = {}
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

	rescue
		usage $!.to_s
	end

	require 'trd/config'
	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)
	api.create_database(name)

	puts "Database '#{name}' is created."
	puts "Use '#{$prog} create-table #{name}.<tabl name> to create a table."
end


def Command.create_table
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} create-table <db name>.<table name>

description:
  Create a table in the database.
	EOF

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	require 'trd/api'

	arg = {}
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

		if name.count('.') != 1
			raise "Invalid table name '#{name}'. Use <db>.<table> format like 'myapp.access'."
		end

		db, table = name.split('.')

	rescue
		usage $!.to_s
	end

	require 'trd/config'
	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)
	api.create_table(db, table)

	puts "Table '#{name}' is created."
end


def Command.drop_database
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} drop-database <db name>

description:
  Delete a database.
	EOF

	arg = {
		:force => false
	}

	op.on('-f', '--force', 'don\'t confirm to delete', TrueClass) {|b|
		arg[:force] = b
	}

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	require 'trd/api'

	arg = {}
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

	rescue
		usage $!.to_s
	end

	require 'trd/config'
	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	unless api.databases.include?(name)
		puts "Database '#{name}' does not exist."
		exit 1
	end

	unless arg[:force]
		print "Are you sure to delete database #{name}? [y/N]:"
		line = STDIN.gets || ""
		unless line =~ /y/i || line =~ /yes/i
			puts "Canceled."
			exit 0
		end
	end

	api.drop_database(name)

	puts "Database '#{name}' is deleted."
end


def Command.drop_table
	op = OptionParser.new
	op.banner = <<-EOF
usage: #{$prog} drop-table <db name>.<table name>

description:
  Delete a table.
	EOF

	arg = {
		:force => false
	}

	op.on('-f', '--force', 'don\'t confirm to delete', TrueClass) {|b|
		arg[:force] = b
	}

	(class<<self;self;end).module_eval do
		define_method(:usage) do |msg|
			puts op.to_s
			puts ""
			puts "error: #{msg}" if msg
			exit 1
		end
	end

	require 'trd/api'

	arg = {}
	TRD::API.option(op, arg)

	begin
		op.parse!(ARGV)
		usage nil if ARGV.length != 1

		name = ARGV[0]

		if name.count('.') != 1
			raise "Invalid table name '#{name}'. Use <db>.<table> format like 'myapp.access'."
		end

		db, table = name.split('.')

	rescue
		usage $!.to_s
	end

	require 'trd/config'
	conf = TRD::Config.read(CONFIG_PATH)

	api = TRD::API.new(arg, conf)

	tables = api.tables(db)
	unless tables
		puts "Database '#{db}' does not exist."
		exit 1
	end
	unless tables.include?(table)
		puts "Table '#{name}' does not exist."
		exit 1
	end

	unless arg[:force]
		print "Are you sure to delete table #{name}? [y/N]:"
		line = STDIN.gets || ""
		unless line =~ /y/i || line =~ /yes/i
			puts "Canceled."
			exit 0
		end
	end

	api.drop_table(db, table)

	puts "Table '#{name}' is deleted."
end

