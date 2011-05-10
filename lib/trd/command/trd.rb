
DEFAULT_CONFIG_PATH = File.join(ENV['HOME'], '.trd', 'trd.conf')

require 'optparse'

$prog = File.basename($0)

op = OptionParser.new
op.banner = <<EOF
usage: #{$prog} [options] COMMAND [args]

options:
EOF

op.summary_indent = "  "

(class<<self;self;end).module_eval do
	define_method(:usage) do |errmsg|
		require 'trd/command/list'
		puts op.to_s
		puts ""
		puts "commands:"
		puts TRD::Command::List.common_help(op.summary_indent)
		puts ""
		puts "See 'trd help COMMAND' for more information on a specific command.
"
		if errmsg
			puts "error: #{errmsg}"
			exit 1
		else
			exit 0
		end
	end
end


module TRD
	DEFAULT_CONFIG_PATH = File.join(ENV['HOME'], '.trd', 'trd.conf')
end

config_path = TRD::DEFAULT_CONFIG_PATH
verbose = false
debug = false

op.on('-c', '--config PATH', "config file path (#{config_path})") {|s|
	config_path = s
}

op.on('-v', '--verbose', "verbose mode", TrueClass) {|b|
	verbose = b
}

op.on('-d', '--debug', "debug mode", TrueClass) {|b|
	debug = b
}

begin
	op.order!(ARGV)
	usage nil if ARGV.empty?

	# TODO -v --verbose

	cmd = ARGV.shift

	TRD::CONFIG_PATH = config_path

rescue
	usage $!.to_s
end

require 'trd/log'
require 'trd/config'
require 'trd/command/list'

if debug
	$log = TRD::Log.new(TRD::Log::LEVEL_TRACE)
	$log.enable_debug
elsif verbose
	$log = TRD::Log.new(TRD::Log::LEVEL_DEBUG)
else
	$log = TRD::Log.new(TRD::Log::LEVEL_WARN)
end

begin
	TRD::Command::List.call(cmd)

rescue TRD::ConfigError
	puts "TreasureData.com account is not configured yet."
	puts "Run '#{$prog} account' first."
end

