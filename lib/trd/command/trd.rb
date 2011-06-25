
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
		puts TRD::Command::List.help(op.summary_indent)
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

config_path = File.join(ENV['HOME'], '.trd', 'trd.conf')
$verbose = false
#$debug = false

op.on('-c', '--config PATH', "path to config file (#{config_path})") {|s|
	config_path = s
}

op.on('-v', '--verbose', "verbose mode", TrueClass) {|b|
	$verbose = b
}

#op.on('-d', '--debug', "debug mode", TrueClass) {|b|
#	$debug = b
#}

begin
	op.order!(ARGV)
	usage nil if ARGV.empty?
	cmd = ARGV.shift
	$TRD_CONFIG_PATH = config_path
rescue
	usage $!.to_s
end

require 'trd/command/list'

method = TRD::Command::List.get_method(cmd)
unless method
	$stderr.puts "'#{cmd}' is not a trd command. Run '#{$prog}' to show the list."
  TRD::Command::List.show_guess(cmd)
	exit 1
end

require 'trd/error'

begin
	method.call
rescue TRD::ConfigError
	$stderr.puts "TreasureData account is not configured yet."
	$stderr.puts "Run '#{$prog} account' first."
rescue
  $stderr.puts "error #{$!.class}: backtrace:"
  $!.backtrace.each {|b|
    $stderr.puts "  #{b}"
  }
  puts ""
  puts $!
end

