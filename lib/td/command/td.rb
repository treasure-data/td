
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
		require 'td/command/list'
		puts op.to_s
		puts ""
		puts "commands:"
		puts TD::Command::List.help(op.summary_indent)
		puts ""
		puts "Type 'td help COMMAND' for more information on a specific command."
		if errmsg
			puts "error: #{errmsg}"
			exit 1
		else
			exit 0
		end
	end
end

config_path = nil
apikey = nil
$verbose = false
#$debug = false

op.on('-c', '--config PATH', "path to config file (~/.td/td.conf)") {|s|
	config_path = s
}

op.on('-k', '--apikey KEY', "use this API key instead of reading the config file") {|s|
  apikey = s
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

  require 'td/config'
  if config_path
    TD::Config.path = config_path
  end
  if apikey
    TD::Config.apikey = apikey
  end
rescue
	usage $!.to_s
end

require 'td/command/list'

method = TD::Command::List.get_method(cmd)
unless method
	$stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
  TD::Command::List.show_guess(cmd)
	exit 1
end

require 'td/error'

begin
	method.call
rescue TD::ConfigError
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

