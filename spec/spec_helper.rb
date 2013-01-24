$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'rspec'
require 'json'

if ENV['SIMPLE_COV']
  # SimpleCov
  # https://github.com/colszowka/simplecov
  require 'simplecov'
  SimpleCov.start do 
    add_filter 'spec/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

require 'td/command/runner'

def execute_td(command_line)
  args = command_line.split(" ")
  original_stdin, original_stderr, original_stdout = $stdin, $stderr, $stdout

  $stdin  = captured_stdin  = StringIO.new
  $stderr = captured_stderr = StringIO.new
  $stdout = captured_stdout = StringIO.new
  class << captured_stdout
    def tty?
      true
    end
  end

  begin
    runner = TreasureData::Command::Runner.new
    $0 = 'td'
    runner.run(args)
  rescue SystemExit
  ensure
    $stdin, $stderr, $stdout = original_stdin, original_stderr, original_stdout
  end

  [captured_stderr.string, captured_stdout.string]
end
