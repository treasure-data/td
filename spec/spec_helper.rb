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

# XXX skip coverage setting if run appveyor. Because, fail to push coveralls in appveyor.
unless ENV['APPVEYOR']
  require 'coveralls'
  Coveralls.wear!('rails')
end

RSpec.configure do |config|
  # This allows you to limit a spec run to individual examples or groups
  # you care about by tagging them with `:focus` metadata. When nothing
  # is tagged with `:focus`, all examples get run. RSpec also provides
  # aliases for `it`, `describe`, and `context` that include `:focus`
  # metadata: `fit`, `fdescribe` and `fcontext`, respectively.
  config.filter_run_when_matching :focus
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

class CallSystemExitError < RuntimeError; end

shared_context 'quiet_out' do
  let(:stdout_io) { StringIO.new }
  let(:stderr_io) { StringIO.new }

  around do |example|
    out = $stdout.dup
    err= $stdout.dup
    begin
      $stdout = stdout_io
      $stderr = stderr_io
      example.call
    ensure
      $stdout = out
      $stderr = err
    end
  end
end
