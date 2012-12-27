require 'spec_helper'
require 'file_reader/shared_context'

require 'stringio'
require 'msgpack'
require 'td/file_reader'

include TreasureData

describe FileReader::MessagePackParsingReader do
  include_context 'error_proc'

  let :dataset do
    [
      {'name' => 'k', 'num' => 12345, 'time' => Time.now.to_i},
      {'name' => 's', 'num' => 34567, 'time' => Time.now.to_i},
      {'name' => 'n', 'num' => 56789, 'time' => Time.now.to_i},
    ]
  end

  let :io do
    StringIO.new(dataset.map(&:to_msgpack).join(""))
  end

  it 'initialize' do
    reader = FileReader::MessagePackParsingReader.new(io, error, {})
    reader.should_not be_nil
  end

  context 'after initialization' do
    let :reader do
      FileReader::MessagePackParsingReader.new(io, error, {})
    end

    it 'forward returns one data' do
      reader.forward.should == dataset[0]
    end

    it 'feeds all dataset' do
      begin
        i = 0
        while line = reader.forward
          line.should == dataset[i]
          i += 1
        end
      rescue RSpec::Expectations::ExpectationNotMetError => e
        fail
      rescue
        io.eof?.should be_true
      end
    end
  end
end
