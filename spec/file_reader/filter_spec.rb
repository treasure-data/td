require 'spec_helper'
require 'file_reader/shared_context'

require 'td/file_reader'

include TreasureData

describe 'FileReader filters' do
  include_context 'error_proc'

  describe FileReader::AutoTypeConvertParserFilter do
  end

  describe FileReader::HashBuilder do
  end

  describe FileReader::TimeParserFilter do
  end

  describe FileReader::SetTimeParserFilter do
  end
end
