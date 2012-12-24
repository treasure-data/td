require 'spec_helper'
require 'file_reader/shared_context'

require 'td/file_reader'

include TreasureData

describe 'FileReader io filters' do
  include_context 'error_proc'

  describe FileReader::DecompressIOFilter do
  end
end
