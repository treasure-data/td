require 'spec_helper'
require 'td/helpers'

module TreasureData
  describe 'format_with_delimiter' do
    it "delimits the number with ',' by default" do
      expect(Helpers.format_with_delimiter(0)).to eq("0")
      expect(Helpers.format_with_delimiter(10)).to eq("10")
      expect(Helpers.format_with_delimiter(100)).to eq("100")
      expect(Helpers.format_with_delimiter(1000)).to eq("1,000")
      expect(Helpers.format_with_delimiter(10000)).to eq("10,000")
      expect(Helpers.format_with_delimiter(100000)).to eq("100,000")
      expect(Helpers.format_with_delimiter(1000000)).to eq("1,000,000")
    end
  end
end
