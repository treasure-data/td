require 'spec_helper'
require 'td/compact_format_yamler'

module TreasureData
  describe TreasureData::CompactFormatYamler do
    describe '.dump' do
      let(:data) {
        {
          'a' => {
            'b' => {
              'c' => 1,
              'd' => 'e'
            },
            'f' => [1, 2, 3]
          }
        }
      }

      let(:comapct_format_yaml) {
        <<-EOS
---
a:
  b: {c: 1, d: e}
  f:
  - 1
  - 2
  - 3
        EOS
      }

      subject { TreasureData::CompactFormatYamler.dump data }

      it 'use compact format for deepest Hash' do
        expect(subject).to eq comapct_format_yaml
      end
    end
  end
end
