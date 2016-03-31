# -*- coding: utf-8 -*-
require 'spec_helper'
require 'file_reader/shared_context'

require 'stringio'
require 'msgpack'
require 'td/file_reader'

include TreasureData

describe 'FileReader parsing readers' do
  include_context 'error_proc'

  shared_examples_for 'forward basics' do
    it 'forward returns one data' do
      expect(reader.forward).to eq(dataset[0])
    end

    it 'feeds all dataset' do
      begin
        i = 0
        while line = reader.forward
          expect(line).to eq(dataset[i])
          i += 1
        end
      rescue RSpec::Expectations::ExpectationNotMetError => e
          fail
      rescue => e
        expect(io.eof?).to be_truthy
      end
    end
  end

  describe FileReader::MessagePackParsingReader do
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
      expect(reader).not_to be_nil
    end

    context 'after initialization' do
      let :reader do
        FileReader::MessagePackParsingReader.new(io, error, {})
      end

      it_should_behave_like 'forward basics'
    end
  end

  test_time = Time.now.to_i
  {
    'csv' => [
      {:delimiter_expr => ',', :quote_char => '"', :encoding => 'utf-8'},
      [
        %!k,123,"fo\no",true,#{test_time}!,
        %!s,456,"Ｔ,Ｄ",false,#{test_time}!,
        %!n,789,"ba""z",false,#{test_time}!,
      ],
      [
        %W(k 123 fo\no true #{test_time}),
        %W(s 456 Ｔ,Ｄ false #{test_time}),
        %W(n 789 ba\"z false #{test_time}),
      ]
    ],
    'tsv' => [
      {:delimiter_expr => "\t"},
      [
        %!k\t123\t"fo\no"\ttrue\t#{test_time}!,
        %!s\t456\t"b,ar"\tfalse\t#{test_time}!,
        %!n\t789\t"ba\tz"\tfalse\t#{test_time}!,
      ],
      [
        %W(k 123 fo\no true #{test_time}),
        %W(s 456 b,ar false #{test_time}),
        %W(n 789 ba\tz false #{test_time}),
      ]
    ]
  }.each_pair { |format, (opts, input, output)|
    describe FileReader::SeparatedValueParsingReader do
      let :dataset do
        output
      end

      let :lines do
        input
      end

      let :io do
        StringIO.new(lines.join($/))
      end

      it "initialize #{format}" do
        reader = FileReader::SeparatedValueParsingReader.new(io, error, opts)
        expect(reader).not_to be_nil
      end

      context "after #{format} initialization" do
        let :reader do
          reader = FileReader::SeparatedValueParsingReader.new(io, error, opts)
        end

        it_should_behave_like 'forward basics'

        context "broken encodings" do
        end
      end
    end
  }
end
