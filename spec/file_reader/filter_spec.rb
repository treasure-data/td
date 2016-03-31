require 'spec_helper'
require 'file_reader/shared_context'

require 'stringio'
require 'td/file_reader'

include TreasureData

describe 'FileReader filters' do
  include_context 'error_proc'

  let :delimiter do
    "\t"
  end

  let :dataset do
    [
      ['hoge', 12345, true,  'null', Time.now.to_s],
      ['foo',  34567, false, 'null', Time.now.to_s],
      ['piyo', 56789, true,  nil,    Time.now.to_s],
    ]
  end

  let :lines do
    dataset.map { |data| data.map(&:to_s).join(delimiter) }
  end

  let :parser do
    io = StringIO.new(lines.join("\n"))
    reader = FileReader::LineReader.new(io, error, {})
    FileReader::DelimiterParser.new(reader, error, :delimiter_expr => delimiter)
  end

  describe FileReader::AutoTypeConvertParserFilter do
    let :options do
      { 
        :null_expr => /\A(?:nil||\-|\\N)\z/i,
        :true_expr => /\A(?:true)\z/i,
        :false_expr => /\A(?:false)\z/i,
      }
    end

    it 'initialize' do
      filter = FileReader::AutoTypeConvertParserFilter.new(parser, error, options)
      expect(filter).not_to be_nil
    end

    context 'after initialization' do
      let :filter do
        FileReader::AutoTypeConvertParserFilter.new(parser, error, options)
      end

      it 'forward returns one converted line' do
        expect(filter.forward).to eq(dataset[0])
      end

      it 'feeds all lines' do
        begin
          i = 0
          while line = filter.forward
            expect(line).to eq(dataset[i])
            i += 1
          end
        rescue
        end
      end
    end
  end

  describe FileReader::HashBuilder do
    let :columns do
      ['str', 'num', 'bool', 'null', 'log_at']
    end

    let :built_dataset do
      # [{"str" => "hoge", "num" => "12345", "bool" => "true" , "null" =>"null", "log_at" => "2012-12-26 05:14:09 +0900"}, ...]
      dataset.map { |data| Hash[columns.zip(data.map(&:to_s))]}
    end

    it 'initialize' do
      builder = FileReader::HashBuilder.new(parser, error, columns)
      expect(builder).not_to be_nil
    end

    context 'after initialization' do
      let :builder do
        FileReader::HashBuilder.new(parser, error, columns)
      end

      it 'forward returns one converted line' do
        expect(builder.forward).to eq(built_dataset[0])
      end

      it 'feeds all lines' do
        begin
          i = 0
          while line = builder.forward
            expect(line).to eq(built_dataset[i])
            i += 1
          end
        rescue
        end
      end

      describe FileReader::TimeParserFilter do
        it "can't be initialized without :time_column option" do
          expect { 
            FileReader::TimeParserFilter.new(parser, error, {})
          }.to raise_error(Exception, /--time-column/)
        end

        it 'initialize' do
          filter = FileReader::TimeParserFilter.new(builder, error, :time_column => 'log_at')
          expect(filter).not_to be_nil
        end

        context 'after initialization' do
          let :timed_dataset do
            require 'time'
            built_dataset.each { |data| data['time'] = Time.parse(data['log_at']).to_i }
          end

          let :filter do
            FileReader::TimeParserFilter.new(builder, error, :time_column => 'log_at')
          end

          it 'forward returns one parse line with parsed log_at' do
            expect(filter.forward).to eq(timed_dataset[0])
          end

          it 'feeds all lines' do
            begin
              i = 0
              while line = filter.forward
                expect(line).to eq(timed_dataset[i])
                i += 1
              end
            rescue
            end
          end

          context 'missing log_at column lines' do
            let :columns do
              ['str', 'num', 'bool', 'null', 'created_at']
            end

            let :error_pattern do
              /^time column 'log_at' is missing/
            end

            it 'feeds all lines' do
              i = 0
              begin
                while line = filter.forward
                  i += 1
                end
              rescue RSpec::Expectations::ExpectationNotMetError => e
                fail
              rescue
                expect(i).to eq(0)
              end
            end
          end

          context 'invalid time format' do
            let :error_pattern do
              /^invalid time format/
            end

            [{:time_column => 'log_at', :time_format => "%d"},
             {:time_column => 'str'}].each { |options|
              let :filter do
                FileReader::TimeParserFilter.new(builder, error, options)
              end

              it 'feeds all lines' do
                i = 0
                begin
                  while line = filter.forward
                    i += 1
                  end
                rescue RSpec::Expectations::ExpectationNotMetError => e
                  fail
                rescue
                  expect(i).to eq(0)
                end
              end
            }
          end
        end
      end

      describe FileReader::SetTimeParserFilter do
        it "can't be initialized without :time_value option" do
          expect { 
            FileReader::SetTimeParserFilter.new(parser, error, {})
          }.to raise_error(Exception, /--time-value/)
        end

        it 'initialize' do
          filter = FileReader::SetTimeParserFilter.new(builder, error, :time_value => Time.now.to_i)
          expect(filter).not_to be_nil
        end

        context 'after initialization' do
          let :time_value do
            Time.now.to_i
          end

          let :timed_dataset do
            built_dataset.each { |data| data['time'] = time_value }
          end

          let :filter do
            FileReader::SetTimeParserFilter.new(builder, error, :time_value => time_value)
          end

          it 'forward returns one converted line with time' do
            expect(filter.forward).to eq(timed_dataset[0])
          end

          it 'feeds all lines' do
            begin
              i = 0
              while line = filter.forward
                expect(line).to eq(timed_dataset[i])
                i += 1
              end
            rescue
            end
          end
        end
      end
    end
  end
end
