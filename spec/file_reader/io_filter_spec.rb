require 'spec_helper'
require 'file_reader/shared_context'

require 'stringio'
require 'td/file_reader'

include TreasureData

describe 'FileReader io filters' do
  include_context 'error_proc'

  describe FileReader::DecompressIOFilter do
    let :lines do
      [
        '{"host":"128.216.140.97","user":"-","method":"GET","path":"/item/sports/2511","code":200,"size":95,"time":1353541928}',
        '{"host":"224.225.147.72","user":"-","method":"GET","path":"/category/electronics","code":200,"size":43,"time":1353541927}',
        '{"host":"172.75.186.56","user":"-","method":"GET","path":"/category/jewelry","code":200,"size":79,"time":1353541925}',
      ]
    end

    let :io do
      StringIO.new(lines.join("\n"))
    end

    let :gzipped_io do
      require 'zlib'

      io = StringIO.new('', 'w+')
      gz = Zlib::GzipWriter.new(io)
      gz.write(lines.join("\n"))
      gz.close
      StringIO.new(io.string)
    end

    it "can't filter with unknown compression" do
      expect { 
        FileReader::DecompressIOFilter.filter(io, error, :compress => 'oreore')
      }.to raise_error(Exception, /unknown compression/)
    end

    describe 'gzip' do
      it "can't be wrapped with un-gzipped io" do
        expect {
          FileReader::DecompressIOFilter.filter(io, error, :compress => 'gzip')
        }.to raise_error(Zlib::GzipFile::Error)
      end

      it 'returns Zlib::GzipReader with :gzip' do
        wrapped = FileReader::DecompressIOFilter.filter(gzipped_io, error, :compress => 'gzip')
        wrapped.should be_an_instance_of(Zlib::GzipReader)
      end

      it 'returns Zlib::GzipReader with auto detection' do
        wrapped = FileReader::DecompressIOFilter.filter(gzipped_io, error, {})
        wrapped.should be_an_instance_of(Zlib::GzipReader)
      end

      context 'after initialization' do
        [{:compress => 'gzip'}, {}].each { |opts|
          let :reader do
            wrapped = FileReader::DecompressIOFilter.filter(gzipped_io, error, opts)
            FileReader::LineReader.new(wrapped, error, {})
          end

          it 'forward_row returns one line' do
            reader.forward_row.should == lines[0]
          end

          it 'feeds all lines' do
            begin
              i = 0
              while line = reader.forward_row
                line.should == lines[i]
                i += 1
              end
            rescue
              gzipped_io.eof?.should be_true
            end
          end
        }
      end
    end

    describe 'plain' do
      it 'returns passed io with :plain' do
        wrapped = FileReader::DecompressIOFilter.filter(io, error, :compress => 'plain')
        wrapped.should be_an_instance_of(StringIO)
      end

      it 'returns passed io with auto detection' do
        wrapped = FileReader::DecompressIOFilter.filter(io, error, {})
        wrapped.should be_an_instance_of(StringIO)
      end
    end
  end
end
