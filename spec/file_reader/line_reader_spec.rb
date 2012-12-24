require 'spec_helper'
require 'file_reader/shared_context'

require 'stringio'
require 'td/file_reader'

include TreasureData

describe FileReader::LineReader do
  include_context 'error_proc'

  let :lines do
    [
      '{"host":"128.216.140.97","user":"-","method":"GET","path":"/item/sports/2511","code":200,"referer":"http://www.google.com/search?ie=UTF-8&q=google&sclient=psy-ab&q=Sports+Electronics&oq=Sports+Electronics&aq=f&aqi=g-vL1&aql=&pbx=1&bav=on.2,or.r_gc.r_pw.r_qf.,cf.osb&biw=3994&bih=421","size":95,"agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7","time":1353541928}',
      '{"host":"224.225.147.72","user":"-","method":"GET","path":"/category/electronics","code":200,"referer":"-","size":43,"agent":"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)","time":1353541927}',
      '{"host":"172.75.186.56","user":"-","method":"GET","path":"/category/jewelry","code":200,"referer":"-","size":79,"agent":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)","time":1353541925}',
    ]
  end

  let :io do
    StringIO.new(lines.join("\n"))
  end

  it 'initialize' do
    ee = io.external_encoding
    FileReader::LineReader.new(io, error, {})
    io.external_encoding.should == ee
  end

  it 'initialize with specifid encoding' do
    ee = io.external_encoding
    FileReader::LineReader.new(io, error, {:encoding => 'utf-8'})
    io.external_encoding.should_not == ee
    io.external_encoding.should == Encoding.find('utf-8')
  end

  context 'after initialization' do
    let :reader do
      FileReader::LineReader.new(io, error, {})
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
        io.eof?.should be_true
      end
    end

    describe FileReader::JSONParser do
      it 'initialize with LineReader' do
        parser = FileReader::JSONParser.new(reader, error, {})
        parser.should_not be_nil
      end

      context 'after initialization' do
        let :parser do
          FileReader::JSONParser.new(reader, error, {})
        end

        it 'forward returns one line' do
          parser.forward.should == JSON.parse(lines[0])
        end

        it 'feeds all lines' do
          begin
            i = 0
            while line = parser.forward
              line.should == JSON.parse(lines[i])
              i += 1
            end
          rescue
            io.eof?.should be_true
          end
        end

        context 'with broken line' do
          let :lines do
            [
              '{"host":"128.216.140.97","user":"-","method":"GET","path":"/item/sports/2511","code":200,"referer":"http://www.google.com/search?ie=UTF-8&q=google&sclient=psy-ab&q=Sports+Electronics&oq=Sports+Electronics&aq=f&aqi=g-vL1&aql=&pbx=1&bav=on.2,or.r_gc.r_pw.r_qf.,cf.osb&biw=3994&bih=421","size":95,"agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7","time":1353541928}',
              '{This is invalid as a JSON}',
              '{"host":"172.75.186.56","user":"-","method":"GET","path":"/category/jewelry","code":200,"referer":"-","size":79,"agent":"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)","time":1353541925}',
            ]
          end

          let :error_pattern do
            /^invalid json format/
          end

          it 'feeds all lines' do
            begin
              i = 0
              while line = parser.forward
                line.should == JSON.parse(lines[i])
                i += 2
              end
            rescue
              io.eof?.should be_true
            end
          end
        end
      end
    end

    describe FileReader::DelimiterParser do
      [/,/, /\t/].each { |format|
        let :lines do
          [
            ['hoge', '12345', Time.now.to_s].join(format.source),
            ['foo',  '34567', Time.now.to_s].join(format.source),
            ['piyo', '56789', Time.now.to_s].join(format.source),
          ]
        end

        it "initialize with LineReader and #{format.source} delimiter" do
          parser = FileReader::DelimiterParser.new(reader, error, {:delimiter_expr => format})
          parser.should_not be_nil
        end

        context 'after initialization' do
          let :parser do
            FileReader::DelimiterParser.new(reader, error, {:delimiter_expr => format})
          end

          it 'forward returns one line' do
            parser.forward.should == lines[0].split(format)
          end
          
          it 'feeds all lines' do
            begin
              i = 0
              while line = parser.forward
                line.should == lines[i].split(format)
                i += 1
              end
            rescue
              io.eof?.should be_true
            end
          end
        end
      }
    end
  end
end
