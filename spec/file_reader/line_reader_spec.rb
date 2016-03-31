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
    if io.respond_to?(:external_encoding)
      ee = io.external_encoding
    end
    FileReader::LineReader.new(io, error, {})
    if io.respond_to?(:external_encoding)
      expect(io.external_encoding).to eq(ee)
    end
  end

  it 'initialize with specifid encoding' do
    if io.respond_to?(:external_encoding)
      original_encoding = io.external_encoding
    end

    # when RUBY_VERSION >= 2.0, default encoding is utf-8.
    # ensure that external_encoding is differ from the original external_encoding(original_encoding).
    if original_encoding == Encoding.find('utf-8')
      specified_encoding = 'sjis'
    else
      specified_encoding = 'utf-8'
    end

    FileReader::LineReader.new(io, error, {:encoding => specified_encoding})
    if io.respond_to?(:external_encoding)
      expect(io.external_encoding).not_to eq(original_encoding)
      expect(io.external_encoding).to eq(Encoding.find(specified_encoding))
    end
  end

  context 'after initialization' do
    let :reader do
      FileReader::LineReader.new(io, error, {})
    end

    it 'forward_row returns one line' do
      expect(reader.forward_row).to eq(lines[0])
    end

    # TODO: integrate with following shared_examples_for
    it 'feeds all lines' do
      begin
        i = 0
        while line = reader.forward_row
          expect(line).to eq(lines[i])
          i += 1
        end
      rescue RSpec::Expectations::ExpectationNotMetError => e
        fail
      rescue
        expect(io.eof?).to be_truthy
      end
    end

    shared_examples_for 'parser iterates all' do |step|
      step = step || 1

      it 'feeds all' do
        begin
          i = 0
          while line = parser.forward
            expect(line).to eq(get_expected.call(i))
            i += step
          end
        rescue RSpec::Expectations::ExpectationNotMetError => e
          fail
        rescue
          expect(io.eof?).to be_truthy
        end
      end
    end

    describe FileReader::JSONParser do
      it 'initialize with LineReader' do
        parser = FileReader::JSONParser.new(reader, error, {})
        expect(parser).not_to be_nil
      end

      context 'after initialization' do
        let :parser do
          FileReader::JSONParser.new(reader, error, {})
        end

        it 'forward returns one line' do
          expect(parser.forward).to eq(JSON.parse(lines[0]))
        end

        let :get_expected do
          lambda { |i| JSON.parse(lines[i]) }
        end

        it_should_behave_like 'parser iterates all'

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

          it_should_behave_like 'parser iterates all', 2
        end
      end
    end

    [',', "\t"].each { |pattern|
      describe FileReader::DelimiterParser do
        let :lines do
          [
            ['hoge', '12345', Time.now.to_s].join(pattern),
            ['foo',  '34567', Time.now.to_s].join(pattern),
            ['piyo', '56789', Time.now.to_s].join(pattern),
          ]
        end

        it "initialize with LineReader and #{pattern} delimiter" do
          parser = FileReader::DelimiterParser.new(reader, error, {:delimiter_expr => Regexp.new(pattern)})
          expect(parser).not_to be_nil
        end

        context 'after initialization' do
          let :parser do
            FileReader::DelimiterParser.new(reader, error, {:delimiter_expr => Regexp.new(pattern)})
          end

          it 'forward returns one line' do
            expect(parser.forward).to eq(lines[0].split(pattern))
          end

          let :get_expected do
            lambda { |i| lines[i].split(pattern) }
          end

          it_should_behave_like 'parser iterates all'
        end
      end
    }

    {
      FileReader::ApacheParser => [
        [
          '58.83.188.60 - - [23/Oct/2011:08:15:46 -0700] "HEAD / HTTP/1.0" 200 277 "-" "-"',
          '127.0.0.1 - - [23/Oct/2011:08:20:01 -0700] "GET / HTTP/1.0" 200 492 "-" "Wget/1.12 (linux-gnu)"',
          '68.64.37.100 - - [24/Oct/2011:01:48:54 -0700] "GET /phpMyAdmin/scripts/setup.php HTTP/1.1" 404 480 "-" "ZmEu"'
        ],
        [
          ["58.83.188.60", "-", "23/Oct/2011:08:15:46 -0700", "HEAD", "/", "200", "277", "-", "-"],
          ["127.0.0.1", "-", "23/Oct/2011:08:20:01 -0700", "GET", "/", "200", "492", "-", "Wget/1.12 (linux-gnu)"],
          ["68.64.37.100", "-", "24/Oct/2011:01:48:54 -0700", "GET", "/phpMyAdmin/scripts/setup.php", "404", "480", "-", "ZmEu"],
        ]
      ],
      FileReader::SyslogParser => [
        [
          'Dec 20 12:41:44 localhost kernel: [4843680.692840] e1000e: eth2 NIC Link is Down',
          'Dec 20 12:41:44 localhost kernel: [4843680.734466] br0: port 1(eth2) entering disabled state',
          'Dec 22 10:42:41 localhost kernel: [5009052.220155] zsh[25578]: segfault at 7fe849460260 ip 00007fe8474fd74d sp 00007fffe3bdf0e0 error 4 in libc-2.11.1.so[7fe847486000+17a000]',
        ],
        [
          ["Dec 20 12:41:44", "localhost", "kernel", nil, "[4843680.692840] e1000e: eth2 NIC Link is Down"],
          ["Dec 20 12:41:44", "localhost", "kernel", nil, "[4843680.734466] br0: port 1(eth2) entering disabled state"],
          ["Dec 22 10:42:41", "localhost", "kernel", nil, "[5009052.220155] zsh[25578]: segfault at 7fe849460260 ip 00007fe8474fd74d sp 00007fffe3bdf0e0 error 4 in libc-2.11.1.so[7fe847486000+17a000]"],
        ]
      ]
    }.each_pair { |parser_class, (input, output)|
      describe parser_class do
        let :lines do
          input
        end

        it "initialize with LineReader" do
          parser = parser_class.new(reader, error, {})
          expect(parser).not_to be_nil
        end

        context 'after initialization' do
          let :parser do
            parser_class.new(reader, error, {})
          end

          it 'forward returns one line' do
            expect(parser.forward).to eq(output[0])
          end

          let :get_expected do
            lambda { |i| output[i] }
          end

          it_should_behave_like 'parser iterates all'

          context 'with broken line' do
            let :lines do
              broken = input.dup
              broken[1] = "Raw text sometimes is broken!"
              broken
            end

            let :error_pattern do
              /^invalid #{parser.instance_variable_get(:@format)} format/
            end

            it_should_behave_like 'parser iterates all', 2
          end
        end
      end
    }
  end
end
