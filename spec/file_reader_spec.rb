require 'spec_helper'
require 'file_reader/shared_context'

require 'stringio'
require 'td/file_reader'

include TreasureData

describe FileReader do
  include_context 'error_proc'

  describe 'initialize' do
    subject { FileReader.new }

    describe '#parser_class' do
      subject { super().parser_class }
      it { is_expected.to be_nil }
    end

    describe '#opts' do
      subject { super().opts }
      it { is_expected.to be_empty }
    end
    [:delimiter_expr, :null_expr, :true_expr, :false_expr].each { |key|
      describe '#default_opts' do
        subject { super().default_opts }
        it { is_expected.to have_key(key); }
      end
    }
  end

  let :reader do
    FileReader.new
  end

  describe 'set_format_template' do
    it 'can set csv' do
      reader.set_format_template('csv')
      expect(reader.instance_variable_get(:@format)).to eq('text')
      expect(reader.opts).to include(:delimiter_expr => /,/)
    end

    it 'can set tsv' do
      reader.set_format_template('tsv')
      expect(reader.instance_variable_get(:@format)).to eq('text')
      expect(reader.opts).to include(:delimiter_expr => /\t/)
    end

    it 'can set apache' do
      reader.set_format_template('apache')
      expect(reader.instance_variable_get(:@format)).to eq('apache')
      expect(reader.opts).to include(:time_column => 'time')
    end

    it 'can set syslog' do
      reader.set_format_template('syslog')
      expect(reader.instance_variable_get(:@format)).to eq('syslog')
      expect(reader.opts).to include(:time_column => 'time')
    end

    it 'can set msgpack' do
      reader.set_format_template('msgpack')
      expect(reader.instance_variable_get(:@format)).to eq('msgpack')
    end

    it 'can set json' do
      reader.set_format_template('json')
      expect(reader.instance_variable_get(:@format)).to eq('json')
    end

    it 'raises when set unknown format' do
      expect {
        reader.set_format_template('oreore')
      }.to raise_error(Exception, /Unknown format: oreore/)
    end
  end

  describe 'init_optparse' do
    def parse_opt(argv, &block)
      op = OptionParser.new
      reader.init_optparse(op)
      op.parse!(argv)
      block.call
    end

    context '-f option' do
      ['-f', '--format'].each { |opt|
        ['csv', 'tsv', 'apache', 'syslog', 'msgpack', 'json'].each { |format|
          it "#{opt} option with #{format}" do
            expect(reader).to receive(:set_format_template).with(format)
            parse_opt([opt, format]) { }
          end
        }
      }
    end

    context 'columns names option' do
      ['-h', '--columns'].each { |opt|
        it "#{opt} option" do
          columns = 'A,B,C'
          parse_opt([opt, columns]) {
            expect(reader.opts).to include(:column_names => columns.split(','))
          }
        end
      }
    end

    context 'columns header option' do
      ['-H', '--column-header'].each { |opt|
        it "#{opt} option" do
          parse_opt([opt]) {
            expect(reader.opts).to include(:column_header => true)
          }
        end
      }
    end

    context 'delimiter between column option' do
      ['-d', '--delimiter'].each { |opt|
        it "#{opt} option" do
          pattern = '!'
          parse_opt([opt, pattern]) {
            expect(reader.opts).to include(:delimiter_expr => Regexp.new(pattern))
          }
        end
      }
    end

    context 'null expression option' do
      it "--null REGEX option" do
        pattern = 'null'
        parse_opt(['--null', pattern]) {
          expect(reader.opts).to include(:null_expr => Regexp.new(pattern))
        }
      end
    end

    context 'true expression option' do
      it "--true REGEX option" do
        pattern = 'true'
        parse_opt(['--true', pattern]) {
          expect(reader.opts).to include(:true_expr => Regexp.new(pattern))
        }
      end
    end

    context 'false expression option' do
      it "--false REGEX option" do
        pattern = 'false'
        parse_opt(['--false', pattern]) {
          expect(reader.opts).to include(:false_expr => Regexp.new(pattern))
        }
      end
    end

    context 'disable automatic type conversion option' do
      ['-S', '--all-string'].each { |opt|
        it "#{opt} option" do
          parse_opt([opt]) {
            expect(reader.opts).to include(:all_string => true)
          }
        end
      }
    end

    context 'name of the time column option' do
      ['-t', '--time-column'].each { |opt|
        it "#{opt} option" do
          name = 'created_at'
          parse_opt([opt, name]) {
            expect(reader.opts).to include(:time_column => name)
          }
        end
      }
    end

    context 'strftime(3) format of the time column option' do
      ['-T', '--time-format'].each { |opt|
        it "#{opt} option" do
          format = '%Y'
          parse_opt([opt, format]) {
            expect(reader.opts).to include(:time_format => format)
          }
        end
      }
    end

    context 'value of the time column option' do
      {'int' => lambda { |t| t.to_i.to_s }, 'formatted' => lambda { |t| t.to_s }}.each_pair { |value_type, converter|
        it "--time-value option with #{value_type}" do
          time = Time.now
          parse_opt(['--time-value', converter.call(time)]) {
            expect(reader.opts).to include(:time_value => time.to_i)
          }
        end
      }
    end

    context 'text encoding option' do
      ['-e', '--encoding'].each { |opt|
        it "#{opt} option" do
          enc = 'utf-8'
          parse_opt([opt, enc]) {
            expect(reader.opts).to include(:encoding => enc)
          }
        end
      }
    end

    context 'compression format option' do
      ['-C', '--compress'].each { |opt|
        it "#{opt} option" do
          format = 'gzip'
          parse_opt([opt, format]) {
            expect(reader.opts).to include(:compress => format)
          }
        end
      }
    end
  end

  describe 'compose_factory' do
    it 'returns Proc object' do
      factory = reader.compose_factory
      expect(factory).to be_an_instance_of(Proc)
    end

    # other specs in parse spec
  end

  describe 'parse' do
    let :dataset_header do
      ['name', 'num', 'created_at', 'flag']
    end

    let :dataset_values do
      [
        ['k', 12345, Time.now.to_s, true],
        ['s', 34567, Time.now.to_s, false],
        ['n', 56789, Time.now.to_s, true],
      ]
    end

    let :dataset do
      dataset_values.map { |data|
        Hash[dataset_header.zip(data)]
      }
    end

    let :time_column do
      'created_at'
    end

    def parse_opt(argv, &block)
      op = OptionParser.new
      reader.init_optparse(op)
      op.parse!(argv)
      block.call
    end

    shared_examples_for 'parse --time-value / --time-column cases' do |format, args|
      it "parse #{format} with --time-value" do
        @time = Time.now.to_i
        parse_opt(%W(-f #{format} --time-value #{@time}) + (args || [])) {
          i = 0
          reader.parse(io, error) { |record|
            expect(record).to eq(dataset[i].merge('time' => @time))
            i += 1
          }
        }
      end

      it "parse #{format} with --time-column" do
        parse_opt(%W(-f #{format} --time-column #{time_column}) + (args || [])) {
          i = 0
          reader.parse(io, error) { |record|
            time = record[time_column]
            time = Time.parse(time).to_i if time.is_a?(String)
            expect(record).to eq(dataset[i].merge('time' => time))
            i += 1
          }
        }
      end
    end

    shared_examples_for 'parse --columns / --column-header cases' do |format|
      converter = "to_#{format}".to_sym

      context 'array format' do
        let :lines do
          dataset_values.map { |data| data.__send__(converter) }
        end

        context 'with --column-columns' do
          it_should_behave_like 'parse --time-value / --time-column cases', format, %W(-h name,num,created_at,flag)
        end

        context 'with --column-header' do
          let :lines do
            [dataset_header.__send__(converter)] + dataset_values.map { |data| data.__send__(converter) }
          end

          it_should_behave_like 'parse --time-value / --time-column cases', format, %W(-H)
        end
      end
    end

    let :io do
      StringIO.new(lines.join("\n"))
    end

    context 'json' do
      require 'json'

      let :lines do
        dataset.map(&:to_json)
      end

      it_should_behave_like 'parse --time-value / --time-column cases', 'json'
      it_should_behave_like 'parse --columns / --column-header cases', 'json'
    end

    context 'msgpack' do
      require 'msgpack'

      let :lines do
        dataset.map(&:to_msgpack)
      end

      let :io do
        StringIO.new(lines.join(""))
      end

      it_should_behave_like 'parse --time-value / --time-column cases', 'msgpack'
      it_should_behave_like 'parse --columns / --column-header cases', 'msgpack'
    end

    [['csv', ','], ['tsv', "\t"]].each { |text_type, pattern|
      context 'text' do
        let :lines do
          dataset_values.map { |data| data.map(&:to_s).join(pattern) }
        end

        it "raises an exception without --column-header or --columns in #{pattern}" do
          parse_opt(%W(-f #{text_type})) {
            expect {
              reader.parse(io, error)
            }.to raise_error(Exception, /--column-header or --columns option is required/)
          }
        end

        context 'with --column-columns' do
          it_should_behave_like 'parse --time-value / --time-column cases', text_type, %W(-h name,num,created_at,flag)
        end

        context 'with --column-header' do
          let :lines do
            [dataset_header.join(pattern)] + dataset_values.map { |data| data.map(&:to_s).join(pattern) }
          end

          it_should_behave_like 'parse --time-value / --time-column cases', text_type, %W(-H)
        end

        # TODO: Add all_string
      end
    }

    {
      'apache' => [
        [
          '58.83.188.60 - - [23/Oct/2011:08:15:46 -0700] "HEAD / HTTP/1.0" 200 277 "-" "-"',
          '127.0.0.1 - - [23/Oct/2011:08:20:01 -0700] "GET / HTTP/1.0" 200 492 "-" "Wget/1.12 (linux-gnu)"',
          '68.64.37.100 - - [24/Oct/2011:01:48:54 -0700] "GET /phpMyAdmin/scripts/setup.php HTTP/1.1" 404 480 "-" "ZmEu"'
        ],
        [
          {"host" => "58.83.188.60", "user" => nil, "time" => 1319382946, "method" => "HEAD", "path" => "/", "code" => 200, "size" => 277, "referer" => nil, "agent" => nil},
          {"host" => "127.0.0.1", "user" => nil, "time" => 1319383201, "method" => "GET", "path" => "/", "code" => 200, "size" => 492, "referer" => nil, "agent" => "Wget/1.12 (linux-gnu)"},
          {"host" => "68.64.37.100", "user" => nil, "time" => 1319446134, "method" => "GET", "path" => "/phpMyAdmin/scripts/setup.php", "code" => 404, "size" => 480, "referer" => nil, "agent" => "ZmEu"},
        ]
      ],
      'syslog' => [
        [
          'Dec 20 12:41:44 localhost kernel: [4843680.692840] e1000e: eth2 NIC Link is Down',
          'Dec 20 12:41:44 localhost kernel: [4843680.734466] br0: port 1(eth2) entering disabled state',
          'Dec 22 10:42:41 localhost kernel[10000]: [5009052.220155] zsh[25578]: segfault at 7fe849460260 ip 00007fe8474fd74d sp 00007fffe3bdf0e0 error 4 in libc-2.11.1.so[7fe847486000+17a000]',
        ],
        [
          {"pid" => nil, "time" => 1355974904, "host" => "localhost", "ident" => "kernel", "message" => "[4843680.692840] e1000e: eth2 NIC Link is Down"},
          {"pid" => nil, "time" => 1355974904, "host" => "localhost", "ident" => "kernel", "message" => "[4843680.734466] br0: port 1(eth2) entering disabled state"},
          {"pid" => 10000, "time" => 1356140561, "host" => "localhost", "ident" => "kernel", "message" => "[5009052.220155] zsh[25578]: segfault at 7fe849460260 ip 00007fe8474fd74d sp 00007fffe3bdf0e0 error 4 in libc-2.11.1.so[7fe847486000+17a000]"},
        ]
      ]
    }.each_pair { |format, (input, output)|
      context format do
        let :lines do
          input
        end

        let :dataset do
          output
        end

        let :time_column do
          'time'
        end

        it_should_behave_like 'parse --time-value / --time-column cases', format
      end
    }
  end
end
