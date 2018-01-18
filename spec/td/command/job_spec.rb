# encoding: utf-8

require 'spec_helper'
require 'td/command/common'
require 'td/command/job'
require 'td/command/list'
require 'tempfile'

module TreasureData::Command
  describe 'job commands' do
    let :command do
      Class.new { include TreasureData::Command }.new
    end

    describe 'write_result' do
      let(:file) { Tempfile.new("job_spec").tap {|s| s.close } }

      let :job do
        job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
        job.instance_eval do
          @result = [[["1", 2.0, {key:3}], 1], [["4", 5.0, {key:6}], 2], [["7", 8.0, {key:9}], 3]]
          @result_size = 3
          @status = 'success'
        end
        job
      end

      describe "using tempfile" do
        let(:tempfile) { "#{file.path}.tmp" }

        subject { command.send(:show_result, job, file, nil, format) }

        context "format: json" do
          let(:format) { "json" }

          it do
            expect(FileUtils).to receive(:mv).with(tempfile, file.path)
            subject
          end
          it do
            expect(command).to receive(:open_file).with(tempfile, "w")
            subject
          end
        end

        context "format: csv" do
          let(:format) { "csv" }

          it do
            expect(FileUtils).to receive(:mv).with(tempfile, file.path)
            subject
          end
          it do
            expect(command).to receive(:open_file).with(tempfile, "w")
            subject
          end
        end

        context "format: tsv" do
          let(:format) { "tsv" }

          it do
            expect(FileUtils).to receive(:mv).with(tempfile, file.path)
            subject
          end
          it do
            expect(command).to receive(:open_file).with(tempfile, "w")
            subject
          end
        end

        context "format: msgpack" do
          let(:format) { "msgpack" }

          before do
            allow(job).to receive(:result_format) # for msgpack
          end

          it do
            expect(FileUtils).to receive(:mv).with(tempfile, file.path)
            subject
          end
          it do
            expect(command).to receive(:open_file).with(tempfile, "wb")
            subject
          end
        end

        context "format: msgpack.gz" do
          let(:format) { "msgpack.gz" }

          before do
            allow(job).to receive(:result_raw)    # for msgpack.gz
          end

          it do
            expect(FileUtils).to receive(:mv).with(tempfile, file.path)
            subject
          end
          it do
            expect(command).to receive(:open_file).with(tempfile, "wb")
            subject
          end
        end
      end

      context 'result without nil' do
        it 'supports json output' do
          command.send(:show_result, job, file, nil, 'json')
          expect(File.read(file.path)).to eq(%Q([["1",2.0,{"key":3}],\n["4",5.0,{"key":6}],\n["7",8.0,{"key":9}]]))
        end

        it 'supports csv output' do
          command.send(:show_result, job, file, nil, 'csv')
          expect(File.read(file.path)).to eq(%Q(1,2.0,"{""key"":3}"\n4,5.0,"{""key"":6}"\n7,8.0,"{""key"":9}"\n))
        end

        it 'supports tsv output' do
          command.send(:show_result, job, file, nil, 'tsv')
          expect(File.read(file.path)).to eq(%Q(1\t2.0\t{"key":3}\n4\t5.0\t{"key":6}\n7\t8.0\t{"key":9}\n))
        end
      end

      context 'result with nil' do
        let :job_id do
          12345
        end

        let :job do
          job = TreasureData::Job.new(nil, job_id, 'hive', 'select * from employee')
          job.instance_eval do
            @result = [[[nil, 2.0, {key:3}], 1]]
            @result_size = 3
            @status = 'success'
          end
          job
        end

        context 'with --column-header option' do
          before do
            allow(job).to receive(:hive_result_schema).and_return([['c0', 'time'], ['c1', 'double'], ['v', nil], ['c3', 'long']])
            client = Object.new
            allow(client).to receive(:job).with(job_id).and_return(job)
            allow(command).to receive(:get_client).and_return(client)
          end

          it 'supports json output' do
            command.send(:show_result, job, file, nil, 'json', { header: true })
            expect(File.read(file.path)).to eq(%Q([{"c0":null,"c1":2.0,"v":{"key":3},"c3":null}]))
          end

          it 'supports csv output' do
            command.send(:show_result, job, file, nil, 'csv', { header: true })
            expect(File.read(file.path)).to eq(%Q(c0,c1,v,c3\nnull,2.0,"{""key"":3}"\n))
          end

          it 'supports tsv output' do
            command.send(:show_result, job, file, nil, 'tsv', { header: true })
            expect(File.read(file.path)).to eq(%Q(c0\tc1\tv\tc3\nnull\t2.0\t{"key":3}\n))
          end
        end

        context 'without --null option' do
          it 'supports json output' do
            command.send(:show_result, job, file, nil, 'json')
            expect(File.read(file.path)).to eq(%Q([[null,2.0,{"key":3}]]))
          end

          it 'supports csv output' do
            command.send(:show_result, job, file, nil, 'csv')
            expect(File.read(file.path)).to eq(%Q(null,2.0,"{""key"":3}"\n))
          end

          it 'supports tsv output' do
            command.send(:show_result, job, file, nil, 'tsv')
            expect(File.read(file.path)).to eq(%Q(null\t2.0\t{"key":3}\n))
          end
        end

        context 'with --null option' do
          it 'dose not effect json output (nil will be shown as null)' do
            command.send(:show_result, job, file, nil, 'json', { null_expr: "NULL" })
            expect(File.read(file.path)).to eq(%Q([[null,2.0,{"key":3}]]))
          end

          context 'csv format' do
            context 'specified string is NULL' do
              let!(:null_expr) { "NULL" }

              it 'shows nill as specified string' do
                command.send(:show_result, job, file, nil, 'csv', { null_expr: null_expr })
                expect(File.read(file.path)).to eq(%Q(NULL,2.0,"{""key"":3}"\n))
              end
            end

            context 'specified string is empty string' do
              let!(:null_expr) { '' }

              it 'shows nill as empty string' do
                command.send(:show_result, job, file, nil, 'csv', { null_expr: null_expr })
                expect(File.read(file.path)).to eq(%Q("",2.0,"{""key"":3}"\n))
              end
            end
          end

          it 'supports tsv output' do
            command.send(:show_result, job, file, nil, 'tsv', { null_expr: "\"\"" })
            expect(File.read(file.path)).to eq(%Q(""\t2.0\t{"key":3}\n))
          end
        end
      end

      context 'without NaN/Infinity' do

        it 'supports json output' do
          command.send(:show_result, job, file, nil, 'json')
          expect(File.read(file.path)).to eq(%Q([["1",2.0,{"key":3}],\n["4",5.0,{"key":6}],\n["7",8.0,{"key":9}]]))
        end

        it 'supports csv output' do
          command.send(:show_result, job, file, nil, 'csv')
          expect(File.read(file.path)).to eq(%Q(1,2.0,"{""key"":3}"\n4,5.0,"{""key"":6}"\n7,8.0,"{""key"":9}"\n))
        end

        it 'supports tsv output' do
          command.send(:show_result, job, file, nil, 'tsv')
          expect(File.read(file.path)).to eq(%Q(1\t2.0\t{"key":3}\n4\t5.0\t{"key":6}\n7\t8.0\t{"key":9}\n))
        end
      end

      context 'with NaN/Infinity' do
        let :job do
          job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
          job.instance_eval do
            inf = Float::INFINITY
            nan = Float::NAN
            @result = [
              [[nan, inf, -inf], 1],
              [[[nan, inf, -inf], 5.0, {key:6}], 2],
              [["7", 8.0, {key:9}], 3],
            ]
            @result_size = 3
            @status = 'success'
          end
          job
        end

        it 'does not support json output' do
          expect { command.send(:show_result, job, file, nil, 'json') }.to raise_error Yajl::EncodeError
        end

        it 'supports csv output' do
          command.send(:show_result, job, file, nil, 'csv')
          expect(File.read(file.path)).to eq <<text
"""NaN""","""Infinity""","""-Infinity"""
"[""NaN"",""Infinity"",""-Infinity""]",5.0,"{""key"":6}"
7,8.0,"{""key"":9}"
text
        end

        it 'supports tsv output' do
          command.send(:show_result, job, file, nil, 'tsv')
          expect(File.read(file.path)).to eq <<text
"NaN"\t"Infinity"\t"-Infinity"
["NaN","Infinity","-Infinity"]\t5.0\t{"key":6}
7\t8.0\t{"key":9}
text
        end
      end
    end

    describe '#job_show' do
      let(:job_id) { "12345" }

      let :job_classs do
        Struct.new(:job_id,
                   :status,
                   :type,
                   :db_name,
                   :priority,
                   :retry_limit,
                   :result_url,
                   :query,
                   :cpu_time,
                   :result_size
                  )
      end

      let :job do
        job_classs.new(job_id,
                       nil,
                       :hive,
                       "db_name",
                       1,
                       1,
                       "test_url",
                       "test_qury",
                       1,
                       3
                      )
      end

      before do
        allow(job).to receive(:finished?).and_return(true)

        client = Object.new
        allow(client).to receive(:job).with(job_id).and_return(job)
        allow(command).to receive(:get_client).and_return(client)
      end

      context 'without --null option' do
        it 'calls #show_result without null_expr option' do
          allow(command).to receive(:show_result).with(job, nil, nil, nil, {:header=>false})
          op = List::CommandParser.new("job:show", %w[job_id], %w[], nil, ["12345"], true)
          command.job_show(op)
        end
      end

      context 'with --null option' do
        it 'calls #show_result with null_expr option' do
          allow(command).to receive(:show_result).with(job, nil, nil, nil, {:header=>false, :null_expr=>"NULL"} )
          op = List::CommandParser.new("job:show", %w[job_id], %w[], nil, ["12345", "--null", "NULL"], true)
          command.job_show(op)
        end

        it 'calls #show_result with null_expr option' do
          allow(command).to receive(:show_result).with(job, nil, nil, nil, {:header=>false, :null_expr=>'""'} )
          op = List::CommandParser.new("job:show", %w[job_id], %w[], nil, ["12345", "--null", '""'], true)
          command.job_show(op)
        end
      end
    end

    describe '#job_list' do
      subject do
        backup = $stdout.dup
        buf = StringIO.new
        op = List::CommandParser.new("job:list", [], [:max], nil, [], true)

        begin
          $stdout = buf
          command.job_list(op)
          buf.string
        ensure
          $stdout = backup
        end
      end

      let(:job_id) { "12345" }

      let :start_at do
        Time.now
      end

      let :client do
        double('client')
      end

      let :jobs do
        [TreasureData::Job.new(client,
                       job_id,
                       :hive,
                       "test_qury",
                       nil,
                       nil,
                       nil,
                       start_at.iso8601,
                       (start_at + 10).iso8601,
                       1,
                       3,
                       nil,
                       "test_url",
                       nil,
                       1,
                       1,
                       nil,
                       "db_name",
                       100,
                       1,
                      )] * 3
      end

      before do
        allow(client).to receive(:jobs).and_return(jobs)
        allow(command).to receive(:get_client).and_return(client)
      end

      it 'should display all job list' do
        expect(subject).to eq <<ACTUAL
+-------+--------+---------------------------+-------------+-------------------+------------+----------+----------+------+----------+---------------+----------+
| JobID | Status | Start                     | Elapsed     | CPUTime           | ResultSize | Priority | Result   | Type | Database | Query         | Duration |
+-------+--------+---------------------------+-------------+-------------------+------------+----------+----------+------+----------+---------------+----------+
| 12345 |        | #{start_at} |         10s |             001ms | 3 B        | HIGH     | test_url | hive | db_name  | test_qury ... | 00:01:40 |
| 12345 |        | #{start_at} |         10s |             001ms | 3 B        | HIGH     | test_url | hive | db_name  | test_qury ... | 00:01:40 |
| 12345 |        | #{start_at} |         10s |             001ms | 3 B        | HIGH     | test_url | hive | db_name  | test_qury ... | 00:01:40 |
+-------+--------+---------------------------+-------------+-------------------+------------+----------+----------+------+----------+---------------+----------+
3 rows in set
ACTUAL
      end
    end

    describe 'multibyte chars' do
      let :multibyte_string do
        # Originally a Windows-31J but in UTF-8 like msgpack-ruby populates
        "\x83\x81\x81[\x83\x8B"
      end

      let :multibyte_row do
        [multibyte_string, 2.0, {multibyte_string => multibyte_string}]
      end

      let(:line_separator) {
        if RUBY_PLATFORM =~ /mswin32|mingw32/
          "\r\n"
        else
          "\n"
        end
      }

      let :job do
        row = multibyte_row
        job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
        job.instance_eval do
          @result = [[row, 1], [row, 2]]
          @result_size = 2
          @status = 'success'
        end
        job
      end

      it 'assumes test setting is correct' do
        # the String is actually in Windows-31J but encoding is UTF-8 msgpack-ruby reports
        expect(multibyte_string.encoding).to eq(Encoding::UTF_8)
        expect(multibyte_string.force_encoding('Windows-31J').encode('UTF-8')).to eq('メール')
      end

      it 'supports json output' do
        row = multibyte_row
        file = Tempfile.new("job_spec").tap {|s| s.close }
        command.send(:show_result, job, file, nil, 'json')
        expect(File.read(file.path, encoding: 'UTF-8')).to eq('[' + [row, row].map { |e| Yajl.dump(e) }.join(",\n") + ']')
      end

      it 'supports csv output' do
        row = multibyte_row.map { |e| dump_column(e) }
        file = Tempfile.new("job_spec").tap {|s| s.close }
        command.send(:show_result, job, file, nil, 'csv')
        expect(File.binread(file.path)).to eq([row, row].map { |e| CSV.generate_line(e, :row_sep => line_separator) }.join)
        expect(File.open(file.path, 'r:Windows-31J').read.encode('UTF-8').split.first).to eq('メール,2.0,"{""メール"":""メール""}"')
      end

      it 'supports tsv output' do
        row = multibyte_row.map { |e| dump_column(e) }
        file = Tempfile.new("job_spec").tap {|s| s.close }
        command.send(:show_result, job, file, nil, 'tsv')
        expect(File.binread(file.path)).to eq([row, row].map { |e| e.join("\t") + line_separator }.join)
        expect(File.open(file.path, 'r:Windows-31J').read.encode('UTF-8').split("\n").first).to eq("メール\t2.0\t{\"メール\":\"メール\"}")
      end
    end

    def dump_column(v)
      s = v.is_a?(String) ? v.to_s : Yajl.dump(v)
      s = s.force_encoding('BINARY')
      s
    end
  end
end
