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
      let(:file) { Tempfile.new("job_spec") }

      let :job do
        job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
        job.instance_eval do
          @result = [[["1", 2.0, {key:3}], 1], [["4", 5.0, {key:6}], 2], [["7", 8.0, {key:9}], 3]]
          @result_size = 3
          @status = 'success'
        end
        job
      end

      context 'result without nil' do
        it 'supports json output' do
          command.send(:show_result, job, file, nil, 'json')
          File.read(file.path).should == %Q([["1",2.0,{"key":3}],\n["4",5.0,{"key":6}],\n["7",8.0,{"key":9}]])
        end

        it 'supports csv output' do
          command.send(:show_result, job, file, nil, 'csv')
          File.read(file.path).should == %Q(1,2.0,"{""key"":3}"\n4,5.0,"{""key"":6}"\n7,8.0,"{""key"":9}"\n)
        end

        it 'supports tsv output' do
          command.send(:show_result, job, file, nil, 'tsv')
          File.read(file.path).should == %Q(1\t2.0\t{"key":3}\n4\t5.0\t{"key":6}\n7\t8.0\t{"key":9}\n)
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
            job.stub(:hive_result_schema).and_return([['c0', 'time'], ['c1', 'double'], ['v', nil], ['c3', 'long']])
            client = Object.new
            client.stub(:job).with(job_id).and_return(job)
            command.stub(:get_client).and_return(client)
          end

          it 'supports json output' do
            command.send(:show_result, job, file, nil, 'json', { header: true })
            File.read(file.path).should == %Q([[null,2.0,{"key":3}]])
          end

          it 'supports csv output' do
            command.send(:show_result, job, file, nil, 'csv', { header: true })
            File.read(file.path).should == %Q(c0,c1,v,c3\nnull,2.0,"{""key"":3}"\n)
          end

          it 'supports tsv output' do
            command.send(:show_result, job, file, nil, 'tsv', { header: true })
            File.read(file.path).should == %Q(c0\tc1\tv\tc3\nnull\t2.0\t{"key":3}\n)
          end
        end

        context 'without --null option' do
          it 'supports json output' do
            command.send(:show_result, job, file, nil, 'json')
            File.read(file.path).should == %Q([[null,2.0,{"key":3}]])
          end

          it 'supports csv output' do
            command.send(:show_result, job, file, nil, 'csv')
            File.read(file.path).should == %Q(null,2.0,"{""key"":3}"\n)
          end

          it 'supports tsv output' do
            command.send(:show_result, job, file, nil, 'tsv')
            File.read(file.path).should == %Q(null\t2.0\t{"key":3}\n)
          end
        end

        context 'with --null option' do
          it 'dose not effect json output (nil will be shown as null)' do
            command.send(:show_result, job, file, nil, 'json', { null_expr: "NULL" })
            File.read(file.path).should == %Q([[null,2.0,{"key":3}]])
          end

          context 'csv format' do
            context 'specified string is NULL' do
              let!(:null_expr) { "NULL" }

              it 'shows nill as specified string' do
                command.send(:show_result, job, file, nil, 'csv', { null_expr: null_expr })
                File.read(file.path).should == %Q(NULL,2.0,"{""key"":3}"\n)
              end
            end

            context 'specified string is empty string' do
              let!(:null_expr) { '' }

              it 'shows nill as empty string' do
                command.send(:show_result, job, file, nil, 'csv', { null_expr: null_expr })
                File.read(file.path).should == %Q("",2.0,"{""key"":3}"\n)
              end
            end
          end

          it 'supports tsv output' do
            command.send(:show_result, job, file, nil, 'tsv', { null_expr: "\"\"" })
            File.read(file.path).should == %Q(""\t2.0\t{"key":3}\n)
          end
        end
      end

      context 'without NaN/Infinity' do

        it 'supports json output' do
          command.send(:show_result, job, file, nil, 'json')
          File.read(file.path).should == %Q([["1",2.0,{"key":3}],\n["4",5.0,{"key":6}],\n["7",8.0,{"key":9}]])
        end

        it 'supports csv output' do
          command.send(:show_result, job, file, nil, 'csv')
          File.read(file.path).should == %Q(1,2.0,"{""key"":3}"\n4,5.0,"{""key"":6}"\n7,8.0,"{""key"":9}"\n)
        end

        it 'supports tsv output' do
          command.send(:show_result, job, file, nil, 'tsv')
          File.read(file.path).should == %Q(1\t2.0\t{"key":3}\n4\t5.0\t{"key":6}\n7\t8.0\t{"key":9}\n)
        end
      end

      context 'with NaN/Infinity' do
        let :job do
          job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
          job.instance_eval do
            @result = [[[0.0/0.0, 1.0/0.0, 1.0/-0.0], 1], [["4", 5.0, {key:6}], 2], [["7", 8.0, {key:9}], 3]]
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
          File.read(file.path).should == %Q("""NaN""","""Infinity""","""-Infinity"""\n4,5.0,"{""key"":6}"\n7,8.0,"{""key"":9}"\n)
        end

        it 'supports tsv output' do
          command.send(:show_result, job, file, nil, 'tsv')
          File.read(file.path).should == %Q("NaN"\t"Infinity"\t"-Infinity"\n4\t5.0\t{"key":6}\n7\t8.0\t{"key":9}\n)
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
        job.stub(:finished?).and_return(true)

        client = Object.new
        client.stub(:job).with(job_id).and_return(job)
        command.stub(:get_client).and_return(client)
      end

      context 'without --null option' do
        it 'calls #show_result without null_expr option' do
          command.stub(:show_result).with(job, nil, nil, nil, {:header=>false})
          op = List::CommandParser.new("job:show", %w[job_id], %w[], nil, ["12345"], true)
          command.job_show(op)
        end
      end

      context 'with --null option' do
        it 'calls #show_result with null_expr option' do
          command.stub(:show_result).with(job, nil, nil, nil, {:header=>false, :null_expr=>"NULL"} )
          op = List::CommandParser.new("job:show", %w[job_id], %w[], nil, ["12345", "--null", "NULL"], true)
          command.job_show(op)
        end

        it 'calls #show_result with null_expr option' do
          command.stub(:show_result).with(job, nil, nil, nil, {:header=>false, :null_expr=>'""'} )
          op = List::CommandParser.new("job:show", %w[job_id], %w[], nil, ["12345", "--null", '""'], true)
          command.job_show(op)
        end
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
        multibyte_string.encoding.should == Encoding::UTF_8
        multibyte_string.force_encoding('Windows-31J').encode('UTF-8').should == 'メール'
      end

      it 'supports json output' do
        row = multibyte_row
        file = Tempfile.new("job_spec")
        command.send(:show_result, job, file, nil, 'json')
        File.read(file.path).should == '[' + [row, row].map { |e| Yajl.dump(e) }.join(",\n") + ']'
      end

      it 'supports csv output' do
        row = multibyte_row.map { |e| dump_column(e) }
        file = Tempfile.new("job_spec")
        command.send(:show_result, job, file, nil, 'csv')
        File.binread(file.path).should == [row, row].map { |e| CSV.generate_line(e) }.join
        File.open(file.path, 'r:Windows-31J').read.encode('UTF-8').split.first.should == 'メール,2.0,"{""メール"":""メール""}"'
      end

      it 'supports tsv output' do
        row = multibyte_row.map { |e| dump_column(e) }
        file = Tempfile.new("job_spec")
        command.send(:show_result, job, file, nil, 'tsv')
        File.binread(file.path).should == [row, row].map { |e| e.join("\t") + "\n" }.join
        File.open(file.path, 'r:Windows-31J').read.encode('UTF-8').split("\n").first.should == "メール\t2.0\t{\"メール\":\"メール\"}"
      end
    end

    def dump_column(v)
      s = v.is_a?(String) ? v.to_s : Yajl.dump(v)
      s = s.force_encoding('BINARY')
      s
    end
  end
end
