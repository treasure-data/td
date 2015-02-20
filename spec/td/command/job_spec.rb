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
      let :job do
        job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
        job.instance_eval do
          @result = [[["1", 2.0, {key:3}], 1], [["4", 5.0, {key:6}], 2], [["7", 8.0, {key:9}], 3]]
          @result_size = 3
          @status = 'success'
        end
        job
      end

      it 'supports json output' do
        file = Tempfile.new("job_spec")
        command.send(:show_result, job, file, nil, 'json')
        File.read(file.path).should == %Q([["1",2.0,{"key":3}],\n["4",5.0,{"key":6}],\n["7",8.0,{"key":9}]])
      end

      it 'supports csv output' do
        file = Tempfile.new("job_spec")
        command.send(:show_result, job, file, nil, 'csv')
        File.read(file.path).should == %Q(1,2.0,"{""key"":3}"\n4,5.0,"{""key"":6}"\n7,8.0,"{""key"":9}"\n)
      end

      it 'supports tsv output' do
        file = Tempfile.new("job_spec")
        command.send(:show_result, job, file, nil, 'tsv')
        File.read(file.path).should == %Q(1\t2.0\t{"key":3}\n4\t5.0\t{"key":6}\n7\t8.0\t{"key":9}\n)
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
