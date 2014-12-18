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

    let :job do
      job = TreasureData::Job.new(nil, 12345, 'hive', 'select * from employee')
      job.instance_eval do
        @result = [[["1", 2.0, {key:3}], 1], [["4", 5.0, {key:6}], 2], [["7", 8.0, {key:9}], 3]]
        @result_size = 3
        @status = 'success'
      end
      job
    end

    describe 'write_result' do
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
  end
end
