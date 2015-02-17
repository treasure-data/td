require 'spec_helper'
require 'td/command/common'
require 'td/config'
require 'td/command/list'
require 'td/command/sched'
require 'td/client/model'
require 'time'

module TreasureData::Command

  describe 'sched_history' do
    it 'runs' do
      client = Object.new
      time = Time.now.xmlschema
      job_params = ['job_id', :type, 'query', 'status', nil, nil, time, time, 123, 456]
      job1 = TreasureData::ScheduledJob.new(client, '2015-02-17 13:22:52 +0900', *job_params)
      job2 = TreasureData::ScheduledJob.new(client, nil, *job_params)
      client.stub(:schedules).and_return([])
      client.stub(:history).and_return([job1, job2])
      command = Class.new { include TreasureData::Command }.new
      command.stub(:get_client).and_return(client)
      op = List::CommandParser.new('sched:history', %w[], %w[], false, [], [])
      expect {
        command.sched_history(op)
      }.to_not raise_exception
    end
  end
end
