require 'spec_helper'
require 'td/command/common'
require 'td/config'
require 'td/command/list'
require 'td/command/sched'
require 'td/client/model'
require 'time'

module TreasureData::Command
  describe TreasureData::Command do
    let(:client) { Object.new }

    let :job_params do
      ['job_id', :type, 'query', 'status', nil, nil, time, time, 123, 456]
    end

    let(:job1) { TreasureData::ScheduledJob.new(client, '2015-02-17 13:22:52 +0900', *job_params) }
    let(:job2) { TreasureData::ScheduledJob.new(client, nil, *job_params) }
    let(:time) { Time.now.xmlschema }
    let(:command) { Class.new { include TreasureData::Command }.new }
    let(:argv) { [] }
    let(:op) { List::CommandParser.new('sched:last_job', %w[], %w[], false, argv, []) }

    describe 'sched_history' do
      before do
        client.stub(:schedules).and_return([])
        client.stub(:history).and_return([job1, job2])
        command.stub(:get_client).and_return(client)
      end

      it 'runs' do
        expect {
          command.sched_history(op)
        }.to_not raise_exception
      end
    end

    describe "sched_last_job" do
      let(:history) { [job1, job2] }

      before do
        command.stub(:get_client).and_return(client)
        command.stub(:get_database) # don't raise NotFoundError
        client.stub(:history).and_return(history)
      end

      subject { command.sched_last_job(op) }

      describe "invoke job:show" do
        shared_examples_for("passing argv to job:show") do
          it "invoke 'job:show [original argv] [first job id]'" do
            TreasureData::Command::Runner.any_instance.should_receive(:run).with(["job:show", *argv, history.first.job_id])
            subject
          end
        end

        context "argv is empty" do
          let(:argv) { [] }
          it_behaves_like "passing argv to job:show"
        end

        context "argv is present" do
          let(:argv) { %w(-v -l 2 --format csv) }
          it_behaves_like "passing argv to job:show"
        end
      end

      context "database is not found" do
        before { client.stub(:history) { raise TreasureData::NotFoundError } }

        it "exit with 1" do
          begin
            subject
          rescue SystemExit => e
            expect(e.status).to eq 1
          end
        end
      end

      context "history is empty" do
        let(:history) { [] }

        it "exit with 1" do
          begin
            subject
          rescue SystemExit => e
            expect(e.status).to eq 1
          end
        end
      end
    end

    describe 'sched_result' do
      let(:history) { [job1, job2] }

      before do
        command.stub(:get_client).and_return(client)
        command.stub(:get_database) # don't raise NotFoundError
        client.stub(:history).and_return(history)
      end

    end
  end
end
