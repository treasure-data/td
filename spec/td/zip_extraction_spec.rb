require 'spec_helper'
require 'td/command/common'
require 'td/command/workflow'
require 'td/updater'
require 'tmpdir'

describe 'ZIP extraction' do
  include_context 'quiet_out'

  around do |example|
    Dir.mktmpdir do |directory|
      @directory = directory
      example.run
    end
  end

  let(:destination) { File.join(@directory, 'upload') }
  let(:archive) { File.join(@directory, 'archive.zip') }

  def write_archive(name)
    Zip::OutputStream.open(archive) do |zip|
      zip.put_next_entry(name)
      zip.write('new content')
    end
  end

  shared_examples 'confined extraction' do
    it 'extracts nested files into the destination' do
      write_archive("#{prefix}bin/java")
      extract
      expect(File.read(File.join(destination, 'bin/java'))).to eq('new content')
    end

    it 'rejects sibling paths sharing the destination prefix before modifying files' do
      sibling = File.join(@directory, 'upload_backup')
      FileUtils.mkdir_p(sibling)
      victim = File.join(sibling, 'owned.sh')
      File.write(victim, 'keep')
      write_archive("#{prefix}../upload_backup/owned.sh")

      expect { extract }.to raise_error(Zip::EntryNameError)
      expect(File.read(victim)).to eq('keep')
    end
  end

  context 'workflow Java installation' do
    let(:prefix) { 'jdk/' }
    let(:command) { Class.new { include TreasureData::Command }.new }

    def extract
      command.send(:extract_zip, archive, destination, 1)
    end

    include_examples 'confined extraction'
  end

  context 'toolbelt update' do
    let(:prefix) { '' }
    let(:updater) { Object.new.extend(TreasureData::Updater::ModuleDefinition) }

    before do
      FileUtils.mkdir_p(destination)
      allow(Dir).to receive(:mktmpdir).and_yield(destination)
      allow(updater).to receive(:updating_lock_path).and_return(File.join(@directory, 'lock'))
      allow(updater).to receive(:updated_client_path).and_return(File.join(@directory, 'updated'))
      allow(updater).to receive(:latest_local_version).and_return('0.0.0')
      allow(updater).to receive(:fetch).and_return('1.0.0')
      allow(updater).to receive(:client_version_from_path).and_return('1.0.0')
      allow(updater).to receive(:package_category).and_return('exe')
      allow(updater).to receive(:stream_fetch) do |url, file|
        IO.copy_stream(archive, file)
      end
    end

    def extract
      updater.update
    end

    include_examples 'confined extraction'
  end
end
