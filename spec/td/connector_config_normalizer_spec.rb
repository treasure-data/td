require 'spec_helper'
require 'td/connector_config_normalizer'
require 'td/client/api_error'

module TreasureData
  describe ConnectorConfigNormalizer do
    describe '#normalized_config' do
      subject { TreasureData::ConnectorConfigNormalizer.new(config).normalized_config }

      context 'has key :in' do
        context 'without :out, :exec' do
          let(:config) { {'in' => {'type' => 's3'}} }

          it { expect(subject).to eq config.merge('out' => {}, 'exec' => {}, 'filters' => []) }
        end

        context 'with :out, :exec, :filters' do
          let(:config) {
            {
              'in'      => {'type' => 's3'},
              'out'     => {'mode' => 'append'},
              'exec'    => {'guess_plugins' => ['json', 'query_string']},
              'filters' => [{'type' => 'speedometer'}]
            }
          }

          it { expect(subject).to eq config }
        end
      end

      context 'has key :config' do
        context 'with :in' do
          let(:config) {
            { 'config' =>
              {
                'in'   => {'type' => 's3'},
                'out'  => {'mode' => 'append'},
                'exec' => {'guess_plugins' => ['json', 'query_string']},
                'filters' => [{'type' => 'speedometer'}]
              }
            }
          }

          it { expect(subject).to eq config['config'] }

        end

        context 'without :in' do
          let(:config) { {'config' => {'type' => 's3'}} }

          it { expect(subject).to eq({'in' => config['config'], 'out' => {}, 'exec' => {}, 'filters' => []}) }
        end
      end

      context 'does not have key :in or :config' do
        let(:config) { {'type' => 's3'} }

        it { expect(subject).to eq({'in' => config, 'out' => {}, 'exec' => {}, 'filters' => []}) }
      end
    end
  end
end
