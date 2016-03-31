require 'rspec'

shared_context 'error_proc' do
  let :error do
    Proc.new { |reason, data|
      expect(reason).to match(error_pattern)
    }
  end
end
