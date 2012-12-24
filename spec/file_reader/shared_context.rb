require 'rspec'

shared_context 'error_proc' do
  let :error do
    Proc.new { |reason, data|
      reason.should match(error_pattern)
    }
  end
end
