require 'td/command/options'
require 'httpclient'

module TreasureData
module Command
  include Options

  proxy = ENV['HTTP_PROXY']
  user_agent = 'TD'
  apikey = Config.apikey
  @@client = HTTPClient.new(proxy, user_agent)
  @@base_endpoint = 'http://development-ec2-api-workflow-753042598.us-east-1.elb.amazonaws.com/api'
  @@base_headers = {'authorization' => "TD #{apikey}"}

  def wf_version(op)
    response = get('version')
    $stdout.puts response.body
  end

  def wf_projects(op)
    response = get('projects')
    $stdout.puts response.body
  end

  def wf_workflows(op)
    name = op.cmd_parse
    response = get('projects', {'project' => name})
    $stdout.puts response.body
  end

  private

  def uri(endpoint)
    return "#{@@base_endpoint}/#{endpoint}"
  end

  def get(path, query = {}, headers = {})
    return @@client.get(uri(path), query, @@base_headers.merge(headers))
  end

end # module Command
end # module TrasureData
