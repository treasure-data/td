
module TreasureData
module Command
  def ip_limit_list(op)
    op.cmd_parse

    client = get_client

    ip_limits = client.ip_limits
    rows = ip_limits.map { |ip_limit|
      {:Organization => ip_limit.org, 'IP Range' => ip_limit.ip_range}
    }

    puts cmd_render_table(rows, :fields => [:Organization, 'IP Range'])

    if rows.empty?
      $stderr.puts "There are no IP range limitations."
      $stderr.puts "Use '#{$prog} ip_limit:set <organization> <ip_range>' to create IP range limitation."
    end
  end

  def ip_limit_show(op)
    organization = op.cmd_parse

    client = get_client

    ip_limits = client.ip_limits
    rows = ip_limits.select { |ip_limit|
      ip_limit.org == organization
    }.map { |ip_limit| {'IP Range' => ip_limit.ip_range} }

    puts cmd_render_table(rows, :fields => ['IP Range'])
  end

  def ip_limit_set(op)
    organization, *ip_ranges = op.cmd_parse

    client = get_client
    client.set_ip_limit(organization, ip_ranges)

    $stderr.puts "IP range limitations [#{ip_ranges.join(' ')}] are set to #{organization}"
  end

  def ip_limit_delete(op)
    organization = op.cmd_parse

    client = get_client
    client.delete_ip_limit(organization)

    $stderr.puts "All IP range limitations are deleted from #{organization}"
  end
end
end
