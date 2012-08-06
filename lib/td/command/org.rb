
module TreasureData
module Command

  def org_show(op)
    name = op.cmd_parse

    client = get_client

    orgs = client.organizations
    org = orgs.find {|org| name == org.name }
    unless org
      $stderr.puts "Organization '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} org:create <name>' to create an organization."
      exit 1
    end

    $stderr.puts "Name         : #{org.name}"
  end

  def org_list(op)
    op.cmd_parse

    client = get_client

    orgs = client.organizations

    rows = []
    orgs.each {|org|
      rows << {:Name => org.name}
    }

    puts cmd_render_table(rows, :fields => [:Name])

    if rows.empty?
      $stderr.puts "There are no organizations."
      $stderr.puts "Use '#{$prog} org:create <name>' to create an organization."
    end
  end

  def org_create(op)
    name = op.cmd_parse

    client = get_client

    client.create_organization(name)

    $stderr.puts "Organization '#{name}' is created."
  end

  def org_delete(op)
    name = op.cmd_parse

    client = get_client

    client.delete_organization(name)

    $stderr.puts "Organization '#{name}' is deleted."
  end

end
end

