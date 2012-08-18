
module TreasureData
module Command

  def role_show(op)
    name = op.cmd_parse

    client = get_client

    roles = client.roles
    role = roles.find {|role| name == role.name }
    unless role
      $stderr.puts "Role '#{name}' does not exist."
      $stderr.puts "Use '#{$prog} role:create <name>' to create a role."
      exit 1
    end

    $stderr.puts "Organization : #{role.org_name}"
    $stderr.puts "Name         : #{role.name}"
    $stderr.puts "Users        : #{role.user_names.join(', ')}"
  end

  def role_list(op)
    op.cmd_parse

    client = get_client

    roles = client.roles

    rows = []
    roles.each {|role|
      rows << {:Name => role.name, :Organization => role.org_name, :Users => role.user_names.join(',')}
    }

    puts cmd_render_table(rows, :fields => [:Name, :Organization, :Users])

    if rows.empty?
      $stderr.puts "There are no roles."
      $stderr.puts "Use '#{$prog} org:create <name>' to create a role."
    end
  end

  def role_create(op)
    org = nil

    op.on('-g', '--org ORGANIZATION', "create the role under this organization") {|s|
      org = s
    }

    name = op.cmd_parse

    client = get_client

    client.create_role(name, org)

    $stderr.puts "Role '#{name}' is created."
  end

  def role_delete(op)
    name = op.cmd_parse

    client = get_client

    client.delete_role(name)

    $stderr.puts "Role '#{name}' is deleted."
  end

  def role_grant(op)
    name, user = op.cmd_parse

    client = get_client

    client.grant_role(name, user)

    $stderr.puts "Role '#{name}' is granted to user '#{user}'."
  end

  def role_revoke(op)
    name, user = op.cmd_parse

    client = get_client

    client.revoke_role(name, user)

    $stderr.puts "Role '#{name}' is revoked from user '#{user}'."
  end

end
end

