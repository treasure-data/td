
module TreasureData
module Command

  def acl_list(op)
    op.cmd_parse

    client = get_client

    acl = client.access_controls

    rows = []
    acl.each {|ac|
      rows << {:Subject => ac.subject, :Action => ac.action, :Scope => ac.scope, :"Grant option" => ac.grant_option}
    }

    puts cmd_render_table(rows, :fields => [:Subject, :Action, :Scope, :"Grant option"])

    if rows.empty?
      $stderr.puts "There are no access controls."
      $stderr.puts "Use '#{$prog} acl:grant <subject> <action> <scope>' to grant permissions."
    end
  end

  def acl_grant(op)
    grant_option = true

    op.on('--no-grant-option', '-N', 'Grant without grant option', TrueClass) {|b|
      grant_option = !b
    }

    subject, action, scope = op.cmd_parse

    client = get_client

    client.grant_access_control(subject, action, scope, grant_option)

    $stderr.puts "Access control [#{subject} #{action} #{scope}] is created #{grant_option ? 'with' : 'without'} grant option."
  end

  def acl_revoke(op)
    subject, action, scope = op.cmd_parse

    client = get_client

    client.revoke_access_control(subject, action, scope)

    $stderr.puts "Access control [#{subject} #{action} #{scope}] is removed."
  end

  # TODO acl_test
end
end
