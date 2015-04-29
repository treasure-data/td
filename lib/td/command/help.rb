
module TreasureData
module Command

  def help(op)
    cmd = op.cmd_parse

    c = List.get_option(cmd)
    if c == nil
       $stderr.puts "'#{cmd}' is not a td command. Run '#{$prog}' to show the list."
       List.show_guess(cmd)
       exit 1

    elsif c.name != cmd && c.group == cmd
      # group command
      $stdout.puts List.cmd_usage(cmd)
      exit 1

    else
      method, cmd_req_connectivity = List.get_method(cmd)
      method.call(['--help'])
    end
  end

  def help_all(op)
    cmd = op.cmd_parse

    TreasureData::Command::List.show_help(op.summary_indent)
    $stdout.puts ""
    $stdout.puts "Type '#{$prog} help COMMAND' for more information on a specific command."
  end

end
end
