
module TreasureData
module Command

  def password_change(op)
    op.cmd_parse

    old_password = nil
    password = nil

    begin
      system "stty -echo"  # TODO termios
      $stdout.print "Old password (typing will be hidden): "
      old_password = STDIN.gets || ""
      old_password = old_password[0..-2]  # strip \n
    rescue Interrupt
      $stderr.print "\ncanceled."
      exit 1
    ensure
      system "stty echo"   # TODO termios
      $stdout.print "\n"
    end

    if old_password.empty?
      $stderr.puts "canceled."
      exit 0
    end

    3.times do
      begin
        system "stty -echo"  # TODO termios
        $stdout.print "New password (typing will be hidden): "
        password = STDIN.gets || ""
        password = password[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        $stdout.print "\n"
      end

      if password.empty?
        $stderr.puts "canceled."
        exit 0
      end

      begin
        system "stty -echo"  # TODO termios
        $stdout.print "Retype new password: "
        password2 = STDIN.gets || ""
        password2 = password2[0..-2]  # strip \n
      rescue Interrupt
        $stderr.print "\ncanceled."
        exit 1
      ensure
        system "stty echo"   # TODO termios
        $stdout.print "\n"
      end

      if password == password2
        break
      end

      $stdout.puts "Doesn't match."
    end

    client = get_client(:ssl => true)

    client.change_my_password(old_password, password)

    $stderr.puts "Password changed."
  end

end
end

