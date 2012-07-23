
# File#size
methods = File.public_instance_methods.map {|m| m.to_sym }
if !methods.include?(:size)
  class File
    def size
      lstat.size
    end
  end
end

