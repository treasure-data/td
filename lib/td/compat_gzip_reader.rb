
methods = Zlib::GzipReader.public_instance_methods
if !methods.include?(:readpartial) && !methods.include?('readpartial')
  class Zlib::GzipReader
    def readpartial(size, out=nil)
      o = read(size)
      if o
        if out
          out.replace(o)
          return out
        else
          return o
        end
      end
      raise EOFError, "end of file reached"
    end
  end
end

