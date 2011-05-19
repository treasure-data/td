
module TRD


class ConfigError < StandardError
end

class ConfigNotFoundError < ConfigError
end

class ConfigParseError < ConfigError
end


class APIError < StandardError
end

class AuthError < APIError
end

class ExistsError < APIError
end

class NotFoundError < APIError
end


end

