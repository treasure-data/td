
module TD


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

class AlreadyExistsError < APIError
end

class NotFoundError < APIError
end


end

