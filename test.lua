-- local iconv = require("iconv")

local a = "안녕하세요"
local cd = iconv.new("cp949","UTF-8") -- to, from
local b, e = cd:iconv(a)

function process(event)
    -- Access the event's fields using the `event:get` function.
    local log_message = event:get("message")

    -- Add a new field
    event:set("custom_field", "example_value")

    -- Modify an existing field if it exists
    if log_message then
        -- Append text to the existing message field
        event:set("message", log_message .. " [processed by Lua script]")
    end

    -- Filter events: Drop events that meet a certain condition
    local log_level = event:get("level")
    if log_level == "debug" then
        -- Drop the event if the log level is 'debug'
        return false
    end

    -- Pass the event for further processing
    return true
end