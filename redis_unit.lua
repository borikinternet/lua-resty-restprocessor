local ru = {}

local to_json = require("cjson").encode
local Redis = require "resty.redis"
local socket 

function ru.init (redis)
	if not socket then
		socket = redis.socket
	end
end

local function create_redis_get_result (res)
	result = {
		res_data = {}
	}
	if res.data == 'false' then
		res.data = false
	elseif res.data == 'true' then
		res.data = true
	end
	if res.key and res.data then
		table.insert(result.res_data,{key = res.key,data = res.data})
	end
	result.get_page = function (self)
		ngx.log(ngx.DEBUG,to_json(self.res_data))
		ngx.req.set_header("Content-Type", "lua/table")
		return self.res_data
	end
	result.get_all = result.get_page
	return result
end

local function create_redis_get_results (res)
	result = {
		res_data = {}
	}
	for key,data in pairs(res) do
		if data == 'false' then
			data = false
		elseif data == 'true' then
			data = true
		end
		table.insert(result.res_data,{key = key, data = data})
	end
	result.get_page = function (self)
		ngx.req.set_header("Content-Type", "lua/table")
		return self.res_data
	end
	result.get_all = result.get_page
	return result
end

ru.get_entity = function (path)
	return function (clause,sort_field,order,fields,condition)
		local redis = Redis:new()
		local ok, err = redis:connect(socket)
		if not ok then 
			ngx.log(ngx.ERR, err)
			return nil,{err = err} 
		end
		if clause.key then
			ok, err = redis:get(path..':'..clause.key)
		else
			local keys, err = redis:keys(path..':*')
			if keys then
				ok = {}
				for _,key in pairs(keys) do
					local val, err = redis:get(key)
					if not err then
						ok[string.gsub(key,'^([^:]*):(.*)$','%2')] = val
					end
				end
			end
		end
		redis:close()
		if not ok then
			return nil,{err = err} 
		elseif ok == ngx.null then
			return create_redis_get_result({})
		elseif clause.key then
			return create_redis_get_result({key = clause.key, data = ok})
		else 
			return create_redis_get_results(ok)
		end
	end
end

ru.set_entity = function (path)
	return function (fields)
		ngx.req.set_header("Content-Type", "lua/table")
		if fields.key==nil or fields.data==nil then return {err = "Key or data does not set"} end
		local redis = Redis:new()
		local ok, err = redis:connect(socket)
		if not ok then return nil,{err = err} end
		ok, err = redis:set(path..':'..fields.key,fields.data)
		redis:close()
		if not ok then 
			return nil,{err = err} 
		else
			return {
				key = fields.key,
				data = fields.data
			}
		end
	end
end

ru.del_entity = function (path)
	return function (key)
		ngx.req.set_header("Content-Type", "lua/table")
		if not key then return {err = "Key does not set"} end
		local redis = Redis:new()
		local ok, err = redis:connect(socket)
		if not ok then return nil,{err = err} end
		local ok, err = redis:del(path..':'..key)
		return ok or nil,{err = err} 
	end
end

return ru
