local lu = {}

local ut = require ("rest-processor.uni_utils")
local to_json = require("cjson").encode

local function gen_get_all(res)
	return function()
		return res
	end
end

local function gen_total_items(res)
	return function()
		return ut.tablelength(res)
	end
end

function lu.init(cst)
	return true
end

function lu.get_entity(entity, cst)
	return function(cause)
		ngx.req.set_header("Content-Type", "lua/function")
		local res = {}
		local lua_module = require (cst.script)
		local lua_res = lua_module.run(cause)
		res.total_items = gen_total_items(lua_res)
		res.get_page = gen_get_all(lua_res)
		res.get_all = gen_get_all(lua_res)
		return res
	end
end

function lu.post_entity(entity, cst)
	return function(fields)
		ngx.req.set_header("Content-Type", "lua/function")
		local res = {}
		local lua_module = require (cst.script)
		local lua_res = lua_module.run(fields)
		return lua_res
	end
end

function lu.put_entity(entity, cst)
	return function(fields)
		ngx.req.set_header("Content-Type", "lua/function")
		local res = {}
		local lua_module = require (cst.script)
		local lua_res = lua_module.run(fields)
		return lua_res
	end
end

function lu.del_entity(entity, cst)
	return function(id)
		ngx.req.set_header("Content-Type", "lua/function")
		local res = {}
		local lua_module = require (cst.script)
		local lua_res = lua_module.run(id)
		return lua_res
	end
end

return lu
