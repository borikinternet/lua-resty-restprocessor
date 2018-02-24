local du = {}

local cjson = require("cjson")

local function create_mysql_du (du, key, val, cst)
	if du[key] then return du end
	local mu = require "rest-processor.mysql_unit"
	local res, err = mu.init(cst)
	if not res then 
		print(cjson.encode(err))
		return nil, err 
	end
	du[key] = {}
	du[key].get = mu.get_entity(val.table,val.key,key)
	du[key].put = mu.put_entity(val.table,val.key,val.locked_fields)
	du[key].post = mu.post_entity(val.table,val.key,key)
	du[key].delete = mu.delete_entity(val.table,val.key,key)
	du[key].subroutines = {}
	if val.subroutines then
		for key1,val1 in pairs(val.subroutines) do
			du[key].subroutines[key1] = mu.create_subroutine(val1.have_result,val1.routinename,val1.fields_order)
		end
	end
	du[key].subroutes = {}
	if val.subroutes then
		for key1,val1 in pairs(val.subroutes) do
			du[key].subroutes[key1] = {}
			du[key].subroutes[key1].get = cst.rest_api.entities[key].subroutes[key1].method.GET and mu.get_subroute(
																cst.rest_api.entities[key].table,
																cst.rest_api.entities[key].key,
																cst.rest_api.entities[key].subroutes[key1].parent_field,
																cst.rest_api.entities[key].subroutes[key1].key_table,
																cst.rest_api.entities[key].subroutes[key1].primary_key,
																cst.rest_api.entities[key].subroutes[key1].parent_key,
																cst.rest_api.entities[key].subroutes[key1].relation_key,
																cst.rest_api.entities[key].subroutes[key1].related_table,
																cst.rest_api.entities[key].subroutes[key1].related_key,
																cst.rest_api.entities[key].subroutes[key1].sql_clause
																)
			du[key].subroutes[key1].put = cst.rest_api.entities[key].subroutes[key1].method.PUT and mu.put_subroute(
																key,val,
																key1,val1,
																cst.rest_api.entities[key].subroutes[key1].key_table,
																cst.rest_api.entities[key].subroutes[key1].primary_key
																)
			du[key].subroutes[key1].post = cst.rest_api.entities[key].subroutes[key1].method.POST and mu.put_subroute(
																key,val,
																key1,val1,
																cst.rest_api.entities[key].subroutes[key1].key_table,
																val.key
																)
			du[key].subroutes[key1].delete = cst.rest_api.entities[key].subroutes[key1].method.DELETE and mu.delete_subroute(
																cst.rest_api.entities[key].subroutes[key1].key_table,
																cst.rest_api.entities[key].subroutes[key1].primary_key
																)
		end
	end
	return du
end

local function create_redis_du (du, key, val, cst)
	if du[key] then return du end
	local ru = require "rest-processor.redis_unit"
	local res = ru.init(cst.redis)
	if not res then return nil end
	du[key] = {}
	du[key].get = ru.get_entity (key)
	du[key].post = ru.set_entity (key)
	du[key].put = ru.set_entity (key)
	du[key].delete = ru.del_entity (key)
	return du
end

local function create_lua_du (du, key, val, cst)
	if du[key] then return du end
	local uu = require "rest-processor.lua_unit"
	local res = uu.init(cst)
	if not res then return nil end
	du[key] = {}
	du[key].get = uu.get_entity (key, val)
	du[key].post = uu.post_entity (key, val)
	du[key].put = uu.put_entity (key, val)
	du[key].delete = uu.del_entity (key, val)
	return du
end

local function create_list_du (du, key, val, cst)
	if du[key] then return du end
	local uu = require "rest-processor.list_unit"
	local res = uu.init(cst)
	if not res then return nil end
	du[key] = {}
	du[key].get = uu.get_entity (key, val)
	du[key].post = uu.post_entity (key, val)
	du[key].put = uu.put_entity (key, val)
	du[key].delete = uu.del_entity (key, val)
	return du
end

du.init = function (self, key, cst)
	local val = cst.rest_api.entities[key]
	if val and val.driver == "mysql" then
		return create_mysql_du (self, key, val, cst)
	elseif val and val.driver == "redis" then
		return create_redis_du (self, key, val, cst)
	elseif val and val.driver == "list" then
		return create_list_du (self, key, val, cst)
	elseif val and val.driver == "lua" then
		return create_lua_du (self, key, val, cst)
	end
end

return du
