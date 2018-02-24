local rest = {}

local cst
local du = require("rest-processor.db_utils")
local to_json = require("cjson").encode
local from_json = require("cjson").decode
local uu = require("rest-processor.uni_utils")
local colors = require("ansicolors")

function rest.init(_cst)
	cst = _cst
end

function get_res_type(accept, ctype)
	local result = "application/json"
	if not accept then 
		accept = "" 
	end
	accept = accept:lower()
	if ctype and not ctype:match("lua/") and not ctype:match("text/plain") then
		result = ctype
	elseif accept:match("text/csv") then
		result = "text/csv"
	elseif accept:match("text/xml") then
		result = "text/xml"
	end
	--[[
	print(colors("%{bright white}Accept: %{bright green}"..to_json(accept)))
	print(colors("%{bright white}Content-Type: %{bright green}"..to_json(result)))
	--]]
	ngx.header.content_type = result
	return result
end

function add_params (params, add)
	if not type(params.res) == "table" then 
		local res = params.res
		params.res = { res = res }
	end
	for key,val in pairs(add) do
		if tonumber(key) then table.insert(params.res, val)
		else params[key] = val end
	end
	return params
end

function rest.api_post(self)
	local path = uu.split(self.params.splat,'/')
	local entity = path[1]
	local id = path[2]
	local subroute = path[3]
	local status = ngx.HTTP_NOT_FOUND
	local fields = {}
	local result = ""
	local headers = ngx.req.get_headers()
	if (headers["content-type"] and headers["content-type"]:match("application/json"))
		or (headers["Content-Type"] and headers["Content-Type"]:match("application/json")) then
		ngx.req.read_body()
		add_params(self.params, from_json(ngx.req.get_body_data()))
	elseif (headers["content-type"] and headers["content-type"]:match("application/x--www--form--urlencoded"))
		or (headers["Content-Type"] and headers["Content-Type"]:match("application/x--www--form--urlencoded")) then
		ngx.req.read_body()
		add_params(self.params, ngx.req.get_post_args())
	end
	--[[
	print(colors("%{bright white}Headers: %{bright green}"..to_json(headers)))
	print(colors("%{bright white}Params: %{bright green}"..to_json(self.params)))
	--]]
	ngx.var.api_params = to_json(self.params)
	if cst.rest_api.entities[entity] then
		local exec_f,fields
		local res, err = du:init(entity, cst)
		if not res then
			ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
			ngx.say(to_json(err))
		end
		if subroute 
			and cst.rest_api.entities[entity].subroutines
			and cst.rest_api.entities[entity].subroutines[subroute] 
			and cst.rest_api.entities[entity].subroutines[subroute].method == "POST" then

			fields = uu.deepcopy(cst.rest_api.entities[entity].subroutines[subroute].fields)
			exec_f = du[entity].subroutines[subroute]
		elseif subroute
			and cst.rest_api.entities[entity].subroutes
			and cst.rest_api.entities[entity].subroutes[subroute]
			and cst.rest_api.entities[entity].subroutes[subroute].method.POST then

			fields = uu.deepcopy(cst.rest_api.entities[subroute].fields)
			exec_f = du[entity].subroutes[subroute].post
		elseif cst.rest_api.entities[entity].POST then
			fields = uu.deepcopy(cst.rest_api.entities[entity].fields)
			exec_f = du[entity].post
		end
		for key,val in pairs(fields) do
			fields[key] = self.params[val]
		end
		fields.res = self.params.res
		if id and subroute and cst.rest_api.entities[entity].subroutines and
			cst.rest_api.entities[entity].subroutines[subroute] and 
			cst.rest_api.entities[entity].subroutines[subroute].key then 
			fields[cst.rest_api.entities[entity].subroutines[subroute].key] = id
		elseif id and subroute and cst.rest_api.entities[entity].subroutes and
			cst.rest_api.entities[entity].subroutes[subroute] and 
			cst.rest_api.entities[subroute].key then
			fields[cst.rest_api.entities[entity].key] = id
		end
		if cst.rest_api.entities[entity].fields_postprocessing then 
			cst.rest_api.entities[entity]:fields_postprocessing() 
		end
		local res, err = exec_f(fields)
		local res_type = get_res_type(headers["Accept"], ngx.header.content_type) 
		if res and not res.err then 
			status = ngx.HTTP_CREATED
			ngx.header.location = 
				cst.rest_api.base_path .. "/" .. entity .. "/" .. tostring(res[cst.rest_api.entities[entity].key]);
			if res_type == 'application/json' then
				result = to_json(fill_entity_result(res, entity))
			elseif res_type == "text/csv" then
				result = fill_csv_entity_result(res, entity)
			else
				if type(res) ~= "string" then
					result = tostring(res)
				else
					result = res
				end
			end
		end
	else
		result = to_json({err = "Can not post entity ".. tostring(entity) .."!"})
	end
	ngx.status = status
	-- print(colors("%{bright white} Result: %{bright green}"..tostring(result)))
	ngx.say(result)
	return status == ngx.HTTP_CREATED
end

function rest.api_put(self)
	local result,e
	local path = uu.split(self.params.splat,'/')
	local entity = path[1]
	local id = tonumber(path[2])
	local subroute = path[3]
	local subroute_id = path[4]
	local status = ngx.HTTP_NOT_FOUND
	if (string.find(ngx.var.CONTENT_TYPE, 'application/json')) then
		add_params(self.params,from_json(ngx.req.get_body_data()))
	elseif (string.find(ngx.var.CONTENT_TYPE, 'application/x-www-form-urlencoded')) then
		add_params(self.params,ngx.req.get_post_args())
	end
	ngx.var.api_params = to_json(self.params)
	if	cst.rest_api.entities[entity] 
		and cst.rest_api.entities[entity].PUT then

		local res, err = du:init(entity, cst)
		if not res then
			ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
			ngx.say(to_json(err))
		end
		local exec_f,fields
		if subroute and 
			cst.rest_api.entities[entity].subroutines[subroute] and 
			cst.rest_api.entities[entity].subroutines[subroute].method == "PUT" then

			fields = uu.deepcopy(cst.rest_api.entities[entity].subroutines[subroute].fields)
			for key,val in pairs(fields) do
				fields[key] = self.params[val]
			end
			fields[cst.rest_api.entities[entity].subroutines[subroute].key] = subroute_id;
			exec_f = du[entity].subroutines[subroute];
		else
			fields = uu.deepcopy(cst.rest_api.entities[entity].fields)
			for key,val in pairs(fields) do
				fields[key] = self.params[val]
			end
			exec_f = du[entity].put
		end
		fields.res = self.params.res
		fields[cst.rest_api.entities[entity].key]=id
		if cst.rest_api.entities[entity].fields_postprocessing then
			cst.rest_api.entities[entity]:fields_postprocessing()
		end
		if id then
			result,e = exec_f(fields)
			local headers = ngx.req.get_headers()
			local res_type = get_res_type(headers["Accept"], ngx.header.content_type) 
			if result then 
				status = ngx.HTTP_OK
			else 
				result = {err = e}
			end
			if res_type == 'application/json' then
				result = to_json(fill_entity_result(result, entity))
			elseif res_type == "text/csv" then
				result = fill_csv_entity_result(result, entity)
			else
				result = tostring(res)
			end
		end
	else
		result = to_json({err = "Can not put entity ".. tostring(entity) .."!"})
	end
	ngx.status = status
	ngx.say(result)
	return status == ngx.HTTP_OK
end

function rest.api_delete(self)
	local result,e
	local path = uu.split(self.params.splat,'/')
	local entity = path[1]
	local id = tonumber(path[2])
	local subroute = path[3]
	local subroute_id = path[4]
	local status = ngx.HTTP_NOT_FOUND
	if cst.rest_api.entities[entity] and cst.rest_api.entities[entity].DELETE then
		local exec_f
		local res, err = du:init(entity, cst)
		if not res then
			ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
			ngx.say(to_json(err))
		end
		if subroute and 
			cst.rest_api.entities[entity].subroutines[subroute] and 
			cst.rest_api.entities[entity].subroutines[subroute].method == "DELETE" then

			id = {
				[cst.rest_api.entities[entity].key] = id
			}
			exec_f = du[entity].subroutines[subroute]
		elseif subroute and 
			cst.rest_api.entities[entity].subroutes[subroute] and 
			cst.rest_api.entities[entity].subroutes[subroute].method.DELETE then
			
			id = {
				[cst.rest_api.entities[entity].key] = id,
				[cst.rest_api.entities[entity].subroutes[subroute].primary_key] = subroute_id
			}
			exec_f = du[entity].subroutes[subroute].delete
		else
			exec_f = du[entity].delete
		end
		if id then
			result,e = exec_f(id)
			if result then status = ngx.HTTP_OK
			else result = {err = e} end
		end
	else
		result = {err = "Entity ".. tostring(entity) .."["..tostring(id)"] can not be deleted!"}
	end
	ngx.status = status
	ngx.say(to_json(result))
	return status == ngx.HTTP_OK
end

function fill_entity_result(res, entity)
	if type(res) ~= "table" then return res end
	local result = {}
	local k,v
	for k,v in pairs(res) do
		if type(v) == "table" then
			result[k] = {}
			for key,val in pairs(v) do
				if cst.rest_api.entities[entity].fields[key] then
					result[k][cst.rest_api.entities[entity].fields[key]] = val end
				if tonumber(key) then
					table.insert(result[k], val) end
			end
		else
			result[k] = v
		end
	end
	if res.prev_res then result.prev_res = res.prev_res end
	return result
end

function fill_subroute_result(res,entity,subroute)
	local result = {}
	local _,v,key,val
	for _,v in pairs(res) do
		result[_] = {}
		for key,val in pairs(v) do
			if cst.rest_api.entities[subroute].fields[key] then 
				result[_][cst.rest_api.entities[subroute].fields[key]] = val
			elseif cst.rest_api.entities[entity].subroutes[subroute].native_fields[key] then 
				result[_][cst.rest_api.entities[entity].subroutes[subroute].native_fields[key]] = val
			elseif cst.rest_api.entities[entity].fields[key] then
				result[_][cst.rest_api.entities[entity].fields[key]] = val
			end
		end
	end
	return result
end

-- TODO Implement csv for subentities
function fill_csv_subroute_result(res, entity, subroute)
end

function fill_csv_entity_result(res, entity, fields)
	if not uu.tablenotempty(res) then return end
	local result = {}
	local key, val
	local headers = {}
	for key in pairs(cst.rest_api.entities[entity].fields) do
		table.insert(headers, cst.rest_api.entities[entity].fields[key])
	end
	table.insert(result, table.concat(headers, ';'))
	local row_num, row
	for row_num, row in pairs(res) do
		local res_row = {}
		for key in pairs(cst.rest_api.entities[entity].fields) do
			if row[key] then
				table.insert(res_row, row[key])
			else
				table.insert(res_row, '')
			end
		end
		table.insert(result, table.concat(res_row, ';'))
	end
	return table.concat(result, '\n')
end

function get_entity_params (params, entity, subroute)
	local result, db_field, rf = {}
	if not params then return result end
	local tabs = {cst.rest_api.entities[entity].fields}
	if subroute and cst.rest_api.entities[subroute] then
		table.insert(tabs, cst.rest_api.entities[subroute].fields)
	end
	for _, tab in pairs(tabs) do
		local r_flds = uu.invert_table(tab)
		for r_fld,val in pairs(params) do
			if r_flds[r_fld] then
				result[r_flds[r_fld]] = val
			elseif r_flds[r_fld:match("^ge_(.*)")] then
				result["ge_"..r_flds[r_fld:match("^ge_(.*)")]] = val
			elseif r_flds[r_fld:match("^g_(.*)")] then
				result["g_"..r_flds[r_fld:match("^g_(.*)")]] = val
			elseif r_flds[r_fld:match("^le_(.*)")] then
				result["le_"..r_flds[r_fld:match("^le_(.*)")]] = val
			elseif r_flds[r_fld:match("^l_(.*)")] then
				result["l_"..r_flds[r_fld:match("^l_(.*)")]] = val
			elseif r_flds[r_fld:match("^rlike_(.*)")] then
				result["rlike_"..r_flds[r_fld:match("^rlike_(.*)")]] = val
			elseif r_flds[r_fld:match("^isnull_(.*)")] then
				result["isnull_"..r_flds[r_fld:match("^isnull_(.*)")]] = val
			elseif r_flds[r_fld:match("^isnotnull_(.*)")] then
				result["isnotnull_"..r_flds[r_fld:match("^isnotnull_(.*)")]] = val
			elseif tonumber(r_fld) then
				table.insert(result.res, val)
			end
		end
	end
	result.count = tonumber(params.count) or cst.defaults.results_per_page
	if result.count==0 then result.count = nil end
	return result
end

function get_subroute_sortfield (entity, subroute, params)
	local sort_field = cst.rest_api.entities[entity].subroutes[subroute].sort_field or 
						cst.rest_api.entities[subroute].sort_field or
						cst.rest_api.entities[entity].sort_field
	if params.sortfield then
		sort_field = get_route_sortfield(entity, params)
		for db_field,field in pairs(cst.rest_api.entities[subroute].fields) do
			if params.sortfield == field then
				sort_field = db_field
				break
			end
		end
	end
	return sort_field
end

function get_route_sortfield (entity, params)
	local sort_field = cst.rest_api.entities[entity].sort_field
	if params.sortfield then
		for db_field,field in pairs(cst.rest_api.entities[entity].fields) do
			if params.sortfield == field then
				sort_field = db_field
				break
			end
		end
	end
	return sort_field
end

function get_db_fulltextsearch_fields (search_fields, entity)
	local db_fields = {};
	if uu.tablenotempty(search_fields) then
		local l_fields = uu.invert_table(cst.rest_api.entities[entity].fields)
		for _,field in pairs(search_fields) do
			table.insert(db_fields,l_fields[field])
		end;
	end;
	return db_fields
end

function gen_error_res (err)
	ngx.status = ngx.HTTP_NOT_FOUND
	ngx.header.X_Count = 0
	ngx.header.content_type = "application/json"
	ngx.say(to_json(err or {err = "Can`t retrieve entity!"}))
end

function uni_get_head (self, method)
	local path = uu.split(self.params.splat,'/')
	local entity = path[1]
	local id = tonumber(path[2])
	local subroute = path[3]
	local page = tonumber(self.params.offset) or tonumber(self.params.page) or 0
	local search_fields = uu.split(self.params["search-fields"],",")
	local search_string = self.params["search-condition"]
	local db_fields = get_db_fulltextsearch_fields(search_fields, entity)

	-- Обрабатываем запрос к вложенным сущностям
	if cst.rest_api.entities[entity] and
		cst.rest_api.entities[entity].subroutes and
		cst.rest_api.entities[entity].subroutes[subroute] and
		cst.rest_api.entities[entity].subroutes[subroute].method.GET then

		local res, err = du:init(entity, cst)
		if not res then 
			gen_error_res (err)
			return false
		end

		if cst.rest_api.entities[entity].key then
			self.params[cst.rest_api.entities[entity].key] = id
		else 
			ngx.status = ngx.HTTP_NOT_FOUND
			ngx.header.X_Count = 0
			ngx.header.content_type = "application/json"
			ngx.say(to_json(err or {err = "Identificator field for parent entity don't defined!"}))
			return false
		end
		local sort_field = get_subroute_sortfield(entity, subroute, self.params)

		local params = get_entity_params(self.params, entity, subroute)
		params[cst.rest_api.entities[entity].key] = id

		local res, err = du[entity].subroutes[subroute].get(id,params,sort_field,db_fields,search_string)
		if not res then 
			gen_error_res (err)
			return false
		end

		ngx.header.X_Count = res.total_items()
		if method == 'get' then 
			local res,e = self.params.count and res.get_page(page) 
											or res.get_all()
			if not uu.tablenotempty(res) then 
				gen_error_res (e)
				return false
			end
			local headers = ngx.req.get_headers()
			local res_type = get_res_type(headers["Accept"], ngx.header.content_type) 

			ngx.status = ngx.HTTP_OK
			if type(res) == "table" then
				if res_type == 'application/json' then
					ngx.say(to_json(fill_subroute_result(res, entity, subroute)))
				elseif res_type == "text/csv" then
					ngx.say(fill_csv_subroute_result(res, entity, subroute))
				else
					ngx.say(tostring(res.res))
				end
			else
				ngx.say(tostring(res))
			end
			return true
		elseif method == 'head' then
			if not ngx.header.X_Count then 
				gen_error_res ()
				return false
			end
			ngx.status = ngx.HTTP_OK
			return true
		end
	end

	-- Обрабатываем запросы к корневым сущностям
	if cst.rest_api.entities[entity] and 
		cst.rest_api.entities[entity].GET then

		local res, err = du:init(entity, cst)
		if not res then 
			gen_error_res (err)
			return false
		end

		local sort_field = get_route_sortfield(entity, self.params)
		local params = get_entity_params(self.params,entity)
		if cst.rest_api.entities[entity].key then
			params[cst.rest_api.entities[entity].key] = id
		end

		local res,err = du[entity].get(params,sort_field,self.params.order,db_fields,search_string)

		if not res then 
			gen_error_res (err)
			return false 
		end

		ngx.header.X_Count = res.total_items()
		if method == 'get' then
			local res,err,errcode,sqlstate = params.count and res.get_page(page) or res.get_all()

			local colors = require "ansicolors"

			if not res then
				ngx.say("bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
				return
			end
			if not uu.tablenotempty(res) and type(res) == "table" then 
				gen_error_res (err)
				return false 
			end
			local headers = ngx.req.get_headers()
			local res_type = get_res_type(headers["Accept"], ngx.header.content_type) 

			ngx.status = ngx.HTTP_OK
			if type(res) == "table" then
				if res_type == 'application/json' then
					ngx.say(to_json(fill_entity_result(res, entity)))
				elseif res_type == "text/csv" then
					ngx.say(fill_csv_entity_result(res, entity))
				else
					ngx.say(tostring(res.res))
				end
			else
				ngx.say(tostring(res))
			end
			return true
		elseif method == 'head' then
			if not ngx.header.X_Count then
				gen_error_res ()
				return true
			end
			ngx.status = ngx.HTTP_OK
			return true
		end
	end

	ngx.status = ngx.HTTP_NOT_FOUND
	ngx.say(to_json({err = "Entity ".. tostring(entity) .." not found!"}))
	return false
end

function rest.api_get(self)
	return uni_get_head(self, 'get')
end

function rest.api_head (self)
	return uni_get_head(self, 'head')
end

return rest
