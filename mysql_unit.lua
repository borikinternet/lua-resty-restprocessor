local mu = {}

local to_json = require("cjson").encode
local uu = require ("rest-processor.uni_utils")
local cst
local colors = require ("ansicolors")

-- stollen from lapis
local function escape_identifier (ident)
  if type(ident)=="table" then
    return ident[1]
  end
  ident = tostring(ident)
  return '`' .. (ident:gsub('`', '``')) .. '`'
end

local function get_db(cst)
	local mysql = require "resty.mysql"
	local db, err = mysql:new()
	if not db then
		return nil, err
	end
	db:set_timeout(1000) -- 1 sec
	local ok, err, errcode, sqlstate = db:connect(cst.mysql)
	if not ok then
		ngx.say("failed to connect: ", err, ": ", errcode, " ", sqlstate)
		return nil, { err = "failed to connect: " .. err .. ": " .. errcode .. " " .. sqlstate }
	end
	return db
end

local function get_total (table, pkey, where)
	return function()
		db = get_db(cst)
		local res, err, errcode, sqlstate = db:query ("SELECT count(0) AS rcount" ..
								" FROM " .. escape_identifier(table) .. " " .. where)
		if res then
			return res[1].rcount
		else
			ngx.say("bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
			return nil, err
		end

	end
end

local function get_page_generator (table, per_page, where)
	return function (offset)
		db = get_db(cst)
		ngx.req.set_header("Content-Type", "lua/table")
		return db:query ("SELECT * FROM " .. escape_identifier(table) .. " " .. where .. 
							" LIMIT " .. tonumber(per_page) .. " OFFSET " .. tonumber(offset))
	end
end

local function get_all_generator (table, where)
	return function()
		db = get_db(cst)
		ngx.req.set_header("Content-Type", "lua/table")
		return db:query ("SELECT * FROM " .. escape_identifier(table) .. " " .. where)
	end
end

function mu.init (_cst)
	--if db then return true end
	cst = _cst
	return true
end

local function get_add_str_from_clause (clause, table, _add_str)
	local add_str = _add_str or ""
	db = get_db(cst)
	for key,val in pairs(clause) do
		if not (key=="count") then 
			local rlike = key:match("^rlike_(.*)")
			local ge = key:match("^ge_(.*)")
			local le = key:match("^le_(.*)")
			local g = key:match("^g_(.*)")
			local l = key:match("^l_(.*)")
			local isnull = key:match("^isnull_(.*)")
			local isnotnull = key:match("^isnotnull_(.*)")
			local op = ge and ">=" or le and "<=" or g and ">" or l and "<" or isnull and " IS NULL" or isnotnull and " IS NOT NULL" or "="
			local colType = db:query("SHOW COLUMNS FROM ".. escape_identifier(tostring(table)) .. 
				" LIKE " .. ngx.quote_sql_str(tostring(rlike or ge or g or le or l or isnull or isnotnull or key)) .. ";")
			if colType and colType[1] then
				colType = colType[1].Type
				if add_str=="" then
					add_str = "WHERE "
				else
					add_str = add_str .. " AND " 
				end
				if isnull or isnotnull then
					add_str = add_str .. escape_identifier(isnull or isnotnull) .. op
				elseif rlike then
					add_str = add_str .. ngx.quote_sql_str(val) .. " RLIKE " .. escape_identifier(rlike)
				elseif (ge or g or le or l) and (colType:match('char') or colType:match('date') or colType:match('time')) then
					add_str = add_str .. escape_identifier(ge or g or le or l) .. op .. ngx.quote_sql_str(val)
				elseif colType:match('char') or colType:match('date') or colType:match('time') then
					add_str = add_str .. escape_identifier(key) .. " LIKE " .. ngx.quote_sql_str('%'..val..'%')
				else
					add_str = add_str .. escape_identifier(ge or g or le or l or key) .. op .. tonumber(val)
				end
			end
		end
	end
	return add_str
end

mu.get_entity = function (table,pkey)
	return function (clause,sort_field,order,fields,condition)
		ngx.req.set_header("Content-Type", "lua/function")
		db = get_db(cst)
		db:query("SET NAMES 'utf8'")
		local add_str = get_add_str_from_clause (clause, table)
		local page,per_page
		if uu.tablenotempty(fields) then
			if add_str=="" then
				add_str = "WHERE (FALSE"
			else
				add_str = add_str .. " AND (FALSE"
			end
			for _,field in pairs(fields) do
				add_str = add_str .. " OR "
				local colType = db:query("SHOW COLUMNS FROM ".. escape_identifier(table) .. " LIKE " .. ngx.quote_sql_str(tostring(field)) .. ";")[1].Type;
				if colType:match('char') then
					add_str = add_str .. ngx.quote_sql_str(tostring(field)) .. " LIKE " .. ngx.quote_sql_str("%"..tostring(condition).."%");
				else
					add_str = add_str .. escape_identifier(field) .. "=" .. ngx.quote_sql_str(condition);
				end;
			end;
			add_str = add_str .. ")"
		end
		add_str = add_str .. " ORDER BY " .. escape_identifier(sort_field);
		if order then add_str = add_str .. " " .. order end;
		local res = {}
		res.get_page = get_page_generator(table, tonumber(clause.count), add_str)
		res.get_all = get_all_generator(table, add_str)
		res.total_items = get_total(table, pkey, add_str)

		return res or {err = err}
	end
end

mu.put_entity = function (table, key, locked_field)
	return function (fields)
		ngx.req.set_header("Content-Type", "lua/table")
		db = get_db(cst)
		db:query("SET NAMES 'utf8'")
		local id
		-- таблица fields должна содержать поле ключа
		if fields[key] then 
			id = fields[key]
			fields[key]=nil
		else
			return {err = "Can`t locate record!"}
		end
		if locked_fields then
			for _,val in pairs(locked_fields) do
				fields[val] = nil
			end
		end
		local sql_str = "UPDATE " .. escape_identifier(tostring(table)) .. " SET "
		local comma_needed = false
		for _key,_val in pairs(fields) do
			if comma_needed then 
				sql_str = sql_str .. ","
			else
				comma_needed = true
			end
			sql_str = sql_str .. escape_identifier(tostring(_key)) .. " = " .. ngx.quote_sql_str(_val)
		end
		sql_str = sql_str .. " WHERE " .. escape_identifier(tostring(key)) .. " = " .. ngx.quote_sql_str(tostring(id))
		local res, err = db:query(sql_str)
		return res or {err = err}
	end
end

mu.post_entity = function (table,key)
	return function (fields)
		ngx.req.set_header("Content-Type", "lua/table")
		db = get_db(cst)
		db:query("SET NAMES 'utf8'")
		fields[key] = nil
		local sql_str = "INSERT INTO " .. escape_identifier(tostring(table)) .. " SET "
		local comma_needed = false
		for _key,_val in pairs(fields) do
			if comma_needed then 
				sql_str = sql_str .. ","
			else
				comma_needed = true
			end
			sql_str = sql_str .. escape_identifier(tostring(_key)) .. " = " .. ngx.quote_sql_str(_val)
		end
		local res, err = db:query(sql_str)
		return res or {err = err}
	end
end

mu.delete_entity = function (table,key)
	return function (id)
		ngx.req.set_header("Content-Type", "lua/table")
		db = get_db(cst)
		local sql_str = "DELETE FROM " .. escape_identifier(tostring(table)) .. " WHERE "
			.. escape_identifier(tostring(key)) .. " = " ngx.quote_sql_str(tostring(id))
		local res, err = db:query(sql_str)
		return res or {err = err}
	end
end

mu.create_subroutine = function (have_result,routinename,fields_order)
	return function (fields)
		ngx.req.set_header("Content-Type", "lua/table")
		db = get_db(cst)
		db:query("SET NAMES 'utf8'")
		local query
		if have_result then
			--noinspection UnusedDef
			query = "SELECT "
		else
			query = "CALL "
		end
		query = query .. escape_identifier(routinename) .. "("
		for id=1,#fields_order do
			if query:sub(-1) ~= "(" then
				query = query .. ',' end
			query = query .. ngx.quote_sql_str(fields[fields_order[id]])
		end
		query = query .. ");"
		return db:query(query)
	end
end

mu.get_subroute = function (
		parent_table,	-- Upper level table
		parent_p_key,	-- primary key of upper level table
		parent_field,	-- field by which upper level table is connected with many-to-many connection table
		key_table,		-- many-to-many connection table
		primary_key,	-- primary key of many-to-many connection table
		parent_key,		-- field by which many-to-many connection table connected to upper level table
		relation_key,	-- field by which many-to-many connection table connected to lower level table
		related_table,	-- lower level table
		related_key,	-- primary key of lower level table
		sql_addstr)		-- this string added to WHERE clause
	return function(id,clause,sort_field)
		ngx.req.set_header("Content-Type", "lua/table")
		db = get_db(cst)
		db:query("SET NAMES 'utf8'")
		local add_str = "INNER JOIN " .. escape_identifier(key_table) .. 
						" ON (" .. escape_identifier(parent_table) .. '.' ..
									escape_identifier(parent_field) ..	"=" ..
								escape_identifier(key_table)..'.'..
									escape_identifier(parent_key)..") "
		if related_table then
			add_str = add_str .. "INNER JOIN " .. escape_identifier(related_table) ..
						" ON (" .. escape_identifier(key_table) .. '.' ..
									escape_identifier(relation_key) .. "=" ..
								escape_identifier(related_table) .. '.' ..
									escape_identifier(related_key) .. ")"
		end
		add_str = add_str .. "WHERE " .. escape_identifier(parent_table) .. '.' 
									.. escape_identifier(parent_p_key).."="..tonumber(id) .. " " 
		if sql_addstr then add_str = add_str .. sql_addstr .. " " end
		add_str = get_add_str_from_clause (clause, parent_table, add_str)
		add_str = get_add_str_from_clause (clause, key_table, add_str)
		add_str = get_add_str_from_clause (clause, related_table, add_str)
		add_str = sort_field and add_str .. " ORDER BY " .. escape_identifier(sort_field) or add_str
		local res = {}
		res.get_page = get_page_generator(parent_table, tonumber(clause.count), add_str)
		res.get_all = get_all_generator(parent_table, add_str)
		res.total_items = get_total(parent_table, parent_p_key, add_str)
		return res or {err = err}
	end
end

local function sqlify_fields(fields)
	local res = ""
	local comma_needed = false
	for key,val in pairs(fields) do
		if comma_needed then 
			res = res .. ","
		else
			comma_needed = true
		end
		res = res .. escape_identifier(tostring(key)) .. " = " .. ngx.quote_sql_str(val)
	end
	return res
end

mu.put_subroute = function (entity, e_cst, sub_entity, sub_cst, table, keys)
	return function(fields)
		ngx.req.set_header("Content-Type", "lua/table")
		db = get_db(cst)
		db:query("SET NAMES 'utf8'")
		local id = {}
		--[[
		print(colors("%{bright white}Fields: %{bright red}"..to_json(fields)))
		print(colors("%{bright white}Fields: %{bright red}"..to_json(keys)))
		--]]
		if type(keys) == "table" then
			for _,key in pairs(keys) do
				id[_] = fields[key]
				fields[key] = nil
			end
		else
			id[keys] = fields[keys]
			fields[keys] = nil
		end
		if not cst.rest_api.entities[sub_entity].update_on_dup then
			local sql_str = "SELECT count(0) AS rcount FROM " 
				.. escape_identifier(tostring(table)) .. " WHERE "
				.. sqlify_fields(id)
			local res, err = db:query(sql_str)
			if res[1].rcount then  
				return {err = 'Such entity already created'}
			end
		end
		q_str = "SELECT " .. escape_identifier(e_cst.table) 
			.. "." .. escape_identifier(sub_cst.parent_field)
			.. " FROM " .. escape_identifier(e_cst.table)
			.. " WHERE " .. escape_identifier(e_cst.table)
			.. "." .. escape_identifier(e_cst.key) 
			.. " = " .. tostring(id[keys]) .. " LIMIT 1";
		local res, err = db:query(q_str)
		if not res or not res[1] or not res[1][sub_cst.parent_field] then 
			return {err = "No parent entity found!"}
		end
		fields[sub_cst.parent_key] = res[1][sub_cst.parent_field]
		sql_str = "INSERT INTO " .. escape_identifier(tostring(table)) 
			.. " SET " 
			.. sqlify_fields(fields)
		if cst.rest_api.entities[sub_entity].update_on_dup then
			fields[sub_cst.parent_key] = nil
			sql_str = sql_str .. " ON DUPLICATE KEY UPDATE "
				.. sqlify_fields(fields)
		end

		local res, err = db:query(sql_str)
		return res or {err = err}
	end
end

mu.delete_subroute = function (table,keys)
	return function(fields)
		ngx.req.set_header("Content-Type", "lua/table")
		local id,res,err
		-- таблица fields должна содержать поле ключа
		if type(keys) == "table" then
			id = {}
			for _,key in pairs(keys) do
				id[key] = fields[key]
			end
		else
			id = fields[keys]
		end
		local sql_str = "SELECT count(0) AS rcount FROM " .. escape_identifier(tostring(table)) .. " WHERE "
		local clause = ""
		local comma_needed = false
		for _key,_val in pairs(id) do
			if comma_needed then 
				clause = clause .. ","
			else
				comma_needed = true
			end
			clause = clause .. escape_identifier(tostring(_key)) .. " = " .. ngx.quote_sql_str(_val)
		end
		db = get_db(cst)
		local res, err = db:query(sql_str .. clause)
		if not res[1].rcount then 
			err = "Can`t find entity!"
		else
			res,err = db:query("DELETE FROM " .. escape_identifier(tostring(table)) .. " WHERE " .. clause)
		end
		return res or {err = err}
	end
end

return mu
