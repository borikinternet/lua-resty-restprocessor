local uu = {}

local ut = require("rest-processor.uni_utils")
local cjson = require("cjson")
local to_json = cjson.encode
local cst

local function gen_get_all(res)
	return function()
		--print(to_json(res))
		return res
	end
end

local function gen_get_page(res)
	return function (page)
		--print(to_json(res))
		return res
	end
end

local function gen_total_items(res)
	return function()
		return ut.tablelength(res)
	end
end

function uu.init (_cst)
	cst = _cst
	return true
end

function uu.get_entity(entity, cst_e)
	local colors = require "ansicolors"
	return function(fields)
		local c_res 
		fields.res = {}
		for _,list in pairs(cst_e.list) do
			local prepeared_uri = list.uri:gsub("(:[%w-_]+)", 
				function(var) return fields[var:sub(2)] end)
			local opts = {
				method = ngx["HTTP_"..list.method]
			}
			for key,val in pairs(list.fields) do
				if fields[val] then
					fields[key] = fields[val]
					fields[val] = nil
				end
			end
			if list.content_type then
				ngx.req.set_header("Accept", list.content_type)
			end
			if list.method == "POST" or list.method == "PUT" then
				if ngx.ctx.content_type and ngx.ctx.content_type:match("application/json") then
					opts.body = to_json(fields)
					ngx.req.set_header("Content-Type", "application/json")
				elseif ngx.ctx.content_type and ngx.ctx.content_type:match("application/x-www-form-urlencoded") then
					opts.body = ngx.encode_args(fields)
					ngx.req.set_header("Content-Type", "application/x-www-form-urlencoded")
				end
			else
				opts.args = ut.deepcopy(fields)
				for key,val in pairs(opts.args) do
					if tonumber(key) then
						opts.args[key] = nil
					elseif type(val) == "table" then
						opts.args[key] = nil
					end
				end
			end

			ngx.req.discard_body()
			--[[
			print(colors("%{bright white}URI: %{bright blue}"..to_json(prepeared_uri)))
			print(colors("%{bright white}Headers: %{bright blue}"..to_json(ngx.req.get_headers())))
			print(colors("%{bright white}Options: %{bright blue}"..to_json(fields)))
			--]]
			c_res = ngx.location.capture(prepeared_uri, opts)
			--[[
			print(colors("%{bright white}Headers: %{bright blue}"..to_json(c_res.header)))
			print(colors("%{bright white}Options: %{bright blue}"..c_res.body))
			--]]

			for header,val in pairs(c_res.header) do
				ngx.header[header] = val
				if string.lower(header) == "content-type" then
					ngx.ctx.content_type = val
				end
			end
			if not c_res.status == ngx.HTTP_OK or c_res.truncated then
				return false, {
					err = "Subquery failed!",
					sub_status = c_res.status,
					sub_body = c_res.body,
					sub_truncated = c_res.truncated
				}
			end

			local res
			if c_res.header["Content-Type"]:match("application/json") then
				res = cjson.decode(c_res.body)
			elseif c_res.header["Content-Type"]:match("application/x-www-form-urlencoded") then
				res = ngx.decode_args(c_res.body, fields.count
												or cst.default.results_per_page 
												or 1000)
			else
				res = c_res.body
			end

			if list.append_results then
				if type(res) == "table" then
					for k,v in pairs(res) do
						if tonumber(k) then
							table.insert(fields.res, v)
						else
							fields.res[k] = v
						end
					end
				else
					table.insert(fields.res, res)
				end
			else
				fields.res = res
			end
			
		end
		--[[
		-- debug
		print(colors("%{yellow}Content-Type: %{blue}"..ngx.header.content_type))
		print(colors("%{yellow}Result: %{blue}"..fields.res))
		--]]
		local res = {}
		res.total_items = gen_total_items(fields.res)
		res.get_page = gen_get_page(fields.res)
		res.get_all = gen_get_all(fields.res)
		return res
	end
end

function uu.post_entity(entity, cst_e)
	return function(fields)
	end
end

function uu.put_entity(entity, cst_e)
	return function(fields)
	end
end

function uu.del_entity(entity, cst_e)
	return function(id)
	end
end

return uu
