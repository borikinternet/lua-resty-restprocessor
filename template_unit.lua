local uu = {}

local function gen_get_all()
	return function()
	end
end

local function gen_get_page()
	return function (page)
	end
end

local function gen_total_items()
	return function()
	end
end

function uu.init ()
	return true
end

function uu.get_entity(entity)
	return function()
		local res = {}
		res.total_items = gen_total_items()
		res.get_page = gen_get_page()
		res.get_all = gen_get_all()
		return res
	end
end

function uu.post_entity(entity)
	return function(fields)
	end
end

function uu.put_entity(entity)
	return function(fields)
	end
end

function uu.del_entity(entity)
	return function(id)
	end
end

return uu
