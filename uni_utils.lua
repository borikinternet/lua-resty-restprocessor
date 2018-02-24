local uu = {}

function uu.invert_table (tab)
	local res = {}, key, val
	for key,val in pairs(tab) do
		res[val] = key
	end
	return res
end

function uu.value_is_double (input,required)
	local err_text
	if required then
		err_text = "Must be numerical value"
	else
		err_text = "Must not be numerical value"
	end
	return tonumber(input) and required, err_text
end

function uu.universal_print (i)
	if type (i) == "table" then
		print "\n["
		for _,j in pairs(i) do
			if type(j) == "table" and _ ~= "__class" and _ ~= "__index" and _ ~= "__parent" then
				if j then
					print ("element " .. _ .. " of type table content: ")
					uu.universal_print(j)
				else
					print ("element " .. _ .. " of type table content: nil")
				end
			elseif type(j) ~= "userdata" then
				print ("element " .. _ .. " of type " .. type(j) .. " content: " .. tostring(j))
			elseif _ == "__class" then
				print "reference to __class"
			elseif _ == "__index" then
				print "reference to __index"
			elseif _ == "__parent" then
				print "reference to __parent"
			end
		end
		print "]\n"
	else
		print (type(i) .. " " .. tostring(i) .. ',\n')
	end
end

function uu.split(str, pat)
	if not (str and pat) then return nil end
	local t = {}  -- NOTE: use {n = 0} in Lua-5.0
	local fpat = "(.-)" .. pat
	local last_end = 1
	local s, e, cap = str:find(fpat, 1)
	while s do
		if s ~= 1 or cap ~= "" then
			table.insert(t,cap)
		end
		last_end = e+1
		s, e, cap = str:find(fpat, last_end)
	end
	if last_end <= #str then
		cap = str:sub(last_end)
		table.insert(t, cap)
	end
	return t
end

function uu.tablelength(T)
	if type(T) ~= "table" then return 0 end
	local count = 0
	for _ in pairs(T) do count = count + 1 end
	return count
end

function uu.tablenotempty(T)
	if type(T) ~= "table" then return false end
	for _ in pairs(T) do return true end
	return false
end

function uu.deepcopy(T)
	local orig_type = type(T)
	local copy
	if orig_type == 'table' then
		copy = {}
		for T_key, T_value in next, T, nil do
			copy[uu.deepcopy(T_key)] = uu.deepcopy(T_value)
		end
		setmetatable(copy, uu.deepcopy(getmetatable(T)))
	else -- number, string, boolean, etc
		copy = T
	end
	return copy
end

return uu
