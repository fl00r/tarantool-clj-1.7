function create_testing_space()
  local s = box.schema.space.create('tester', {id = 1,
                                               temporary = true,
                                               if_not_exists = true})
  s:create_index('id_idx', {
                   type = 'HASH',
                   parts = {1, 'UNSIGNED'},
                   unique = true,
                   if_not_exists = true
  })
  s:create_index('name_idx', {
                   type = 'TREE',
                   parts = {2, 'STR', 3, 'STR'},
                   if_not_exists = true
  })
end

function drop_testing_space()
  box.schema.space.drop(1)
end
