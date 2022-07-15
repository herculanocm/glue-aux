SELECT	
	distinct
	
	'%s' as 'table_name',
	c.name 'column_name',
	t.Name 'data_type',
	ISNULL(i.is_primary_key,
	0) 'primary_key'
	FROM
	sys.columns c
	INNER JOIN
	
	sys.types t ON
	c.user_type_id = t.user_type_id
	LEFT OUTER JOIN
	
	sys.index_columns ic ON
	ic.object_id = c.object_id
	AND ic.column_id = c.column_id
	LEFT OUTER JOIN
	
	sys.indexes i ON
	ic.object_id = i.object_id
	AND ic.index_id = i.index_id
	WHERE
	c.object_id = OBJECT_ID('%s')
	order by
	1
