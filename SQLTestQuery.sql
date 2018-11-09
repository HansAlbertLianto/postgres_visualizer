SELECT *
FROM publication
WHERE pub_id IN (SELECT id
								FROM inproceedings
                                WHERE cross_ref = (SELECT pub_key
																FROM proceedings
																WHERE title = '19th Americas Conference on Information Systems AMCIS 2013 Chicago Illinois USA August 15-17 2013'
                                                                AND publication_year = 2013)
                                                                )
	AND pub_id IN (SELECT pub_id
								FROM authored
                                WHERE author_id = (SELECT id
														FROM author
														WHERE name = 'Markus Bick')
                                                        );
