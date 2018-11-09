SELECT publication.*
FROM publication
WHERE publication.pub_id IN (SELECT inproceedings.id
								FROM inproceedings
                                WHERE inproceedings.cross_ref = (SELECT proceedings.pub_key
																FROM proceedings
																WHERE proceedings.title = '19th Americas Conference on Information Systems AMCIS 2013 Chicago Illinois USA August 15-17 2013'
                                                                AND proceedings.publication_year = 2013)
                                                                )
	AND publication.pub_id IN (SELECT authored.pub_id
								FROM authored
                                WHERE authored.author_id = (SELECT author.id
														FROM author
														WHERE author.name = 'Markus Bick')
                                                        );
