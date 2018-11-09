SELECT 'Article', COUNT(article.id)
FROM article, publication
WHERE article.id = publication.pub_id
  AND publication.publication_year >= 2000
  AND publication.publication_year <= 2017
  
UNION
  
SELECT 'Inproceeding', COUNT(inproceedings.id)
FROM inproceedings, publication
WHERE inproceedings.id = publication.pub_id 
  AND publication.publication_year >= 2000
  AND publication.publication_year <= 2017
  
UNION

SELECT 'Book', COUNT(book.id)
FROM book, publication
WHERE book.id = publication.pub_id
  AND publication.publication_year >= 2000
  AND publication.publication_year <= 2017
  
UNION
  
SELECT 'Incollection', COUNT(incollection.id)
FROM Incollection, publication
WHERE Incollection.id = publication.pub_id
  AND publication.publication_year >= 2000
  AND publication.publication_year <= 2017
  
UNION
  
SELECT 'Proceedings', COUNT(proceedings.id)
FROM proceedings, publication
WHERE proceedings.id = publication.pub_id
  AND publication.publication_year >= 2000
  AND publication.publication_year <= 2017

