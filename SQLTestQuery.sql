SELECT DISTINCT author.name
FROM publication, book, authored, author
WHERE book.publisher = 'Elsevier'
AND publication.pub_id = book.id
AND publication.pub_id = authored.pub_id
AND authored.author_id = author.id
AND publication.title LIKE '%data%';