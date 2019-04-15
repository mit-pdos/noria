QUERY ChairContext: SELECT GroupContext_chairs_chair.id, GroupContext_chairs_chair.`group` FROM GroupContext_chairs_chair;

QUERY Authors5: SELECT GroupContext_authors_5.id, GroupContext_authors_5.`group` FROM GroupContext_authors_5;

QUERY ChairPaperList: SELECT PaperList_chairschair.id, PaperList_chairschair.author FROM PaperList_chairschair;

QUERY AuthorPaperList: SELECT PaperList_authors5.id, PaperList_authors5.author FROM PaperList_authors5;

--QUERY AuthorContext: SELECT GroupContext_authors_1.id FROM GroupContext_authors_1
--           WHERE GroupContext_authors_1.id = ?;
-- In above query, for some reason, GC_a_1.uid and GC_a_1.gid can't be queried, get error they don't exist.
-- If select just id, result of lookup in main.rs is 3 empty lists.

--QUERY ChairContext: SELECT * FROM GroupContext_chairs_chair
--           WHERE GroupContext_chairs_chair.id = ?;
