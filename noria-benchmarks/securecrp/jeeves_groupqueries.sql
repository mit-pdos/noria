QUERY ChairContext: SELECT * FROM GroupContext_chairs_chair
           WHERE GroupContext_chairs_chair.id = ?;

QUERY UserContext: SELECT * FROM UserContext_2
           WHERE UserContext_2.id = ?;

QUERY Coauthors: SELECT * FROM PaperCoauthor
           WHERE author = ?;

QUERY Reviews: SELECT * FROM Review
           WHERE paper = ?;
