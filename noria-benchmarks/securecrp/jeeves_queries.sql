--QUERY PaperList: SELECT Paper.*, LatestPaperVersion.title AS latest_version_title
--           FROM Paper
--           JOIN (SELECT *
--                 FROM PaperVersion
--		 GROUP BY PaperVersion.paper
--                 ORDER BY PaperVersion.time
--                 LIMIT 1)
--                AS LatestPaperVersion
--           ON (Paper.id = LatestPaperVersion.paper);

--QUERY LatestPaperVersion: SELECT *
LatestPaperVersion: SELECT *
                 FROM PaperVersion
		 WHERE PaperVersion.paper = ?
		 GROUP BY PaperVersion.paper
                 ORDER BY PaperVersion.time
                 LIMIT 10;

QUERY PaperList: SELECT Paper.*
           FROM Paper
--	   WHERE Paper.id = ?;
	   JOIN LatestPaperVersion
	   ON (Paper.id = LatestPaperVersion.paper);

QUERY ReviewList: SELECT Paper.id, Review.reviewer, Review.contents, Review.score_novelty, Review.score_presentation, Review.score_technical, Review.score_confidence, Paper.author as author
           FROM Paper
--	   WHERE Paper.id = ?
	   JOIN Review
	   ON (Paper.id = Review.paper);
