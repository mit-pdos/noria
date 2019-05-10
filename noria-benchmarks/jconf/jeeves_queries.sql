LatestPaperVersion: SELECT *
                 FROM PaperVersion
		 WHERE PaperVersion.paper = ?
		 GROUP BY PaperVersion.paper
                 ORDER BY PaperVersion.time
                 LIMIT 10;

QUERY PaperList: SELECT Paper.*
           FROM Paper
	   JOIN LatestPaperVersion
	   ON (Paper.id = LatestPaperVersion.paper);

QUERY ReviewList: SELECT Paper.id, Review.reviewer, Review.contents, Review.score_novelty, Review.score_presentation, Review.score_technical, Review.score_confidence, Paper.author as author
           FROM Paper
	   JOIN Review
	   ON (Paper.id = Review.paper);
