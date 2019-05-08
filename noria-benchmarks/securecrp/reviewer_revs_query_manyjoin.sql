QUERY PapersReviewedPrelim: SELECT Review.contents as contents, Review.paper as paper, Review.reviewer as reviewer
                FROM Review
                JOIN ReviewAssignment
                ON Review.paper = ReviewAssignment.paper;
QUERY PapersReviewed: SELECT * FROM PapersReviewedPrelim
                JOIN ReviewAssignment
                ON PapersReviewedPrelim.reviewer = ReviewAssignment.username;
-- The above is an attempt to do a multi-column JOIN, which is not currently supported in Noria.

MyPapersReviewed: SELECT * FROM PapersReviewed
                  WHERE PapersReviewed.reviewer = "4";
-- The above is effectively a fake security node

QUERY ReviewList: SELECT *
            FROM Review
            JOIN MyPapersReviewed
            ON MyPapersReviewed.paper = Review.paper;

-- This set of queries tries to implement the policy "a reviewer R can see reviews for paper P only
-- once R has submitted a review for P".
