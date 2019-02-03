QUERY posts: select * from Post where p_cid=?;
QUERY public_posts: select * from Post WHERE Post.p_private=0 and p_cid=?;
QUERY private_posts: select * from Post WHERE Post.p_private=1 and p_cid=?;
QUERY tas: select * from Role WHERE r_role = 1 and r_cid=?;
--QUERY ta_posts: select * from Post WHERE Post.p_author in (select r_uid from Role WHERE Role.r_role=1) and p_cid=?;
QUERY enrolled_in: select r_cid from Role where r_uid=?;
QUERY enrolled_students: select r_uid from Role where r_cid=?;
QUERY vote_count: select postId, COUNT(userId) AS votes FROM Vote GROUP BY postId;
