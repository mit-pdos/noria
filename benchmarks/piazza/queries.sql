posts: select * from Post where classId=?;
public_posts: select * from Post WHERE Post.private=0 and classId=?;
private_posts: select * from Post WHERE Post.private=1 and classId=?;
tas: select * from Role WHERE role = 1 and classId=?;
ta_posts: select * from Post WHERE Post.authorId in (select userId from Role WHERE Role.role=1) and classId=?;
enrolled_in: select classId from Role where userId=?;
enrolled_students: select userId from Role where classId=?;
-- vote_count: select postId, COUNT(userId) AS votes FROM Vote GROUP BY postId;