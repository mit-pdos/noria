# all_posts: select * from Post;
# private_posts: select * from Post WHERE Post.private=1;
all_tas: select userId from Role WHERE role=1;
all_ta_posts: select * from Post WHERE Post.authorId in (select userId from Role WHERE Role.role=1);