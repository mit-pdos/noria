all_posts: select * from Post;
private_posts: select * from Post WHERE Post.private=1;

# all_ta_posts: select * from Post, Role WHERE Post.authorId = Role.userId and Role.role=1;