CREATE TABLE `Friend` (
  `usera` int(11) NOT NULL,
  `userb` int(11) NOT NULL,
);

CREATE TABLE `Album` (
  `a_id` int(11) NOT NULL auto_increment,
  `u_id` int(11) NOT NULL,
  `public` tinyint(1) NOT NULL,
  PRIMARY KEY  (`a_id`)
);

CREATE TABLE `Photo` (
  `p_id` varchar(12) NOT NULL,
  `a_id` int(11) NOT NULL,
  PRIMARY KEY  (`p_id`)
);
