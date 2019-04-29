CREATE TABLE `Friend` (
  `usera` int(11) NOT NULL,
  `userb` int(11) NOT NULL,
);

CREATE TABLE `Album` (
  `a_aid` int(11) NOT NULL auto_increment,
  `a_uid` int(11) NOT NULL,
  `public` tinyint(1) NOT NULL,
  PRIMARY KEY  (`a_aid`)
);

CREATE TABLE `Photo` (
  `p_id` varchar(12) NOT NULL,
  `p_aid` int(11) NOT NULL,
  PRIMARY KEY  (`p_id`)
);
