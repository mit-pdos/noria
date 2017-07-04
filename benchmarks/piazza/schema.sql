--
-- Table structure for table `Post`
--

CREATE TABLE `Post` (
  `postId` int(11) NOT NULL auto_increment,
  `classId` int(11) NOT NULL,
  `authorId` int(11) NOT NULL,
  `content` text NOT NULL default '',
  `private` tinyint(1) NOT NULL default '0',
  PRIMARY KEY  (`postId`),
  UNIQUE KEY `postId` (`postId`),
  KEY `classId` (`classId`),
  KEY `authorId` (`authorId`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `User`
--

CREATE TABLE `User` (
  `userId` int(11) NOT NULL auto_increment,
  PRIMARY KEY  (`userId`),
  UNIQUE KEY `userId` (`userId`),
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `Class`
--

CREATE TABLE `Class` (
  `classId` int(11) NOT NULL auto_increment,
  PRIMARY KEY  (`classId`),
  UNIQUE KEY `classId` (`classId`),
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `Post`
--

CREATE TABLE `Role` (
  `userId` int(11) NOT NULL,
  `classId` int(11) NOT NULL,
  `role` tinyint(1) NOT NULL default '0',
  PRIMARY KEY  (`userId`),
  UNIQUE KEY `userId` (`userId`),
  KEY `classId` (`classId`),
) ENGINE=MyISAM DEFAULT CHARSET=utf8;