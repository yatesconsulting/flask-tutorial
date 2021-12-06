DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS post;
DROP TABLE IF EXISTS invdelform;
DROP TABLE IF EXISTS invdelformdetails;

CREATE TABLE user (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL
);

CREATE TABLE post (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  author_id INTEGER NOT NULL,
  created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  FOREIGN KEY (author_id) REFERENCES user (id)
);

CREATE TABLE invdelform (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  schooldeleting TEXT NULL,
  workorder INTEGER NULL,
  notes TEXT NULL,
  username TEXT NULL,
  active INTEGER DEFAULT 1 NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE invdelformdetails (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  invid INTEGER NOT NULL,
  tag TEXT NULL,
  description TEXT NULL,
  delcode TEXT NULL,
  itinitials TEXT NULL,
  dateitcleared TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (invid) REFERENCES InvDelForms (id)
);

-- https://www.autoitscript.com/forum/topic/182930-best-way-to-store-dates-in-sqlite/
-- date('now') returns '2016-06-05'
-- CREATE TABLE "A" (
  -- "Id" INTEGER NOT NULL PRIMARY KEY, 
  -- "Item1" CHAR DEFAULT (datetime('now', 'localtime', '+5 minutes', '+12.3456 seconds')));