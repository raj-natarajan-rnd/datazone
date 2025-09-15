BEGIN;

-- Optional: clear existing sample rows for the CMIDs we’re about to use
DELETE FROM acme.hu_inet_population WHERE cmid IN ('000000001','000000002','000000003','000000004','000000005');
DELETE FROM acme.hu_inet_roster     WHERE cmid IN ('000000001','000000002','000000003','000000004','000000005');
DELETE FROM acme.hu_inet_housing    WHERE cmid IN ('000000001','000000002','000000003','000000004','000000005');

-----------------------------------------------------------------------
-- 1) Parents first: HU (Internet) Housing  — PK(cmid):contentReference[oaicite:1]{index=1}
-----------------------------------------------------------------------
INSERT INTO acme.hu_inet_housing (cmid, broadbnd, ten, val, yrbltw)
VALUES
  ('000000001','1','1','000250000','2010'),
  ('000000002','1','2','000175000','2005'),
  ('000000003','0','3','000095000','1998'),
  ('000000004','1','1','000420000','2018'),
  ('000000005','1','2','000310000','2012');

-- Notes:
-- broadbnd: has broadband? (varchar(1))
-- ten: tenure (owner/renter/etc.) (varchar(1))
-- val: property value (varchar(9))
-- yrbltw: four-digit year built (varchar(4))
-- Table & columns defined in acme.hu_inet_housing:contentReference[oaicite:2]{index=2}.

-----------------------------------------------------------------------
-- 2) Children: HU (Internet) Population — PK(cmid, pnum), FK(cmid)→HU:contentReference[oaicite:3]{index=3}
-----------------------------------------------------------------------
INSERT INTO acme.hu_inet_population (cmid, pnum, age, sex, mar, wrk)
VALUES
  ('000000001','001','034','1','1','1'),
  ('000000001','002','032','2','1','1'),
  ('000000002','001','045','1','2','1'),
  ('000000002','002','043','2','2','0'),
  ('000000003','001','029','1','1','1'),
  ('000000003','002','027','2','1','1'),
  ('000000004','001','052','1','2','0'),
  ('000000004','002','049','2','2','0'),
  ('000000005','001','039','1','1','1'),
  ('000000005','002','007','1','0','0');

-- A few fields only, as requested:
-- age (varchar(3)), sex (varchar(1)), mar (varchar(1)), wrk (varchar(1)).
-- Full table definition: acme.hu_inet_population with FK to HU:contentReference[oaicite:4]{index=4}.

-----------------------------------------------------------------------
-- 3) Children: HU (Internet) Roster — PK(cmid), FK(cmid)→HU:contentReference[oaicite:5]{index=5}
-----------------------------------------------------------------------
INSERT INTO acme.hu_inet_roster (cmid, rostaf01, rostal01, rostam01, roststay01, rcemail)
VALUES
  ('000000001','ALEXANDER_01','ALEX','1','1','cmid1@example.com'),
  ('000000002','BENJAMIN_02','BENJ','1','0','cmid2@example.com'),
  ('000000003','CHARLOTTE_03','CHAR','0','0','cmid3@example.com'),
  ('000000004','DANIEL_04','DAN','1','1','cmid4@example.com'),
  ('000000005','ELIZABETH_05','ELI','0','1','cmid5@example.com');

-- Chosen small subset:
-- rostaf01 (13), rostal01 (20), rostam01 (1), roststay01 (1), rcemail (50).
-- Full table & FK(cmid)→hu_inet_housing(cmid): acme.hu_inet_roster:contentReference[oaicite:6]{index=6}.

COMMIT;
