BEGIN;

-- Clear old sample rows
DELETE FROM acme.hu_inet_population WHERE cmid BETWEEN '000000001' AND '000000010';
DELETE FROM acme.hu_inet_roster     WHERE cmid BETWEEN '000000001' AND '000000010';
DELETE FROM acme.hu_inet_housing    WHERE cmid BETWEEN '000000001' AND '000000010';

-----------------------------------------------------------------------
-- 1) Parents: HU (Internet) Housing:contentReference[oaicite:0]{index=0}
-----------------------------------------------------------------------
INSERT INTO acme.hu_inet_housing (cmid, broadbnd, ten, val, yrbltw, bds, rnt, tax)
VALUES
  ('000000001','Y','O','000250000','2010','03','001800','002400'),
  ('000000002','Y','R','000175000','2005','02','001350','001900'),
  ('000000003','N','O','000095000','1998','02','000900','001200'),
  ('000000004','Y','O','000420000','2018','04','002600','003600'),
  ('000000005','Y','R','000310000','2012','03','001950','002800'),
  ('000000006','Y','O','000515000','2020','04','002750','003900'),
  ('000000007','N','R','000140000','2003','02','001200','001700'),
  ('000000008','Y','O','000360000','2016','03','002100','003100'),
  ('000000009','Y','R','000225000','2011','03','001650','002300'),
  ('000000010','N','O','000105000','1999','02','000950','001250');

-- broadbnd = Y/N, ten = O (Owner), R (Renter)

-----------------------------------------------------------------------
-- 2) Children: HU (Internet) Population:contentReference[oaicite:1]{index=1}
-----------------------------------------------------------------------
INSERT INTO acme.hu_inet_population (cmid, pnum, age, sex, mar, wrk, sch, wag, rac_wht)
VALUES
  ('000000001','001','34','M','M','Y','G','000650','Y'),
  ('000000001','002','32','F','M','Y','C','000610','Y'),
  ('000000002','001','45','M','M','Y','H','000820','Y'),
  ('000000002','002','43','F','M','N','0','000000','Y'),
  ('000000003','001','29','M','S','Y','C','000540','Y'),
  ('000000003','002','27','F','S','Y','G','000520','Y'),
  ('000000004','001','52','M','M','N','H','000000','Y'),
  ('000000004','002','49','F','M','N','0','000000','Y'),
  ('000000005','001','39','M','M','Y','C','000700','Y'),
  ('000000005','002','07','M','S','N','0','000000','Y'),
  ('000000006','001','31','F','M','Y','G','000590','Y'),
  ('000000006','002','03','M','S','N','0','000000','Y'),
  ('000000007','001','41','M','M','Y','H','000760','Y'),
  ('000000008','001','36','F','M','Y','C','000680','Y'),
  ('000000009','001','50','M','M','Y','H','000900','Y'),
  ('000000009','002','48','F','M','N','0','000000','Y'),
  ('000000010','001','26','M','S','Y','C','000510','Y');

-- sex = M/F, mar = M (Married), S (Single)
-- wrk = Y/N (employed), sch = C (College), H (HighSchool), G (Graduate), 0 (none)
-- rac_wht = Y/N

-----------------------------------------------------------------------
-- 3) Children: HU (Internet) Roster:contentReference[oaicite:2]{index=2}
-----------------------------------------------------------------------
INSERT INTO acme.hu_inet_roster
  (cmid, rcemail, remail, rostaf01, rostal01, rostam01, mortwo01, away01, anotherx, roststay01)
VALUES
  ('000000001','cmid1@example.com','resp1@example.com','ALEX_01','ALEX','Y','Y','N','N','Y'),
  ('000000002','cmid2@example.com','resp2@example.com','BEN_02','BEN','Y','N','N','N','N'),
  ('000000003','cmid3@example.com','resp3@example.com','CHAR_03','CHAR','N','Y','Y','N','N'),
  ('000000004','cmid4@example.com','resp4@example.com','DAN_04','DAN','Y','N','N','Y','Y'),
  ('000000005','cmid5@example.com','resp5@example.com','ELI_05','ELI','N','Y','N','N','Y'),
  ('000000006','cmid6@example.com','resp6@example.com','FRK_06','FRK','Y','N','N','N','Y'),
  ('000000007','cmid7@example.com','resp7@example.com','GRC_07','GRC','N','Y','Y','N','N'),
  ('000000008','cmid8@example.com','resp8@example.com','HEN_08','HEN','Y','N','N','N','Y'),
  ('000000009','cmid9@example.com','resp9@example.com','ISA_09','ISA','Y','N','N','Y','N'),
  ('000000010','cmid10@example.com','resp10@example.com','JAC_10','JAC','N','Y','N','N','Y');

-- rostam01, mortwo01, away01, anotherx, roststay01 now Y/N instead of 1/0

COMMIT;
