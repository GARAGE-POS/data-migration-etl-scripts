INSERT INTO app.Services (Name, NameAr, Description, DescriptionAr, ImagePath, DisplayOrder, StatusID, UpdatedAt, CreatedAt) VALUES
('Car Maintenance Service', N'خدمة صيانة السيارات', NULL, NULL, NULL, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Oil Change Service', N'خدمة تغيير الزيت', NULL, NULL, NULL, 2, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Tire Service', N'خدمة الإطارات', NULL, NULL, NULL, 3, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Wash Service', N'خدمة غسيل السيارات', NULL, NULL, NULL, 4, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Detailing Service', N'خدمة تلميع السيارات', NULL, NULL, NULL, 5, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Auto Electrical Service', N'خدمة كهرباء السيارات', NULL, NULL, NULL, 6, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Programming Service', N'خدمة برمجة السيارات', NULL, NULL, NULL, 7, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Computer Diagnostics Service', N'خدمة فحص كمبيوتر', NULL, NULL, NULL, 8, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Radiator & Cooling System Service', N'خدمة الرديتر والتبريد', NULL, NULL, NULL, 9, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Exhaust & Muffler Service', N'خدمة العادم والشكمان', NULL, NULL, NULL, 10, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('EV Service & Maintenance Service', N'خدمة صيانة السيارات الكهربائية', NULL, NULL, NULL, 11, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Wheel Alignment & Balancing Service', N'خدمة وزن الأذرعة وترصيص', NULL, NULL, NULL, 12, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Tires Service', N'خدمة الإطارات', NULL, NULL, NULL, 13, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Alloy Wheel Repair & Caliper Painting Service', N'خدمة إصلاح الجنط وتلوين الكاليبر', NULL, NULL, NULL, 14, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('oil change service', N'خدمة تغيير الزيت', NULL, NULL, NULL, 15, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('battries Service', N'خدمة محلات البطاريات', NULL, NULL, NULL, 16, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Battery Shops Service', N'خدمة محلات البطاريات', NULL, NULL, NULL, 17, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Oil Shops Service', N'خدمة محلات الزيوت', NULL, NULL, NULL, 18, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Inspection Service', N'خدمة فحص السيارات', NULL, NULL, NULL, 19, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Electronics Installation Shops Service', N'خدمة محلات إلكترونيات السيارات', NULL, NULL, NULL, 20, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Key & Remote Programming Shops Service', N'خدمة محلات برمجة المفاتيح والريموت', NULL, NULL, NULL, 21, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Diagnostic Tools & OBD/Key Programming Stores Service', N'خدمة متاجر أجهزة فحص OBD وبرمجة مفاتيح', NULL, NULL, NULL, 22, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Auto Electircal Service', N'خدمة كهرباء السيارات', NULL, NULL, NULL, 23, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Car Accessories & Interior Decoration Shops Service', N'خدمة محلات زينة السيارات والديكور', NULL, NULL, NULL, 24, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Specialized Car Lighting Stores Service', N'خدمة متاجر إضاءة سيارات متخصصة', NULL, NULL, NULL, 25, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);


-- Landmark
INSERT INTO app.Landmarks (Name, NameAr, ImagePath, StatusID, UpdatedAt, CreatedAt) VALUES
('Center', N'مركز', NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Inside a Gas Station', N'داخل محطة وقود', NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Mobile', N'متنقل', NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Amenities
INSERT INTO app.Amenities (Name, NameAr, StatusID, ImagePath, UpdatedAt, CreatedAt) VALUES
('Hosting Area', N'منطقة استقبال', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Toilets', N'دورات مياه', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Free WiFi', N'واي فاي مجاني', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Praying Area', N'مصلى', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Coffee Corner', N'ركن القهوة', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Charging Stations', N'محطات شحن', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Tires Check Station', N'محطة فحص الإطارات', 1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- AppSource
INSERT INTO app.AppSources (Name, NameAr, StatusID, CreatedAt, UpdatedAt, UserID) VALUES
('TikTok', N'تيك توك', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('Instagram', N'إنستغرام', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('Facebook', N'فيسبوك', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('Word of Mouth', N'من خلال التوصيات (Word of Mouth)', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('X (Twitter)', N'X (منصة X)', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('YouTube', N'يوتيوب', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('LinkedIn', N'لينكدإن', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2),
('Events', N'فعاليات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 2);

INSERT INTO app.ItemTypes (Name, NameAr, StatusID, CreatedAt, UpdatedAt) VALUES
('AC Filter', N'فلتر مكيفات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Air Filter', N'فلتر هواء', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('ATF', N'زيت ناقل الحركة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Battery', N'بطاريات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Battery Filter', N'فلتر بطاريات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Brake Cleaner', N'منظف الفرامل', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Brake Fluid', NULL, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Cleaning', N'تنظيف', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Clips & Clamp', N'مشابك وكليبسات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Diesel Filter', N'فلتر الديزل', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Filter Transmission', N'فلتر ناقل الحركة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Free Services', N'خدمات مجانية', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Fuel Filter', N'فلتر الوقود', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Gasket Transmission', N'جلبة ناقل الحركة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Gears Oil', N'زيت التروس', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Grease', N'شحم', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Hydraulic', N'هيدروليك', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Injector Cleaner', N'منظف البخاخات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Maintenance', N'صيانة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Nut', N'صامولة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Oil', N'زيت', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Oil Filter', N'فلتر زيت', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Other', N'أخرى', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Parts', N'قطع', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Product', N'منتج', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Radiator', N'رديتر', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Silicon ', N'سيليكون', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Spark Plug', N'بواجي', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Washer', N'غسالة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
('Wiper', N'مساحات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- WITH CTE AS (
--     SELECT *,
--            ROW_NUMBER() OVER (
--                PARTITION BY trim(Name)
--                ORDER BY ItemTypeID ASC
--            ) AS RN
--     FROM app.ItemTypes
-- )
-- DELETE FROM app.ItemTypes
-- WHERE ItemTypeID IN (
--     SELECT ItemTypeID FROM CTE WHERE RN > 1
-- );



-- Countries
INSERT INTO app.Countries (CountryName, TaxPercentage, Currency, ConversionRate, CountryNameAr) VALUES 
('Saudi Arabia', 15.00, 'SAR', 1.00, N'المملكة العربية السعودية'),
('United Arab Emirates', 5.00, 'AED', 1.021, N'الإمارات العربية المتحدة'),
('Oman', 5.00, 'OMR', 9.74, N'سلطنة عُمان'),
('Qatar', 5.00, 'QAR', 1.03, N'قطر'),
('Kuwait', 5.00, 'KWD', 12.35, N'الكويت'),
('Afghanistan', 10.00, 'AFN', 0.05, N'أفغانستان'),
('Yemen', 5.00, 'YER', 0.015, N'اليمن');


select * from app.Cities

-- Cities: Saudi Arabia
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES 
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Asir', 'Asia/Riyadh', 'Abha', N'أبها'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Jazan', 'Asia/Riyadh', 'Abu Arish', N'أبو عريش'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Riyadh', 'Asia/Riyadh', 'Al Aflaj', N'الأفلاج'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Al Ahsa', N'الأحساء'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Bahah', 'Asia/Riyadh', 'Al Bahah', N'الباحة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Al Hofuf', N'الهفوف'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Riyadh', 'Asia/Riyadh', 'Al Majma''ah', N'المجمعة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Qassim', 'Asia/Riyadh', 'Al Mithnab', N'المذنب'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Al-Ahsa', N'الأحساء'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Riyadh', 'Asia/Riyadh', 'Al-Kharj', N'الخرج'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Qassim', 'Asia/Riyadh', 'Buraidah', N'بريدة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Dammam', N'الدمام'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Jawf', 'Asia/Riyadh', 'Dumat Al-Jandal', N'دومة الجندل'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Ha''il', 'Asia/Riyadh', 'Ha''il', N'حائل'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Hafar Al-Batin', N'حفر الباطن'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Jazan', 'Asia/Riyadh', 'Jazan', N'جازان'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Makkah', 'Asia/Riyadh', 'Jeddah', N'جدة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Jubail', N'الجبيل'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Asir', 'Asia/Riyadh', 'Khamis Mushait', N'خميس مشيط'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Khobar', N'الخبر'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Madinah', 'Asia/Riyadh', 'Madina', N'المدينة المنورة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Makkah', 'Asia/Riyadh', 'Makkah', N'مكة المكرمة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Najran', 'Asia/Riyadh', 'Najran', N'نجران'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Qassim', 'Asia/Riyadh', 'Qassim', N'القصيم'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Eastern Province', 'Asia/Riyadh', 'Qatif', N'القطيف'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Makkah', 'Asia/Riyadh', 'Rabigh', N'رابغ'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Northern Borders', 'Asia/Riyadh', 'Rafha', N'رفحاء'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Riyadh', 'Asia/Riyadh', 'Riyadh', N'الرياض'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Jazan', 'Asia/Riyadh', 'Sabya', N'صبيا'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Jawf', 'Asia/Riyadh', 'Sakaka', N'سكاكا'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Makkah', 'Asia/Riyadh', 'Ta''if', N'الطائف'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Tabuk', 'Asia/Riyadh', 'Tabuk', N'تبوك'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Qassim', 'Asia/Riyadh', 'Unaizah', N'عنيزة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Qassim', 'Asia/Riyadh', 'Uyun Al Jawa', N'عيون الجواء'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Al Madinah', 'Asia/Riyadh', 'Yanbu', N'ينبع'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Riyadh', 'Asia/Riyadh', 'Zulfy', N'الزلفي'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Saudi Arabia'), 'Asir', 'Asia/Riyadh', 'Bisha', N'بيشة');

-- Cities: United Arab Emirates
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName='United Arab Emirates'), 'Sharjah', 'Asia/Dubai', 'Sharjah', N'الشارقة');
-- Cities: Oman
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName='Oman'), 'Dhofar', 'Asia/Muscat', 'Salalah', N'صلالة'),
((SELECT CountryID FROM app.Countries WHERE CountryName='Oman'), 'Muscat', 'Asia/Muscat', 'Muscat', N'مسقط');
-- Cities: Qatar
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName='Qatar'), 'Doha', 'Asia/Qatar', 'Doha', N'الدوحة');
-- Cities: Kuwait
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName='Kuwait'), 'Al Asimah', 'Asia/Kuwait', 'Kuwait City', N'مدينة الكويت');
-- Cities: Afghanistan
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName='Afghanistan'), 'Kabul', 'Asia/Kabul', 'Kabul', N'كابول');
-- Cities: Yemen
INSERT INTO app.Cities (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName='Yemen'), 'Sana''a', 'Asia/Aden', 'Sana''a', N'صنعاء');


-- WITH CTE AS (
--     SELECT *,
--            ROW_NUMBER() OVER (
--                PARTITION BY CityName
--                ORDER BY CityID ASC
--            ) AS RN
--     FROM app.Cities
-- )
-- DELETE FROM app.Cities
-- WHERE CityID IN (
--     SELECT CityID FROM CTE WHERE RN > 1
-- );



-- Units
INSERT INTO app.Units ([Name], [Description], NameAr, StatusID, CreatedAt, UpdatedAt)
VALUES
	('kg',    'Kilogram(s)',  N'كيلوجرام', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('gm',    'Gram(s)',      N'جرام', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('ltr.',  'Litre(s)',     N'لتر', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('Pcs.',  'Pieces',       N'قطعة / قطع', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('btl.',  'Bottle',       N'زجاجة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('doz.',  'Dozen',        N'درزن/12 قطعة/ دزينة', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('ml.',   'Millilitre',   N'ملليتر', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('qtr.',  'Quarter',      N'ربع', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
	('full.', 'Full',         N'فل/كامل ', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),   
	('half.', 'Half',         N'نص / نصف', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)



-- PaymentModes
INSERT INTO [app].[PaymentModes]
    ([Name], [NameAr], StatusID, LastUpdatedBy, CreatedBy, CreatedAt, UpdatedAt)
VALUES
    ('Cash',         N'نقدي', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Card',         N'بطاقة', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Multi Payment',N'دفع متعدد', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Credit',       N'ائتمان', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Tabby',        N'تابي', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Tamara',       N'تمارا', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('StcPay',       N'إس تي سي باي', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('BankTransfer', N'تحويل بنكي', 1, NULL , NULL, GETDATE(), GETDATE());



CREATE TABLE [app].[ETLcdc] (
    [TableName] NVARCHAR(255) PRIMARY KEY,
    [MaxIndex] BIGINT NOT NULL
);


CREATE TABLE [app].[SyncItems] (
    [OldItemID]  BIGINT         PRIMARY KEY,
    [CategoryID] BIGINT         NULL,
    [Name]       NVARCHAR (MAX) NULL,
);


CREATE TABLE [app].[SyncCategories] (
    [OldCategoryID] BIGINT         PRIMARY KEY,
    [AccountID]     BIGINT         NULL,
    [Name]          NVARCHAR (MAX) NULL
);


CREATE TABLE [app].[SyncCities] (
    [OldCityID]  BIGINT         PRIMARY KEY,
    [CityID] BIGINT         NULL,
    [CountryID] BIGINT         NULL
);


CREATE TABLE [app].[SyncAmenities] (
    [OldAmenitiesID]  BIGINT         PRIMARY KEY,
    [AmenitiesID] BIGINT         NULL,
);


CREATE TABLE [app].[SyncAppSources] (
    [OldAppSourceID]  BIGINT         PRIMARY KEY,
    [AppSourceID] BIGINT         NULL,
);

CREATE TABLE [app].[SyncPaymentModes] (
    [OldPaymentModeID]  BIGINT         PRIMARY KEY,
    [PaymentModeID] BIGINT         NULL,
);



CREATE TABLE [app].[SyncServices] (
    [OldServiceID]  BIGINT         PRIMARY KEY,
    [ServiceID] BIGINT         NULL,
);

CREATE TABLE [app].[SyncUnits] (
    [OldUnitID]  BIGINT         PRIMARY KEY,
    [UnitID] BIGINT         NULL,
);

