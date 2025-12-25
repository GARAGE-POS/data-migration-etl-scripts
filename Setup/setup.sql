ALTER TABLE [app].[Countries]
ADD [CountryNameAr] NVARCHAR(MAX)


-- Countries
INSERT INTO app.Countries (CountryName, CountryNameAr, TaxPercentage, Currency, ConversionRate) VALUES
('Saudi Arabia', N'السعودية', 15.00, 'SAR', 1.00),
('United Arab Emirates', N'الإمارات العربية المتحدة', 5.00, 'AED', 1.02),
('Bahrain', N'البحرين', 10.00, 'BHD', 0.10),
('Kuwait', N'الكويت', 5.00, 'KWD', 12.35),
('Qatar', N'قطر', 5.00, 'QAR', 1.03),
('Oman', N'عُمان', 5.00, 'OMR', 9.74),
('Jordan', N'الأردن', 16.00, 'JOD', 0.71),
('Lebanon', N'لبنان', 11.00, 'LBP', 1507.00),
('Egypt', N'مصر', 14.00, 'EGP', 30.90),
('Morocco', N'المغرب', 20.00, 'MAD', 10.10),
('Afghanistan', N'أفغانستان', 10.00, 'AFN', 0.05),
('Yemen', N'اليمن', 5.00, 'YER', 0.02);



ALTER TABLE [app].[Cities]
ADD [CityNameAr] NVARCHAR(MAX)

-- Cities: Saudi Arabia
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
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
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName = 'United Arab Emirates'),
 'Sharjah', 'Asia/Dubai', 'Sharjah', N'الشارقة');

-- Cities: Oman
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName = 'Oman'),
 'Dhofar', 'Asia/Muscat', 'Salalah', N'صلالة'),
((SELECT CountryID FROM app.Countries WHERE CountryName = 'Oman'),
 'Muscat', 'Asia/Muscat', 'Muscat', N'مسقط');

-- Cities: Qatar
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName = 'Qatar'),
 'Doha', 'Asia/Qatar', 'Doha', N'الدوحة');

-- Cities: Kuwait
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName = 'Kuwait'),
 'Al Asimah', 'Asia/Kuwait', 'Kuwait City', N'مدينة الكويت');

-- Cities: Afghanistan
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName = 'Afghanistan'),
 'Kabul', 'Asia/Kabul', 'Kabul', N'كابول');

-- Cities: Yemen
INSERT INTO [app].[Cities] (CountryID, District, Timezone, CityName, CityNameAr) VALUES
((SELECT CountryID FROM app.Countries WHERE CountryName = 'Yemen'),
 'Sana''a', 'Asia/Aden', 'Sana''a', N'صنعاء');


INSERT INTO [app].[PaymentModes]
    ([Name], [NameAr], [StatusID], [LastUpdatedBy], [CreatedBy], [CreatedAt], [UpdatedAt])
VALUES
    ('Cash',         N'نقدي', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Card',         N'بطاقة', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Multi Payment',N'دفع متعدد', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Credit',       N'ائتمان', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Tabby',        N'تابي', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('Tamara',       N'تمارا', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('StcPay',       N'إس تي سي باي', 1, NULL , NULL, GETDATE(), GETDATE()),
    ('BankTransfer', N'تحويل بنكي', 1, NULL , NULL, GETDATE(), GETDATE());



INSERT INTO [app].[ItemTypes] ([Name], [NameAr], [CreatedAt], [UpdatedAt], [StatusID]) VALUES
('OIL', N'زيت', '2025-11-29 21:38:17.2372520 +00:00', '2025-11-29 21:38:17.2372521 +00:00', 1),
('OIL FILTER', N'فلتر زيت', '2025-11-29 21:38:17.2373094 +00:00', '2025-11-29 21:38:17.2373095 +00:00', 1),
('SERVICE', N'خدمة', '2025-11-29 21:38:17.2373096 +00:00', '2025-11-29 21:38:17.2373096 +00:00', 1),
('OTHER', N'أخرى', '2025-11-29 21:38:17.2373097 +00:00', '2025-11-29 21:38:17.2373098 +00:00', 1),
('Car Wash', N'غسيل سيارات', '2025-12-04 09:53:20.8966667 +00:00', '2025-12-04 09:53:20.8966667 +00:00', 1);


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

