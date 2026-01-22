import os
import warnings
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
import numpy as np

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | SuperUserSetup | %(message)s")

warnings.filterwarnings('ignore')
load_dotenv()


# -------------------- Connections --------------------
def get_engine(server_env, db_env, user_env, pw_env) -> Engine:
    conn_string = (
        f"DRIVER={os.getenv('AZURE_ODBC_DRIVER', '{ODBC Driver 18 for SQL Server}')};"
        f"SERVER={os.getenv(server_env)};"
        f"DATABASE={os.getenv(db_env)};"
        f"UID={os.getenv(user_env)};"
        f"PWD={os.getenv(pw_env)};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    quoted = quote_plus(conn_string)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
    logging.info(f'Connected to {os.getenv(db_env)} at {os.getenv(server_env)}')
    return engine

def source_db_conn(): return get_engine('AZURE_SERVER','AZURE_DATABASE','AZURE_USERNAME','AZURE_PASSWORD')
def target_db_conn(): return get_engine('STAGE_SERVER','STAGE_DATABASE','STAGE_USERNAME','STAGE_PASSWORD')



# -------------------- Main --------------------
def main():

    target = target_db_conn()

    try:
        with target.begin() as conn:  # Transaction-safe

            conn.execute(text("""

                -- Accounts
                INSERT INTO app.Accounts (CompanyName, CompanyNameAr, RepresentativeFirstName, RepresentativeLastName, CompanyEmail, CompanyCode, RepresentativeContactNo, CompanyContactNo,  CreatedAt, UpdatedAt, statusID, CRNo, VATNo, PrimaryBusiness, SocialMediaJson)
                VALUES ('Karage for Information Technology', N'شركة كراج لتقنية تكنولوجيا المعلومات', 'Mohammed', 'Abu Musa', 'm.abumusa@karage.co', 'POS-Karage','+966561158223', '+966920015563' , GETDATE(), GETDATE(), 1, '7016315405', '312998361200003', 'Oil Change', {str({})});

                -- Locations
                INSERT INTO app.Locations (AccountID, Name, NameAr, Description, DescriptionAr, RepresentativeName, RepresentativeContactNo, ConctactNo, Email, Createdat, UpdatedAt, CityID, StatusID, IsFeatured, IsHQ, IsActiveMyKarage, ShortAddress, BuildingNumber, StreetName, SecondaryNumber, District, PostalCode, SocialMediaJson, AmenitiesJson, ServicesJson, LocationImagesJson, WorkingHours)
                VALUES ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'), 'Karage HQ', N'إدارة كراج', 'Garage Management Office - HQ', N'مركز إدارة كراج', 'Mohammed Abu Musa', '+966561158223', '+966920015563', 'm.abumusa@karage.co', GETDATE(), GETDATE(), 1, 1,1,1,1, 'RQRA2935', '2935', 'Prince Majid Ibn Abdulaziz', '7337', 'Al Rayan dist.', '14213', {str({})}, '[]','[]','[]', '08:00-17:00');
                
                -- AspNetUsers
                INSERT INTO app.AspNetUsers (FirstName, LastName, IsEmailVerified, IsContactNoVerified, UserType, CreatedAt, UpdatedAt, StatusID, CityID, Designation, UserName, NormalizedUserName, Email, NormalizedEmail, EmailConfirmed, PasscodeHash, PasswordHash, SecurityStamp, PhoneNumberConfirmed, TwoFactorEnabled, LockoutEnabled, AccessFailedCount)
                VALUES  ('Mohammed',  'Abu Musa', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('m.abumusa@karage.co'), 'm.abumusa@karage.co', UPPER('m.abumusa@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),
                        ('Shariq',  'Malik', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('shariqmalik@garage.sa'), 'shariqmalik@garage.sa', UPPER('shariqmalik@garage.sa'), 0, :password, :password , :stamp, 0,0,0,0),
                        ('Rafi',  '', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('rafi@garage.sa'), 'rafi@garage.sa', UPPER('rafi@garage.sa'), 0, :password, :password , :stamp, 0,0,0,0),
                        ('Hazik',  '', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('hazik@karage.co'), 'hazik@karage.co', UPPER('hazik@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),
                        ('Eatessam',  '', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('eatessam@karage.co'), 'eatessam@karage.co', UPPER('eatessam@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),
                        ('Areen',  'Hejjo', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('areen.hejjo@gmail.com'), 'areen.hejjo@gmail.com', UPPER('areen.hejjo@gmail.com'), 0, :password, :password , :stamp, 0,0,0,0),
                        ('Mosab',  'Musa', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('m.musa@karage.co'), 'm.musa@karage.co', UPPER('m.musa@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Mohanned',  '', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'SuperAdmin',  'Super Admin', UPPER('mohanned@garage.sa'), 'mohanned@garage.sa', UPPER('mohanned@garage.sa'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Nouf',  'Bakalka', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountsManager',  'Account''s Manager', UPPER('n.bakalka@garage.sa'), 'n.bakalka@garage.sa', UPPER('n.bakalka@garage.sa'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Ali',  'Alwuqayyan', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountsManager',  'Account''s Manager', UPPER('a.alwuqayyan@karage.co'), 'a.alwuqayyan@karage.co', UPPER('a.alwuqayyan@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Omar',  'Albadrani', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountsManager',  'Account''s Manager', UPPER('o.albadrani@karage.co'), 'o.albadrani@karage.co', UPPER('o.albadrani@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Hilah',  'Aljabr', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('h.aljabr@karage.co'), 'h.aljabr@karage.co', UPPER('h.aljabr@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Malik',  'Alharbi', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('m.alharbi@karage.co'), 'm.alharbi@karage.co', UPPER('m.alharbi@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Manar',  'Alsatami', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('m.alsatami@karage.co'), 'm.alsatami@karage.co', UPPER('m.alsatami@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Mohammed',  'Alfarid', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('mo.alharbi@karage.co'), 'mo.alharbi@karage.co', UPPER('mo.alharbi@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Abdulmalik',  'Alharbi', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('a.alharbi@karage.co'), 'a.alharbi@karage.co', UPPER('a.alharbi@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Rawan',  'Alkhazim', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('r.alkhazim@karage.co'), 'r.alkhazim@karage.co', UPPER('r.alkhazim@karage.co'), 0, :password, :password , :stamp, 0,0,0,0),  
                        ('Samar',  'Alannaz', 0, 0, 'User', GETDATE(), GETDATE(), 1, 10, 'AccountManager',  'Account Manager', UPPER('samar@karage.co'), 'samar@karage.co', UPPER('samar@karage.co'), 0, :password, :password , :stamp, 0,0,0,0);

                -- UserAccounts
                INSERT INTO app.UserAccounts (UserID, AccountID)
                SELECT 
                    Id AS UserID,
                    (SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co') AS AccountID
                FROM app.AspNetUsers;           
                       
                -- AspNetRoles
                MERGE app.AspNetRoles o
                USING (
                    VALUES  ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'SuperAdmin', UPPER('SuperAdmin'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'AccountsManager', UPPER('AccountsManager'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'AccountManager', UPPER('AccountManager'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'AccountOwner', UPPER('AccountOwner'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'BranchManager', UPPER('BranchManager'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'StockManager', UPPER('StockManager'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'GeneralManager', UPPER('GeneralManager'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'Cashier', UPPER('Cashier'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'Technician', UPPER('Technician'), 1),
                            ((SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co'),	'Assistant', UPPER('Assistant'), 1)
                ) n (AccountID, Name, NormalizedName, IsSystemRole)
                ON o.AccountID = n.AccountID AND o.Name = n.Name
                WHEN NOT MATCHED THEN
                    INSERT (AccountID, Name, NormalizedName, IsSystemRole) VALUES (n.AccountID, n.Name, n.NormalizedName, n.IsSystemRole);
            
                -- AspNetRoleClaims
                INSERT INTO app.AspNetRoleClaims (AccountID, RoleID, ClaimType, ClaimValue)
                SELECT
                    r.AccountID,
                    r.Id,
                    rp.ClaimType,
                    rp.ClaimValue
                FROM app.AspNetRoles r
                JOIN app.RolesAndPermissions rp ON r.Name = rp.Role
                WHERE r.AccountID=(SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co');
                    
                -- AspNetUserRoles
                INSERT INTO app.AspNetUserRoles (UserID, RoleID, AccountID)
                SELECT 
                    Id,
                    (SELECT Id FROM app.AspNetRoles WHERE Name='SuperAdmin' AND AccountID=(SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co')),
                    NULL
                FROM app.AspNetUsers WHERE Designation='SuperAdmin';

                INSERT INTO app.AspNetUserRoles (UserID, RoleID, AccountID)
                SELECT 
                    Id,
                    (SELECT Id FROM app.AspNetRoles WHERE Name='AccountsManager' AND AccountID=(SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co')),
                    (SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co')
                FROM app.AspNetUsers WHERE Designation='AccountsManager';

                INSERT INTO app.AspNetUserRoles (UserID, RoleID, AccountID)
                SELECT 
                    Id,
                    (SELECT Id FROM app.AspNetRoles WHERE Name='AccountManager' AND AccountID=(SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co')),
                    (SELECT AccountID FROM app.Accounts WHERE CompanyEmail='m.abumusa@karage.co')
                FROM app.AspNetUsers WHERE Designation='AccountManager';

                -- Bays
                INSERT INTO app.Bays (Name, LocationID, CreatedAt, UpdatedAt, StatusID)
                VALUES ('Karage Bay', (SELECT LocationID FROM app.Locations WHERE Name='Karage HQ'), GETDATE(), GETDATE(), 1);

                -- Warehouses            
                INSERT INTO app.Warehouses (IsMainStore, Name, LocationID, CreatedAt, UpdatedAt, StatusID)
                VALUES (0, 'Karage HQ Warehouse', (SELECT LocationID FROM app.Locations WHERE Name='Karage HQ'), GETDATE(), GETDATE(), 1); 

                -- AppSources
                INSERT INTO app.AppSources (Name, NameAr, StatusID, CreatedAt, UpdatedAt, UserID) VALUES
                ('TikTok', N'تيك توك', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('Instagram', N'إنستغرام', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('Facebook', N'فيسبوك', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('Word of Mouth', N'من خلال التوصيات (Word of Mouth)', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('X (Twitter)', N'X (منصة X)', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('YouTube', N'يوتيوب', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('LinkedIn', N'لينكدإن', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%')),
                ('Events', N'فعاليات', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, (SELECT Id FROM app.AspNetUsers WHERE Email LIKE 'm.abumusa%'));

                -- AddOns
                INSERT INTO app.AddOns (AddOnName, Version, CreatedAt, UpdateAt, StatusID)
                VALUES ('ZATCA E-Invoice Compliance', '1.0', GETDATE(), GETDATE(), 1);

                -- Subscriptions
                INSERT INTO app.Subscriptions (CRMID, AccountID, SubscriptionName, SubscriptionType, StartDate, ExpiryDate, NumberOfTerminals, PaymentTerm, CreatedAt, UpdatedAt, StatusID)
                VALUES ('CRM-2024-001',	1, NULL, 1,	'2024-01-01 00:00:00.0000000 +00:00', '2025-01-01 00:00:00.0000000 +00:00', 5, 365,	'2024-01-01 00:00:00.0000000 +00:00', '2026-01-20 08:28:23.1987185 +00:00', 1);

                -- SubscriptionAddOns
                INSERT INTO app.SubscriptionAddOns (SubscriptionID, AddOnID, StartDate, EndDate, CreatedAt, UpdatedAt, StatusID)
                VALUES (1, 1, '2024-01-01 00:00:00.0000000 +00:00', '2025-01-01 00:00:00.0000000 +00:00', '2026-01-20 11:56:51.7133333 +00:00',	'2026-01-20 11:56:51.7133333 +00:00', 1);
                
            """),
            {'password':os.getenv('SU_PASSWORD'), 'stamp':os.getenv('SEC_STAMP')})
        logging.info(f'Super Users setuped successfully.')


    except Exception as e:
        logging.error(f'Failed to Setup the DB: {e}')
        raise    

if __name__ == '__main__':
    main()
