CONSTRAINT [FK_AppSources_AspNetUsers_UserID] FOREIGN KEY ([UserID]) 
REFERENCES [app].[AspNetUsers] ([Id]) ON DELETE CASCADE;


ALTER TABLE app.AppSources 
DROP CONSTRAINT FK_AppSources_AspNetUsers_UserID; 


ALTER TABLE app.AppSources 
ADD CONSTRAINT FK_AppSources_Accounts_AccountID FOREIGN KEY (UserID) 
REFERENCES app.Accounts(AccountID) ON DELETE CASCADE;


ALTER TABLE app.Cars
DROP CONSTRAINT FK_Cars_Models_ModelID;


ALTER TABLE app.Cars
ADD CONSTRAINT FK_Cars_Models_ModelID FOREIGN KEY (ModelID) 
REFERENCES app.Models(ModelID);


ALTER TABLE [app].[Locations]
DROP CONSTRAINT FK_Locations_Landmarks_LandmarkID