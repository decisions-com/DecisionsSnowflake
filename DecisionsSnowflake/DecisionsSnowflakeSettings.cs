using DecisionsFramework.Data.ORMapper;
using DecisionsFramework.ServiceLayer;
using DecisionsFramework.ServiceLayer.Actions;
using DecisionsFramework.ServiceLayer.Actions.Common;
using DecisionsFramework.ServiceLayer.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

/// <summary>
/// This class is used to create a single settings object that can be used
/// by flows and steps.  It will show up in the portal in /System/Settings for 
/// Portal Administrators.
/// </summary>
namespace DecisionsSnowflake
{
    [ORMEntity]
    [DataContract]
    public class DecisionsSnowflakeSettings : AbstractModuleSettings
    {
        public DecisionsSnowflakeSettings()
        {
            EntityName = "DecisionsSnowflake Custom App Settings";
        }

        [ORMField]
        public string MyCompanySystemCode { get; set; }

        public override BaseActionType[] GetActions(AbstractUserContext userContext, EntityActionType[] types)
        {
            List<BaseActionType> results = new List<BaseActionType>(base.GetActions(userContext, types));

            results.Add(new EditObjectAction(typeof(DecisionsSnowflakeSettings), "Edit", null, "Edit", ()=> { return this; }, SaveChanges));

            return results.ToArray();
        }

        private void SaveChanges(AbstractUserContext userContext, object obj)
        {
            new DynamicORM().Store((IORMEntity)obj);
        }
    }
}
