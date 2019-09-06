using System.Reflection;
using Autofac;
using Jvaldez.Net.Sql.IoC;

namespace Jvaldez.Net.Sql.Utility
{
    public class AutoFacRegistration : CoreRegistrationConventions
    {
        protected override void Load(ContainerBuilder builder)
        {
            var dataAccess = Assembly.GetExecutingAssembly();
            
            builder.Register<IIocMaster>(ctx => IocMaster.Instance);

            LoadAllBasicRegistrationConventions(dataAccess, builder);
        }
    }
}
