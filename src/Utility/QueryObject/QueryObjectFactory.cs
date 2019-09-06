using Jvaldez.Net.Sql.Utility.QueryObjectInsert;

namespace Jvaldez.Net.Sql.Utility.QueryObject
{
    public class QueryObjectFactory
    {
        public InsertQueryObject<T> CreateInsertObject<T>()
        {
            return new InsertQueryObject<T>();
        }
    }
}