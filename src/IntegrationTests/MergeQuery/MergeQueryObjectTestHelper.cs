using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlBulkLoader;
using ivaldez.Sql.SqlMergeQueryObject;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestHelper
    {
        public MergeQueryObjectTestHelper()
        {
            DatabaseService = new TestingDatabaseService();
            DatabaseService.CreateTestDatabase();
            DataService = new TestingDataService(
                DatabaseService,
                new MergeQueryObject(
                    new BulkLoader(),
                    new ExpressionToSql()));
        }

        public TestingDataService DataService { get; set; }

        public TestingDatabaseService DatabaseService { get; set; }
    }
}