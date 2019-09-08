using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForLogging
    {
        [Fact]
        public void ShouldLogMajorEvents()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] { t.Pk },
                KeepPrimaryKeyInInsertStatement = false
            };

            var infoLogger = new List<string>();
            request.InfoLogger = info => infoLogger.Add(info);

            helper.DataService.Merge(request);

            infoLogger.Single(x => x.Contains("SELECT TOP(0)")).Should().NotBeNull();
            infoLogger.Single(x => x.Contains("Bulk loading data to temp table")).Should().NotBeNull();
            infoLogger.Single(x => x.Contains("CREATE CLUSTERED INDEX [IdxPrimaryKey] ON")).Should().NotBeNull();
            infoLogger.Single(x => x.Contains("MERGE INTO CTE_T AS T")).Should().NotBeNull();
            infoLogger.Single(x => x.Contains("DROP TABLE")).Should().NotBeNull();
        }

        [Fact]
        public void ShouldLogErrors()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample_DOESNOTEXIST",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] { t.Pk },
                KeepPrimaryKeyInInsertStatement = false
            };

            var errorLogger = new List<string>();
            request.ErrorLogger = info => errorLogger.Add(info);

            Assert.Throws<SqlException> (() => { helper.DataService.Merge(request); });

            errorLogger.Count.Should().Be(1);
            errorLogger.Single(x => x.Contains("Invalid object name 'dbo.Sample_DOESNOTEXIST'")).Should().NotBeNull();
        }
    }
}
