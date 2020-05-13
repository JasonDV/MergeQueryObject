using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForCustomExceptionHandling
    {
            [Fact]
        public void ShouldAllowCustomExceptionHandlingInRequestObjectConnection()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyDifferentNamePrimaryKeyDto
                {
                    PkPrimaryKey = 100,
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyDifferentNamePrimaryKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.PkPrimaryKey},
                KeepPrimaryKeyInInsertStatement = true,
                KeepIdentityColumnValueOnInsert = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateActive = false,
                BulkLoaderOptions =
                    t => t.With(c => c.PkPrimaryKey, "Pk")
            };

            int loopCount = 1;

            //**

            request.ExecuteSql = (connection, sql, req) =>
            {
                while (loopCount <= 3)
                {
                    try
                    {
                        if (loopCount == 1)
                        {
                            sql = "SELECT * FROM THROWanERRORtable";
                        }

                        if (loopCount == 2)
                        {
                            throw new Exception("Some non-SQL error");
                        }

                        connection.Execute(sql, commandTimeout: req.SqlCommandTimeout);
                    }
                    catch (Exception ex)
                    {
                        request.InfoLogger(ex.Message);

                        if (typeof(SqlException).IsAssignableFrom(ex.GetType()) == false)
                        {
                            throw;
                        }
                    }

                    loopCount++;
                }
            };

            Assert.Throws<Exception>(() =>
            {
                helper.DataService.Merge(request);
            });

            loopCount.Should().Be(2);
        }

        private static int CustomConnectionWithExceptionHandling(int loopCount, string sql, SqlConnection connection,
            MergeRequest<SampleSurrogateKeyDifferentNamePrimaryKeyDto> req, MergeRequest<SampleSurrogateKeyDifferentNamePrimaryKeyDto> request)
        {
            while (loopCount <= 3)
            {
                try
                {
                    if (loopCount == 1)
                    {
                        sql = "SELECT * FROM THROWanERRORtable";
                    }

                    if (loopCount == 2)
                    {
                        throw new Exception("Some non-SQL error");
                    }

                    connection.Execute(sql, commandTimeout: req.SqlCommandTimeout);
                }
                catch (Exception ex)
                {
                    request.InfoLogger(ex.Message);

                    if (typeof(SqlException).IsAssignableFrom(ex.GetType()) == false)
                    {
                        throw;
                    }
                }

                loopCount++;
            }

            return loopCount;
        }
    }
}
