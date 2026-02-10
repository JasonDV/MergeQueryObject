using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForRetryLogic
    {
        // Custom exception for testing
        public class TransientDatabaseException : Exception
        {
            public TransientDatabaseException(string message) : base(message) { }
        }

        [Fact]
        public void ShouldRetryOnSpecifiedExceptionType()
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
                BulkLoaderOptions = t => t.With(c => c.PkPrimaryKey, "Pk"),
                RetryCount = 2,
                RetryDelayMilliseconds = 100,
                RetryOnExceptionTypes = new[] { typeof(TransientDatabaseException) }
            };

            int executionCount = 0;
            var infoLogs = new List<string>();

            request.InfoLogger = message => infoLogs.Add(message);

            request.ExecuteSql = (connection, sql, req) =>
            {
                executionCount++;
                
                if (executionCount == 1)
                {
                    // Simulate a transient error on first attempt
                    throw new TransientDatabaseException("Simulated transient error");
                }
                
                // Succeed on second attempt
                connection.Execute(sql, commandTimeout: req.SqlCommandTimeout);
            };

            // Should succeed after retry
            helper.DataService.Merge(request);

            // Verify that we retried
            executionCount.Should().Be(2);
            
            // Verify retry log messages
            infoLogs.Should().Contain(log => log.Contains("Retry attempt 1 of 2"));
        }

        [Fact]
        public void ShouldNotRetryOnNonMatchingExceptionType()
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
                BulkLoaderOptions = t => t.With(c => c.PkPrimaryKey, "Pk"),
                RetryCount = 2,
                RetryDelayMilliseconds = 100,
                RetryOnExceptionTypes = new[] { typeof(TransientDatabaseException) }
            };

            int executionCount = 0;

            request.ExecuteSql = (connection, sql, req) =>
            {
                executionCount++;
                throw new InvalidOperationException("Non-transient error");
            };

            // Should throw without retry
            Assert.Throws<InvalidOperationException>(() =>
            {
                helper.DataService.Merge(request);
            });

            // Verify that we did NOT retry
            executionCount.Should().Be(1);
        }

        [Fact]
        public void ShouldNotRetryWhenRetryCountIsZero()
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
                BulkLoaderOptions = t => t.With(c => c.PkPrimaryKey, "Pk"),
                RetryCount = 0,
                RetryOnExceptionTypes = new[] { typeof(TransientDatabaseException) }
            };

            int executionCount = 0;

            request.ExecuteSql = (connection, sql, req) =>
            {
                executionCount++;
                throw new TransientDatabaseException("Simulated transient error");
            };

            // Should throw without retry
            Assert.Throws<TransientDatabaseException>(() =>
            {
                helper.DataService.Merge(request);
            });

            // Verify that we did NOT retry
            executionCount.Should().Be(1);
        }

        [Fact]
        public void ShouldNotRetryWhenRetryExceptionTypesIsNull()
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
                BulkLoaderOptions = t => t.With(c => c.PkPrimaryKey, "Pk"),
                RetryCount = 2,
                RetryOnExceptionTypes = null
            };

            int executionCount = 0;

            request.ExecuteSql = (connection, sql, req) =>
            {
                executionCount++;
                throw new TransientDatabaseException("Simulated transient error");
            };

            // Should throw without retry
            Assert.Throws<TransientDatabaseException>(() =>
            {
                helper.DataService.Merge(request);
            });

            // Verify that we did NOT retry
            executionCount.Should().Be(1);
        }

        [Fact]
        public void ShouldExhaustRetriesAndThrowLastException()
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
                BulkLoaderOptions = t => t.With(c => c.PkPrimaryKey, "Pk"),
                RetryCount = 2,
                RetryDelayMilliseconds = 50,
                RetryOnExceptionTypes = new[] { typeof(TransientDatabaseException) }
            };

            int executionCount = 0;
            var infoLogs = new List<string>();

            request.InfoLogger = message => infoLogs.Add(message);

            request.ExecuteSql = (connection, sql, req) =>
            {
                executionCount++;
                throw new TransientDatabaseException("Persistent transient error");
            };

            // Should throw after exhausting retries
            Assert.Throws<TransientDatabaseException>(() =>
            {
                helper.DataService.Merge(request);
            });

            // Verify that we retried the maximum number of times (original + 2 retries = 3 total)
            executionCount.Should().Be(3);
            
            // Verify retry log messages
            infoLogs.Should().Contain(log => log.Contains("Retry attempt 1 of 2"));
            infoLogs.Should().Contain(log => log.Contains("Retry attempt 2 of 2"));
        }

        [Fact]
        public void ShouldRespectRetryDelay()
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
                BulkLoaderOptions = t => t.With(c => c.PkPrimaryKey, "Pk"),
                RetryCount = 1,
                RetryDelayMilliseconds = 200,
                RetryOnExceptionTypes = new[] { typeof(TransientDatabaseException) }
            };

            int executionCount = 0;
            var startTime = DateTime.Now;
            var elapsedTimeBeforeSecondAttempt = TimeSpan.Zero;

            request.ExecuteSql = (connection, sql, req) =>
            {
                executionCount++;
                
                if (executionCount == 2)
                {
                    elapsedTimeBeforeSecondAttempt = DateTime.Now - startTime;
                    connection.Execute(sql, commandTimeout: req.SqlCommandTimeout);
                    return;
                }
                
                throw new TransientDatabaseException("Simulated transient error");
            };

            helper.DataService.Merge(request);

            executionCount.Should().Be(2);
            
            // Verify that the delay was respected (should be at least 200ms)
            elapsedTimeBeforeSecondAttempt.TotalMilliseconds.Should().BeGreaterOrEqualTo(200);
        }
    }
}
