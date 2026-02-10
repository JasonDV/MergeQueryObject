using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using ivaldez.Sql.SqlBulkLoader;

namespace ivaldez.Sql.SqlMergeQueryObject
{
    /// <summary>
    /// MergeQueryObject
    /// An abstraction for the SQL Merge statement with many options and efficiencies.
    /// Author: Jason Valdez
    /// Repository: https://github.com/JasonDV/MergeQueryObject
    /// </summary>
    public class MergeQueryObject : IMergeQueryObject
    {
        private readonly IBulkLoader _bulkLoader;
        private readonly ExpressionToSql _expressionToSql;

        public MergeQueryObject(
            IBulkLoader bulkLoader,
            ExpressionToSql expressionToSql)
        {
            _bulkLoader = bulkLoader;
            _expressionToSql = expressionToSql;
        }

        public void Merge<T>(
            SqlConnection connection,
            MergeRequest<T> request)
        {
            var tableCreated = false;
            var tempTableName = "";
            var attemptCount = 0;
            var maxAttempts = request.RetryCount + 1;
            Exception lastException = null;

            while (attemptCount < maxAttempts)
            {
                try
                { 
                    var response = BuildTempTableClone(
                        connection,
                        request,
                        tableName =>
                        {
                            tempTableName = tableName;
                            tableCreated = true;
                        });

                    var sql = GetMergeSql(response.TempTableName, request, response.BulkLoaderRenameRules);

                    request.InfoLogger($"SQL: {sql}");

                    request.ExecuteSql(connection, sql, request);
                    
                    // Success - break out of retry loop
                    return;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    request.ErrorLogger(ex.Message);
                    
                    // Check if we should retry
                    var shouldRetry = ShouldRetryOnException(ex, request.RetryOnExceptionTypes, attemptCount, maxAttempts);
                    
                    if (shouldRetry)
                    {
                        attemptCount++;
                        request.InfoLogger($"Retry attempt {attemptCount} of {request.RetryCount} after {request.RetryDelayMilliseconds}ms delay");
                        
                        // Note: This is a synchronous blocking operation
                        if (request.RetryDelayMilliseconds > 0)
                        {
                            System.Threading.Thread.Sleep(request.RetryDelayMilliseconds);
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
                finally
                {
                    if (tableCreated && string.IsNullOrEmpty(tempTableName) == false)
                    {
                        try
                        {
                            var dropSql = $@"DROP TABLE {tempTableName};";
                            request.InfoLogger(dropSql);
                            request.ExecuteSql(connection, dropSql, request);
                        }
                        catch (Exception dropEx)
                        {
                            request.ErrorLogger($"Failed to drop temp table: {dropEx.Message}");
                        }
                        
                        // Reset for potential retry
                        tableCreated = false;
                        tempTableName = "";
                    }
                }
            }
            
            // If we exhausted all retries, throw the last exception
            if (lastException != null)
            {
                throw lastException;
            }
        }

        private bool ShouldRetryOnException(Exception ex, Type[] retryOnExceptionTypes, int attemptCount, int maxAttempts)
        {
            // No more retries available
            if (attemptCount + 1 >= maxAttempts)
            {
                return false;
            }
            
            // No retry exception types specified
            if (retryOnExceptionTypes == null || retryOnExceptionTypes.Length == 0)
            {
                return false;
            }
            
            // Check if the exception type matches any of the specified types
            var exceptionType = ex.GetType();
            foreach (var retryType in retryOnExceptionTypes)
            {
                if (retryType.IsAssignableFrom(exceptionType))
                {
                    return true;
                }
            }
            
            return false;
        }

        private BuildTempTableCloneResponse BuildTempTableClone<T>(
            SqlConnection connection,
            MergeRequest<T> request,
            Action<string> tableCreated)
        {
            var tempTablePrefix = request.UseRealTempTable ? "dbo." : "##";
            var tempTableName = $"{tempTablePrefix}MergeObjectTemp" + Guid.NewGuid().ToString().Replace("-", "");

            //**

            var bulkLoaderContext = _bulkLoader.InsertWithOptions(
                tempTableName,
                connection,
                request.KeepIdentityColumnValueOnInsert,
                request.DataToMerge);

            foreach (var items in request.GetColumnsToExcludeExpressionOnUpdate())
            {
                bulkLoaderContext.Without(items);
            }

            request.BulkLoaderOptions?.Invoke(bulkLoaderContext);

            var bulkLoaderRenameRules = bulkLoaderContext.GetRenameRules();

            //**

            var propertyNames = request
                .TargetPropertyNames
                .Except(request.GetColumnsToExcludeExpressionOnUpdate())
                .Except(request.GetColumnsToExcludeExpressionOnInsert())
                .ToArray();

            var targetPropertyNames = AddBrackets(ApplyRenameRules(bulkLoaderRenameRules, propertyNames));
            
            var tempSql = $@"
SELECT TOP(0) {string.Join(",", targetPropertyNames)}
INTO {tempTableName}
FROM {request.TargetTableName} NOLOCK
";

            request.InfoLogger($"SQL: {tempSql}");
            request.ExecuteSql(connection, tempSql, request);

            tableCreated(tempTableName);

            //**

            bulkLoaderContext.Execute();

            //**

            request.InfoLogger("Bulk loading data to temp table");

            var primaryKey = AddBrackets(GetRenamedPrimaryKey(request, bulkLoaderRenameRules));
            var primaryKeyList = primaryKey
                .Select(t => $"{t} ASC");
            var primaryKeyFieldList = string.Join(",", primaryKeyList);

            var tempSqlIndex = $@"
CREATE CLUSTERED INDEX [IdxPrimaryKey] ON {tempTableName}
(
	{primaryKeyFieldList}
)
";
            request.InfoLogger($"SQL: {tempSqlIndex}");
            request.ExecuteSql(connection, tempSqlIndex, request);

            return new BuildTempTableCloneResponse
            {
                TempTableName = tempTableName,
                BulkLoaderRenameRules = bulkLoaderRenameRules
            };
        }

        private string GetMergeSql<T>(string tempTableName,
            MergeRequest<T> request,
            IReadOnlyDictionary<string, string> bulkLoaderRenameRules)
        {
            var primaryKey = GetRenamedPrimaryKey(request, bulkLoaderRenameRules);
            var joinList = primaryKey
                .Select(t => $"(T.[{t}] = S.[{t}] OR (T.[{t}] IS NULL AND S.[{t}] IS NULL))");
            var onClause = string.Join(" AND " + Environment.NewLine, joinList);

            var identityInsertOn = request.KeepIdentityColumnValueOnInsert
                ? $"SET IDENTITY_INSERT {request.TargetTableName} ON;"
                : "";
            var identityInsertOff = request.KeepIdentityColumnValueOnInsert
                ? $"SET IDENTITY_INSERT {request.TargetTableName} OFF;"
                : "";

            var whereClause = _expressionToSql.GenerateWhereClause(request.TargetDataSetFilter);

            var whenNotMatchedBySource = GetNotMatchedBySourceBlock(request);

            var mergeSql = $@"
{identityInsertOn}
WITH CTE_T
AS 
(
    SELECT * 
    FROM {request.TargetTableName} AS T
    {whereClause}
)
MERGE INTO CTE_T AS T
USING {tempTableName} AS S 
ON {onClause}
{GetUpdateBlock(request, bulkLoaderRenameRules)}

{GetInsertBlock(request, bulkLoaderRenameRules)}

{whenNotMatchedBySource}
;
{identityInsertOff}
";
            return mergeSql;
        }

        private string GetNotMatchedBySourceBlock<T>(MergeRequest<T> request)
        {
            if (request.WhenNotMatchedDeleteBehavior == DeleteBehavior.None)
            {
                return "--NO delete requested" + Environment.NewLine;
            }

            var sql = "";

            if (request.WhenNotMatchedDeleteBehavior == DeleteBehavior.MarkIsDelete)
            {
                sql = $@"
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET {request.WhenNotMatchedDeleteFieldName} = 1
";
            }

            if (request.WhenNotMatchedDeleteBehavior == DeleteBehavior.Delete)
            {
                sql = @"
WHEN NOT MATCHED BY SOURCE THEN
    DELETE
";
            }

            return sql;
        }


        private string GetUpdateBlock<T>(MergeRequest<T> request,
            IReadOnlyDictionary<string, string> bulkLoaderRenameRules)
        {
            if (request.OnMergeUpdateActive == false)
            {
                return "--NO update block due to update command being deactivated" + Environment.NewLine;
            }

            var retObj = new StringBuilder();

            retObj.AppendLine(@"UPDATE");
            retObj.AppendLine(@"SET");

            var targetPropertyNames = request
                .TargetPropertyNames
                .Except(request.GetPrimaryKey())
                .Except(request.GetColumnsToExcludeExpressionOnUpdate())
                .ToArray();

            if (targetPropertyNames.Any() == false)
            {
                return "--NO properties to update" + Environment.NewLine;
            }

            targetPropertyNames = ApplyRenameRules(bulkLoaderRenameRules, targetPropertyNames);

            var list = new List<string>();
            foreach (var propertyName in targetPropertyNames)
            {
                list.Add($@"[{propertyName}] = S.[{propertyName}]");
            }

            retObj.AppendLine(string.Join(",", list));

            return $@"
WHEN MATCHED THEN  
    {retObj}
";
        }

        private string GetInsertBlock<T>(MergeRequest<T> request,
            IReadOnlyDictionary<string, string> bulkLoaderRenameRules)
        {
            if (request.OnMergeInsertActive == false)
            {
                return "--NO insert block due to insert command being deactivated" + Environment.NewLine;
            }

            var retObj = new StringBuilder();

            var targetPropertyNames = request
                .TargetPropertyNames
                .Except(request.GetColumnsToExcludeExpressionOnInsert())
                .ToArray();

            if (targetPropertyNames.Any() == false)
            {
                return "--NO properties to insert" + Environment.NewLine;
            }

            targetPropertyNames = ApplyRenameRules(bulkLoaderRenameRules, targetPropertyNames);

            if (request.KeepPrimaryKeyInInsertStatement == false)
            {
                targetPropertyNames = targetPropertyNames
                    .Except(request.GetPrimaryKey())
                    .ToArray();
            }

            var names = targetPropertyNames.Select(x => $@"[{x}]").ToArray();
            var names2 = targetPropertyNames.Select(x => $@"S.[{x}]").ToArray();

            retObj.AppendLine(@"INSERT");
            retObj.AppendLine(@"(");

            retObj.AppendLine(string.Join(",", names));

            retObj.AppendLine(@")");
            retObj.AppendLine(@"VALUES");
            retObj.AppendLine(@"(");
            retObj.AppendLine(string.Join(",", names2));
            retObj.AppendLine(@")");

            return $@"
WHEN NOT MATCHED BY TARGET THEN
    {retObj}
";
        }

        private static string[] ApplyRenameRules(IReadOnlyDictionary<string, string> bulkLoaderRenameRules,
            string[] targetPropertyNames)
        {
            var retObj = new string[targetPropertyNames.Length];

            for (var i = 0; i < targetPropertyNames.Length; i++)
            {
                var key = targetPropertyNames[i];

                if (bulkLoaderRenameRules.ContainsKey(key))
                {
                    retObj[i] = bulkLoaderRenameRules[key];
                }
                else
                {
                    retObj[i] = key;
                }
            }

            return retObj.ToArray();
        }

        private static string[] GetRenamedPrimaryKey<T>(MergeRequest<T> request,
            IReadOnlyDictionary<string, string> bulkLoaderRenameRules)
        {
            var primaryKey = request.GetPrimaryKey();

            for (var i = 0; i < primaryKey.Length; i++)
            {
                var key = primaryKey[i];

                if (bulkLoaderRenameRules.ContainsKey(key))
                {
                    primaryKey[i] = bulkLoaderRenameRules[key];
                }
            }

            return primaryKey;
        }

        private class BuildTempTableCloneResponse
        {
            public string TempTableName { get; set; }
            public IReadOnlyDictionary<string, string> BulkLoaderRenameRules { get; set; }
        }

        private IEnumerable<string> AddBrackets(IEnumerable<string> applyRenameRules)
        {
            foreach (var applyRenameRule in applyRenameRules)
            {
                yield return "[" + applyRenameRule + "]";
            }
        }
    }
}