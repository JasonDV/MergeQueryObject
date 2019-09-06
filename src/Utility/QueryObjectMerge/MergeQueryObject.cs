using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Dapper;
using Jvaldez.Net.Sql.Utility.BulkLoaderUtility;
using Jvaldez.Net.Sql.Utility.ExpressionUtilities;

namespace Jvaldez.Net.Sql.Utility.QueryObjectMerge
{
    public interface IMergeQueryObject
    {
        void Merge<T>(
            SqlConnection connection,
            MergeRequest<T> request);
    }

    public class MergeQueryObject : IMergeQueryObject
    {
        private readonly IBulkLoader _bulkLoader;
        private readonly ExpressionToSql _expressionToSql;

        private class BuildTempTableCloneResponse
        {
            public string TempTableName { get; set; }
            public IReadOnlyDictionary<string, string> BulkLoaderRenameRules { get; set; }
        }

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

            try
            {
                var response = BuildTempTableClone(
                    connection,
                    request,
                    (tableName) =>
                    {
                        tempTableName = tableName;
                        tableCreated = true;
                    });

                var sql = GetMergeSql(response.TempTableName, request, response.BulkLoaderRenameRules);

                connection.Execute(sql, null, null, 0, null);
            }
            catch (Exception ex)
            {
                request.InfoLogger(ex.Message);
                throw;
            }
            finally
            {
                if (tableCreated && string.IsNullOrEmpty(tempTableName) == false)
                {
                    var dropSql = $@"DROP TABLE {tempTableName};";
                    request.InfoLogger(dropSql);
                    connection.Execute(dropSql, null, null, 0, null);
                }
            }
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

            var targetPropertyNames = ApplyRenameRules<T>(bulkLoaderRenameRules, propertyNames);

            var tempSql = $@"
SELECT TOP(0) {string.Join(",", targetPropertyNames)}
INTO {tempTableName}
FROM {request.TargetTableName} NOLOCK
";

            request.InfoLogger($"SQL: {tempSql}");
            connection.Execute(tempSql, null, null, 0, null);

            tableCreated(tempTableName);

            //**

            bulkLoaderContext.Execute();

            //**

            request.InfoLogger("Bulk loading data to temp table");

            var primaryKey = GetRenamedPrimaryKey(request, bulkLoaderRenameRules);
            var primaryKeyList = primaryKey
                .Select(t => $"[{t}] ASC");
            var primaryKeyFieldList = string.Join(",", primaryKeyList);

            var tempSqlIndex = $@"
CREATE CLUSTERED INDEX [IdxPrimaryKey] ON {tempTableName}
(
	{primaryKeyFieldList}
)
";
            request.InfoLogger($"SQL: {tempSqlIndex}");
            connection.Execute(tempSqlIndex, null, null, 0, null);

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
                .Select(t => $"T.{t} = S.{t}");
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

            string sql = "";

            if (request.WhenNotMatchedDeleteBehavior == DeleteBehavior.MarkIsDelete)
            {
                sql = @"
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET IsDeleted = 1
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

            targetPropertyNames = ApplyRenameRules<T>(bulkLoaderRenameRules, targetPropertyNames);

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
            if (request.OnMergeUpdateOnly)
            {
                return "--NO insert block due to update only command" + Environment.NewLine;
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

            targetPropertyNames = ApplyRenameRules<T>(bulkLoaderRenameRules, targetPropertyNames);

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

        private static string[] ApplyRenameRules<T>(IReadOnlyDictionary<string, string> bulkLoaderRenameRules, string[] targetPropertyNames)
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

        private static string[] GetRenamedPrimaryKey<T>(MergeRequest<T> request, IReadOnlyDictionary<string, string> bulkLoaderRenameRules)
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
    }
}